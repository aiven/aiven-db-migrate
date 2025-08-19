# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate import PGMigrateResult
from aiven_db_migrate.migrate.clusters import PGSource, PGTarget
from aiven_db_migrate.migrate.errors import PGDataDumpFailedError, PGMigrateValidationFailedError, PGSchemaDumpFailedError
from aiven_db_migrate.migrate.models import (
    PGDatabase, PgDumpStatus, PgDumpTask, PgDumpType, PGExtension, PGMigrateMethod, PGMigrateStatus, PGMigrateTask, PGRole,
    PGRoleStatus, PGRoleTask, PGTable
)
from aiven_db_migrate.migrate.pgutils import find_pgbin_dir
from collections import deque
from concurrent import futures
from packaging.version import Version
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Union

import logging
import os
import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions
import psycopg2.extras
import random
import string
import subprocess
import sys
import threading
import time

logger = logging.getLogger("PGMigrate")
MAX_CLI_LEN = 2097152  # getconf ARG_MAX


class PGMigrate:
    """PostgreSQL migrator"""
    source: PGSource
    target: PGTarget
    pgbin: Path
    createdb: bool
    max_workers: int
    max_replication_lag: int
    stop_replication: bool
    verbose: bool
    mangle: bool

    def __init__(
        self,
        *,
        source_conn_info: Union[str, Dict[str, Any]],
        target_conn_info: Union[str, Dict[str, Any]],
        pgbin: Optional[str] = None,
        createdb: bool = True,
        max_workers: int = 4,
        max_replication_lag: int = -1,
        stop_replication: bool = False,
        verbose: bool = False,
        mangle: bool = False,
        filtered_db: Optional[str] = None,
        excluded_roles: Optional[str] = None,
        excluded_extensions: Optional[str] = None,
        skip_tables: Optional[List[str]] = None,
        with_tables: Optional[List[str]] = None,
        replicate_extensions: bool = True,
        skip_db_version_check: bool = False,
    ):
        if skip_tables and with_tables:
            raise Exception("Can only specify a skip table list or a with table list")
        self.source = PGSource(
            conn_info=source_conn_info,
            filtered_db=filtered_db,
            excluded_roles=excluded_roles,
            excluded_extensions=excluded_extensions,
            mangle=mangle,
        )
        self.target = PGTarget(
            conn_info=target_conn_info,
            filtered_db=filtered_db,
            excluded_roles=excluded_roles,
            excluded_extensions=excluded_extensions,
            mangle=mangle,
        )
        self.skip_tables = self._convert_table_names(skip_tables)
        self.with_tables = self._convert_table_names(with_tables)
        if pgbin is None:
            self.pgbin = pgbin
        else:
            self.pgbin = Path(pgbin)
        # include commands to create db in pg_dump output
        self.createdb = createdb
        # TODO: have "--max-workers" in CLI
        self.max_workers = max_workers
        self.max_replication_lag = max_replication_lag
        self.stop_replication = stop_replication
        self.verbose = verbose
        self.mangle = mangle
        self.replicate_extensions = replicate_extensions
        self.skip_db_version_check = skip_db_version_check

    def _convert_table_names(self, tables: Optional[List[str]]) -> Set[PGTable]:
        ret: Set[PGTable] = set()
        if not tables:
            return ret
        for t in tables:
            sub_names = self.target.c("SELECT * FROM parse_ident(%s)", args=[t])[0]["parse_ident"]
            if len(sub_names) > 3:
                raise ValueError(f"Table name containing more than two dots not allowed: {t}")
            if len(sub_names) == 1:
                ret.add(PGTable(table_name=sub_names[0], schema_name=None, db_name=None, extension_name=None))
            elif len(sub_names) == 2:
                ret.add(PGTable(table_name=sub_names[1], schema_name=sub_names[0], db_name=None, extension_name=None))
            else:
                ret.add(
                    PGTable(table_name=sub_names[2], schema_name=sub_names[1], db_name=sub_names[0], extension_name=None)
                )
        return ret

    def filter_tables(self, db: PGDatabase) -> Optional[List[str]]:
        """
            Given a database, it will attempt to return a list of tables that should be data dumped / replicated
            based on the skip table list, with table list and the replicate extensions flag
            Returning an empty value signals the downstream caller to replicate / dump the entire database
            The replicate extensions flag is applied after the 2 lists, meaning that if those are empty but a given
            database has tables belonging to an extension and the flag is set to false, then we should NOT
            replicate / dump the entire database
        """
        logger.debug(
            "Filtering tables for db %r, and skip tables %r and with tables %r", db, self.skip_tables, self.with_tables
        )
        if not self.skip_tables and not self.with_tables and self.replicate_extensions:
            return None
        if not db.tables:
            return []
        ret: Set[PGTable] = set()
        if self.skip_tables:
            # the db tables should be properly populated on all 3 fields, so we can consider one of the user passed ones
            # to be equivalent the table name is the same AND the schema name is missing or the same AND the
            # db name is missing or the same
            for t in db.tables:
                found: Optional[PGTable] = None
                for s in self.skip_tables:
                    # we can add it if the table name differs or the schema name differs or the db name differs
                    if (
                        t.table_name == s.table_name and (not s.schema_name or t.schema_name == s.schema_name)
                        and (not s.db_name or t.db_name == s.db_name)
                    ):
                        found = t
                        break
                if not found:
                    ret.add(t)
        elif self.with_tables:
            for t in db.tables:
                found = None
                for w in self.with_tables:
                    # we consider it equivalent if the name is the same and the schema is missing or the same
                    # and the db name is missing or the same
                    if (
                        t.table_name == w.table_name and (not w.schema_name or t.schema_name == w.schema_name)
                        and (not w.db_name or t.db_name == w.db_name)
                    ):
                        found = t
                        break
                if found:
                    ret.add(found)
        elif not self.replicate_extensions:
            ret = set(db.tables)

        if not self.replicate_extensions:
            ret = {t for t in ret if t.extension_name is None}
        # -t <table_name> + connection params and other pg_dump / pg_restore params
        total_table_len = sum(4 + len(str(t)) for t in ret)
        if total_table_len + 200 > MAX_CLI_LEN:
            raise ValueError("Table count exceeding safety limit")
        quoted = []
        for table in ret:
            if table.schema_name:
                name = self.source.c(
                    "SELECT quote_ident(%s) || '.' || quote_ident(%s) as table_name",
                    args=(table.schema_name, table.table_name),
                )[0]["table_name"]
            else:
                name = self.source.c("SELECT quote_ident(%s) as table_name", args=[table.table_name])[0]["table_name"]
            quoted.append(name)
        return quoted

    def filter_extensions(self, db: PGDatabase) -> Optional[List[str]]:
        """
        Given a database, return installed extensions on the source without
        the ones that have explicitly been excluded from the migration.
        """
        return [e.name for e in self.source.databases[db.dbname].pg_ext if e.name not in self.target.excluded_extensions]

    def _check_different_servers(self) -> None:
        """Check if source and target are different servers."""
        source = (self.source.conn_info["host"], self.source.conn_info.get("port"))
        target = (self.target.conn_info["host"], self.target.conn_info.get("port"))
        if source == target:
            raise PGMigrateValidationFailedError("Migrating to the same server is not supported")

    def _check_db_versions(self) -> None:
        """Check that the version of the target database is the same or more recent than the source database."""
        if self.source.version.major > self.target.version.major:
            if self.skip_db_version_check:
                logger.warning(
                    "Migrating to older PostgreSQL server version is not recommended. Source: %s, Target: %s" %
                    (self.source.version, self.target.version)
                )
            else:
                raise PGMigrateValidationFailedError("Migrating to older major PostgreSQL server version is not supported")

    def _check_databases(self):
        for db in self.source.databases.values():
            dbname = db.dbname
            if db.error:
                logger.info("Access to source database %r is rejected", dbname)
            elif dbname not in self.target.databases:
                if not self.createdb:
                    raise PGMigrateValidationFailedError(
                        f"Database {dbname!r} doesn't exist in target (not creating databases)"
                    )
                else:
                    logger.info("Database %r will be created in target", dbname)
            else:
                if self.target.databases[dbname].error:
                    logger.info("Database %r already exists in target but access is rejected", dbname)
                else:
                    logger.info("Database %r already exists in target", dbname)

    def _check_database_size(self, max_size: float):
        dbs_size = 0
        for dbname, source_db in self.source.databases.items():
            only_tables = self.filter_tables(db=source_db)
            db_size = self.source.get_size(dbname=dbname, only_tables=only_tables)
            dbs_size += db_size
        if dbs_size > max_size:
            raise PGMigrateValidationFailedError(
                f"Databases do not fit to the required maximum size ({dbs_size} > {max_size})"
            )

    def _check_pg_lang(self):
        source_lang = {lan["lanname"] for lan in self.source.pg_lang}
        target_lang = {lan["lanname"] for lan in self.target.pg_lang}
        missing = source_lang - target_lang
        if missing:
            raise PGMigrateValidationFailedError("Languages not installed in target: {}".format(", ".join(sorted(missing))))

    def _check_pg_ext(self):
        source_db: PGDatabase
        target_db: PGDatabase
        source_ext: PGExtension
        target_ext: PGExtension
        for source_db in self.source.databases.values():
            if source_db.error:
                # access failed/rejected, skip extension check
                continue
            dbname = source_db.dbname
            for source_ext in source_db.pg_ext:
                if source_ext.name in self.target.excluded_extensions:
                    logger.info("Extension %r will not be installed in target", source_ext.name)
                    continue

                if dbname in self.target.databases:
                    target_db = self.target.databases[dbname]
                    try:
                        target_ext = next(e for e in target_db.pg_ext if e.name == source_ext.name)
                    except StopIteration:
                        logger.info("Extension %r is not installed in target database %r", source_ext.name, dbname)
                    else:
                        logger.info(
                            "Extension %r is installed in source and target database %r, source version: %r, "
                            "target version: %r", source_ext.name, dbname, source_ext.version, target_ext.version
                        )
                        if Version(source_ext.version) <= Version(target_ext.version):
                            continue
                        msg = (
                            f"Installed extension {source_ext.name!r} in target database {dbname!r} is older than "
                            f"in source, target version: {target_ext.version}, source version: {source_ext.version}"
                        )
                        logger.error(msg)
                        raise PGMigrateValidationFailedError(msg)

                # check if extension is available for installation in target
                try:
                    target_ext = next(e for e in self.target.pg_ext if e.name == source_ext.name)
                except StopIteration:
                    msg = f"Extension {source_ext.name!r} is not available for installation in target"
                    logger.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                logger.info(
                    "Extension %r version %r available for installation in target, source version: %r", target_ext.name,
                    target_ext.version, source_ext.version
                )

                if Version(target_ext.version) < Version(source_ext.version):
                    msg = (
                        f"Extension {target_ext.name!r} version available for installation in target is too old, "
                        f"source version: {source_ext.version}, target version: {target_ext.version}"
                    )
                    logger.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                # check if we can install this extension
                if target_ext.name in self.target.pg_ext_whitelist:
                    logger.info("Extension %r is whitelisted in target", target_ext.name)
                elif target_ext.trusted:
                    logger.info("Extension %r is trusted in target", target_ext.name)
                elif target_ext.superuser and not self.target.is_superuser:
                    msg = f"Installing extension {target_ext.name!r} in target requires superuser"
                    logger.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                # schema dump creates extension to target db
                logger.info(
                    "Extension %r version %r will be installed in target database %r", target_ext.name, target_ext.version,
                    dbname
                )

    def _check_aiven_pg_security_agent(self) -> None:
        """Check that the source complies with the Aiven PostgreSQL security agent requirements, if used.

        If the target database uses Aiven PostgreSQL security agent, check that the source database
        doesn't have any superuser roles that are not allowed in the target database.

        Note:
            When the target does not use Aiven PostgreSQL security agent, which is allowed, no check needs
            to be done, and no error is raised.

        Raises:
            PGMigrateValidationFailedError: If the source database has superuser roles that are forbidden
                by the Aiven PostgreSQL security agent, in the target database.
        """
        if not self.target.has_aiven_gatekeeper:
            return

        logger.info("Aiven PostgreSQL security agent is used by the target.")

        # Get the list of roles that are in the source but not in the target.
        missing_roles = set(self.source.pg_roles.keys()) - set(self.target.pg_roles.keys())
        # Only the superuser roles.
        su_missing_roles = {r for r in missing_roles if self.source.pg_roles[r].rolsuper}
        # The only allowed superuser roles in the target.
        reserved_roles = set(self.target.get_security_agent_reserved_roles())

        unauthorized_roles = su_missing_roles - reserved_roles

        if unauthorized_roles:
            logger.error("Some superuser roles from source database are not allowed in target database.")

            exc_message = (
                f"Some superuser roles from source database {tuple(unauthorized_roles)} are not "
                "allowed in target database. You can delete them from the source or exclude them from "
                "the migration if you are sure your database will function properly without them."
            )

            raise PGMigrateValidationFailedError(exc_message)

        logger.info("All superuser roles from source database are allowed in target database.")

    def _warn_if_pg_lobs(self):
        """Warn if large objects are present in source databases."""
        for source_db in self.source.databases.values():
            if source_db.error:
                # access failed/rejected, skip lobs check
                continue
            if self.source.large_objects_present(dbname=source_db.dbname):
                logger.warning(
                    "Large objects detected: large objects are not compatible with logical replication: https://www.postgresql.org/docs/14/logical-replication-restrictions.html"
                )

    def _migrate_roles(self) -> Dict[str, PGRoleTask]:
        roles: Dict[str, PGRoleTask] = dict()
        rolname: str
        role: PGRole
        for rolname, role in self.source.pg_roles.items():
            if rolname in self.target.pg_roles:
                logger.warning("Role %r already exists in target", rolname)
                roles[role.rolname] = PGRoleTask(
                    rolname=rolname,
                    status=PGRoleStatus.exists,
                    message="role already exists",
                )
                continue
            logger.info("Creating role %r to target", role)
            sql = "CREATE ROLE {} WITH {} {} {} {} {} {} {}".format(
                role.safe_rolname,
                "SUPERUSER" if role.rolsuper else "NOSUPERUSER",
                "CREATEDB" if role.rolcreatedb else "NOCREATEDB",
                "CREATEROLE" if role.rolcreaterole else "NOCREATEROLE",
                "INHERIT" if role.rolinherit else "NOINHERIT",
                "LOGIN" if role.rolcanlogin else "NOLOGIN",
                "REPLICATION" if role.rolreplication else "NOREPLICATION",
                "BYPASSRLS" if role.rolbypassrls else "NOBYPASSRLS",
            )
            params: List[Any] = []
            if role.rolconnlimit != -1:
                sql += " CONNECTION LIMIT %s"
                params.append(role.rolconnlimit)
            if role.rolpassword:
                sql += " PASSWORD %s"
                params.append(role.rolpassword)
            if role.rolvaliduntil:
                sql += " VALID UNTIL '{}'".format(role.rolvaliduntil)
            try:
                self.target.c(sql, args=params, return_rows=0)
            except psycopg2.ProgrammingError as err:
                roles[role.rolname] = PGRoleTask(
                    rolname=rolname,
                    status=PGRoleStatus.failed,
                    message=err.diag.message_primary,
                )
            else:
                if role.rolconfig:
                    for conf in role.rolconfig:
                        key, value = conf.split("=", 1)
                        logger.info("Setting config for role %r: %s = %s", role.rolname, key, value)
                        self.target.c(f'ALTER ROLE {role.safe_rolname} SET "{key}" = %s', args=(value, ), return_rows=0)
                roles[role.rolname] = PGRoleTask(
                    rolname=rolname,
                    rolpassword=role.rolpassword,
                    status=PGRoleStatus.created,
                    message="role created",
                )

        return roles

    def _log_stream(self, stream, label: str, log_level: int, buffer: Optional[deque]):
        msg_template = f"{label} %s"
        for line in stream:
            decoded_line = line.decode(errors="replace").strip()
            if decoded_line:
                logger.log(log_level, msg_template, decoded_line)
                if buffer is not None:
                    buffer.append(decoded_line)
        stream.close()

    def _build_pg_restore_cmd(self, target_conn_str: str, createdb: bool = False) -> List[str]:
        pg_restore_cmd = [
            str(self.pgbin / "pg_restore"),
            "-d",
            target_conn_str,
        ]
        if self.verbose:
            pg_restore_cmd.append("--verbose")
        if createdb:
            pg_restore_cmd.append("--create")
        return pg_restore_cmd

    def _pg_dump_pipe_pg_restore(
        self, *, pg_dump_cmd: Sequence[str], pg_restore_cmd: Sequence[str], dbname: str, pg_dump_type: PgDumpType
    ) -> PgDumpTask:
        pg_dump = subprocess.Popen(pg_dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pg_restore = subprocess.Popen(pg_restore_cmd, stdin=pg_dump.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # allow pg_dump to receive a SIGPIPE if pg_restore exists
        pg_dump.stdout.close()

        pg_dump_log_label = f"[pg_dump({pg_dump.pid})][{pg_dump_type.value}][{dbname}]"
        pg_restore_log_label = f"[pg_restore({pg_restore.pid})][{pg_dump_type.value}][{dbname}]"

        # We need to detect a log entry in pg_restore indicating that the restore has completed but with errors.
        # It writes this at the end, so we're saving its last log entries.
        pg_restore_log_buffer = deque(maxlen=10)
        threads = [
            threading.Thread(
                target=self._log_stream, daemon=True, args=(pg_dump.stderr, pg_dump_log_label, logging.WARNING, None)
            ),
            threading.Thread(
                target=self._log_stream,
                daemon=True,
                args=(pg_restore.stderr, pg_restore_log_label, logging.WARNING, pg_restore_log_buffer)
            ),
            threading.Thread(
                target=self._log_stream, daemon=True, args=(pg_restore.stdout, pg_restore_log_label, logging.INFO, None)
            )
        ]
        for t in threads:
            t.start()

        tracked_proc = {pg_dump_log_label: pg_dump, pg_restore_log_label: pg_restore}
        while tracked_proc:
            for proc_log_label, proc in list(tracked_proc.items()):
                try:
                    retcode = proc.wait(timeout=120)
                    logger.info("Process %s exited with code %s", proc_log_label, retcode)
                    tracked_proc.pop(proc_log_label)
                except subprocess.TimeoutExpired:
                    logger.info("Process %s is still running...", proc_log_label)

        for t in threads:
            t.join(timeout=10)
            if t.is_alive():
                logger.warning("Logs reading thread %s did not finish in time, it may still be running", t.name)

        pg_restore_errors_warning = None
        for log_line in pg_restore_log_buffer:
            if 'errors ignored on restore' in log_line:
                pg_restore_errors_warning = log_line

        if pg_dump.returncode == 0 and pg_restore.returncode == 0:
            status = PgDumpStatus.success
        elif pg_dump.returncode == 0 and pg_restore.returncode != 0 and pg_restore_errors_warning:
            status = PgDumpStatus.with_warnings
        else:
            status = PgDumpStatus.failed

        return PgDumpTask(
            status=status,
            dbname=dbname,
            type=pg_dump_type,
            pg_dump_returncode=pg_dump.returncode,
            pg_restore_returncode=pg_restore.returncode,
            pg_restore_warnings=pg_restore_errors_warning,
        )

    def _dump_schema(self, *, db: PGDatabase) -> None:
        logger.info("Dumping schema from database %r", db.dbname)

        pg_dump_cmd = [
            str(self.pgbin / "pg_dump"),
            # Setting owner requires superuser when generated script is run or the same user that owns
            # all of the objects in the script. Using '--no-owner' gives ownership of all the objects to
            # the user who is running the script.
            "--no-owner",
            # Skip COMMENT statements as they require superuser (--no-comments is available in pg 11).
            # "--no-comments",
            "--schema-only",
            "-Fc",
            self.source.conn_str(dbname=db.dbname),
        ]

        # PG 13 and older versions do not support `--extension` option.
        # The migration still succeeds with some unharmful error messages in the output.
        if db and self.source.version >= Version("14"):
            pg_dump_cmd.extend([f"--extension={ext}" for ext in self.filter_extensions(db)])

        if self.createdb:
            pg_restore_cmd = self._build_pg_restore_cmd(self.target.conn_str(), createdb=True)
        else:
            pg_restore_cmd = self._build_pg_restore_cmd(self.target.conn_str(dbname=db.dbname), createdb=False)

        subtask: PgDumpTask = self._pg_dump_pipe_pg_restore(
            dbname=db.dbname, pg_dump_type=PgDumpType.schema, pg_dump_cmd=pg_dump_cmd, pg_restore_cmd=pg_restore_cmd
        )
        if subtask.status == PgDumpStatus.failed:
            raise PGSchemaDumpFailedError(f"Failed to dump schema: {subtask!r}")

    def _dump_data(self, *, db: PGDatabase) -> PGMigrateStatus:
        logger.info("Dumping data from database %r", db.dbname)
        pg_dump_cmd = [
            str(self.pgbin / "pg_dump"),
            "-Fc",
            "--data-only",
            self.source.conn_str(dbname=db.dbname),
        ]
        tables = self.filter_tables(db) or []
        pg_dump_cmd.extend([f"--table={w}" for w in tables])

        # PG 13 and older versions do not support `--extension` option.
        # The migration still succeeds with some unharmful error messages in the output.
        if self.source.version >= Version("14"):
            pg_dump_cmd.extend([f"--extension={ext}" for ext in self.filter_extensions(db)])

        pg_restore_cmd = self._build_pg_restore_cmd(self.target.conn_str(dbname=db.dbname), createdb=False)
        subtask: PgDumpTask = self._pg_dump_pipe_pg_restore(
            dbname=db.dbname,
            pg_dump_type=PgDumpType.data,
            pg_dump_cmd=pg_dump_cmd,
            pg_restore_cmd=pg_restore_cmd,
        )
        if subtask.status == PgDumpStatus.failed:
            raise PGDataDumpFailedError(f"Failed to dump data: {subtask!r}")
        return PGMigrateStatus.done

    def _wait_for_replication(self, *, dbname: str, check_interval: float = 2.0):
        while True:
            in_sync, write_lsn = self.source.replication_in_sync(dbname=dbname, max_replication_lag=self.max_replication_lag)
            if in_sync and self.target.replication_in_sync(
                dbname=dbname, write_lsn=write_lsn, max_replication_lag=self.max_replication_lag
            ):
                break
            time.sleep(check_interval)

    def _db_replication(self, *, db: PGDatabase) -> PGMigrateStatus:
        dbname = db.dbname
        try:
            tables = self.filter_tables(db) or []
            self.source.create_publication(dbname=dbname, only_tables=tables)
            self.source.create_replication_slot(dbname=dbname)
            self.target.create_subscription(conn_str=self.source.conn_str(dbname=dbname), dbname=dbname)

        except psycopg2.ProgrammingError as e:
            logger.error("Encountered error: %r, cleaning up", e)

            # clean-up replication objects, avoid leaving traces specially in source
            self.target.cleanup(dbname=dbname)
            self.source.cleanup(dbname=dbname)
            raise

        logger.info("Logical replication setup successful for database %r", dbname)
        if self.max_replication_lag > -1:
            self._wait_for_replication(dbname=dbname)
        if self.stop_replication:
            self.target.cleanup(dbname=dbname)
            self.source.cleanup(dbname=dbname)
            return PGMigrateStatus.done

        # leaving replication running
        return PGMigrateStatus.running

    def _db_migrate(self, *, pgtask: PGMigrateTask) -> PGMigrateStatus:
        """Migrate, executes in thread"""
        if pgtask.source_db.error:
            raise pgtask.source_db.error
        if pgtask.target_db and pgtask.target_db.error:
            raise pgtask.target_db.error

        self._dump_schema(db=pgtask.source_db)
        self.target.refresh_db(db=pgtask.source_db)

        fallback_to_dump = pgtask.method is None

        # if method is not yet specified we'll try replication first and dump
        # second
        if self.source.replication_available and fallback_to_dump:
            pgtask.method = PGMigrateMethod.replication

        if pgtask.method == PGMigrateMethod.replication:
            try:
                return self._db_replication(db=pgtask.source_db)
            except psycopg2.ProgrammingError as err:
                if err.pgcode == psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE and fallback_to_dump:
                    logger.warning("Logical replication failed with error: %r, fallback to dump", err.diag.message_primary)
                else:
                    # unexpected error
                    raise

        pgtask.method = PGMigrateMethod.dump
        return self._dump_data(db=pgtask.source_db)

    def validate(self, dbs_max_total_size: Optional[float] = None):
        """
        Do best effort validation whether all the bits and pieces are in place for migration to succeed.
        * Migrating to same server is not supported (doable but requires obviously different dbname)
        * Migrating to older pg version is not supported
        * pgdump needs to be from the same version as source
        * Check that databases exist in target (if --no-createdb)
        * Check that all languages installed in source are also available in target
        * Check that all extensions installed in source databases are either installed or available for installation
          in target
        * Check that all superusers in source are authorized in target. It can be denied by aiven-gatekeeper.
        * Check that large objects are not present in the source database. If present, issue a warning.
        """
        try:
            if self.stop_replication and self.max_replication_lag < 0:
                raise PGMigrateValidationFailedError("Stopping replication requires also '--max-replication-lag' >= 0")
            self._check_different_servers()
            self._check_db_versions()
            # pgdump cannot be older than the source version, cannot be newer than the target version,
            # but it can be newer than the source version: source <= pgdump <= target

            if self.pgbin is None:
                self.pgbin = find_pgbin_dir(str(self.source.version), max_pgversion=str(self.target.version))

            self._check_databases()
            if dbs_max_total_size is not None:
                self._check_database_size(max_size=dbs_max_total_size)
            self._check_pg_lang()
            self._check_pg_ext()
            self._check_aiven_pg_security_agent()
            self._warn_if_pg_lobs()
        except KeyError as err:
            raise PGMigrateValidationFailedError("Invalid source or target connection string") from err
        except ValueError as err:
            logger.error(err)
            raise PGMigrateValidationFailedError(str(err)) from err
        except psycopg2.OperationalError as err:
            logger.error(err)
            raise PGMigrateValidationFailedError(str(err)) from err

    def migrate(self, force_method: Optional[PGMigrateMethod] = None) -> PGMigrateResult:
        """Migrate source database(s) to target"""
        self.validate()

        if self.source.replication_available:
            # Figuring out the max number of simultaneous logical replications is bit tedious to do,
            # https://www.postgresql.org/docs/current/logical-replication-config.html
            # Using 2 for now.
            logger.info("Logical replication available in source (%s)", self.source.version)
            for p, s in (
                (
                    self.source.params,
                    "Source",
                ),
                (
                    self.target.params,
                    "Target",
                ),
            ):
                logger.info(
                    "%s: max_replication_slots = %s, max_logical_replication_workers = %s, "
                    "max_worker_processes = %s, wal_level = %s, max_wal_senders = %s", s, p["max_replication_slots"],
                    p["max_logical_replication_workers"], p["max_worker_processes"], p["wal_level"], p["max_wal_senders"]
                )
            max_workers = 2
        else:
            logger.info("Logical replication not available in source (%s)", self.source.version)
            max_workers = min(self.max_workers, os.cpu_count() or 2)

        logger.info("Using max %d workers", max_workers)

        result = PGMigrateResult()

        # roles are global, let's migrate them first
        pg_roles: Dict[str, PGRoleTask] = self._migrate_roles()
        r: PGRoleTask
        for r in pg_roles.values():
            result.pg_roles[r.rolname] = r.result()

        tasks: Dict[futures.Future, PGMigrateTask] = {}

        with futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="aiven_db_migrate") as executor:
            for source_db in self.source.databases.values():
                target_db = self.target.databases.get(source_db.dbname)
                pgtask = PGMigrateTask(source_db=source_db, target_db=target_db, method=force_method)
                task_future = executor.submit(self._db_migrate, pgtask=pgtask)
                tasks[task_future] = pgtask

        for future in futures.as_completed(tasks):
            task = tasks[future]
            assert future.done()
            task.error = future.exception()
            if task.error:
                task.status = PGMigrateStatus.failed
            elif future.cancelled():
                task.status = PGMigrateStatus.cancelled
            else:
                task.status = future.result()

        logger.debug("Waiting for tasks: %r", tasks)
        futures.wait(tasks)

        t: PGMigrateTask
        for t in tasks.values():
            result.pg_databases[t.source_db.dbname] = t.result()

        return result


def main(args=None, *, prog="pg_migrate"):
    """CLI for PostgreSQL migration tool"""
    import argparse

    parser = argparse.ArgumentParser(description="PostgreSQL migration tool.", prog=prog)
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "-s",
        "--source",
        help=(
            "Source PostgreSQL server, either postgres:// uri or libpq connection string. "
            "Required if `SOURCE_SERVICE_URI` environment variable is not set."
        ),
        required=not os.getenv("SOURCE_SERVICE_URI"),
    )
    parser.add_argument(
        "-t",
        "--target",
        help=(
            "Target PostgreSQL server, either postgres:// uri or libpq connection string. "
            "Required if `TARGET_SERVICE_URI` environment variable is not set."
        ),
        required=not os.getenv("TARGET_SERVICE_URI"),
    )
    parser.add_argument(
        "-f", "--filtered-db", help="Comma separated list of databases to filter out during migrations", required=False
    )
    parser.add_argument(
        "-xr",
        "--excluded-roles",
        help="Comma separated list of database roles to exclude during migrations",
        required=False,
    )
    parser.add_argument(
        "-xe",
        "--excluded-extensions",
        help="Comma separated list of database extensions to exclude during migrations",
        required=False,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output.")
    parser.add_argument(
        "--no-createdb", action="store_false", dest="createdb", help="Don't automatically create database(s) in target."
    )
    parser.add_argument(
        "-m",
        "--mangle",
        action="store_true",
        help="Mangle the DB name for the purpose of having a predictable identifier len",
    )
    parser.add_argument(
        "--max-replication-lag",
        type=int,
        default=-1,
        help="Max replication lag in bytes to wait for, by default no wait (no effect when replication isn't available).",
    )
    parser.add_argument(
        "--stop-replication",
        action="store_true",
        help=(
            "Stop replication, by default replication is left running (no effect when replication isn't available). "
            "Requires also '--max-replication-lag' >= 0, i.e. wait until replication lag in bytes is less than/equal "
            "to given max replication lag and then stop replication."
        ),
    )
    parser.add_argument("--validate", action="store_true", help="Run only best effort validation.")
    table_common_help = (
        " Table names can be qualified by name only, schema name and table name or DB , schema and table name."
        " Tables with no DB specified will attempt a match against all present databases."
        " Tables with no schema specified will attempt a match against all schemas in all databases."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-table",
        action="append",
        help=(
            "When specified, the migration method will include the named tables only instead of all tables. "
            "Can be specified multiple times. Cannot be used together with --skip-table." + table_common_help
        ),
    )
    group.add_argument(
        "--skip-table",
        action="append",
        help=(
            "When specified, the migration method will include all tables except the named tables. "
            "Can be specified multiple times. Cannot be used together with --with-table." + table_common_help
        ),
    )
    parser.add_argument(
        "--replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_true",
        default=True,
        help=(
            "logical replication should try to add tables "
            "belonging to extensions to the publication definition (default)"
        ),
    )
    parser.add_argument(
        "--no-replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_false",
        help="Do not add tables belonging to extensions to the publication definition",
    )
    parser.add_argument(
        "--force-method",
        default=None,
        help="Force the migration method to be used as either replication or dump.",
    )
    parser.add_argument(
        "--dbs-max-total-size",
        type=int,
        default=-1,
        help="Max total size of databases to be migrated, ignored by default",
    )
    parser.add_argument(
        "--pgbin",
        type=str,
        default=None,
        help="Path to pg_dump and other postgresql binaries",
    )
    parser.add_argument(
        "--skip-db-version-check",
        action="store_true",
        help="Skip PG version check between source and target",
    )
    parser.add_argument(
        "--show-passwords",
        action="store_true",
        help="Show generated placeholder roles passwords in the output, by default passwords are not shown",
    )
    parser.add_argument(
        "--migration-id",
        default=''.join(random.choices(string.ascii_letters + string.digits, k=4)),
        help="This identifier will be added to all logs"
    )

    args = parser.parse_args(args)
    log_format = f"%(asctime)s\t{args.migration_id}\t%(name)s\t%(levelname)s\t%(message)s"
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

    pg_mig = PGMigrate(
        source_conn_info=os.getenv("SOURCE_SERVICE_URI") or args.source,
        target_conn_info=os.getenv("TARGET_SERVICE_URI") or args.target,
        pgbin=args.pgbin,
        createdb=args.createdb,
        max_replication_lag=args.max_replication_lag,
        stop_replication=args.stop_replication,
        verbose=args.verbose,
        filtered_db=args.filtered_db,
        excluded_roles=args.excluded_roles,
        excluded_extensions=args.excluded_extensions,
        mangle=args.mangle,
        skip_tables=args.skip_table,
        with_tables=args.with_table,
        replicate_extensions=args.replicate_extension_tables,
        skip_db_version_check=args.skip_db_version_check,
    )

    method = None
    if args.force_method:
        try:
            method = PGMigrateMethod[args.force_method]
        except KeyError as e:
            raise ValueError(f"Unsupported migration method '{args.force_method}'") from e

    if args.validate:
        dbs_max_total_size = None if args.dbs_max_total_size == -1 else args.dbs_max_total_size
        pg_mig.validate(dbs_max_total_size=dbs_max_total_size)
    else:
        result: PGMigrateResult = pg_mig.migrate(force_method=method)
        logger.info("Roles:")
        for role in result.pg_roles.values():
            rolpassword = role["rolpassword"]
            if rolpassword is None:
                rolpassword = "-"
            elif not args.show_passwords:
                rolpassword = "<hidden>"
            logger.info(
                "  rolname: {!r}, rolpassword: {!r}, status: {!r}, message: {!r}".format(
                    role["rolname"], rolpassword, role["status"], role["message"]
                )
            )
        logger.info("Databases:")
        has_failures = False
        for db in result.pg_databases.values():
            if db["status"] == PGMigrateStatus.failed:
                has_failures = True
            logger.info(
                "  dbaname: {!r}, method: {!r}, status: {!r}, message: {!r}".format(
                    db["dbname"], db["method"], db["status"], db["message"]
                )
            )
        if has_failures:
            sys.exit("Database migration did not succeed")


if __name__ == "__main__":
    main()
