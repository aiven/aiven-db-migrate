# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.clusters import PGSource, PGTarget
from aiven_db_migrate.migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.migrate.models import (
    DBMigrateResult, DumpTaskResult, DumpType, PGDatabase, PGExtension, PGMigrateMethod, PGMigrateResult, PGMigrateStatus,
    PGRoleStatus, PGTable, ReplicationSetupResult, RoleMigrateTask, ValidationResult
)
from aiven_db_migrate.migrate.pgutils import build_pg_dump_cmd, build_pg_restore_cmd, find_pgbin_dir, run_pg_dump_pg_restore
from concurrent import futures
from packaging.version import Version
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import logging
import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions
import psycopg2.extras
import time

logger = logging.getLogger("PGMigrate")
MAX_CLI_LEN = 2097152  # getconf ARG_MAX


class PGMigrate:
    """PostgreSQL migrator"""
    def __init__(
        self,
        *,
        source_conn_info: Union[str, Dict[str, Any]],
        target_conn_info: Union[str, Dict[str, Any]],
        pgbin: Optional[str] = None,
        createdb: bool = True,
        max_workers: int = 2,
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
        migrate_method: PGMigrateMethod = PGMigrateMethod.replication_with_dump_fallback,
    ):
        if skip_tables and with_tables:
            raise ValueError("Can only specify a skip table list or a with table list")
        self.excluded_extensions = set(excluded_extensions.split(",")) if excluded_extensions else None
        self.source = PGSource(
            conn_info=source_conn_info,
            filtered_db=filtered_db,
            excluded_roles=excluded_roles,
            mangle=mangle,
        )
        self.target = PGTarget(
            conn_info=target_conn_info,
            filtered_db=filtered_db,
            excluded_roles=excluded_roles,
            mangle=mangle,
        )
        self.skip_tables = self._convert_table_names(skip_tables)
        self.with_tables = self._convert_table_names(with_tables)
        self.pgbin = Path(pgbin) if pgbin else None
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
        self.migrate_method = migrate_method

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
        ret = set()
        if self.skip_tables:
            # the db tables should be properly populated on all 3 fields, so we can consider one of the user passed ones
            # to be equivalent the table name is the same AND the schema name is missing or the same AND the
            # db name is missing or the same
            for t in db.tables:
                found = None
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

    def get_extensions_to_migrate(self, dbname: str) -> list[str] | None:
        """
        Given a database, return installed extensions on the source without
        the ones that have explicitly been excluded from the migration.
        """
        if self.source.version < Version("14") or not self.excluded_extensions:
            # None means no filtering, migrate all
            return None
        return [e.name for e in self.source.databases[dbname].pg_ext if e.name not in self.excluded_extensions]

    def _check_different_servers(self) -> None:
        """Check if source and target are different servers."""
        source = (self.source.conn_info["host"], self.source.conn_info.get("port"))
        target = (self.target.conn_info["host"], self.target.conn_info.get("port"))
        if source == target:
            raise PGMigrateValidationFailedError("Migrating to the same server is not supported")

    def _check_db_versions(self) -> None:
        """Check that the version of the target database is the same or more recent than the source database."""
        if self.skip_db_version_check:
            return
        if self.source.version < Version("12"):
            raise PGMigrateValidationFailedError("Source PostgreSQL version must be 12 or newer")
        if self.target.version < Version("12"):
            raise PGMigrateValidationFailedError("Target PostgreSQL version must be 12 or newer")
        if self.source.version.major > self.target.version.major:
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
            dbs_size += db_size  # type: ignore[assignment]
        if dbs_size > max_size:
            raise PGMigrateValidationFailedError(
                f"Databases do not fit to the required maximum size ({dbs_size} > {max_size})"
            )

    def _check_pg_lang(self):
        source_lang = {lan["lanname"] for lan in self.source.pg_lang}
        target_lang = {lan["lanname"] for lan in self.target.pg_lang}
        missing = source_lang - target_lang
        if missing:
            missing_lang = ", ".join(sorted(missing))
            raise PGMigrateValidationFailedError(f"Languages not installed in target: {missing_lang}")

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
                if self.excluded_extensions and source_ext.name in self.excluded_extensions:
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
                except StopIteration as e:
                    msg = f"Extension {source_ext.name!r} is not available for installation in target"
                    logger.error(msg)
                    raise PGMigrateValidationFailedError(msg) from e

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
                    "Large objects detected: large objects are not compatible with logical replication: "
                    "https://www.postgresql.org/docs/14/logical-replication-restrictions.html"
                )

    def _log_replication_params(self):
        for pg_cluster, label in ((self.source, "Source"), (self.target, "Target")):
            if pg_cluster and pg_cluster.params:
                logger.info(
                    "%s: max_replication_slots = %s, max_logical_replication_workers = %s, "
                    "max_worker_processes = %s, wal_level = %s, max_wal_senders = %s", label,
                    pg_cluster.params.get("max_replication_slots"), pg_cluster.params.get("max_logical_replication_workers"),
                    pg_cluster.params.get("max_worker_processes"), pg_cluster.params.get("wal_level"),
                    pg_cluster.params.get("max_wal_senders")
                )

    def _migrate_roles(self) -> list[RoleMigrateTask]:
        roles = {}
        for rolname, role in self.source.pg_roles.items():
            if rolname in self.target.pg_roles:
                logger.warning("Role %r already exists in target", rolname)
                roles[role.rolname] = RoleMigrateTask(
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
            params: list[str | int] = []
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
                roles[role.rolname] = RoleMigrateTask(
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
                roles[role.rolname] = RoleMigrateTask(
                    rolname=rolname,
                    rolpassword=role.rolpassword,
                    status=PGRoleStatus.created,
                    message="role created",
                )

        return list(roles.values())

    def _dump_schema(self, *, dbname: str) -> DumpTaskResult:
        logger.info("Dumping schema from database %r", dbname)
        assert self.pgbin
        pg_dump_cmd = build_pg_dump_cmd(
            self.pgbin,
            self.source.conn_str(dbname=dbname),
            extensions=self.get_extensions_to_migrate(dbname),
            schema_only=True,
            no_owner=True,
            verbose=self.verbose
        )
        pg_restore_cmd = build_pg_restore_cmd(
            self.pgbin,
            self.target.conn_str() if self.createdb else self.target.conn_str(dbname=dbname),
            createdb=self.createdb,
            verbose=self.verbose
        )
        try:
            return run_pg_dump_pg_restore(
                dbname=dbname, pg_dump_type=DumpType.schema, pg_dump_cmd=pg_dump_cmd, pg_restore_cmd=pg_restore_cmd
            )
        except Exception as e:
            logger.exception("Schema dump failed for database %s", dbname)
            return DumpTaskResult(
                status=PGMigrateStatus.failed,
                type=DumpType.schema,
                error=e,
            )

    def _dump_data(self, *, db: PGDatabase) -> DumpTaskResult:
        dbname = db.dbname
        logger.info("Dumping data from database %r", dbname)
        assert self.pgbin
        pg_dump_cmd = build_pg_dump_cmd(
            self.pgbin,
            self.source.conn_str(dbname=dbname),
            extensions=self.get_extensions_to_migrate(dbname),
            tables=self.filter_tables(db),
            data_only=True,
            verbose=self.verbose
        )
        pg_restore_cmd = build_pg_restore_cmd(self.pgbin, self.target.conn_str(dbname=dbname), verbose=self.verbose)
        try:
            return run_pg_dump_pg_restore(
                dbname=dbname,
                pg_dump_type=DumpType.data,
                pg_dump_cmd=pg_dump_cmd,
                pg_restore_cmd=pg_restore_cmd,
            )
        except Exception as e:
            logger.exception("Data dump failed for database %s", dbname)
            return DumpTaskResult(
                status=PGMigrateStatus.failed,
                type=DumpType.data,
                error=e,
            )

    def _wait_for_replication(self, *, dbname: str, check_interval: float = 2.0):
        while True:
            in_sync, write_lsn = self.source.replication_in_sync(dbname=dbname, max_replication_lag=self.max_replication_lag)
            if in_sync and self.target.replication_in_sync(
                dbname=dbname, write_lsn=write_lsn, max_replication_lag=self.max_replication_lag
            ):
                break
            time.sleep(check_interval)

    def _setup_logical_replication(self, *, db: PGDatabase) -> ReplicationSetupResult:
        dbname = db.dbname
        try:
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
                return ReplicationSetupResult(status=PGMigrateStatus.done)

            # leaving replication running
            return ReplicationSetupResult(status=PGMigrateStatus.running)

        except Exception as e:
            logger.error("Logical replication setup failed for database %s. %r", dbname, e)
            return ReplicationSetupResult(
                status=PGMigrateStatus.failed,
                error=e,
            )

    def _db_migrate(
        self, *, migrate_method: PGMigrateMethod, source_db: PGDatabase, target_db: PGDatabase | None
    ) -> DBMigrateResult:
        """Migrate, executes in thread"""
        dmr = DBMigrateResult(
            status=PGMigrateStatus.done,  # could change later
            dbname=source_db.dbname,
            migrate_method=migrate_method,
        )
        try:
            if source_db.error:
                dmr.source_db_error = source_db.error
                dmr.status = PGMigrateStatus.failed
                return dmr
            if target_db and target_db.error:
                dmr.target_db_error = target_db.error
                dmr.status = PGMigrateStatus.failed
                return dmr

            dmr.dump_schema_result = self._dump_schema(dbname=source_db.dbname)
            if dmr.dump_schema_result.status == PGMigrateStatus.failed:
                dmr.status = PGMigrateStatus.failed
                return dmr

            self.target.refresh_db(db=source_db)
            if migrate_method == PGMigrateMethod.schema_only:
                pass
            elif migrate_method == PGMigrateMethod.dump:
                dmr.dump_data_result = self._dump_data(db=source_db)
                dmr.status = dmr.dump_data_result.status
            elif migrate_method == PGMigrateMethod.replication:
                dmr.replication_setup_result = self._setup_logical_replication(db=source_db)
                dmr.status = dmr.replication_setup_result.status
            elif migrate_method == PGMigrateMethod.replication_with_dump_fallback:
                dmr.replication_setup_result = self._setup_logical_replication(db=source_db)
                dmr.status = dmr.replication_setup_result.status
                if dmr.status == PGMigrateStatus.failed:
                    logger.warning("Replication setup failed, falling back to dump")
                    dmr.dump_data_result = self._dump_data(db=source_db)
                    dmr.status = dmr.dump_data_result.status
            else:
                raise NotImplementedError(f"Migration method {migrate_method} is not implemented")
            return dmr
        except Exception as e:
            logger.exception("Migration failed for database %s", source_db.dbname)
            dmr.status = PGMigrateStatus.failed
            dmr.error = e
            return dmr

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

    def migrate(self) -> PGMigrateResult:
        """Migrate source database(s) to target"""
        pg_migrate_result = PGMigrateResult()
        try:
            self.validate()
            pg_migrate_result.validation = ValidationResult(status=PGMigrateStatus.done)
        except Exception as e:
            logger.exception("Validation failed, migration cannot continue")
            pg_migrate_result.validation = ValidationResult(status=PGMigrateStatus.failed, error=e)
            return pg_migrate_result

        self._log_replication_params()
        # roles are global, let's migrate them first
        pg_migrate_result.role_migrate_results = self._migrate_roles()

        # migrate databases in parallel using threads
        future_dbname_map = {}
        logger.info("Using %d workers", self.max_workers)
        with futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="aiven_db_migrate") as executor:
            for source_db in self.source.databases.values():
                target_db = self.target.databases.get(source_db.dbname)
                task_future = executor.submit(
                    self._db_migrate, migrate_method=self.migrate_method, source_db=source_db, target_db=target_db
                )
                future_dbname_map[task_future] = source_db.dbname

        for future in futures.as_completed(future_dbname_map):
            dbname = future_dbname_map[future]
            try:
                db_migrate_result = future.result()
            except Exception as e:
                db_migrate_result = DBMigrateResult(
                    dbname=dbname,
                    status=PGMigrateStatus.failed,
                    migrate_method=self.migrate_method,
                    error=e,
                )
            pg_migrate_result.db_migrate_results.append(db_migrate_result)
        return pg_migrate_result
