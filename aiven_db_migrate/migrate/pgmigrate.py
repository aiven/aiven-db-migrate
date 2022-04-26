# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.errors import (
    PGDataDumpFailedError, PGDataNotFoundError, PGMigrateValidationFailedError, PGSchemaDumpFailedError, PGTooMuchDataError
)
from aiven_db_migrate.migrate.pgutils import (
    create_connection_string, find_pgbin_dir, get_connection_info, validate_pg_identifier_length, wait_select
)
from aiven_db_migrate.migrate.version import __version__
from concurrent import futures
from contextlib import contextmanager, suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
from distutils.version import LooseVersion
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

import enum
import hashlib
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

MAX_CLI_LEN = 2097152  # getconf ARG_MAX


@dataclass
class PGExtension:
    name: str
    version: str
    superuser: bool = True
    trusted: Optional[bool] = None


@dataclass(frozen=True)
class PGTable:
    db_name: Optional[str]
    schema_name: Optional[str]
    table_name: str
    extension_name: Optional[str]

    def __str__(self) -> str:
        if not self.schema_name:
            return self.table_name
        return f"{self.schema_name}.{self.table_name}"

    def __hash__(self) -> int:
        return ((hash(self.db_name) & 0xFFFF000000000000) ^ (hash(self.schema_name) & 0x0000FFFF00000000) ^
                (hash(self.table_name) & 0x00000000FFFFFFFF))

    def __eq__(self, other):
        if not isinstance(other, PGTable):
            return False
        return (
            self.table_name == other.table_name and self.schema_name == other.schema_name and self.db_name == other.db_name
            and self.extension_name == other.extension_name
        )


@dataclass
class PGDatabase:
    dbname: str
    tables: Set[PGTable]
    pg_ext: List[PGExtension] = field(default_factory=list)
    has_aiven_extras: bool = False
    error: Optional[BaseException] = None


@dataclass
class PGRole:
    rolname: str
    rolsuper: bool
    rolinherit: bool
    rolcreaterole: bool
    rolcreatedb: bool
    rolcanlogin: bool
    rolreplication: bool
    rolconnlimit: int
    # placeholder password
    rolpassword: Optional[str]
    rolvaliduntil: Optional[datetime]
    rolbypassrls: bool
    rolconfig: List[str]
    safe_rolname: str


class PGCluster:
    """PGCluster is a collection of databases managed by a single PostgreSQL server instance"""
    conn_info: Dict[str, Any]
    _databases: Dict[str, PGDatabase]
    _params: Dict[str, str]
    _version: Optional[LooseVersion]
    _attributes: Optional[Dict[str, Any]]
    _pg_ext: Optional[List[PGExtension]]
    _pg_ext_whitelist: Optional[List[str]]
    _pg_lang: Optional[List[Dict[str, Any]]]
    _pg_roles: Dict[str, PGRole]
    _mangle: bool

    def __init__(
        self,
        conn_info: Union[str, Dict[str, Any]],
        filtered_db: Optional[str] = None,
        filtered_roles: Optional[str] = None,
        mangle: bool = False
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        self.conn_info = get_connection_info(conn_info)
        self.conn_lock = threading.RLock()
        self.db_lock = threading.Lock()
        self._databases = dict()
        self._params = dict()
        self._version = None
        self._attributes = None
        self._pg_ext = None
        self._pg_ext_whitelist = None
        self._pg_lang = None
        self._pg_roles = dict()
        if filtered_db:
            self.filtered_db = filtered_db.split(",")
        else:
            self.filtered_db = []
        if filtered_roles:
            self.filtered_roles = filtered_roles.split(",")
        else:
            self.filtered_roles = []
        if "application_name" not in self.conn_info:
            self.conn_info["application_name"] = f"aiven-db-migrate/{__version__}"
        self._mangle = mangle

    def conn_str(self, *, dbname: str = None) -> str:
        conn_info: Dict[str, Any] = deepcopy(self.conn_info)
        if dbname:
            conn_info["dbname"] = dbname
        conn_info["application_name"] = conn_info["application_name"] + "/" + self.mangle_db_name(conn_info["dbname"])
        return create_connection_string(conn_info)

    def connect_timeout(self):

        try:
            return int(self.conn_info.get("connect_timeout", os.environ.get("PGCONNECT_TIMEOUT", "")))
        except ValueError:
            return None

    @contextmanager
    def _cursor(self, *, dbname: str = None) -> RealDictCursor:
        conn: Optional[psycopg2.extensions.connection] = None
        conn_info: Dict[str, Any] = deepcopy(self.conn_info)
        if dbname:
            conn_info["dbname"] = dbname
        # we are modifying global objects (pg_catalog.pg_subscription, pg_catalog.pg_replication_slots)
        # from multiple threads; allow only one connection at time
        self.conn_lock.acquire()
        try:
            conn = psycopg2.connect(**conn_info, async_=True)
            wait_select(conn, self.connect_timeout())
            yield conn.cursor(cursor_factory=RealDictCursor)
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()
            self.conn_lock.release()

    def c(
        self,
        query: Union[str, sql.Composable],
        *,
        args: Sequence[Any] = None,
        dbname: str = None,
        return_rows: int = -1,
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        with self._cursor(dbname=dbname) as cur:
            try:
                cur.execute(query, args)
                wait_select(cur.connection)
            except KeyboardInterrupt:
                # We wrap the whole execute+wait block to make sure we cancel
                # the query in all cases, which we couldn't if KeyboardInterupt
                # was only handled inside wait_select.
                cur.connection.cancel()
                raise
            if return_rows:
                results = cur.fetchall()
        if return_rows > 0 and len(results) != return_rows:
            error = "expected {} rows, got {}".format(return_rows, len(results))
            self.log.error(error)
            if len(results) < return_rows:
                raise PGDataNotFoundError(error)
            raise PGTooMuchDataError(error)
        return results

    @property
    def params(self) -> Dict[str, str]:
        if not self._params:
            params = self.c("SHOW ALL")
            self._params = {p["name"]: p["setting"] for p in params}
        return self._params

    @property
    def version(self) -> LooseVersion:
        if self._version is None:
            # will make this work on ubuntu, for strings like '12.5 (Ubuntu 12.5-1.pgdg18.04+1)'
            self._version = LooseVersion(self.params["server_version"].split(" ")[0])
        return self._version

    @property
    def attributes(self) -> Dict[str, Any]:
        if self._attributes is None:
            self._attributes = self.c("SELECT * FROM pg_catalog.pg_roles where rolname = current_user", return_rows=1)[0]
        return self._attributes

    def _set_db(self, *, dbname: str, with_extras: bool = False) -> List[PGExtension]:
        # try to install aiven_extras and fail silently
        if with_extras:
            try:
                self.c("CREATE EXTENSION aiven_extras CASCADE", dbname=dbname)
            except psycopg2.ProgrammingError as e:
                self.log.info(e)
        try:
            exts = self.c("SELECT extname as name, extversion as version FROM pg_catalog.pg_extension", dbname=dbname)
            pg_ext = [PGExtension(name=e["name"], version=e["version"]) for e in exts]
            try:
                next(e for e in pg_ext if e.name == "aiven_extras")
            except StopIteration:
                has_aiven_extras = False
            else:
                has_aiven_extras = True
            extension_tables = {}
            extension_tables_res = self.c(
                "SELECT extname, extconfig FROM pg_catalog.pg_extension WHERE extconfig IS NOT NULL", dbname=dbname
            )
            for row in extension_tables_res:
                for table_id in row["extconfig"]:
                    extension_tables[table_id] = row["extname"]
            ret = self.c(
                """SELECT pg_class.oid AS table_id,
                        pg_catalog.pg_class.relname AS table_name,
                        pg_catalog.pg_namespace.nspname AS schema_name
                        FROM pg_catalog.pg_class JOIN pg_catalog.pg_namespace
                            ON (pg_catalog.pg_class.relnamespace=pg_catalog.pg_namespace.oid)
                            JOIN pg_catalog.pg_tables ON
                                (pg_catalog.pg_tables.schemaname=pg_catalog.pg_namespace.nspname
                                AND pg_catalog.pg_tables.tablename=pg_catalog.pg_class.relname)
                    WHERE pg_catalog.pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema')""",
                dbname=dbname,
            )
            tables = set()
            for t in ret:
                ext_name = extension_tables.get(t["table_id"])
                tables.add(
                    PGTable(
                        db_name=dbname, table_name=t["table_name"], schema_name=t["schema_name"], extension_name=ext_name
                    )
                )
            self._databases[dbname] = PGDatabase(
                dbname=dbname, pg_ext=pg_ext, has_aiven_extras=has_aiven_extras, tables=tables
            )
        except psycopg2.OperationalError as err:
            self.log.warning("Couldn't connect to database %r: %r", dbname, err)
            self._databases[dbname] = PGDatabase(dbname=dbname, error=err, tables=set())
        return []

    @property
    def databases(self) -> Dict[str, PGDatabase]:
        filtered = ["template0", "template1"]
        if self.filtered_db:
            filtered.extend(self.filtered_db)
        db_list = ",".join(f"'{db}'" for db in filtered)
        with self.db_lock:
            dbs = self.c(f"SELECT datname FROM pg_catalog.pg_database WHERE datname NOT IN ({db_list})")
            for db in dbs:
                if db["datname"] not in self._databases:
                    self._set_db(dbname=db["datname"])
            return self._databases

    @property
    def pg_ext(self) -> List[PGExtension]:
        """Available extensions"""
        if self._pg_ext is None:
            # Starting from PotsgreSQL 13, extensions have a trusted flag that means
            # they can be created without being superuser.
            trusted_field = ", extver.trusted" if self.version >= LooseVersion("13") else ""
            exts = self.c(
                f"""
                SELECT extver.name, extver.version, extver.superuser {trusted_field}
                FROM pg_catalog.pg_available_extension_versions extver
                JOIN pg_catalog.pg_available_extensions ext
                    ON (extver.name = ext.name AND extver.version = ext.default_version)
                """
            )
            self._pg_ext = [
                PGExtension(name=e["name"], version=e["version"], superuser=e["superuser"], trusted=e.get("trusted"))
                for e in exts
            ]
        return self._pg_ext

    @property
    def pg_ext_whitelist(self) -> List[str]:
        """Whitelisted extensions, https://github.com/dimitri/pgextwlist"""
        if self._pg_ext_whitelist is None:
            try:
                wlist = self.c("SHOW extwlist.extensions", return_rows=1)[0]
            except psycopg2.ProgrammingError as err:
                if "unrecognized configuration parameter" not in str(err):
                    raise
                self._pg_ext_whitelist = list()
            else:
                self._pg_ext_whitelist = [e.strip() for e in wlist["extwlist.extensions"].split(",")]
        return self._pg_ext_whitelist

    @property
    def pg_lang(self) -> List[Dict[str, Any]]:
        if self._pg_lang is None:
            self._pg_lang = self.c("SELECT * FROM pg_catalog.pg_language")
        return self._pg_lang

    @property
    def pg_roles(self) -> Dict[str, PGRole]:
        filtered_roles = ["rdstopmgr"]
        if self.filtered_roles:
            filtered_roles.extend(self.filtered_roles)
        roles_list = ",".join(f"'{role}'" for role in filtered_roles)

        if not self._pg_roles:
            # exclude system roles
            roles = self.c(
                f"SELECT quote_ident(rolname) as safe_rolname, * FROM pg_catalog.pg_roles WHERE oid > 16384 AND rolname NOT IN ({roles_list})"
            )
            for r in roles:
                rolname = r["rolname"]
                # create semi-random placeholder password for role with login
                rolpassword = (
                    "placeholder_{}".format("".join(random.choices(string.ascii_lowercase, k=16)))
                    if r["rolcanlogin"] else None
                )
                self._pg_roles[rolname] = PGRole(
                    rolname=rolname,
                    rolsuper=r["rolsuper"],
                    rolinherit=r["rolinherit"],
                    rolcreaterole=r["rolcreaterole"],
                    rolcreatedb=r["rolcreatedb"],
                    rolcanlogin=r["rolcanlogin"],
                    rolreplication=r["rolreplication"],
                    rolconnlimit=r["rolconnlimit"],
                    rolpassword=rolpassword,
                    rolvaliduntil=r["rolvaliduntil"],
                    rolbypassrls=r["rolbypassrls"],
                    rolconfig=r["rolconfig"] or [],
                    safe_rolname=r["safe_rolname"]
                )
        return self._pg_roles

    @property
    def replication_available(self) -> bool:
        return self.version >= "10"

    @property
    def user_can_replicate(self) -> bool:
        """
        Check if user has REPLICATION privilege. Note that even if this privilege is missing user might still
        be able to replicate as many service providers have modified PostgreSQL; e.g. AWS RDS uses "rds_replication"
        role for its main user account which doesn't have REPLICATION privilege.
        """
        if "rolreplication" in self.attributes and self.attributes["rolreplication"]:
            return True
        return False

    @property
    def is_superuser(self) -> bool:
        return self.attributes["rolsuper"]

    def has_aiven_extras(self, *, dbname: str) -> bool:
        """Check if aiven_extras extension is installed in database"""
        if dbname in self.databases:
            return self.databases[dbname].has_aiven_extras
        return False

    def refresh_db(self, *, db: PGDatabase) -> bool:
        if db.dbname in self.databases:
            with self.db_lock:
                self._set_db(dbname=db.dbname, with_extras=True)
            return True
        return False

    @staticmethod
    def in_sync(replication_lag: Optional[int], max_replication_lag: int) -> bool:
        return replication_lag <= max_replication_lag if replication_lag is not None else False

    def mangle_db_name(self, db_name: str) -> str:
        if not self._mangle:
            return db_name
        return hashlib.md5(db_name.encode()).hexdigest()


class PGSource(PGCluster):
    """Source PostgreSQL cluster"""
    def get_size(self, *, dbname, only_tables: Optional[List[str]] = None) -> float:
        if only_tables == []:
            return 0
        if only_tables is not None:
            query = "SELECT SUM(pg_total_relation_size(tablename)) AS size FROM UNNEST(%s) AS tablename"
            args = [only_tables]
        else:
            query = "SELECT pg_database_size(oid) AS size FROM pg_catalog.pg_database WHERE datname = %s"
            args = [dbname]
        result = self.c(query, args=args, dbname=dbname)
        return result[0]["size"] or 0

    def create_publication(self, *, dbname: str, only_tables: Optional[List[str]] = None) -> str:
        mangled_name = self.mangle_db_name(dbname)
        pubname = f"aiven_db_migrate_{mangled_name}_pub"
        validate_pg_identifier_length(pubname)

        pub_options: Union[List[str], str]
        pub_options = ["INSERT", "UPDATE", "DELETE"]
        if self.version >= "11":
            pub_options.append("TRUNCATE")
        pub_options = ",".join(pub_options)
        has_aiven_extras = self.has_aiven_extras(dbname=dbname)
        pub_scope_logging = "all tables" if not only_tables else ",".join(only_tables)
        self.log.info(
            "Creating publication %r for %s in database %r, has_aiven_extras: %s", pubname, pub_scope_logging, dbname,
            has_aiven_extras
        )
        # publications as per database so connect to given database
        if has_aiven_extras:
            if only_tables:
                table_params = tuple(only_tables)
                tables_subst = ",".join(" %s" for _ in only_tables)
                query = f"SELECT 1 FROM aiven_extras.pg_create_publication(%s, %s, {tables_subst})"
            else:
                table_params = ()
                query = "SELECT 1 FROM aiven_extras.pg_create_publication_for_all_tables(%s, %s)"
            self.c(query, args=(
                pubname,
                pub_options,
            ) + table_params, dbname=dbname, return_rows=0)
        else:
            # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
            if not only_tables:
                publication_scope = "ALL TABLES"
            else:
                publication_scope = "TABLE " + ", ".join(only_tables)
            self.c(
                f"CREATE PUBLICATION {pubname} FOR {publication_scope} WITH (publish = %s)",
                args=(pub_options, ),
                dbname=dbname,
                return_rows=0
            )

        return pubname

    def create_replication_slot(self, *, dbname: str) -> str:
        mangled_name = self.mangle_db_name(dbname)
        slotname = f"aiven_db_migrate_{mangled_name}_slot"
        validate_pg_identifier_length(slotname)

        self.log.info("Creating replication slot %r in database %r", slotname, dbname)
        slot = self.c(
            "SELECT * FROM pg_catalog.pg_create_logical_replication_slot(%s, %s, FALSE)",
            args=(
                slotname,
                "pgoutput",
            ),
            dbname=dbname,
            return_rows=1
        )[0]
        self.log.info("Created replication slot %r in database %r", slot, dbname)

        return slotname

    def get_publication(self, *, dbname: str, pubname: str) -> Dict[str, Any]:
        # publications as per database so connect to given database
        result = self.c("SELECT * FROM pg_catalog.pg_publication WHERE pubname = %s", args=(pubname, ), dbname=dbname)
        return result[0] if result else {}

    def get_replication_slot(self, *, dbname: str, slotname: str) -> Dict[str, Any]:
        result = self.c(
            "SELECT * from pg_catalog.pg_replication_slots WHERE database = %s AND slot_name = %s",
            args=(
                dbname,
                slotname,
            ),
            dbname=dbname
        )
        return result[0] if result else {}

    def replication_in_sync(self, *, dbname: str, slotname: str, max_replication_lag: int) -> Tuple[bool, str]:
        exists = self.c(
            "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name = %s", args=(slotname, ), dbname=dbname
        )
        if not exists:
            self.log.warning("Replication slot %r doesn't exist in database %r", slotname, dbname)
            return False, ""
        if self.has_aiven_extras(dbname=dbname):
            schema = "aiven_extras"
        else:
            # Requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS;
            # doesn't fail with permission error but all returned lsn's (log sequence numbers) are NULL.
            schema = "pg_catalog"
        status = self.c(
            f"""
            SELECT stat.pid, stat.client_addr, stat.state, stat.sync_state, stat.write_lsn,
                pg_wal_lsn_diff(pg_current_wal_lsn(), stat.write_lsn)::BIGINT AS replication_lag,
                pg_wal_lsn_diff(sent_lsn, stat.write_lsn)::BIGINT AS write_lag,
                pg_wal_lsn_diff(sent_lsn, stat.flush_lsn)::BIGINT AS flush_lag,
                pg_wal_lsn_diff(sent_lsn, stat.replay_lsn)::BIGINT AS replay_lag
            FROM {schema}.pg_stat_replication stat
            JOIN pg_catalog.pg_replication_slots slot ON (stat.pid = slot.active_pid)
            WHERE slot.slot_name = %s
            """,
            args=(slotname, ),
            dbname=dbname
        )
        if status:
            self.log.info("Replication status for slot %r in database %r: %r", slotname, dbname, status[0])
            return self.in_sync(status[0]["replication_lag"], max_replication_lag), status[0]["write_lsn"]

        self.log.warning("Replication status not available for %r in database %r", slotname, dbname)
        return False, ""

    def cleanup(self, *, dbname: str, pubname: str, slotname: str):
        # publications as per database so connect to correct database
        pub = self.get_publication(dbname=dbname, pubname=pubname)
        if pub:
            self.log.info("Dropping publication %r from database %r", pub, dbname)
            self.c("DROP PUBLICATION {}".format(pub["pubname"]), dbname=dbname, return_rows=0)
        slot = self.get_replication_slot(dbname=dbname, slotname=slotname)
        if slot:
            self.log.info("Dropping replication slot %r from database %r", slot, dbname)
            self.c(
                "SELECT 1 FROM pg_catalog.pg_drop_replication_slot(%s)",
                args=(slot["slot_name"], ),
                dbname=dbname,
                return_rows=0
            )


class PGTarget(PGCluster):
    """Target PostgreSQL cluster"""
    def create_subscription(self, *, conn_str: str, pubname: str, slotname: str, dbname: str) -> str:
        mangled_name = self.mangle_db_name(dbname)
        subname = f"aiven_db_migrate_{mangled_name}_sub"
        validate_pg_identifier_length(subname)

        has_aiven_extras = self.has_aiven_extras(dbname=dbname)
        self.log.info(
            "Creating subscription %r to %r with slot %r, has_aiven_extras: %s", subname, pubname, slotname, has_aiven_extras
        )
        if has_aiven_extras:
            self.c(
                "SELECT 1 FROM aiven_extras.pg_create_subscription(%s, %s, %s, %s)",
                args=(
                    subname,
                    conn_str,
                    pubname,
                    slotname,
                ),
                dbname=dbname,
                return_rows=0
            )
        else:
            # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
            self.c(
                f"""
                CREATE SUBSCRIPTION {subname} CONNECTION %s PUBLICATION {pubname}
                WITH (slot_name=%s, create_slot=FALSE, copy_data=TRUE)
                """,
                args=(
                    conn_str,
                    slotname,
                ),
                dbname=dbname,
                return_rows=0
            )

        return subname

    def get_subscription(self, *, dbname: str, subname: str) -> Dict[str, Any]:
        if self.has_aiven_extras(dbname=dbname):
            result = self.c(
                "SELECT * FROM aiven_extras.pg_list_all_subscriptions() WHERE subname = %s", args=(subname, ), dbname=dbname
            )
        else:
            # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
            result = self.c("SELECT * FROM pg_catalog.pg_subscription WHERE subname = %s", args=(subname, ), dbname=dbname)
        return result[0] if result else {}

    def replication_in_sync(self, *, dbname: str, subname: str, write_lsn: str, max_replication_lag: int) -> bool:
        status = self.c(
            """
            SELECT stat.*,
            pg_wal_lsn_diff(stat.received_lsn, %s)::BIGINT AS replication_lag
            FROM pg_catalog.pg_stat_subscription stat
            WHERE subname = %s
            """,
            args=(
                write_lsn,
                subname,
            ),
            dbname=dbname
        )
        if status:
            self.log.info(
                "Replication status for subscription %r in database %r: %r, write_lsn: %r", subname, dbname, status[0],
                write_lsn
            )
            return self.in_sync(status[0]["replication_lag"], max_replication_lag)

        self.log.warning("Replication status not available for %r in database %r", subname, dbname)
        return False

    def cleanup(self, *, dbname: str, subname: str):
        sub = self.get_subscription(dbname=dbname, subname=subname)
        if sub:
            self.log.info("Dropping subscription %r from database %r", sub["subname"], dbname)
            if self.has_aiven_extras(dbname=dbname):
                # NOTE: this drops also replication slot from source
                self.c(
                    "SELECT * FROM aiven_extras.pg_drop_subscription(%s)",
                    args=(sub["subname"], ),
                    dbname=dbname,
                    return_rows=0
                )
            else:
                # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
                self.c("ALTER SUBSCRIPTION {} DISABLE".format(sub["subname"]), dbname=dbname, return_rows=0)
                self.c("ALTER SUBSCRIPTION {} SET (slot_name = NONE)".format(sub["subname"]), dbname=dbname, return_rows=0)
                self.c("DROP SUBSCRIPTION {}".format(sub["subname"]), dbname=dbname, return_rows=0)


@enum.unique
class PGRoleStatus(str, enum.Enum):
    created = "created"
    exists = "exists"
    failed = "failed"


@dataclass
class PGRoleTask:
    message: str
    rolname: str
    status: PGRoleStatus
    rolpassword: Optional[str] = None

    def result(self) -> Dict[str, Optional[str]]:
        return {
            "message": self.message,
            "rolname": self.rolname,
            "rolpassword": self.rolpassword,
            "status": self.status.name,
        }


@enum.unique
class PGMigrateMethod(str, enum.Enum):
    dump = "dump"
    replication = "replication"


@enum.unique
class PGMigrateStatus(str, enum.Enum):
    cancelled = "cancelled"
    done = "done"
    failed = "failed"
    running = "running"


@dataclass
class PGMigrateTask:
    source_db: PGDatabase
    target_db: Optional[PGDatabase]
    error: Optional[BaseException] = None
    method: Optional[PGMigrateMethod] = None
    status: Optional[PGMigrateStatus] = None

    def resolve(self, future: futures.Future):
        assert future.done()
        self.error = future.exception()
        if self.error:
            self.status = PGMigrateStatus.failed
        elif future.cancelled():
            self.status = PGMigrateStatus.cancelled
        else:
            self.status = future.result()

    def result(self) -> Dict[str, Optional[str]]:
        dbname = self.source_db.dbname
        if self.error:
            message = str(self.error)
        elif self.target_db:
            message = "migrated to existing database"
        else:
            message = "created and migrated database"
        method = self.method.name if self.method else None
        status = self.status.name if self.status else None
        return {
            "dbname": dbname,
            "message": message,
            "method": method,
            "status": status,
        }


@dataclass
class PGSubTask:
    pid: int
    returncode: int
    stderr: bytes
    stdout: bytes


DEFAULT_MAX_WORKERS = 4


@dataclass
class PGMigrateResult:
    pg_databases: Dict[str, Any] = field(default_factory=dict)
    pg_roles: Dict[str, Any] = field(default_factory=dict)


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
        createdb: bool = True,
        max_workers: int = DEFAULT_MAX_WORKERS,
        max_replication_lag: int = -1,
        stop_replication: bool = False,
        verbose: bool = False,
        mangle: bool = False,
        filtered_db: Optional[str] = None,
        filtered_roles: Optional[str] = None,
        skip_tables: Optional[List[str]] = None,
        with_tables: Optional[List[str]] = None,
        replicate_extensions: bool = True,
    ):
        if skip_tables and with_tables:
            raise Exception("Can only specify a skip table list or a with table list")
        self.log = logging.getLogger(self.__class__.__name__)
        self.source = PGSource(
            conn_info=source_conn_info, filtered_db=filtered_db, filtered_roles=filtered_roles, mangle=mangle
        )
        self.target = PGTarget(
            conn_info=target_conn_info, filtered_db=filtered_db, filtered_roles=filtered_roles, mangle=mangle
        )
        self.skip_tables = self._convert_table_names(skip_tables)
        self.with_tables = self._convert_table_names(with_tables)
        self.pgbin = Path()
        # include commands to create db in pg_dump output
        self.createdb = createdb
        # TODO: have "--max-workers" in CLI
        self.max_workers = max_workers
        self.max_replication_lag = max_replication_lag
        self.stop_replication = stop_replication
        self.verbose = verbose
        self.mangle = mangle
        self.replicate_extensions = replicate_extensions

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
        self.log.debug(
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
        # -t <table_name> + connection params and other pg_dump / psql params
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

    def _check_databases(self):
        for db in self.source.databases.values():
            dbname = db.dbname
            if db.error:
                self.log.info("Access to source database %r is rejected", dbname)
            elif dbname not in self.target.databases:
                if not self.createdb:
                    raise PGMigrateValidationFailedError(
                        f"Database {dbname!r} doesn't exist in target (not creating databases)"
                    )
                else:
                    self.log.info("Database %r will be created in target", dbname)
            else:
                if self.target.databases[dbname].error:
                    self.log.info("Database %r already exists in target but access is rejected", dbname)
                else:
                    self.log.info("Database %r already exists in target", dbname)

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
                if dbname in self.target.databases:
                    target_db = self.target.databases[dbname]
                    try:
                        target_ext = next(e for e in target_db.pg_ext if e.name == source_ext.name)
                    except StopIteration:
                        self.log.info("Extension %r is not installed in target database %r", source_ext.name, dbname)
                    else:
                        self.log.info(
                            "Extension %r is installed in source and target database %r, source version: %r, "
                            "target version: %r", source_ext.name, dbname, source_ext.version, target_ext.version
                        )
                        if LooseVersion(source_ext.version) <= LooseVersion(target_ext.version):
                            continue
                        msg = (
                            f"Installed extension {source_ext.name!r} in target database {dbname!r} is older than "
                            f"in source, target version: {target_ext.version}, source version: {source_ext.version}"
                        )
                        self.log.error(msg)
                        raise PGMigrateValidationFailedError(msg)

                # check if extension is available for installation in target
                try:
                    target_ext = next(e for e in self.target.pg_ext if e.name == source_ext.name)
                except StopIteration:
                    msg = f"Extension {source_ext.name!r} is not available for installation in target"
                    self.log.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                self.log.info(
                    "Extension %r version %r available for installation in target, source version: %r", target_ext.name,
                    target_ext.version, source_ext.version
                )

                if LooseVersion(target_ext.version) < LooseVersion(source_ext.version):
                    msg = (
                        f"Extension {target_ext.name!r} version available for installation in target is too old, "
                        f"source version: {source_ext.version}, target version: {target_ext.version}"
                    )
                    self.log.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                # check if we can install this extension
                if target_ext.name in self.target.pg_ext_whitelist:
                    self.log.info("Extension %r is whitelisted in target", target_ext.name)
                elif target_ext.trusted:
                    self.log.info("Extension %r is trusted in target", target_ext.name)
                elif target_ext.superuser and not self.target.is_superuser:
                    msg = f"Installing extension {target_ext.name!r} in target requires superuser"
                    self.log.error(msg)
                    raise PGMigrateValidationFailedError(msg)

                # schema dump creates extension to target db
                self.log.info(
                    "Extension %r version %r will be installed in target database %r", target_ext.name, target_ext.version,
                    dbname
                )

    def _migrate_roles(self) -> Dict[str, PGRoleTask]:
        roles: Dict[str, PGRoleTask] = dict()
        rolname: str
        role: PGRole
        for rolname, role in self.source.pg_roles.items():
            if rolname in self.target.pg_roles:
                self.log.warning("Role %r already exists in target", rolname)
                roles[role.rolname] = PGRoleTask(
                    rolname=rolname,
                    status=PGRoleStatus.exists,
                    message="role already exists",
                )
                continue
            self.log.info("Creating role %r to target", role)
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
                try:
                    if role.rolconfig:
                        for conf in role.rolconfig:
                            key, value = conf.split("=", 1)
                            self.log.info("Setting config for role %r: %s = %s", role.rolname, key, value)
                            self.target.c(f'ALTER ROLE {role.safe_rolname} SET "{key}" = %s', args=(value, ), return_rows=0)
                    roles[role.rolname] = PGRoleTask(
                        rolname=rolname,
                        rolpassword=role.rolpassword,
                        status=PGRoleStatus.created,
                        message="role created",
                    )
                # display warning when ProgrammingErrorERROR 42501: InsufficientPrivilege: permission denied to set parameter for a role
                except psycopg2.errors.InsufficientPrivilege:
                    self.log.warning(
                        f'Setting [{role.rolname}]: [{key}] = [{value}] failed.  psycopg2.errors.InsufficientPrivilege'
                    )
        return roles

    @staticmethod
    def _decode_output_line(line: bytes):
        try:
            return line.decode("utf-8")
        except UnicodeDecodeError:
            return line.decode("iso-8859-1")

    def _pg_dump_pipe_psql(self, *, pg_dump_cmd: Sequence[str], target_conn_str: str) -> PGSubTask:
        psql_cmd = [
            "psql",
            "--no-psqlrc",
            "--echo-all" if self.verbose else "--echo-errors",
            # "--variable=ON_ERROR_STOP=1",
            target_conn_str,
        ]
        # https://docs.python.org/3.7/library/subprocess.html#replacing-shell-pipeline
        pg_dump = subprocess.Popen(pg_dump_cmd, stdout=subprocess.PIPE)
        psql = subprocess.Popen(psql_cmd, stdin=pg_dump.stdout, stdout=subprocess.PIPE)
        # allow pg_dump to receive a SIGPIPE if psql exists
        pg_dump.stdout.close()
        stdout, stderr = psql.communicate()
        if self.verbose:
            for line in stdout.split(b"\n") if stdout else []:
                print(self._decode_output_line(line))
        for line in stderr.split(b"\n") if stderr else []:
            self.log.warning(self._decode_output_line(line))
        return PGSubTask(pid=psql.pid, returncode=psql.returncode, stdout=stdout, stderr=stderr)

    def _dump_schema(
        self,
        *,
        db: Optional[PGDatabase],
    ):
        if db:
            dbname: Optional[str] = db.dbname
        else:
            dbname = None

        self.log.info("Dumping schema from database %r", dbname)

        pg_dump_cmd = [
            str(self.pgbin / "pg_dump"),
            # Setting owner requires superuser when generated script is run or the same user that owns
            # all of the objects in the script. Using '--no-owner' gives ownership of all the objects to
            # the user who is running the script.
            "--no-owner",
            # Skip COMMENT statements as they require superuser (--no-comments is available in pg 11).
            # "--no-comments",
            "--schema-only",
            self.source.conn_str(dbname=dbname),
        ]

        if self.createdb:
            pg_dump_cmd.insert(-1, "--create")
            # db is created and connected
            dbname = None
        subtask: PGSubTask = self._pg_dump_pipe_psql(
            pg_dump_cmd=pg_dump_cmd, target_conn_str=self.target.conn_str(dbname=dbname)
        )
        if subtask.returncode != 0:
            raise PGSchemaDumpFailedError(f"Failed to dump schema: {subtask!r}")

    def _dump_data(self, *, db: PGDatabase) -> PGMigrateStatus:
        dbname = db.dbname
        self.log.info("Dumping data from database %r", dbname)
        pg_dump_cmd = [
            str(self.pgbin / "pg_dump"),
            "--data-only",
            self.source.conn_str(dbname=dbname),
        ]
        tables = self.filter_tables(db) or []
        pg_dump_cmd.extend([f"--table={w}" for w in tables])
        subtask: PGSubTask = self._pg_dump_pipe_psql(
            pg_dump_cmd=pg_dump_cmd, target_conn_str=self.target.conn_str(dbname=dbname)
        )
        if subtask.returncode != 0:
            raise PGDataDumpFailedError(f"Failed to dump data: {subtask!r}")
        return PGMigrateStatus.done

    def _wait_for_replication(self, *, dbname: str, slotname: str, subname: str, check_interval: float = 2.0):
        while True:
            in_sync, write_lsn = self.source.replication_in_sync(
                dbname=dbname, slotname=slotname, max_replication_lag=self.max_replication_lag
            )
            if in_sync and self.target.replication_in_sync(
                dbname=dbname, subname=subname, write_lsn=write_lsn, max_replication_lag=self.max_replication_lag
            ):
                break
            time.sleep(check_interval)

    def _db_replication(self, *, db: PGDatabase) -> PGMigrateStatus:
        dbname = db.dbname
        pubname = slotname = subname = None
        try:
            tables = self.filter_tables(db) or []
            pubname = self.source.create_publication(dbname=dbname, only_tables=tables)
            slotname = self.source.create_replication_slot(dbname=dbname)
            subname = self.target.create_subscription(
                conn_str=self.source.conn_str(dbname=dbname), pubname=pubname, slotname=slotname, dbname=dbname
            )
        except psycopg2.ProgrammingError as e:
            self.log.error("Encountered error: %r, cleaning up", e)
            if subname:
                self.target.cleanup(dbname=dbname, subname=subname)
            if pubname and slotname:
                self.source.cleanup(dbname=dbname, pubname=pubname, slotname=slotname)
            raise

        self.log.info("Logical replication setup successful for database %r", dbname)
        if self.max_replication_lag > -1:
            self._wait_for_replication(dbname=dbname, slotname=slotname, subname=subname)
        if self.stop_replication:
            self.target.cleanup(dbname=dbname, subname=subname)
            self.source.cleanup(dbname=dbname, pubname=pubname, slotname=slotname)
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
                    self.log.warning("Logical replication failed with error: %r, fallback to dump", err.diag.message_primary)
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
        """
        try:
            if self.stop_replication and self.max_replication_lag < 0:
                raise PGMigrateValidationFailedError("Stopping replication requires also '--max-replication-lag' >= 0")
            if (
                self.source.conn_info["host"] == self.target.conn_info["host"]
                and self.source.conn_info["port"] == self.target.conn_info["port"]
            ):
                raise PGMigrateValidationFailedError("Migrating to the same server is not supported")
            if self.source.version > self.target.version:
                raise PGMigrateValidationFailedError("Migrating to older PostgreSQL server version is not supported")
            # pgdump cannot be older than the source version, cannot be newer than the target version
            # but it can be newer than the source version: source <= pgdump <= target
            self.pgbin = find_pgbin_dir(str(self.source.version), max_pgversion=str(self.target.version))
            self._check_databases()
            if dbs_max_total_size is not None:
                self._check_database_size(max_size=dbs_max_total_size)
            self._check_pg_lang()
            self._check_pg_ext()
        except KeyError as err:
            raise PGMigrateValidationFailedError("Invalid source or target connection string") from err
        except ValueError as err:
            self.log.error(err)
            raise PGMigrateValidationFailedError(str(err)) from err
        except psycopg2.OperationalError as err:
            self.log.error(err)
            raise PGMigrateValidationFailedError(str(err)) from err

    def migrate(self, force_method: Optional[PGMigrateMethod] = None) -> PGMigrateResult:
        """Migrate source database(s) to target"""
        self.validate()

        if self.source.replication_available:
            # Figuring out the max number of simultaneous logical replications is bit tedious to do,
            # https://www.postgresql.org/docs/current/logical-replication-config.html
            # Using 2 for now.
            self.log.info("Logical replication available in source (%s)", self.source.version)
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
                self.log.info(
                    "%s: max_replication_slots = %s, max_logical_replication_workers = %s, "
                    "max_worker_processes = %s, wal_level = %s, max_wal_senders = %s", s, p["max_replication_slots"],
                    p["max_logical_replication_workers"], p["max_worker_processes"], p["wal_level"], p["max_wal_senders"]
                )
            max_workers = 2
        else:
            self.log.info("Logical replication not available in source (%s)", self.source.version)
            max_workers = min(self.max_workers, os.cpu_count() or 2)

        self.log.info("Using max %d workers", max_workers)

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
                pgtask: PGMigrateTask = PGMigrateTask(source_db=source_db, target_db=target_db, method=force_method)
                task: futures.Future = executor.submit(self._db_migrate, pgtask=pgtask)
                tasks[task] = pgtask
                task.add_done_callback(pgtask.resolve)

        self.log.debug("Waiting for tasks: %r", tasks)
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
        "-s", "--source", help="Source PostgreSQL server, either postgres:// uri or libpq connection string.", required=True
    )
    parser.add_argument(
        "-t", "--target", help="Target PostgreSQL server, either postgres:// uri or libpq connection string.", required=True
    )
    parser.add_argument(
        "-fr", "--filtered-roles", help="Comma separated list of roles to filter out during migrations", required=False
    )
    parser.add_argument(
        "-f", "--filtered-db", help="Comma separated list of databases to filter out during migrations", required=False
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
        help="Max replication lag in bytes to wait for, by default no wait (no effect when replication isn't available)."
    )
    parser.add_argument(
        "--stop-replication",
        action="store_true",
        help=(
            "Stop replication, by default replication is left running (no effect when replication isn't available)."
            " Requires also '--max-replication-lag' >= 0, i.e. wait until replication lag in bytes is less than/equal"
            " to given max replication lag and then stop replication."
        )
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
        help="When specified, the migration method will include the named tables only instead of all tables."
        " Can be specified multiple times. Cannot be used together with --skip-table." + table_common_help
    )
    group.add_argument(
        "--skip-table",
        action="append",
        help="When specified, the migration method will include all tables except the named tables."
        " Can be specified multiple times. Cannot be used together with --with-table." + table_common_help
    )
    parser.add_argument(
        "--replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_true",
        default=True,
        help="logical replication should try to add tables "
        "belonging to extensions to the publication definition (default)"
    )
    parser.add_argument(
        "--no-replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_false",
        help="Do not add tables belonging to extensions to the publication definition"
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

    args = parser.parse_args(args)
    log_format = "%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s"
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

    pg_mig = PGMigrate(
        source_conn_info=args.source,
        target_conn_info=args.target,
        createdb=args.createdb,
        max_replication_lag=args.max_replication_lag,
        stop_replication=args.stop_replication,
        verbose=args.verbose,
        filtered_db=args.filtered_db,
        filtered_roles=args.filtered_roles,
        mangle=args.mangle,
        skip_tables=args.skip_table,
        with_tables=args.with_table,
        replicate_extensions=args.replicate_extension_tables,
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
        print()
        print("Roles:")
        for role in result.pg_roles.values():
            print(
                "  rolname: {!r}, rolpassword: {!r}, status: {!r}, message: {!r}".format(
                    role["rolname"], role["rolpassword"], role["status"], role["message"]
                )
            )
        print()
        print("Databases:")
        has_failures = False
        for db in result.pg_databases.values():
            if db["status"] == PGMigrateStatus.failed:
                has_failures = True
            print(
                "  dbaname: {!r}, method: {!r}, status: {!r}, message: {!r}".format(
                    db["dbname"], db["method"], db["status"], db["message"]
                )
            )
        print()
        if has_failures:
            sys.exit("Database migration did not succeed")


if __name__ == "__main__":
    main()
