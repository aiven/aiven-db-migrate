# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.errors import PGDataNotFoundError, PGTooMuchDataError
from aiven_db_migrate.migrate.models import PGDatabase, PGExtension, PGRole, PGTable, ReplicationObjectType
from aiven_db_migrate.migrate.pgutils import (
    create_connection_string, get_connection_info, validate_pg_identifier_length, wait_select
)
from aiven_db_migrate.migrate.version import __version__
from contextlib import contextmanager, suppress
from copy import deepcopy
from packaging.version import Version
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import hashlib
import logging
import os
import psycopg2
import random
import string
import threading


class PGCluster:
    """PGCluster is a collection of databases managed by a single PostgreSQL server instance"""
    DB_OBJECT_PREFIX = "aiven_db_migrate"
    conn_info: Dict[str, Any]
    _databases: Dict[str, PGDatabase]
    _params: Dict[str, str]
    _version: Optional[Version]
    _attributes: Optional[Dict[str, Any]]
    _pg_ext: Optional[List[PGExtension]]
    _pg_ext_whitelist: Optional[List[str]]
    _pg_lang: Optional[List[Dict[str, Any]]]
    _pg_roles: Dict[str, PGRole]
    _mangle: bool
    _has_aiven_gatekeper: Optional[bool]

    def __init__(
        self,
        conn_info: Union[str, Dict[str, Any]],
        filtered_db: Optional[str] = None,
        excluded_roles: Optional[str] = None,
        mangle: bool = False,
    ):
        self.log = logging.getLogger(self.__class__.__name__)
        self.conn_info = get_connection_info(conn_info)
        self.conn_lock = threading.RLock()
        self.db_lock = threading.Lock()
        self._databases = {}
        self._params = {}
        self._version = None
        self._attributes = None
        self._pg_ext = None
        self._pg_ext_whitelist = None
        self._pg_lang = None
        self._pg_roles = {}
        self.filtered_db = filtered_db.split(",") if filtered_db else []
        self.excluded_roles = excluded_roles.split(",") if excluded_roles else []
        if "application_name" not in self.conn_info:
            self.conn_info["application_name"] = f"aiven-db-migrate/{__version__}"
        self._mangle = mangle
        self._has_aiven_gatekeper = None

    def conn_str(self, *, dbname: Optional[str] = None) -> str:
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
    def _cursor(self, *, dbname: Optional[str] = None) -> RealDictCursor:
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
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SET search_path='';")
            wait_select(cursor.connection)
            yield cursor
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()
            self.conn_lock.release()

    def c(
        self,
        query: Union[str, sql.Composable],
        *,
        args: Optional[Sequence[Any]] = None,
        dbname: Optional[str] = None,
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
            error = f"expected {return_rows} rows, got {len(results)}"
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
    def version(self) -> Version:
        if self._version is None:
            # will make this work on ubuntu, for strings like '12.5 (Ubuntu 12.5-1.pgdg18.04+1)'
            self._version = Version(self.params["server_version"].split(" ")[0])
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
            except (psycopg2.ProgrammingError, psycopg2.NotSupportedError) as e:
                self.log.info("Failed to create aiven_extras extension in %r: %r", dbname, e)
        try:
            exts = self.c("SELECT extname as name, extversion as version FROM pg_catalog.pg_extension", dbname=dbname)
            pg_ext = [PGExtension(name=e["name"], version=e["version"]) for e in exts]
            has_aiven_extras = any(e.name == "aiven_extras" for e in pg_ext)
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
                dbname=dbname, pg_ext=pg_ext, has_aiven_extras=has_aiven_extras, tables=list(tables)
            )
        except psycopg2.OperationalError as err:
            self.log.warning("Couldn't connect to database %r: %r", dbname, err)
            self._databases[dbname] = PGDatabase(dbname=dbname, error=err, tables=[])
        return []

    @property
    def databases(self) -> Dict[str, PGDatabase]:
        filtered = ["template0", "template1"]
        if self.filtered_db:
            filtered.extend(self.filtered_db)
        db_params = ",".join(["%s"] * len(filtered))
        with self.db_lock:
            dbs = self.c(
                f"SELECT datname FROM pg_catalog.pg_database WHERE datname NOT IN ({db_params})",
                args=filtered,
            )
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
            trusted_field = ", extver.trusted" if self.version >= Version("13") else ""
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
                self._pg_ext_whitelist = []
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
        if not self._pg_roles:
            # exclude system roles
            roles = self.c("SELECT quote_ident(rolname) as safe_rolname, * FROM pg_catalog.pg_roles WHERE oid > 16384")
            # Filter out excluded roles.
            roles = [r for r in roles if r["rolname"] not in self.excluded_roles]

            for r in roles:
                rolname = r["rolname"]
                # create semi-random placeholder password for role with login
                pwd_suffix = "".join(random.choices(string.ascii_lowercase, k=16))
                rolpassword = f"placeholder_{pwd_suffix}" if r["rolcanlogin"] else None
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
    def replication_slots_count(self) -> int:
        return int(self.c("SELECT COUNT(1) FROM pg_replication_slots")[0]["count"])

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

    @property
    def has_aiven_gatekeeper(self) -> bool:
        """Check if ``aiven-gatekeeper`` shared library is installed.

        This value is cached as it's not expected to change during runtime.

        See:
            https://github.com/aiven/aiven-pg-security
        """
        if self._has_aiven_gatekeper is None:
            result = self.c("SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'")
            if not result:
                self._has_aiven_gatekeper = False
            else:
                self._has_aiven_gatekeper = "aiven_gatekeeper" in str(result[0]["setting"]).split(",")

        return self._has_aiven_gatekeper

    @property
    def is_pg_security_agent_enabled(self) -> bool:
        """Check if Aiven's ``pg_security_agent`` is enabled."""
        if not self.has_aiven_gatekeeper:
            return False

        result = self.c("SELECT name, setting FROM pg_settings WHERE name = 'aiven.pg_security_agent'")
        if result:
            return result[0]["setting"] == "on"
        return False

    def get_security_agent_reserved_roles(self) -> List[str]:
        """Get list of roles that are reserved through Aiven's ``pg_security_agent``."""
        if not self.is_pg_security_agent_enabled:
            return []

        result = self.c("SELECT name, setting FROM pg_settings WHERE name = 'aiven.pg_security_agent_reserved_roles'")

        if not result:
            return []

        return result[0]["setting"].split(",")

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

    def get_replication_object_name(self, dbname: str, replication_obj_type: ReplicationObjectType) -> str:
        mangled_name = self.mangle_db_name(dbname)
        return f"{self.DB_OBJECT_PREFIX}_{mangled_name}_{replication_obj_type.value}"

    def get_publication_name(self, dbname: str) -> str:
        return self.get_replication_object_name(
            dbname=dbname,
            replication_obj_type=ReplicationObjectType.PUBLICATION,
        )

    def get_subscription_name(self, dbname: str) -> str:
        return self.get_replication_object_name(
            dbname=dbname,
            replication_obj_type=ReplicationObjectType.SUBSCRIPTION,
        )

    def get_replication_slot_name(self, dbname: str) -> str:
        return self.get_replication_object_name(
            dbname=dbname,
            replication_obj_type=ReplicationObjectType.REPLICATION_SLOT,
        )


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
        pubname = self.get_publication_name(dbname)
        validate_pg_identifier_length(pubname)

        pub_options: Union[List[str], str]
        pub_options = ["INSERT", "UPDATE", "DELETE"]
        if self.version >= Version("11"):
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
        slotname = self.get_replication_slot_name(dbname)

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

    def get_publication(self, *, dbname: str) -> Dict[str, Any]:
        pubname = self.get_publication_name(dbname)
        # publications as per database so connect to given database
        result = self.c("SELECT * FROM pg_catalog.pg_publication WHERE pubname = %s", args=(pubname, ), dbname=dbname)
        return result[0] if result else {}

    def get_replication_slot(self, *, dbname: str) -> Dict[str, Any]:
        slotname = self.get_replication_slot_name(dbname)
        result = self.c(
            "SELECT * from pg_catalog.pg_replication_slots WHERE database = %s AND slot_name = %s",
            args=(
                dbname,
                slotname,
            ),
            dbname=dbname
        )
        return result[0] if result else {}

    def replication_in_sync(self, *, dbname: str, max_replication_lag: int) -> Tuple[bool, str]:
        slotname = self.get_replication_slot_name(dbname)
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

    def large_objects_present(self, *, dbname: str) -> bool:
        result = self.c("SELECT EXISTS(SELECT 1 FROM pg_largeobject_metadata)", dbname=dbname)
        if result:
            return result[0]["exists"]
        self.log.warning("Unable to determine if large objects present in database %r", dbname)
        return False

    def cleanup(self, *, dbname: str):
        self._cleanup_replication_object(dbname=dbname, replication_object_type=ReplicationObjectType.PUBLICATION)
        self._cleanup_replication_object(dbname=dbname, replication_object_type=ReplicationObjectType.REPLICATION_SLOT)

    def _cleanup_replication_object(self, dbname: str, replication_object_type: ReplicationObjectType):
        rep_obj_type_display_name = replication_object_type.get_display_name()

        rep_obj_name = self.get_replication_object_name(
            dbname=dbname,
            replication_obj_type=replication_object_type,
        )
        try:
            if replication_object_type is ReplicationObjectType.PUBLICATION:
                delete_query = f"DROP PUBLICATION {rep_obj_name};"
                args = None
            elif replication_object_type is ReplicationObjectType.REPLICATION_SLOT:
                delete_query = "SELECT 1 FROM pg_catalog.pg_drop_replication_slot(%s)"
                args = (rep_obj_name, )
            else:
                # cleanup only handles publications and replication slots in source.
                return

            self.log.info(
                "Dropping %r %r from database %r",
                rep_obj_type_display_name,
                rep_obj_name,
                dbname,
            )
            self.c(delete_query, args=args, dbname=dbname, return_rows=0)
        except Exception as exc:
            self.log.error(
                "Failed to drop %r %r for database %r: %r",
                rep_obj_type_display_name,
                rep_obj_name,
                dbname,
                exc,
            )


class PGTarget(PGCluster):
    """Target PostgreSQL cluster"""
    def create_subscription(self, *, conn_str: str, dbname: str) -> str:
        pubname = self.get_publication_name(dbname)
        slotname = self.get_replication_slot_name(dbname)

        subname = self.get_subscription_name(dbname)
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

    def get_subscription(self, *, dbname: str) -> Dict[str, Any]:
        subname = self.get_subscription_name(dbname)
        if self.has_aiven_extras(dbname=dbname):
            result = self.c(
                "SELECT * FROM aiven_extras.pg_list_all_subscriptions() WHERE subname = %s", args=(subname, ), dbname=dbname
            )
        else:
            # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
            result = self.c("SELECT * FROM pg_catalog.pg_subscription WHERE subname = %s", args=(subname, ), dbname=dbname)
        return result[0] if result else {}

    def replication_in_sync(self, *, dbname: str, write_lsn: str, max_replication_lag: int) -> bool:
        subname = self.get_subscription_name(dbname)
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

    def cleanup(self, *, dbname: str):
        subname = self.get_subscription_name(dbname)
        try:
            if not self.get_subscription(dbname=dbname):
                return

            self.log.info("Dropping subscription %r from database %r", subname, dbname)
            if self.has_aiven_extras(dbname=dbname):
                # NOTE: this drops also replication slot from source
                self.c(
                    "SELECT * FROM aiven_extras.pg_drop_subscription(%s)", args=(subname, ), dbname=dbname, return_rows=0
                )
            else:
                # requires superuser or superuser-like privileges, such as "rds_replication" role in AWS RDS
                self.c("ALTER SUBSCRIPTION {} DISABLE".format(subname), dbname=dbname, return_rows=0)
                self.c("ALTER SUBSCRIPTION {} SET (slot_name = NONE)".format(subname), dbname=dbname, return_rows=0)
                self.c("DROP SUBSCRIPTION {}".format(subname), dbname=dbname, return_rows=0)

        except Exception as exc:
            self.log.error(
                "Failed to drop subscription %r for database %r: %r",
                subname,
                dbname,
                exc,
            )
