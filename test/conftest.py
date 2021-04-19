# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from __future__ import annotations

from aiven_db_migrate.migrate.pgutils import find_pgbin_dir
from contextlib import contextmanager
from datetime import datetime
from distutils.version import LooseVersion
from pathlib import Path
from psycopg2.extras import RealDictCursor
from test.utils import Timer
from typing import Any, Callable, Dict, List, Tuple

import logging
import os
import psycopg2
import psycopg2.extras
import pytest
import re
import subprocess

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")


class PGRunner:
    pgbin: Path
    pgdata: Path
    pgport: int
    pgversion: str
    aiven_extras_available: bool = False
    defaultdb: str = "postgres"
    superuser: str = "postgres"
    testuser: str = "testuser"
    cleanups: List[Callable]

    def __init__(self, *, pgversion: str, pgdata: Path, pgport: int = 5432):
        self.log = logging.getLogger(self.__class__.__name__)
        self.pgbin = find_pgbin_dir(pgversion)
        self.pgversion = pgversion
        self.pgport = pgport
        self.pgdata = pgdata
        self.cleanups = list()

    def init(self) -> PGRunner:
        self.log.info("Initializing pg %s in %r", self.pgversion, self.pgdata)
        cmd = (
            self.pgbin / "pg_ctl",
            "init",
            "-D",
            self.pgdata,
            "-o",
            "--encoding utf-8",
        )
        subprocess.run(cmd, check=True)
        return self

    def make_conf(self, **kwargs) -> PGRunner:
        with open(self.pgdata / "postgresql.conf", "r+") as fp:
            lines = fp.read().splitlines()
            fp.seek(0)
            fp.truncate()
            config = {}
            for line in lines:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key, val = re.split(r"\s*=\s*", line, 1)
                config[key] = re.sub(r"\s*(#.*)?$", "", val)
            config.update(
                # disable fsync and synchronous_commit to speed up the tests a bit
                fsync="off",
                synchronous_commit="off",
                # synchronous_commit="local",
                # don't need to wait for autovacuum workers when shutting down
                autovacuum="off",
                # extensions whitelisting, https://github.com/dimitri/pgextwlist
                session_preload_libraries="'pgextwlist'",
            )
            config.update(kwargs)
            lines = ["{} = {}\n".format(key, val) for key, val in sorted(config.items())]
            fp.write("".join(lines))
        return self

    def make_hba_conf(self, *, dbname: str, user: str, auth: str) -> PGRunner:
        with open(self.pgdata / "pg_hba.conf", "r+") as fp:
            lines = fp.read().splitlines()
            fp.seek(0)
            fp.truncate()
            lines = ["{}\n".format(line.strip()) for line in lines if line and not line.startswith("#")]
            lines.insert(0, f"local\t{dbname}\t{user}\t{auth}\n")
            fp.write("".join(lines))
        return self

    def start(self, timeout: int = 10):
        self.log.info("Starting pg %s in %r", self.pgversion, self.pgdata)
        cmd: Tuple
        cmd = (
            self.pgbin / "pg_ctl",
            "start",
            "-D",
            self.pgdata,
            "-o",
            f"-k {self.pgdata} -p {self.pgport} -c listen_addresses=",
        )
        subprocess.run(cmd, check=True)
        # wait until ready to accept connections
        cmd = (
            self.pgbin / "pg_isready",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
        )
        timer = Timer(timeout=timeout, what=f"pg {self.pgversion} is ready")
        while timer.loop(log=self.log):
            p = subprocess.run(cmd, check=False)
            if p.returncode == 0:
                break

    def reload(self):
        """Re-read configuration files (postgresql.conf, pg_hba.conf, etc.)"""
        cmd = (
            self.pgbin / "pg_ctl",
            "reload",
            "-D",
            self.pgdata,
        )
        subprocess.run(cmd, check=True)

    def stop(self, timeout: int = 10):
        self.log.info("Stopping pg %s in %r", self.pgversion, self.pgdata)
        cmd = (
            self.pgbin / "pg_ctl",
            "stop",
            "-D",
            self.pgdata,
            "-m",
            "smart",
            "-t",
            str(timeout),
        )
        subprocess.run(cmd, check=True)

    def create_superuser(self, username: str = None):
        if username is None:
            username = self.superuser
        cmd = (
            self.pgbin / "createuser",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
            "--superuser",
            username,
        )
        subprocess.run(cmd, check=True)

    def create_user(self, *, username: str, createdb: bool = True, createrole: bool = True, replication: bool = True):
        cmd = (
            self.pgbin / "createuser",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
            "--createdb" if createdb else "--no-createdb",
            "--createrole" if createrole else "--no-createrole",
            "--replication" if replication else "--no-replication",
            username,
        )
        subprocess.run(cmd, check=True)

    def create_role(
        self,
        *,
        username: str,
        password: str = None,
        superuser: bool = False,
        createdb: bool = True,
        createrole: bool = True,
        inherit: bool = True,
        login: bool = True,
        replication: bool = False,
        bypassrls: bool = False,
        connlimit: int = -1,
        validuntil: datetime = None,
        **kwargs,
    ):
        if login:
            assert password, "Password must be set for roles with login"
        sql = "CREATE ROLE {} WITH {} {} {} {} {} {} {}".format(
            username,
            "SUPERUSER" if superuser else "NOSUPERUSER",
            "CREATEDB" if createdb else "NOCREATEDB",
            "CREATEROLE" if createrole else "NOCREATEROLE",
            "INHERIT" if inherit else "NOINHERIT",
            "LOGIN" if login else "NOLOGIN",
            "REPLICATION" if replication else "NOREPLICATION",
            "BYPASSRLS" if bypassrls else "NOBYPASSRLS",
        )
        params: List[Any] = []
        if connlimit != -1:
            sql += " CONNECTION LIMIT %s"
            params.append(connlimit)
        if password:
            sql += " PASSWORD %s"
            params.append(password)
        if validuntil:
            sql += f" VALID UNTIL '{validuntil}'"
        config: Dict[str, Any] = {}
        config.update(kwargs)
        with self.cursor(username=self.superuser) as cur:
            cur.execute(sql, params)
            for key, value in config.items():
                cur.execute(f"ALTER ROLE {username} SET {key} = %s", (value, ))
        self.log.info("Created role %r in %r", username, self.pgdata)

    def create_db(self, *, dbname: str, owner: str = None):
        if owner is None:
            owner = self.testuser
        cmd = (
            self.pgbin / "createdb",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
            "--owner",
            owner,
            dbname,
        )
        subprocess.run(cmd, check=True)

    def conn_info(self, *, username: str = None, dbname: str = None) -> Dict[str, Any]:
        if username is None:
            username = self.testuser
        if dbname is None:
            dbname = self.defaultdb
        return {
            "dbname": dbname,
            "host": self.pgdata,
            "port": self.pgport,
            "user": username,
        }

    def super_conn_info(self, *, dbname: str = None) -> Dict[str, Any]:
        return self.conn_info(username=self.superuser, dbname=dbname)

    @contextmanager
    def cursor(self, *, username: str = None, dbname: str = None, autocommit: bool = True) -> RealDictCursor:
        conn = None
        try:
            conn = psycopg2.connect(**self.conn_info(username=username, dbname=dbname))
            conn.autocommit = autocommit
            yield conn.cursor(cursor_factory=RealDictCursor)
        finally:
            if conn is not None:
                conn.close()

    def drop_db(self, *, dbname: str):
        self.log.info("Dropping database %r from %r", dbname, self.pgdata)
        cmd = (
            self.pgbin / "dropdb",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
            dbname,
        )
        subprocess.run(cmd, check=True)

    def drop_dbs(self):
        with self.cursor(username=self.superuser) as cur:
            cur.execute(
                "SELECT datname from pg_catalog.pg_database WHERE NOT datistemplate AND datname <> %s", (self.defaultdb, )
            )
            dbs = cur.fetchall()
        for db in dbs:
            self.drop_db(dbname=db["datname"])

    def drop_user(self, *, username):
        self.log.info("Dropping user %r from %r", username, self.pgdata)
        cmd = (
            self.pgbin / "dropuser",
            "-h",
            self.pgdata,
            "-p",
            str(self.pgport),
            username,
        )
        subprocess.run(cmd, check=True)

    def create_extension(self, *, extname: str, extversion: str = None, dbname: str, grantee: str = None):
        if grantee is None:
            grantee = self.testuser
        sql = f"CREATE EXTENSION IF NOT EXISTS {extname}"
        if extversion:
            sql += f" WITH VERSION {extversion}"
        if LooseVersion(self.pgversion) > "9.5":
            sql += " CASCADE"
        try:
            with self.cursor(username=self.superuser, dbname=dbname) as cur:
                cur.execute(sql)
                # wait until extension is installed
                timer = Timer(timeout=10, what=f"{extname} is installed")
                while timer.loop(log=self.log):
                    cur.execute("SELECT * FROM pg_catalog.pg_extension WHERE extname = %s", (extname, ))
                    ext = cur.fetchone()
                    if ext:
                        self.log.info(
                            "Installed extension %r to db %r in %r, version: %s", ext["extname"], dbname, self.pgdata,
                            ext["extversion"]
                        )
                        break
        except psycopg2.OperationalError as err:
            assert "No such file or directory" in str(err)
            # extension not available
            return False
        else:
            return True

    def drop_extension(self, *, extname: str, dbname: str):
        sql = f"DROP EXTENSION IF EXISTS {extname}"
        if LooseVersion(self.pgversion) > "9.5":
            sql += " CASCADE"
        with self.cursor(username=self.superuser, dbname=dbname) as cur:
            cur.execute(sql)

    def have_aiven_extras(self, *, dbname: str, grantee: str = None):
        if grantee is None:
            grantee = self.testuser
        extname = "aiven_extras"
        if self.create_extension(extname=extname, dbname=dbname, grantee=grantee):
            # grant schema usage and select on view
            with self.cursor(username=self.superuser, dbname=dbname) as cur:
                cur.execute(f"GRANT USAGE ON SCHEMA {extname} TO {grantee}")
                cur.execute(f"GRANT SELECT ON aiven_extras.pg_stat_replication TO {grantee}")
            return True
        return False

    def drop_aiven_extras(self, *, dbname: str):
        extname = "aiven_extras"
        self.drop_extension(extname=extname, dbname=dbname)

    def list_pubs(self, *, dbname: str):
        """Get all publications created in the database"""
        with self.cursor(dbname=dbname) as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_publication")
            return cur.fetchall()

    def list_slots(self):
        """Get all replication slots that currently exist on the database cluster"""
        with self.cursor() as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_replication_slots")
            return cur.fetchall()

    def list_subs(self):
        """Get all existing logical replication subscriptions across all databases of a cluster"""
        # requires superuser
        with self.cursor(username=self.superuser) as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_subscription")
            return cur.fetchall()

    def list_roles(self):
        with self.cursor() as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_roles")
            return cur.fetchall()

    def add_cleanup(self, cleanup: Callable):
        self.cleanups.append(cleanup)


@contextmanager
def setup_pg(tmp: Path, pgversion: str):
    pgdata = tmp / "pgdata"
    pgdata.mkdir()
    pg = PGRunner(pgversion=pgversion, pgdata=pgdata)
    pg.init().make_conf(wal_level="logical").start()

    # create test users
    pg.create_superuser()
    pg.create_user(username=pg.testuser)

    try:
        yield pg
    finally:
        pg.stop()


# Dynamically generating fixtures taken from https://github.com/pytest-dev/pytest/issues/2424


def generate_pg_fixture(*, name: str, pgversion: str, scope="module"):
    @pytest.fixture(scope=scope)
    def pg_fixture(tmp_path_factory):
        with setup_pg(tmp_path_factory.mktemp(name), pgversion) as pg:
            yield pg

    return pg_fixture


def inject_pg_fixture(*, name: str, pgversion: str, scope="module"):
    globals()[name] = generate_pg_fixture(name=name, pgversion=pgversion, scope=scope)


SUPPORTED_PG_VERSIONS = ["9.5", "9.6", "10", "11", "12"]
pg_cluster_for_tests: List[str] = list()
pg_source_and_target_for_tests: List[Tuple[str, str]] = list()
pg_source_and_target_for_replication_tests: List[Tuple[str, str]] = list()


def generate_fixtures():
    pg_source_versions: List[str] = list()
    pg_target_versions: List[str] = list()

    version = os.getenv("PG_SOURCE_VERSION", os.getenv("PG_VERSION"))
    if version:
        assert version in SUPPORTED_PG_VERSIONS, f"Supported pg versions are: {SUPPORTED_PG_VERSIONS}"
        pg_source_versions.append(version)
    else:
        pg_source_versions = SUPPORTED_PG_VERSIONS

    version = os.getenv("PG_TARGET_VERSION")
    if version:
        assert version in SUPPORTED_PG_VERSIONS, f"Supported pg versions are: {SUPPORTED_PG_VERSIONS}"
        pg_target_versions.append(version)
    else:
        pg_target_versions = SUPPORTED_PG_VERSIONS

    for source in pg_source_versions:
        name_prefix = "pg{}".format(source.replace(".", ""))
        source_name = f"{name_prefix}_source"
        inject_pg_fixture(name=source_name, pgversion=source)
        for target in pg_target_versions:
            if LooseVersion(source) > LooseVersion(target):
                continue
            name_prefix = "pg{}".format(target.replace(".", ""))
            target_name = f"{name_prefix}_target"
            inject_pg_fixture(name=target_name, pgversion=target)
            pg_source_and_target_for_tests.append((source_name, target_name))
            if LooseVersion(source) >= "10":
                pg_source_and_target_for_replication_tests.append((source_name, target_name))
    for version in set(pg_source_versions).union(pg_target_versions):
        fixture_name = "pg{}".format(version.replace(".", ""))
        inject_pg_fixture(name=fixture_name, pgversion=version)
        pg_cluster_for_tests.append(fixture_name)


generate_fixtures()


def test_pg_source_and_target_for_tests():
    print(pg_source_and_target_for_tests)


def test_pg_source_and_target_for_replication_tests():
    print(pg_source_and_target_for_replication_tests)


@pytest.fixture(name="pg_cluster", params=pg_cluster_for_tests, scope="function")
def fixture_pg_cluster(request):
    """Returns a fixture parametrized on the union of all source and target pg versions."""
    cluster_runner = request.getfixturevalue(request.param)
    yield cluster_runner
    for cleanup in cluster_runner.cleanups:
        cleanup()
    cluster_runner.cleanups.clear()
    cluster_runner.drop_dbs()


@pytest.fixture(name="pg_source_and_target", params=pg_source_and_target_for_tests, scope="function")
def fixture_pg_source_and_target(request):
    source, target = request.param
    # run the fixture function
    source = request.getfixturevalue(source)
    target = request.getfixturevalue(target)
    if request.cls:
        request.cls.source = source
        request.cls.target = target
        yield
    else:
        yield source, target
    # cleanup functions
    for cleanup in source.cleanups + target.cleanups:
        cleanup()
    source.cleanups.clear()
    target.cleanups.clear()
    # cleanup created db's
    source.drop_dbs()
    target.drop_dbs()


@pytest.fixture(name="pg_source_and_target_replication", params=pg_source_and_target_for_replication_tests, scope="function")
def fixture_pg_source_and_target_replication(request):
    source, target = request.param
    # run the fixture function
    source = request.getfixturevalue(source)
    target = request.getfixturevalue(target)
    if request.cls:
        request.cls.source = source
        request.cls.target = target
        yield
    else:
        yield source, target
    # cleanup functions
    for cleanup in source.cleanups + target.cleanups:
        cleanup()
    source.cleanups.clear()
    target.cleanups.clear()
    # cleanup created db's
    source.drop_dbs()
    target.drop_dbs()
