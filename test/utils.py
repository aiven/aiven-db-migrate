# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from __future__ import annotations

from aiven_db_migrate.migrate.pgmigrate import PGTarget
from aiven_db_migrate.migrate.pgutils import find_pgbin_dir
from contextlib import contextmanager
from datetime import datetime
from packaging.version import Version
from pathlib import Path
from psycopg2._psycopg import connection
from psycopg2.extras import RealDictCursor
from typing import Any, Callable, Dict, Iterator, List, Tuple

import datetime
import logging
import psycopg2
import random
import re
import string
import subprocess
import threading
import time

SUPPORTED_PG_VERSIONS = ["13", "14", "15", "16", "17"]


def random_string(length=20):
    return "".join(random.choices(string.ascii_lowercase, k=length))


class Timeout(Exception):
    """Timeout"""


class TimerBase:
    def __init__(self):
        self._start = self.now()

    @staticmethod
    def now():
        return time.monotonic()

    def start_time(self):
        return self._start

    def reset(self):
        self._start = self.now()

    def elapsed(self):
        """Return seconds since starting timer"""
        return self.now() - self._start

    def elapsed_absolute(self):
        """Return timestamp for starting timer"""
        return datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=self.elapsed())


class Timer(TimerBase):
    """Multipurpose timer"""
    def __init__(self, *, timeout=None, sleep=1.0, what=None):
        super().__init__()
        self._what = what or "operation to complete"
        self._timeout = timeout
        self._sleep = sleep
        self._next_sleep_value = self._calculate_next_sleep_value()
        self._iters = 0
        self._last_sleep_start = self._start
        self._event = threading.Event()

    def get_timeout_value(self):
        return self._timeout

    def reset(self):
        super().reset()
        self._iters = 0

    def loop(self, *, raise_timeout=True, log=None):
        """Helper function to implement waiting loops like:
        timer = Timer(sleep=5, timeout=60)
        while timer.loop():
            if x:
                break
        which sleeps on every iteration after the first and raises an error on timeout
        """
        self._iters += 1
        if self._iters == 1:
            return True

        if self.timeout(raise_timeout=raise_timeout):
            return False  # timed out

        # Wait a bit and keep going
        if log:
            log.info("Waiting for %s, %.2fs elapsed", self._what, self.elapsed())
        self.sleep()
        return True

    def timeout(self, raise_timeout=False):
        """Return True if we are past the timeout moment"""
        if self._timeout is None:
            return False  # never timeout

        timeout_occurred = self.elapsed() >= self._timeout
        if raise_timeout and timeout_occurred:
            msg = "Timeout waiting for {} ({:.2f} seconds)".format(self._what, self._timeout)
            if isinstance(raise_timeout, Exception):
                raise Timeout(msg) from raise_timeout
            raise Timeout(msg)

        return timeout_occurred

    def time_to_timeout(self):
        """Return time until timer will timeout.

        <0 when timeout is already passed.
        Use .timeout() instead if you want to check whether timer has expired.
        """
        if self._timeout is None:
            # This is timer counting upwards, calling this method does not make much sense
            return None
        return self._timeout - self.elapsed()

    def next_sleep_length(self):
        """Return length of the next sleep in seconds"""
        sleep_time = self._next_sleep_value - min(self.now() - self._last_sleep_start, 0)
        if self._timeout is not None:
            # never sleep past timeout deadline
            sleep_time = min(sleep_time, (self.start_time() + self._timeout) - self.now())

        return max(sleep_time, 0.0)

    def interrupt(self):
        """Make a possible sleep() return immediately"""
        self._event.set()

    def is_interrupted(self):
        """Returns True if the timer has been interrupted and next call to sleep() will return immediately"""
        return self._event.is_set()

    def set_expired(self):
        """Set timer to timed out"""
        if self._timeout is not None:
            self._start = self.now() - self._timeout

    def sleep(self):
        """
        Sleep until next attempt should be performed or we are interrupted

        Attempt to synchronize exiting this method every 'sleep' interval,
        i.e. time spent outside this method is taken into account.
        """

        sleep_time = self.next_sleep_length()
        self._next_sleep_value = self._calculate_next_sleep_value()
        if sleep_time > 0.0:
            # only sleep if not long enough time was spent between iterations outside this function
            self._last_sleep_start = self.now()
            if self._event.wait(timeout=self.next_sleep_length()):
                self._event.clear()

    def _calculate_next_sleep_value(self):
        if not isinstance(self._sleep, tuple):
            return self._sleep
        return random.randrange(*self._sleep)


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
        subprocess.run(cmd, check=True, env={"TZ": "UTC"})
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
        with self.connection(username=username, dbname=dbname) as conn:
            conn.autocommit = autocommit
            yield conn.cursor(cursor_factory=RealDictCursor)

    @contextmanager
    def connection(
        self,
        *,
        username: str | None = None,
        dbname: str | None = None,
        connection_factory: type[connection] | None = None
    ) -> psycopg2.extensions.connection:
        conn = None
        try:
            conn = psycopg2.connect(
                **self.conn_info(username=username, dbname=dbname), connection_factory=connection_factory
            )
            yield conn
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

    def get_all_db_names(self) -> list[str]:
        with self.cursor(username=self.superuser) as cur:
            cur.execute(
                "SELECT datname from pg_catalog.pg_database WHERE NOT datistemplate AND datname <> %s", (self.defaultdb, )
            )
            dbs = cur.fetchall()

        return [db["datname"] for db in dbs]

    def drop_dbs(self):
        for db_name in self.get_all_db_names():
            self.drop_db(dbname=db_name)

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
            sql += f" WITH VERSION '{extversion}'"
        if Version(self.pgversion) > Version("9.5"):
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
        if Version(self.pgversion) > Version("9.5"):
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
def modify_pg_security_agent_reserved_roles(target: PGRunner) -> Iterator[str]:
    """Modify the list of reserved roles of the target database.

    Returns:
        The name of the superuser role that was added to the list of reserved roles.
    """
    authorized_roles = PGTarget(conn_info=target.super_conn_info()).get_security_agent_reserved_roles()
    superuser = random_string()
    authorized_roles_str = ",".join(authorized_roles)
    modified_authorized_roles_str = ",".join(authorized_roles + [superuser])

    target.make_conf(**{"aiven.pg_security_agent_reserved_roles": f"'{modified_authorized_roles_str}'"}).stop()
    target.start()

    try:
        yield superuser
    finally:
        target.make_conf(**{"aiven.pg_security_agent_reserved_roles": f"'{authorized_roles_str}'"}).stop()
        target.start()
