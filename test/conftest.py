# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from _pytest.tmpdir import TempPathFactory
from aiven_db_migrate.migrate.pgmigrate import PGTarget
from contextlib import contextmanager
from copy import copy
from distutils.version import LooseVersion
from functools import partial, wraps
from pathlib import Path
from psycopg2.extras import LogicalReplicationConnection, ReplicationCursor
from test.utils import PGRunner, SUPPORTED_PG_VERSIONS
from typing import Callable, cast, Iterator, List, Tuple, TypeVar
from unittest.mock import patch

import logging
import os
import psycopg2
import pytest

R = TypeVar("R")

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")


@pytest.fixture(scope="module", name="pg_system_roles")
def fixture_pg_system_roles() -> list[str]:
    return ["companyuser", "postgres", "some_superuser"]


@contextmanager
def setup_pg(tmp: Path, pgversion: str, *, with_gatekeeper: bool = False, system_roles: list[str]) -> Iterator[PGRunner]:
    pgdata = tmp / "pgdata"
    pgdata.mkdir()
    pg = PGRunner(pgversion=pgversion, pgdata=pgdata)

    extra_conf = {}
    if with_gatekeeper:
        system_roles_str = ",".join(system_roles)  # users that can be assigned superuser
        extra_conf["shared_preload_libraries"] = "aiven_gatekeeper"
        extra_conf["aiven.pg_security_agent_reserved_roles"] = f"'{system_roles_str}'"

    pg.init().make_conf(wal_level="logical", **extra_conf).start()

    # create test users
    pg.create_superuser()
    pg.create_user(username=pg.testuser)

    try:
        yield pg
    finally:
        pg.stop()


# Dynamically generating fixtures taken from https://github.com/pytest-dev/pytest/issues/2424


def generate_pg_fixture(*, name: str, pgversion: str, scope="module", with_gatekeeper: bool = False):
    @pytest.fixture(scope=scope)
    def pg_fixture(tmp_path_factory: TempPathFactory, pg_system_roles: list[str]) -> Iterator[PGRunner]:
        with setup_pg(
            tmp_path_factory.mktemp(name), pgversion, with_gatekeeper=with_gatekeeper, system_roles=pg_system_roles
        ) as pg:
            yield pg

    return pg_fixture


def inject_pg_fixture(*, name: str, pgversion: str, scope="module", with_gatekeeper: bool = False):
    globals()[name] = generate_pg_fixture(name=name, pgversion=pgversion, scope=scope, with_gatekeeper=with_gatekeeper)


pg_cluster_for_tests: List[str] = []
pg_source_and_target_for_tests: List[Tuple[str, str]] = []
pg_unsafe_source_and_target_for_tests: List[Tuple[str, str]] = []


def generate_fixtures():
    pg_source_versions: List[str] = []
    pg_target_versions: List[str] = []

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
        pg_target_versions = copy(SUPPORTED_PG_VERSIONS)

    for source in pg_source_versions:
        name_prefix = "pg{}".format(source.replace(".", ""))
        source_name = f"{name_prefix}_source"
        inject_pg_fixture(name=source_name, pgversion=source, with_gatekeeper=False)
        for target in pg_target_versions:
            if LooseVersion(source) > LooseVersion(target):
                continue
            name_prefix = "pg{}".format(target.replace(".", ""))
            target_name = f"{name_prefix}_target"
            unsafe_target_name = f"{target_name}_unsafe"
            inject_pg_fixture(name=target_name, pgversion=target, with_gatekeeper=True)
            pg_source_and_target_for_tests.append((source_name, target_name))
            inject_pg_fixture(name=unsafe_target_name, pgversion=target, with_gatekeeper=False)
            pg_unsafe_source_and_target_for_tests.append((source_name, unsafe_target_name))
    for version in set(pg_source_versions).union(pg_target_versions):
        fixture_name = "pg{}".format(version.replace(".", ""))
        inject_pg_fixture(name=fixture_name, pgversion=version, with_gatekeeper=True)
        pg_cluster_for_tests.append(fixture_name)


generate_fixtures()


def test_pg_source_and_target_for_tests():
    print(pg_source_and_target_for_tests)


@pytest.fixture(name="pg_cluster", params=pg_cluster_for_tests, scope="function")
def fixture_pg_cluster(request):
    """Returns a fixture parametrized on the union of all source and target pg versions."""
    cluster_runner = request.getfixturevalue(request.param)
    yield cluster_runner
    for cleanup in cluster_runner.cleanups:
        cleanup()
    cluster_runner.cleanups.clear()
    cluster_runner.drop_dbs()


def clean_replication_slots_for_runner(pg_runner: PGRunner) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """Parametrized decorator to clean replication slots for a given PGRunner instance."""
    def clean_replication_slots(function: Callable[..., R]) -> Callable[..., R]:
        """Decorator that schedules a drop of all replication slots created by the decorated function."""
        def _drop_replication_slot(pg_runner_: PGRunner, slot_name: str) -> None:
            """Drop a replication slot, will try to find it in all databases."""
            for db in pg_runner_.get_all_db_names():
                try:
                    with pg_runner_.connection(
                        username=pg_runner_.superuser, dbname=db, connection_factory=LogicalReplicationConnection
                    ) as log_conn:
                        log_cursor: ReplicationCursor
                        with log_conn.cursor() as log_cursor:
                            log_cursor.drop_replication_slot(slot_name)
                            logging.info("Dropped replication slot %s on %s", slot_name, db)
                except psycopg2.errors.UndefinedObject:
                    pass
                else:
                    break  # Found it, no need to try other databases.

        @wraps(function)
        def wrapper(self: PGTarget, *args, slotname: str, **kwargs) -> R:
            subname = function(self, *args, slotname=slotname, **kwargs)

            pg_runner.cleanups.append(partial(_drop_replication_slot, pg_runner_=pg_runner, slot_name=slotname))

            return subname

        return wrapper

    return clean_replication_slots


@contextmanager
def make_pg_source_and_target(request: FixtureRequest) -> Iterator[Tuple[PGRunner, PGRunner]]:
    """Returns a fixture parametrized on the union of all source and target pg versions.

    This is expected to be used in a fixture that is parametrized with a list of tuples of
    source and target fixture names.

    If the fixture is used in a class, the attributes ``source`` and ``target`` are also set
    on the class.
    """
    source_fixture_name, target_fixture_name = request.param
    # run the fixture function
    source: PGRunner = request.getfixturevalue(source_fixture_name)
    target: PGRunner = request.getfixturevalue(target_fixture_name)

    # Patch PGTarget.create_subscription to add the cleanup of created logical slots to the cleanup list.
    # We do this because in some rare cases, the teardown already drops the table while the replication
    # slot is still active, which causes the drop to fail. In the cleanup, it is executed after all
    # connections are closed, so it should always succeed.
    patched_create_subscription = clean_replication_slots_for_runner(target)(PGTarget.create_subscription)
    with patch("aiven_db_migrate.migrate.pgmigrate.PGTarget.create_subscription", patched_create_subscription):
        if request.cls:
            request.cls.source = source
            request.cls.target = target

        try:
            yield source, target
        finally:
            # cleanup functions
            for cleanup in source.cleanups + target.cleanups:
                cleanup()
            source.cleanups.clear()
            target.cleanups.clear()
            # cleanup created db's
            source.drop_dbs()
            target.drop_dbs()


@pytest.fixture(
    name="pg_source_and_target",
    params=pg_source_and_target_for_tests,
    scope="function",
    ids=["{}-{}".format(*entry) for entry in pg_source_and_target_for_tests]
)
def fixture_pg_source_and_target(request):
    """Generate a source and target ``PGRunner``s for all the requested versions.

    Note:
        The source databases are vanilla PG, whereas the target databases are hardened,
        using ``shared_preload_libraries = aiven_gatekeeper``.
    """
    with make_pg_source_and_target(request) as (source, target):
        yield source, target


@pytest.fixture(
    name="pg_source_and_target_unsafe",
    params=pg_unsafe_source_and_target_for_tests,
    scope="function",
    ids=["{}-{}".format(*entry) for entry in pg_unsafe_source_and_target_for_tests]
)
def fixture_pg_source_and_target_unsafe(request):
    """Generate a source and an unsafe target ``PGRunner``s for all the requested versions.

    Note:
        Both the source and target databases are vanilla PG (no ``shared_preload_libraries``).
    """
    with make_pg_source_and_target(request) as (source, target):
        yield source, target
