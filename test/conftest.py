# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from __future__ import annotations

from _pytest.fixtures import FixtureRequest
from _pytest.tmpdir import TempPathFactory
from contextlib import contextmanager
from copy import copy
from distutils.version import LooseVersion
from pathlib import Path
from test.utils import PGRunner, SUPPORTED_PG_VERSIONS
from typing import Iterator, List, Tuple

import logging
import os
import pytest

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


@contextmanager
def make_pg_source_and_target(request: FixtureRequest) -> Iterator[Tuple[PGRunner, PGRunner]]:
    """Returns a fixture parametrized on the union of all source and target pg versions.

    This is expected to be used in a fixture that is parametrized with a list of tuples of
    source and target fixture names.

    If the fixture is used in a class, the attributes ``source`` and ``target`` are also set
    on the class.
    """
    source, target = request.param
    # run the fixture function
    source = request.getfixturevalue(source)
    target = request.getfixturevalue(target)
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
