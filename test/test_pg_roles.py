# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.migrate.pgmigrate import PGMigrate, PGMigrateResult, PGTarget
from datetime import datetime
from test.utils import modify_pg_security_agent_reserved_roles, PGRunner, random_string
from typing import Tuple

import pytest
import time


def test_pg_roles_with_no_password(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    new_roles = {random_string() for _ in range(10)}

    def _cleanup():
        for username in new_roles:
            source.drop_user(username=username)

    source.add_cleanup(_cleanup)
    for username in new_roles:
        source.create_role(username=username, login=False)

    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = new_roles | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] in new_roles
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert not role["rolpassword"]


def test_pg_roles_with_placeholder_password(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    new_roles = {random_string() for _ in range(10)}

    def _cleanup():
        for username in new_roles:
            source.drop_user(username=username)

    source.add_cleanup(_cleanup)
    for username in new_roles:
        source.create_role(username=username, password=random_string())

    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = new_roles | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] in new_roles
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert role["rolpassword"].startswith("placeholder_")


def test_pg_roles_rolconfig(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    username = random_string()
    source.add_cleanup(lambda: source.drop_user(username=username))
    source.create_role(username=username, password=random_string(), statement_timeout=12345, search_path="foobar")
    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = {username} | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] == username
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert role["rolpassword"].startswith("placeholder_")


def test_pg_roles_superusers(pg_source_and_target_unsafe: Tuple[PGRunner, PGRunner]):
    """Test that superusers are not created on target **without** a superuser connection.

    Note:
        This tests the behaviour of an unsafe target, which would not have ``shared_preload_libraries = aiven_gatekeeper``.
    """
    source, target = pg_source_and_target_unsafe
    username = random_string()
    source.add_cleanup(lambda: source.drop_user(username=username))
    source.create_role(username=username, password=random_string(), superuser=True)
    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = {username} | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] == username
            assert role["status"] == "failed"
            assert role["message"] == "must be superuser to create superusers"
            assert not role["rolpassword"]

    roles = set(r["rolname"] for r in target.list_roles())
    assert username not in roles


def test_pg_roles_replication_users(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    username = random_string()
    source.add_cleanup(lambda: source.drop_user(username=username))
    source.create_role(username=username, password=random_string(), replication=True)
    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = {username} | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] == username
            assert role["status"] == "failed"
            assert role["message"] == "must be superuser to create replication users"
            assert not role["rolpassword"]

    roles = set(r["rolname"] for r in target.list_roles())
    assert username not in roles


def test_pg_roles_as_superuser(pg_source_and_target_unsafe: Tuple[PGRunner, PGRunner]):
    """Test that superusers are successfully created on target **with** a superuser connection.

    Note:
        This tests the behaviour of an unsafe target, which would not have ``shared_preload_libraries = aiven_gatekeeper``.
    """
    source, target = pg_source_and_target_unsafe
    superuser = random_string()
    repuser = random_string()
    source.add_cleanup(lambda: source.drop_user(username=superuser))
    source.add_cleanup(lambda: source.drop_user(username=repuser))
    source.create_role(username=superuser, password=random_string(), superuser=True)
    source.create_role(username=repuser, password=random_string(), replication=True)
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.super_conn_info(), createdb=True, verbose=True
    )
    existing_roles = set(pg_mig.target.pg_roles.keys())
    new_roles = {superuser, repuser}
    all_roles = new_roles | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] in new_roles
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert role["rolpassword"].startswith("placeholder_")


def test_pg_roles_valid_until(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    username = random_string()
    validuntil: datetime = datetime.fromtimestamp(time.time() + 60)
    source.add_cleanup(lambda: source.drop_user(username=username))
    source.create_role(username=username, password=random_string(), validuntil=validuntil)
    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = {username} | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] == username
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert role["rolpassword"].startswith("placeholder_")

    role = next(r for r in target.list_roles() if r["rolname"] == username)
    assert role["rolvaliduntil"].date() == validuntil.date()
    assert role["rolvaliduntil"].time() == validuntil.time()


def test_pg_roles_connection_limit(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    username = random_string()
    connlimit = 42
    source.add_cleanup(lambda: source.drop_user(username=username))
    source.create_role(username=username, password=random_string(), connlimit=connlimit)
    pg_mig = PGMigrate(source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=True, verbose=True)
    existing_roles = set(pg_mig.target.pg_roles.keys())
    all_roles = {username} | existing_roles
    result: PGMigrateResult = pg_mig.migrate()

    for role in result.pg_roles.values():
        assert role["rolname"] in all_roles
        if role["rolname"] in existing_roles:
            assert role["status"] == "exists"
            assert role["message"] == "role already exists"
        else:
            assert role["rolname"] == username
            assert role["status"] == "created"
            assert role["message"] == "role created"
            assert role["rolpassword"].startswith("placeholder_")

    role = next(r for r in target.list_roles() if r["rolname"] == username)
    assert role["rolconnlimit"] == connlimit


def test_migration_fails_with_additional_superuser_roles(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    """Test that it fails when we try to migrate superuser roles that are not in the reserved roles list of the target."""
    source, target = pg_source_and_target
    superuser1 = random_string()
    superuser2 = random_string()
    source.add_cleanup(lambda: source.drop_user(username=superuser1))
    source.add_cleanup(lambda: source.drop_user(username=superuser2))
    source.create_role(username=superuser1, password=random_string(), superuser=True)
    source.create_role(username=superuser2, password=random_string(), superuser=True)
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.super_conn_info(), createdb=True, verbose=True
    )

    assert pg_mig.target.is_pg_security_agent_enabled

    with pytest.raises(
        PGMigrateValidationFailedError,
        match=r"Some superuser roles from source database .* are not allowed in target database.*",
    ):
        pg_mig.migrate()


def test_migration_succeeds_when_additional_superuser_roles_are_excluded(
    pg_source_and_target: Tuple[PGRunner, PGRunner],
) -> None:
    """Test that migration succeeds when non allowed superuser roles are excluded."""
    source, target = pg_source_and_target
    regularuser = random_string()
    superuser1 = random_string()
    superuser2 = random_string()
    source.add_cleanup(lambda: source.drop_user(username=regularuser))
    source.add_cleanup(lambda: source.drop_user(username=superuser1))
    source.add_cleanup(lambda: source.drop_user(username=superuser2))
    source.create_role(username=regularuser, password=random_string())
    source.create_role(username=superuser1, password=random_string(), superuser=True)
    source.create_role(username=superuser2, password=random_string(), superuser=True)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.super_conn_info(),
        createdb=True,
        verbose=True,
        excluded_roles=f"{superuser1},{superuser2}",
    )
    assert pg_mig.target.is_pg_security_agent_enabled
    result = pg_mig.migrate()

    assert result.pg_roles.keys() == {regularuser, source.testuser}


def test_migration_succeeds_with_authorized_superuser_role(pg_source_and_target: Tuple[PGRunner, PGRunner]) -> None:
    """Test that it succeeds when we try to migrate a superuser role that is in the reserved roles list of the target."""
    source, target = pg_source_and_target

    with modify_pg_security_agent_reserved_roles(target) as superuser:
        source.add_cleanup(lambda: source.drop_user(username=superuser))
        source.create_role(username=superuser, password=random_string(), superuser=True)

        pg_mig = PGMigrate(
            source_conn_info=source.conn_info(), target_conn_info=target.super_conn_info(), createdb=True, verbose=True
        )

        reserved_roles = pg_mig.target.get_security_agent_reserved_roles()

        assert pg_mig.target.is_pg_security_agent_enabled
        assert superuser in pg_mig.target.get_security_agent_reserved_roles(), str(reserved_roles)

        result: PGMigrateResult = pg_mig.migrate()

        assert superuser in result.pg_roles
        assert result.pg_roles[superuser]["status"] == "created"
        assert result.pg_roles[superuser]["message"] == "role created"

        # Get the specificities of this role
        perms = pg_mig.target.c(f"SELECT * FROM pg_roles WHERE rolname = %s", args=(superuser, ), return_rows=1)[0]
        assert perms["rolsuper"] is True


def test_user_cannot_see_reserved_roles(pg_source_and_target: Tuple[PGRunner, PGRunner]) -> None:
    """Test that a user cannot see the reserved roles."""
    source, target = pg_source_and_target

    authorized_roles = PGTarget(conn_info=target.conn_info()).get_security_agent_reserved_roles()

    assert not authorized_roles


def test_superuser_can_see_reserved_roles(
    pg_source_and_target: Tuple[PGRunner, PGRunner], pg_system_roles: list[str]
) -> None:
    """Test that a superuser can see the reserved roles."""
    source, target = pg_source_and_target

    authorized_roles = PGTarget(conn_info=target.super_conn_info()).get_security_agent_reserved_roles()

    assert set(authorized_roles) == set(pg_system_roles)
