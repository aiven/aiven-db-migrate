# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.pgmigrate import PGMigrate, PGMigrateResult
from datetime import datetime
from test.conftest import PGRunner
from test.utils import random_string
from typing import Tuple

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


def test_pg_roles_superusers(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
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


def test_pg_roles_as_superuser(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
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
