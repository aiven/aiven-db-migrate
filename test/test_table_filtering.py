from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from test.conftest import PGRunner
from test.utils import random_string, Timer
from typing import Dict, Set, Tuple

import psycopg2
import pytest

#  pylint: disable=invalid-string-quote


@pytest.mark.parametrize(["skip", "with_extension"], [
    [False, False],
    [True, True],
    [True, False],
    [False, True],
])
def test_extension_table_filtering(
    pg_source_and_target_replication: Tuple[PGRunner, PGRunner], skip: bool, with_extension: bool
):
    source, target = pg_source_and_target_replication
    db_name = random_string(6)
    other_db_name = random_string(6)
    source.create_db(dbname=db_name)
    source.create_db(dbname=other_db_name)
    tables = [f'"ta. \'ble{i}"' for i in range(4)]
    to_filter = tables[:2]
    pg_mig = PGMigrate(
        source_conn_info=source.super_conn_info(),
        target_conn_info=target.super_conn_info(),
        verbose=True,
        with_tables=None if skip else to_filter,
        skip_tables=to_filter if skip else None,
        replicate_extensions=with_extension,
    )
    for t in tables:
        pg_mig.source.c(f"CREATE TABLE {t} (foo int)", return_rows=0, dbname=db_name)
    pg_mig.source.c(f"CREATE EXTENSION postgis CASCADE", return_rows=0, dbname=db_name)
    # pylint: disable=protected-access
    pg_mig.source._set_db(dbname=db_name)
    # pylint: enable=protected-access
    db = pg_mig.source.databases[db_name]
    # sanity
    assert db.tables is not None
    filtered_names = pg_mig.filter_tables(db)
    for name in to_filter:
        if skip:
            assert f"public.{name}" not in filtered_names, filtered_names
        else:
            assert f"public.{name}" in filtered_names, filtered_names
    if with_extension and skip:
        assert "public.spatial_ref_sys" in filtered_names, filtered_names
    else:
        assert "public.spatial_ref_sys" not in filtered_names, filtered_names

    pg_mig = PGMigrate(
        source_conn_info=source.super_conn_info(),
        target_conn_info=target.super_conn_info(),
        verbose=True,
        with_tables=None,
        skip_tables=None,
        replicate_extensions=with_extension,
    )
    for t in tables:
        pg_mig.source.c(f"CREATE TABLE {t} (foo int)", return_rows=0, dbname=other_db_name)
    pg_mig.source.c(f"CREATE EXTENSION postgis CASCADE", return_rows=0, dbname=other_db_name)
    # pylint: disable=protected-access
    pg_mig.source._set_db(dbname=other_db_name)
    # pylint: enable=protected-access
    db = pg_mig.source.databases[other_db_name]
    filtered_names = pg_mig.filter_tables(db)
    if with_extension:
        assert not filtered_names, filtered_names
    else:
        assert set(filtered_names) == {f"public.{t}" for t in tables}, filtered_names


@pytest.mark.parametrize(["skip", "with_db", "with_schema"], [
    [True, False, False],
    [False, False, False],
    [True, False, True],
    [False, False, True],
    [True, True, True],
    [False, True, True],
])
def test_table_filtering(
    pg_source_and_target_replication: Tuple[PGRunner, PGRunner], skip: bool, with_db: bool, with_schema: bool
):
    source, target = pg_source_and_target_replication
    db_name = random_string(6)
    other_db_name = random_string(6)
    schema_name = "schema" if with_schema else "public"
    source.create_db(dbname=db_name)
    if with_db:
        source.create_db(dbname=other_db_name)
    tables_names = [f'"ta .\'ble{i}"' for i in range(4)]
    tables = [f"{schema_name}.{t}" for t in tables_names]
    if with_schema and with_db:
        tables = [f"{db_name}.{schema_name}.{t}" for t in tables_names]
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        verbose=True,
        with_tables=None if skip else tables,
        skip_tables=tables if skip else None,
        replicate_extensions=True,
    )
    if with_schema:
        pg_mig.source.c(f"CREATE SCHEMA {schema_name}", return_rows=0, dbname=db_name)
    if with_db:
        pg_mig.source.c(f"CREATE SCHEMA {schema_name}", return_rows=0, dbname=other_db_name)
    # create all tables
    for t in tables_names:
        if with_db:
            pg_mig.source.c(f"CREATE TABLE {schema_name}.{t} (foo int)", return_rows=0, dbname=other_db_name)
        if with_schema:
            pg_mig.source.c(f"CREATE TABLE {schema_name}.{t} (foo int)", return_rows=0, dbname=db_name)
        else:
            pg_mig.source.c(f"CREATE TABLE {t} (foo int)", return_rows=0, dbname=db_name)
    # pylint: disable=protected-access
    pg_mig.source._set_db(dbname=db_name)
    other_db = None
    if with_db:
        pg_mig.source._set_db(dbname=other_db_name)
        other_db = pg_mig.source.databases[other_db_name]
    # pylint: enable=protected-access
    db = pg_mig.source.databases[db_name]
    # sanity
    assert db.tables is not None
    assert len(db.tables) == len(tables)

    filtered = pg_mig.filter_tables(db)
    if skip:
        assert not filtered
    else:
        assert len(filtered) == len(tables)
        if not with_db:
            assert set(filtered) == set(tables)
        # with a db there the strings do not match
    if other_db and not skip:
        # skip means comparison is reversed
        no_match_filter = pg_mig.filter_tables(other_db)
        assert not no_match_filter


@pytest.mark.parametrize(
    ["superuser", "extras"],
    [
        [True, True],
        [False, False],
        [True, False],
        [False, True],
    ],
)
def test_replicate_filter_with(pg_source_and_target_replication: Tuple[PGRunner, PGRunner], superuser: bool, extras):
    source, target = pg_source_and_target_replication
    db_name = random_string(6)
    other_db_name = random_string(6)
    for db in [db_name, other_db_name]:
        for runner in [source, target]:
            runner.create_db(dbname=db)
            if extras:
                runner.create_extension(
                    extname="aiven_extras",
                    dbname=db,
                    grantee=runner.superuser if superuser else runner.testuser,
                )
                runner.have_aiven_extras(dbname=db, grantee=runner.superuser if superuser else runner.testuser)
    table_names = [f'"ta .\'ble{i}"' for i in range(3)]
    for db in [db_name, other_db_name]:
        with source.cursor(dbname=db) as c:
            for t in table_names:
                c.execute(f"CREATE TABLE {t} (foo INT)")
                c.execute(f"INSERT INTO {t} (foo) VALUES (1), (2), (3)")

    only_tables = [
        f'{db_name}.public."ta .\'ble0"',
        f'{db_name}.public."ta .\'ble1"',
        f'{other_db_name}.public."ta .\'ble2"',
    ]
    pg_mig = PGMigrate(
        source_conn_info=source.super_conn_info() if superuser else source.conn_info(),
        target_conn_info=target.super_conn_info() if superuser else target.conn_info(),
        verbose=False,
        with_tables=only_tables,
        createdb=True,
        replicate_extensions=True,
    )
    try:
        result = pg_mig.migrate()
        for db in {db_name, other_db_name}:
            assert db in set(pg_mig.target.databases.keys())
            assert db in result.pg_databases
            if extras or superuser:
                assert result.pg_databases[db]["method"] == "replication", result.pg_databases[db]
            else:
                assert result.pg_databases[db]["method"] == "dump", result.pg_databases[db]
        matched_tables: Dict[str, Set] = {db_name: set(), other_db_name: set()}
        desired = {db_name: {'"ta .\'ble0"', '"ta .\'ble1"'}, other_db_name: {'"ta .\'ble2"'}}
        timer = Timer(timeout=30, sleep=1, what="Waiting for data to replicate")
        while timer.loop():
            if desired == matched_tables:
                break
            for db, tables in desired.items():
                if matched_tables[db] == tables:
                    continue
                for t in tables:
                    if t in matched_tables[db]:
                        continue
                    count = pg_mig.target.c(f"SELECT COUNT(1) FROM {t}", dbname=db)[0]
                    if count["count"] == 3:
                        matched_tables[db].add(t)
        desired = {other_db_name: {'"ta .\'ble0"', '"ta .\'ble1"'}, db_name: {'"ta .\'ble2"'}}
        for db, tables in desired.items():
            for t in tables:
                count = pg_mig.target.c(f"SELECT COUNT(1) FROM {t}", dbname=db)[0]
                assert count["count"] == 0, count

    finally:
        for db in [db_name, other_db_name, "postgres"]:
            try:
                with target.cursor(username=target.superuser, dbname=db, autocommit=True) as cur:
                    cur.execute(f"ALTER SUBSCRIPTION aiven_db_migrate_{db}_sub DISABLE")
                    cur.execute(f"DROP SUBSCRIPTION IF EXISTS aiven_db_migrate_{db}_sub CASCADE")
            except psycopg2.Error:
                pass
            try:
                pg_mig.source.cleanup(
                    dbname=db, pubname=f"aiven_db_migrate_{db}_pub", slotname=f"aiven_db_migrate_{db}_slot"
                )
            except:  # pylint: disable=bare-except
                pass
