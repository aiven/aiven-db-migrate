# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.clusters import PGSource, PGTarget
from packaging.version import Version
from test.utils import PGRunner, random_string, Timer
from typing import Tuple

import psycopg2
import psycopg2.errorcodes
import pytest


@pytest.mark.parametrize("aiven_extras", [True, False])
def test_replication(pg_source_and_target: Tuple[PGRunner, PGRunner], aiven_extras: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    tblname = random_string()

    # have the db in both source and target
    source.create_db(dbname=dbname)
    target.create_db(dbname=dbname)

    # create table with some data in source db
    with source.cursor(dbname=dbname) as cur:
        cur.execute(f"CREATE TABLE public.{tblname} (something INT)")
        cur.execute(f"INSERT INTO public.{tblname} VALUES (1), (2)")

    # create table in target
    with target.cursor(dbname=dbname) as cur:
        cur.execute(f"CREATE TABLE public.{tblname} (something INT)")

    if aiven_extras:
        # have aiven-extras in both source and target
        if not source.have_aiven_extras(dbname=dbname) or not target.have_aiven_extras(dbname=dbname):
            pytest.skip("aiven-extras not available")
        pg_source = PGSource(source.conn_info())
        pg_target = PGTarget(target.conn_info())
    else:
        pg_source = PGSource(source.super_conn_info())
        pg_target = PGTarget(target.super_conn_info())

    assert pg_source.has_aiven_extras(dbname=dbname) if aiven_extras else pg_source.is_superuser
    assert pg_target.has_aiven_extras(dbname=dbname) if aiven_extras else pg_target.is_superuser

    pubname = pg_source.create_publication(dbname=dbname)
    slotname = pg_source.create_replication_slot(dbname=dbname)
    # verify that pub and replication slot exixts
    pub = pg_source.get_publication(dbname=dbname)
    assert pub
    assert pub["pubname"] == pubname
    slot = pg_source.get_replication_slot(dbname=dbname)
    assert slot
    assert slot["slot_name"] == slotname
    assert slot["slot_type"] == "logical"

    conn_str = pg_source.conn_str(dbname=dbname)
    subname = pg_target.create_subscription(conn_str=conn_str, dbname=dbname)
    # verify that sub exists
    sub = pg_target.get_subscription(dbname=dbname)
    assert sub
    assert sub["subname"] == subname
    assert sub["subenabled"]
    assert pubname in sub["subpublications"]

    # have some more data in source
    pg_source.c(f"INSERT INTO public.{tblname} VALUES (3), (4), (5)", dbname=dbname, return_rows=0)

    # wait until replication is in sync
    timer = Timer(timeout=10, what="replication in sync")
    while timer.loop():
        in_sync, write_lsn = pg_source.replication_in_sync(dbname=dbname, max_replication_lag=0)
        if in_sync and pg_target.replication_in_sync(dbname=dbname, write_lsn=write_lsn, max_replication_lag=0):
            break

    # verify that all data has been replicated
    timer = Timer(timeout=10, what="all data replicated")
    while timer.loop():
        count = pg_target.c(f"SELECT count(*) FROM public.{tblname}", dbname=dbname, return_rows=1)[0]
        if int(count["count"]) == 5:
            break

    pg_target.cleanup(dbname=dbname)
    pg_source.cleanup(dbname=dbname)

    # verify that pub, replication slot and sub are dropped
    assert not source.list_pubs(dbname=dbname)
    assert not source.list_slots()
    assert not target.list_subs()


def test_replication_no_aiven_extras_no_superuser(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    dbname = random_string()
    source.create_db(dbname=dbname)
    target.create_db(dbname=dbname)

    pg_source = PGSource(source.conn_info())
    assert not pg_source.has_aiven_extras(dbname=dbname)
    assert not pg_source.is_superuser

    pg_target = PGTarget(target.conn_info())
    assert not pg_target.has_aiven_extras(dbname=dbname)
    assert not pg_target.is_superuser

    # creating publication for all tables in db should fail with insufficient privilege
    with pytest.raises(psycopg2.ProgrammingError) as err:
        pg_source.create_publication(dbname=dbname)
    assert err.value.pgcode == psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE
    assert err.value.diag.message_primary == "must be superuser to create FOR ALL TABLES publication"

    # creating subscription should fail with insufficient privilege
    with pytest.raises(psycopg2.ProgrammingError) as err:
        pg_target.create_subscription(conn_str=pg_source.conn_str(), dbname=dbname)
    assert err.value.pgcode == psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE

    privilege_error_message = "must be superuser to create subscriptions"
    # error message was changed
    if pg_target.version >= Version("16"):
        privilege_error_message = "permission denied to create subscription"

    assert err.value.diag.message_primary == privilege_error_message

    # verify that there's no leftovers
    assert not source.list_pubs(dbname=dbname)
    assert not source.list_slots()
    assert not target.list_subs()
