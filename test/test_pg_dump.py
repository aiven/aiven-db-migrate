# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from test.conftest import PGRunner
from test.utils import random_string
from typing import Tuple

import pytest


@pytest.mark.parametrize("createdb", [True, False])
def test_dump(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    tblname = random_string()

    # create db and table with some data in source
    source.create_db(dbname=dbname)
    with source.cursor(dbname=dbname) as cur:
        cur.execute(f"CREATE TABLE {tblname} (something INT)")
        cur.execute(f"INSERT INTO {tblname} VALUES (1), (2), (3), (4), (5)")

    if not createdb:
        # create existing db to target
        target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    # evaluates pgbin dir (pg_dump needs to be from same version as source)
    pg_mig.validate()

    # dump both schema and data
    pg_mig._dump_schema(dbname=dbname)  # pylint: disable=protected-access
    pg_mig._dump_data(dbname=dbname)  # pylint: disable=protected-access

    # verify that db/table migrated to target
    exists = pg_mig.target.c(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name= %s",
        args=(
            "public",
            tblname,
        ),
        dbname=dbname
    )
    assert exists
    count = pg_mig.target.c(f"SELECT count(*) FROM {tblname}", dbname=dbname, return_rows=1)[0]
    assert int(count["count"]) == 5
