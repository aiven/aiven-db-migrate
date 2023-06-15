from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from test.utils import PGRunner, random_string
from typing import Tuple

import psycopg2
import pytest


def test_db_size(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target

    db_name = random_string(6)
    other_db_name = random_string(6)

    source.create_db(dbname=db_name)
    source.create_db(dbname=other_db_name)

    pg_mig = PGMigrate(
        source_conn_info=source.super_conn_info(),
        target_conn_info=target.super_conn_info(),
        verbose=True,
    )

    # Create few tables and insert some data
    tables = [f'table_{i}' for i in range(4)]
    for dbname in {db_name, other_db_name}:
        with source.cursor(dbname=dbname) as c:
            for t in tables:
                c.execute(f"DROP TABLE IF EXISTS {t}")
                c.execute(f"CREATE TABLE {t} (foo INT)")
                c.execute(f"INSERT INTO {t} (foo) VALUES (1), (2), (3)")

    size = pg_mig.source.get_size(dbname=db_name, only_tables=[])
    assert size == 0

    size = pg_mig.source.get_size(dbname=db_name)
    assert size >= 0  # returns slightly different values per pg version

    size = pg_mig.source.get_size(dbname=db_name, only_tables=tables)
    assert size == 32768

    size = pg_mig.source.get_size(dbname=db_name, only_tables=tables[:1])
    assert size == 8192

    with pytest.raises(psycopg2.OperationalError):
        size = pg_mig.source.get_size(dbname="notfound")
