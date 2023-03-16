# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.pgmigrate import PGMigrate
from test.conftest import PGRunner
from test.utils import random_string
from typing import Tuple
from unittest.mock import MagicMock, patch

import pytest


def test_dbs_max_total_size_check(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    dbnames = {random_string() for _ in range(3)}

    for dbname in dbnames:
        source.create_db(dbname=dbname)
        target.create_db(dbname=dbname)

    # This DB seems to be created outside the tests
    dbnames.add("postgres")

    # Create few tables and insert some data
    tables = [f'table_{i}' for i in range(4)]
    for dbname in dbnames:
        with source.cursor(dbname=dbname) as c:
            for t in tables:
                c.execute(f"DROP TABLE IF EXISTS {t}")
                c.execute(f"CREATE TABLE {t} (foo INT)")
                c.execute(f"INSERT INTO {t} (foo) VALUES (1), (2), (3)")

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
    )

    with patch(
        "aiven_db_migrate.migrate.pgmigrate.PGMigrate._check_database_size", side_effect=pg_mig._check_database_size
    ) as mock_db_size_check:
        # DB size check is not run
        pg_mig.validate()
        mock_db_size_check.assert_not_called()

        mock_db_size_check.reset_mock()

        # DB size check with max size of zero
        with pytest.raises(PGMigrateValidationFailedError) as e:
            pg_mig.validate(dbs_max_total_size=0)
        assert "Databases do not fit to the required maximum size" in str(e)
        mock_db_size_check.assert_called_once_with(max_size=0)

        mock_db_size_check.reset_mock()

        # DB size check with enough size
        pg_mig.validate(dbs_max_total_size=1073741824)
        mock_db_size_check.assert_called_once_with(max_size=1073741824)

    # Test with DB name filtering
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
        filtered_db=",".join(dbnames),
    )

    with patch(
        "aiven_db_migrate.migrate.pgmigrate.PGMigrate._check_database_size", side_effect=pg_mig._check_database_size
    ) as mock_db_size_check:
        # Should pass as all DBs are filtered out from size calculations
        pg_mig.validate(dbs_max_total_size=0)
        mock_db_size_check.assert_called_once_with(max_size=0)

    # Test with table filtering

    # Include all tables in "skip_tables"
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
        skip_tables=tables,  # skip all tables
    )

    with patch(
        "aiven_db_migrate.migrate.pgmigrate.PGMigrate._check_database_size", side_effect=pg_mig._check_database_size
    ) as mock_db_size_check:
        # Should pass as all tables are filtered out from size calculations
        pg_mig.validate(dbs_max_total_size=0)
        mock_db_size_check.assert_called_once_with(max_size=0)

    # Only the first table is included
    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
        with_tables=tables[:1],  # include only one table
    )
    with patch(
        "aiven_db_migrate.migrate.pgmigrate.PGMigrate._check_database_size", side_effect=pg_mig._check_database_size
    ) as mock_db_size_check:
        # This fails as one table is included in check and it should have data
        with pytest.raises(PGMigrateValidationFailedError) as e:
            pg_mig.validate(dbs_max_total_size=0)
        assert "Databases do not fit to the required maximum size" in str(e)
        mock_db_size_check.assert_called_once_with(max_size=0)

        # Should easily fit
        pg_mig.validate(dbs_max_total_size=1073741824)


def test_large_object_warnings(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    dbnames = {random_string() for _ in range(3)}
    for dbname in dbnames:
        source.create_db(dbname=dbname)
        target.create_db(dbname=dbname)
    dbnames.add(source.defaultdb)
    dbnames.add(target.defaultdb)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
    )

    pg_mig.log = MagicMock()

    # Create a large object in the source
    with source.cursor(dbname=source.defaultdb) as cur:
        cur.execute("SELECT lo_create(0)")

    with patch(
        "aiven_db_migrate.migrate.pgmigrate.PGMigrate._check_pg_lobs", side_effect=pg_mig._check_pg_lobs
    ) as mock_lobs_check:
        pg_mig.validate()
        mock_lobs_check.assert_called_once()
        pg_mig.log.warning.assert_called_with(
            "Large objects detected: large objects are not compatible with logical replication: https://www.postgresql.org/docs/14/logical-replication-restrictions.html"
        )
