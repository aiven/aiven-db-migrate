# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.errors import PGMigrateFailureReason, PGMigrateValidationFailedError
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from distutils.version import LooseVersion
from test.conftest import PGRunner
from test.utils import random_string
from typing import Tuple
from unittest.mock import patch

import pytest


# our fixtures are created with the (apparently intentional) constraint of source <= target, but we need to trigger
# the reverse case as well.
@pytest.mark.parametrize("reverse_source_target", [True, False])
def test_dbs_migration_succeeds_or_fails(pg_source_and_target: Tuple[PGRunner, PGRunner], reverse_source_target: bool):
    source, target = reversed(pg_source_and_target) if reverse_source_target else pg_source_and_target
    source_version = LooseVersion(source.pgversion)
    target_version = LooseVersion(target.pgversion)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        createdb=False,
        verbose=True,
    )

    try:
        pg_mig.validate()
        assert target_version >= source_version, "Migration should succeed only when target version >= source version"
    except PGMigrateValidationFailedError as e:
        assert target_version < source_version, "Migration should fail only when target version < source version"
        assert e.reason == PGMigrateFailureReason.cannot_migrate_to_older_server_version


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
