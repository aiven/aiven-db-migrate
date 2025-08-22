# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.clusters import PGCluster
from aiven_db_migrate.migrate.models import (
    DBMigrateResult, DumpTaskResult, DumpType, PGDatabase, PGMigrateMethod, PGMigrateStatus, ReplicationSetupResult
)
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from contextlib import nullcontext as does_not_raise
from unittest import mock

import psycopg2
import pytest


@mock.patch.object(PGMigrate, "_dump_schema", return_value=DumpTaskResult(status=PGMigrateStatus.done, type=DumpType.schema))
@mock.patch.object(PGMigrate, "_dump_data", return_value=DumpTaskResult(status=PGMigrateStatus.done, type=DumpType.data))
@mock.patch.object(PGMigrate, "_setup_logical_replication", return_value=ReplicationSetupResult(status=PGMigrateStatus.done))
@mock.patch.object(PGCluster, "refresh_db")
@pytest.mark.parametrize(
    "method", [PGMigrateMethod.dump, PGMigrateMethod.replication, PGMigrateMethod.replication_with_dump_fallback]
)
def test_force_method(mock_refresh_db, mock_db_replication, mock_dump_data, mock_dump_schema, method):
    pg_mig = PGMigrate(source_conn_info="postgresql://source", target_conn_info="postgresql://target")
    pg_mig._db_migrate(
        migrate_method=method,
        source_db=PGDatabase(dbname="test_source", tables=[]),
        target_db=PGDatabase(dbname="test_target", tables=[])
    )

    mock_dump_schema.assert_called()
    mock_refresh_db.assert_called()

    if method == PGMigrateMethod.dump:
        mock_dump_data.assert_called()
        mock_db_replication.assert_not_called()
    elif method == PGMigrateMethod.replication:
        mock_dump_data.assert_not_called()
        mock_db_replication.assert_called()
    elif method is None:
        mock_db_replication.assert_called()
        mock_dump_data.assert_not_called()
