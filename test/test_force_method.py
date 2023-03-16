# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.pgmigrate import PGCluster, PGDatabase, PGMigrate, PGMigrateMethod, PGMigrateTask
from contextlib import nullcontext as does_not_raise
from unittest import mock

import psycopg2
import pytest


@mock.patch.object(PGCluster, "params", new_callable=mock.PropertyMock, return_value={"server_version": "11.13"})
@mock.patch.object(PGMigrate, "_dump_schema")
@mock.patch.object(PGMigrate, "_dump_data")
@mock.patch.object(PGMigrate, "_db_replication")
@mock.patch.object(PGCluster, "refresh_db")
@pytest.mark.parametrize("method", [PGMigrateMethod.dump, PGMigrateMethod.replication, None])
def test_force_method(mock_refresh_db, mock_db_replication, mock_dump_data, mock_dump_schema, mock_params, method):
    pg_mig = PGMigrate(source_conn_info="postgresql://source", target_conn_info="postgresql://target")
    pg_task = PGMigrateTask(
        source_db=PGDatabase(dbname="test_source", tables=[]),
        target_db=PGDatabase(dbname="test_target", tables=[]),
        method=method
    )

    pg_mig._db_migrate(pgtask=pg_task)

    mock_dump_schema.assert_called()
    mock_refresh_db.assert_called()
    mock_params.assert_called()

    if method == PGMigrateMethod.dump:
        mock_dump_data.assert_called()
        mock_db_replication.assert_not_called()
    elif method == PGMigrateMethod.replication:
        mock_dump_data.assert_not_called()
        mock_db_replication.assert_called()
    elif method is None:
        mock_db_replication.assert_called()
        mock_dump_data.assert_not_called()


@mock.patch.object(PGCluster, "params", new_callable=mock.PropertyMock, return_value={"server_version": "11.13"})
@mock.patch.object(PGMigrate, "_dump_schema")
@mock.patch.object(PGMigrate, "_dump_data")
@mock.patch.object(PGMigrate, "_db_replication", side_effect=psycopg2.ProgrammingError)
@mock.patch.object(PGCluster, "refresh_db")
@pytest.mark.parametrize("method", [PGMigrateMethod.replication, None])
def test_force_method_failure(mock_refresh_db, mock_db_replication, mock_dump_data, mock_dump_schema, mock_params, method):
    pg_mig = PGMigrate(source_conn_info="postgresql://source", target_conn_info="postgresql://target")
    pg_task = PGMigrateTask(
        source_db=PGDatabase(dbname="test_source", tables=[]),
        target_db=PGDatabase(dbname="test_target", tables=[]),
        method=method
    )

    if method == PGMigrateMethod.replication:
        # if we are forcing the use of replication and it fails, psycopg2.ProgramminError gets raised
        context = pytest.raises(psycopg2.ProgrammingError)
    else:
        # otherwise we should fallback to using dump and not raise exception
        context = does_not_raise()

    with mock.patch.object(
        psycopg2.ProgrammingError,
        "pgcode",
        new_callable=mock.PropertyMock,
        return_value=psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE
    ):
        with context:
            pg_mig._db_migrate(pgtask=pg_task)

    mock_dump_schema.assert_called()
    mock_refresh_db.assert_called()
    mock_params.assert_called()

    if method == PGMigrateMethod.replication:
        mock_dump_data.assert_not_called()
        mock_db_replication.assert_called()
    elif method is None:
        mock_db_replication.assert_called()
        mock_dump_data.assert_called()
