# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.pgmigrate import main, PGCluster, PGMigrate
from unittest import mock

import pytest
import re


@mock.patch.object(PGMigrate, "_check_aiven_pg_security_agent")
@mock.patch.object(PGCluster, "params", new_callable=mock.PropertyMock, return_value={"server_version": "11.13"})
@mock.patch.object(PGCluster, "databases", new_callable=mock.PropertyMock, return_value={})
@mock.patch.object(PGCluster, "pg_lang", new_callable=mock.PropertyMock, return_value={})
@mock.patch.object(PGMigrate, "migrate")
@pytest.mark.parametrize("validate", [True, False])
def test_main(mock_migrate, mock_lang, mock_databases, mock_params, mock_check_security, validate):
    args = [
        "pg_migrate",
        "-s",
        "postgresql://source",
        "-t",
        "postgresql://target",
    ]
    if validate:
        args.append("--validate")

    with mock.patch("sys.argv", args):
        main()
        if validate:
            mock_check_security.assert_called()
            mock_params.assert_called()
            mock_databases.assert_called()
            mock_lang.assert_called()
            mock_migrate.assert_not_called()
        else:
            mock_check_security.assert_not_called()
            mock_params.assert_not_called()
            mock_databases.assert_not_called()
            mock_lang.assert_not_called()
            mock_migrate.assert_called_with(force_method=None)


@mock.patch.object(PGCluster, "params", new_callable=mock.PropertyMock, return_value={"server_version": "11.13"})
@mock.patch.object(PGCluster, "databases", new_callable=mock.PropertyMock, return_value={})
@mock.patch.object(PGCluster, "pg_lang", new_callable=mock.PropertyMock, return_value={})
@mock.patch.object(PGMigrate, "migrate")
@pytest.mark.parametrize(
    "method,validate", [("dump", True), ("dump", False), ("replication", True), ("replication", False), (None, True),
                        (None, False)]
)
def test_main_force_method(mock_migrate, mock_lang, mock_databases, mock_params, method, validate):
    args = [
        "pg_migrate",
        "--source",
        "postgres://source/defaultdb",
        "--target",
        "postgres://target/defaultdb",
        "--force-method",
        method,
    ]

    with mock.patch("sys.argv", args):
        main()
        mock_migrate.assert_called_with(force_method=method)

    mock_params.reset_mock()
    mock_databases.reset_mock()
    mock_lang.reset_mock()
    mock_migrate.reset_mock()

    with mock.patch(
        "sys.argv", [
            "pg_migrate",
            "-s",
            "postgresql://source",
            "-t",
            "postgresql://target",
            "--force-method",
            "noop",
        ]
    ):
        with pytest.raises(ValueError, match=re.escape("Unsupported migration method 'noop'")):
            main()

        mock_params.assert_not_called()
        mock_databases.assert_not_called()
        mock_lang.assert_not_called()
        mock_migrate.assert_not_called()
