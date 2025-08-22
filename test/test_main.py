# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.__main__ import pg_main
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from unittest import mock

import pytest


@mock.patch.object(PGMigrate, "validate")
@mock.patch.object(PGMigrate, "migrate")
@pytest.mark.parametrize("validate", [True, False])
def test_main(mock_migrate, mock_validate, validate):
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
        pg_main()
        if validate:
            mock_validate.assert_called_once()
        else:
            mock_migrate.assert_called_once()
