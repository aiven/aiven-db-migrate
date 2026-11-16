# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.__main__ import pg_main
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from unittest import mock

import json
import pytest
import subprocess
import sys


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


@pytest.mark.parametrize(
    "scenario, expected_status",
    [("validation_failed", 1), ("database_failed", 1), ("role_failed", 1),
     ("database_cancelled", 1), ("done", 0), ("running", 0)],
)
def test_migration_exit_status(scenario, expected_status, tmp_path):
    report_path = tmp_path / "report.json"
    script = """
import runpy
import sys
from unittest import mock
from aiven_db_migrate.migrate.models import (
    DBMigrateResult, PGMigrateMethod, PGMigrateResult, PGMigrateStatus,
    PGRoleStatus, RoleMigrateTask, ValidationResult,
)
from aiven_db_migrate.migrate.pgmigrate import PGMigrate

scenario, report_path = sys.argv[1:]
report = PGMigrateResult(validation=ValidationResult(status=PGMigrateStatus.done))
if scenario == "validation_failed":
    report.validation = ValidationResult(status=PGMigrateStatus.failed)
elif scenario == "role_failed":
    report.role_migrate_results = [RoleMigrateTask(message="mock failure", rolname="example", status=PGRoleStatus.failed)]
else:
    status = {"database_failed": "failed", "database_cancelled": "cancelled"}.get(scenario, scenario)
    report.db_migrate_results = [DBMigrateResult(
        status=PGMigrateStatus(status), dbname="example", migrate_method=PGMigrateMethod.dump,
    )]
with mock.patch.object(PGMigrate, "migrate", return_value=report) as migrate:
    sys.argv = ["pg_migrate", "pg", "-s", "postgresql://source", "-t", "postgresql://target",
                "--report-file", report_path]
    try:
        runpy.run_module("aiven_db_migrate.migrate", run_name="__main__")
    finally:
        migrate.assert_called_once()
"""
    result = subprocess.run(
        [sys.executable, "-c", script, scenario, str(report_path)], capture_output=True, text=True, check=False,
    )
    assert "Migration report:" in result.stderr
    assert json.loads(report_path.read_text())["validation"]
    assert result.returncode == expected_status, result.stderr
