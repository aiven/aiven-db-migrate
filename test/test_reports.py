# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.models import (
    DBMigrateResult, PGMigrateMethod, PGMigrateResult, PGMigrateStatus, RoleMigrateTask, ValidationResult
)
from aiven_db_migrate.migrate.reports import save_migration_report
from pathlib import Path

import tempfile

EXPECTED_RAW_REPORT = '''{
  "validation": {
    "status": "failed",
    "error": {
      "type": "ValueError",
      "message": "Bad value"
    }
  },
  "role_migrate_results": [
    {
      "message": "Role migrated successfully",
      "rolname": "test_role",
      "status": "done",
      "rolpassword": null
    }
  ],
  "db_migrate_results": [
    {
      "status": "done",
      "dbname": "test_db",
      "migrate_method": "dump",
      "error": null,
      "source_db_error": null,
      "target_db_error": null,
      "dump_schema_result": null,
      "dump_data_result": null,
      "replication_setup_result": null
    }
  ]
}'''


def test_save_migration_report():
    pg_migrate_result = PGMigrateResult(
        validation=ValidationResult(
            status=PGMigrateStatus.failed,
            error=ValueError("Bad value"),
        ),
        role_migrate_results=[
            RoleMigrateTask(
                rolname="test_role", rolpassword=None, status=PGMigrateStatus.done, message="Role migrated successfully"
            )
        ],
        db_migrate_results=[
            DBMigrateResult(
                status=PGMigrateStatus.done,
                dbname="test_db",
                migrate_method=PGMigrateMethod.dump,
                dump_schema_result=None,
                dump_data_result=None,
                replication_setup_result=None,
                error=None,
                source_db_error=None,
                target_db_error=None
            )
        ],
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        report_path = Path(tmpdir) / "migration_report.json"
        save_migration_report(report_path, pg_migrate_result)
        with open(report_path) as f:
            raw_report = f.read()
            assert raw_report == EXPECTED_RAW_REPORT
