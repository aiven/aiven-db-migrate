# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.models import PGMigrateResult
from dataclasses import asdict
from pathlib import Path

import json
import logging

logger = logging.getLogger("PGMigrateReport")


def log_migration_report(pg_migrate_report: PGMigrateResult, show_passwords: bool = False):
    logger.info("Migration report:")
    if pg_migrate_report.validation:
        logger.info("|--Validation result: %s", pg_migrate_report.validation.status.name)
        if pg_migrate_report.validation.error:
            logger.error("|----Error: %s", pg_migrate_report.validation.error)
    logger.info("|--Roles:")
    for role in pg_migrate_report.role_migrate_results:
        rolpassword = role.rolpassword
        if rolpassword is None:
            rolpassword = "-"
        elif not show_passwords:
            rolpassword = "<hidden>"
        logger.info(
            "|----rolname: %s, rolpassword: %s, status: %s, message: %s", role.rolname, rolpassword, role.status.name,
            role.message
        )
    logger.info("|--Databases:")
    for db in pg_migrate_report.db_migrate_results:
        logger.info(
            "|----'%s' - %s (method: %s)",
            db.dbname,
            db.status.name,
            db.migrate_method,
        )

        for stage_name, stage_result in [("Dump schema", db.dump_schema_result),
                                         ("Replication setup", db.replication_setup_result),
                                         ("Dump data", db.dump_data_result)]:
            if stage_result:
                logger.info(
                    "|------Stage '%s' status: %s%s", stage_name, stage_result.status.name,
                    f". Error: '{repr(stage_result.error)}'" if stage_result.error else ''
                )

        for error_name, error in [("Source DB error", db.source_db_error), ("Target DB error", db.target_db_error),
                                  ("Unexpected error", db.error)]:
            if error:
                logger.info("|------%s: '%r'", error_name, error)


def save_migration_report(report_path: Path, pg_migrate_report: PGMigrateResult):
    def exc_serializer(obj):
        if isinstance(obj, Exception):
            return {
                "type": type(obj).__name__,
                "message": str(obj),
            }
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    json_report = json.dumps(asdict(pg_migrate_report), indent=2, default=exc_serializer)
    with open(report_path, "w") as f:
        f.write(json_report)
