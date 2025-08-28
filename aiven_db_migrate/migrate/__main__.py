# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.models import PGMigrateMethod
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from aiven_db_migrate.migrate.reports import log_migration_report, save_migration_report
from pathlib import Path

import argparse
import logging
import os
import random
import string
import sys


def pg_main(args=None, *, prog="pg_migrate"):
    """CLI for PostgreSQL migration tool"""
    parser = argparse.ArgumentParser(description="PostgreSQL migration tool.", prog=prog)
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument(
        "-s",
        "--source",
        help=(
            "Source PostgreSQL server, either postgres:// uri or libpq connection string. "
            "Required if `SOURCE_SERVICE_URI` environment variable is not set."
        ),
        required=not os.getenv("SOURCE_SERVICE_URI"),
    )
    parser.add_argument(
        "-t",
        "--target",
        help=(
            "Target PostgreSQL server, either postgres:// uri or libpq connection string. "
            "Required if `TARGET_SERVICE_URI` environment variable is not set."
        ),
        required=not os.getenv("TARGET_SERVICE_URI"),
    )
    parser.add_argument(
        "-f", "--filtered-db", help="Comma separated list of databases to filter out during migrations", required=False
    )
    parser.add_argument(
        "-xr",
        "--excluded-roles",
        help="Comma separated list of database roles to exclude during migrations",
        required=False,
    )
    parser.add_argument(
        "-xe",
        "--excluded-extensions",
        help="Comma separated list of database extensions to exclude during migrations",
        required=False,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output.")
    parser.add_argument(
        "--no-createdb", action="store_false", dest="createdb", help="Don't automatically create database(s) in target."
    )
    parser.add_argument(
        "-m",
        "--mangle",
        action="store_true",
        help="Mangle the DB name for the purpose of having a predictable identifier len",
    )
    parser.add_argument(
        "--max-replication-lag",
        type=int,
        default=-1,
        help="Max replication lag in bytes to wait for, by default no wait (no effect when replication isn't available).",
    )
    parser.add_argument(
        "--stop-replication",
        action="store_true",
        help=(
            "Stop replication, by default replication is left running (no effect when replication isn't available). "
            "Requires also '--max-replication-lag' >= 0, i.e. wait until replication lag in bytes is less than/equal "
            "to given max replication lag and then stop replication."
        ),
    )
    parser.add_argument("--validate", action="store_true", help="Run only best effort validation.")
    table_common_help = (
        " Table names can be qualified by name only, schema name and table name or DB , schema and table name."
        " Tables with no DB specified will attempt a match against all present databases."
        " Tables with no schema specified will attempt a match against all schemas in all databases."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--with-table",
        action="append",
        help=(
            "When specified, the migration method will include the named tables only instead of all tables. "
            "Can be specified multiple times. Cannot be used together with --skip-table." + table_common_help
        ),
    )
    group.add_argument(
        "--skip-table",
        action="append",
        help=(
            "When specified, the migration method will include all tables except the named tables. "
            "Can be specified multiple times. Cannot be used together with --with-table." + table_common_help
        ),
    )
    parser.add_argument(
        "--replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_true",
        default=True,
        help=(
            "logical replication should try to add tables "
            "belonging to extensions to the publication definition (default)"
        ),
    )
    parser.add_argument(
        "--no-replicate-extension-tables",
        dest="replicate_extension_tables",
        action="store_false",
        help="Do not add tables belonging to extensions to the publication definition",
    )
    parser.add_argument(
        "--force-method",
        default=None,
        help="Force the migration method to be used as either replication or dump.",
    )
    parser.add_argument(
        "--dbs-max-total-size",
        type=int,
        default=-1,
        help="Max total size of databases to be migrated, ignored by default",
    )
    parser.add_argument(
        "--pgbin",
        type=str,
        default=None,
        help="Path to pg_dump and other postgresql binaries",
    )
    parser.add_argument(
        "--skip-db-version-check",
        action="store_true",
        help="Skip PG version check between source and target",
    )
    parser.add_argument(
        "--show-passwords",
        action="store_true",
        help="Show generated placeholder roles passwords in the output, by default passwords are not shown",
    )
    parser.add_argument(
        "--migration-id",
        default=''.join(random.choices(string.ascii_letters + string.digits, k=4)),
        help="This identifier will be added to all logs"
    )
    parser.add_argument(
        "--report-file",
        type=str,
        default=None,
        help="File to save the migration report in JSON format. If not specified, the report will not be saved.",
    )

    args = parser.parse_args(args)
    log_format = f"%(asctime)s\t{args.migration_id}\t%(name)s\t%(levelname)s\t%(message)s"
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)

    migrate_method = PGMigrateMethod.replication_with_dump_fallback
    if args.force_method:
        try:
            migrate_method = PGMigrateMethod[args.force_method]
        except KeyError as e:
            raise ValueError(f"Unsupported migration method '{args.force_method}'") from e

    pg_mig = PGMigrate(
        source_conn_info=os.getenv("SOURCE_SERVICE_URI") or args.source,
        target_conn_info=os.getenv("TARGET_SERVICE_URI") or args.target,
        pgbin=args.pgbin,
        createdb=args.createdb,
        max_replication_lag=args.max_replication_lag,
        stop_replication=args.stop_replication,
        verbose=args.verbose,
        filtered_db=args.filtered_db,
        excluded_roles=args.excluded_roles,
        excluded_extensions=args.excluded_extensions,
        mangle=args.mangle,
        skip_tables=args.skip_table,
        with_tables=args.with_table,
        replicate_extensions=args.replicate_extension_tables,
        skip_db_version_check=args.skip_db_version_check,
        migrate_method=PGMigrateMethod(migrate_method),
    )

    if args.validate:
        dbs_max_total_size = None if args.dbs_max_total_size == -1 else args.dbs_max_total_size
        pg_mig.validate(dbs_max_total_size=dbs_max_total_size)
    else:
        pg_migrate_report = pg_mig.migrate()
        log_migration_report(pg_migrate_report, show_passwords=args.show_passwords)
        if args.report_file:
            save_migration_report(Path(args.report_file), pg_migrate_report)


if __name__ == "__main__":

    commands = {"pg": pg_main}
    argv = sys.argv[1:]
    c = None
    if argv:
        c = argv.pop(0)

    if not c or c not in commands:
        print(f"Available commands: {', '.join(commands)}")
        sys.exit(1)

    commands[c](argv)
