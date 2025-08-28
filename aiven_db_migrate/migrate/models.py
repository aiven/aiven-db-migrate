# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from dataclasses import dataclass, field
from datetime import datetime

import enum


class ReplicationObjectType(enum.StrEnum):
    PUBLICATION = "pub"
    SUBSCRIPTION = "sub"
    REPLICATION_SLOT = "slot"

    def get_display_name(self) -> str:
        return self.name.replace("_", " ").lower()


@dataclass
class PGExtension:
    name: str
    version: str
    superuser: bool = True
    trusted: bool | None = None


@dataclass(frozen=True)
class PGTable:
    db_name: str | None
    schema_name: str | None
    table_name: str
    extension_name: str | None

    def __str__(self) -> str:
        if not self.schema_name:
            return self.table_name
        return f"{self.schema_name}.{self.table_name}"

    def __hash__(self) -> int:
        return ((hash(self.db_name) & 0xFFFF000000000000) ^ (hash(self.schema_name) & 0x0000FFFF00000000) ^
                (hash(self.table_name) & 0x00000000FFFFFFFF))

    def __eq__(self, other):
        if not isinstance(other, PGTable):
            return False
        return (
            self.table_name == other.table_name and self.schema_name == other.schema_name and self.db_name == other.db_name
            and self.extension_name == other.extension_name
        )


@dataclass
class PGDatabase:
    dbname: str
    tables: list[PGTable]
    pg_ext: list[PGExtension] = field(default_factory=list)
    has_aiven_extras: bool = False
    error: BaseException | None = None


@dataclass
class PGRole:
    rolname: str
    rolsuper: bool
    rolinherit: bool
    rolcreaterole: bool
    rolcreatedb: bool
    rolcanlogin: bool
    rolreplication: bool
    rolconnlimit: int
    # placeholder password
    rolpassword: str | None = field(repr=False)
    rolvaliduntil: datetime | None
    rolbypassrls: bool
    rolconfig: list[str]
    safe_rolname: str


@enum.unique
class PGRoleStatus(enum.StrEnum):
    created = "created"
    exists = "exists"
    failed = "failed"


@dataclass
class RoleMigrateTask:
    message: str
    rolname: str
    status: PGRoleStatus
    rolpassword: str | None = field(repr=False, default=None)


@enum.unique
class PGMigrateMethod(enum.StrEnum):
    dump = "dump"
    replication = "replication"
    replication_with_dump_fallback = "replication_with_dump_fallback"
    schema_only = "schema_only"


@enum.unique
class PGMigrateStatus(enum.StrEnum):
    cancelled = "cancelled"
    done = "done"
    failed = "failed"
    running = "running"


class DumpType(enum.StrEnum):
    schema = "S"
    data = "D"


@dataclass
class DumpTaskResult:
    status: PGMigrateStatus
    type: DumpType
    error: BaseException | None = None
    pg_dump_returncode: int | None = None
    pg_restore_returncode: int | None = None
    pg_restore_warnings: str | None = None


@dataclass
class ReplicationSetupResult:
    status: PGMigrateStatus
    error: BaseException | None = None


@dataclass
class ValidationResult:
    status: PGMigrateStatus
    error: BaseException | None = None


@dataclass
class DBMigrateResult:
    status: PGMigrateStatus
    dbname: str
    migrate_method: PGMigrateMethod
    error: BaseException | None = None  # General error during migration which is not related to a specific stage
    source_db_error: BaseException | None = None
    target_db_error: BaseException | None = None
    dump_schema_result: DumpTaskResult | None = None
    dump_data_result: DumpTaskResult | None = None
    replication_setup_result: ReplicationSetupResult | None = None


@dataclass
class PGMigrateResult:
    validation: ValidationResult | None = None
    role_migrate_results: list[RoleMigrateTask] = field(default_factory=list)
    db_migrate_results: list[DBMigrateResult] = field(default_factory=list)
