# Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import enum


class ReplicationObjectType(enum.Enum):
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
    trusted: Optional[bool] = None


@dataclass(frozen=True)
class PGTable:
    db_name: Optional[str]
    schema_name: Optional[str]
    table_name: str
    extension_name: Optional[str]

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
    tables: Set[PGTable]
    pg_ext: List[PGExtension] = field(default_factory=list)
    has_aiven_extras: bool = False
    error: Optional[BaseException] = None


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
    rolpassword: Optional[str] = field(repr=False)
    rolvaliduntil: Optional[datetime]
    rolbypassrls: bool
    rolconfig: List[str]
    safe_rolname: str


@enum.unique
class PGRoleStatus(str, enum.Enum):
    created = "created"
    exists = "exists"
    failed = "failed"


@dataclass
class PGRoleTask:
    message: str
    rolname: str
    status: PGRoleStatus
    rolpassword: Optional[str] = field(repr=False, default=None)

    def result(self) -> Dict[str, Optional[str]]:
        return {
            "message": self.message,
            "rolname": self.rolname,
            "rolpassword": self.rolpassword,
            "status": self.status.name,
        }


@enum.unique
class PGMigrateMethod(str, enum.Enum):
    dump = "dump"
    replication = "replication"


@enum.unique
class PGMigrateStatus(str, enum.Enum):
    cancelled = "cancelled"
    done = "done"
    failed = "failed"
    running = "running"


class PgDumpType(enum.Enum):
    schema = "S"
    data = "D"


class PgDumpStatus(enum.Enum):
    success = "success"
    with_warnings = "with_warnings"
    failed = "failed"


@dataclass
class PgDumpTask:
    status: PgDumpStatus
    type: PgDumpType
    dbname: str
    pg_dump_returncode: int
    pg_restore_returncode: int
    pg_restore_warnings: str | None


@dataclass
class PGMigrateResult:
    pg_databases: Dict[str, Any] = field(default_factory=dict)
    pg_roles: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PGMigrateTask:
    source_db: PGDatabase
    target_db: Optional[PGDatabase]
    error: Optional[BaseException] = None
    method: Optional[PGMigrateMethod] = None
    status: Optional[PGMigrateStatus] = None

    def result(self) -> Dict[str, Optional[str]]:
        dbname = self.source_db.dbname
        if self.error:
            message = str(self.error)
        elif self.target_db:
            message = "migrated to existing database"
        else:
            message = "created and migrated database"
        method = self.method.name if self.method else None
        status = self.status.name if self.status else None
        return {
            "dbname": dbname,
            "message": message,
            "method": method,
            "status": status,
        }
