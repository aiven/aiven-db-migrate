# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
import enum


class PGDataNotFoundError(Exception):
    pass


class PGTooMuchDataError(Exception):
    pass


class PGSchemaDumpFailedError(Exception):
    pass


class PGDataDumpFailedError(Exception):
    pass


# this is currently use to support a very specific error, but should be employed
# any time we launch an exception, to let the user have a better understanding
# about what has happened.
@enum.unique
class PGMigrateFailureReason(enum.Enum):
    cannot_migrate_to_older_server_version = "cannot_migrate_to_older_server_version"
    unknown = "unknown"


class PGMigrateValidationFailedError(Exception):
    def __init__(self, message, *, reason: PGMigrateFailureReason = PGMigrateFailureReason.unknown):
        super().__init__(message)
        self.reason = reason
