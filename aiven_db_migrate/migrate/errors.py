# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/


class PGDataNotFoundError(Exception):
    pass


class PGTooMuchDataError(Exception):
    pass


class PGMigrateValidationFailedError(Exception):
    pass


class PGDumpError(Exception):
    pass


class PGRestoreError(Exception):
    pass
