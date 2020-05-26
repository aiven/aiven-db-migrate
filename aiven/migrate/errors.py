# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/


class PGDataNotFoundError(Exception):
    pass


class PGTooMuchDataError(Exception):
    pass


class PGSchemaDumpFailedError(Exception):
    pass


class PGDataDumpFailedError(Exception):
    pass


class PGMigrateValidationFailedError(Exception):
    pass
