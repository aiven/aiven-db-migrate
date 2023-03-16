from pkgutil import extend_path
# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.pgmigrate import PGMigrate, PGMigrateResult  # noqa


__path__ = extend_path(__path__, __name__)  # type: ignore
