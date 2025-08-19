# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.migrate.models import PGDatabase, PGExtension
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from packaging.version import Version
from test.utils import PGRunner, random_string
from typing import Tuple

import pytest


@pytest.mark.parametrize("createdb", [True, False])
def test_defaults(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbnames = {random_string() for _ in range(3)}

    for dbname in dbnames:
        source.create_db(dbname=dbname)
        if not createdb:
            # create existing db to target
            target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    pg_mig.validate()
    for dbname in dbnames:
        pg_mig._dump_schema(db=PGDatabase(dbname=dbname, tables=set()))  # pylint: disable=protected-access

    # reset databases so that they and installed extensions get queried from server again
    setattr(pg_mig.target, "_databases", {})

    for dbname in dbnames:
        for ext1 in pg_mig.source.databases[dbname].pg_ext:
            ext2 = next(e for e in pg_mig.target.databases[dbname].pg_ext if e.name == ext1.name)
            assert Version(ext2.version) >= Version(ext1.version)


@pytest.mark.parametrize("createdb", [True, False])
def test_extension_requires_superuser(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    extname = "pg_stat_statements"

    source.create_db(dbname=dbname)
    source.create_extension(extname=extname, dbname=dbname)
    if not createdb:
        # create existing db to target
        target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    with pytest.raises(PGMigrateValidationFailedError) as err:
        pg_mig.validate()
    assert str(err.value) == f"Installing extension '{extname}' in target requires superuser"


def test_migration_succeeds_when_extensions_that_require_superuser_are_excluded(
    pg_source_and_target: Tuple[PGRunner, PGRunner]
) -> None:
    source, target = pg_source_and_target
    dbname = random_string()
    extensions = {"pg_freespacemap", "pg_visibility"}

    source.create_db(dbname=dbname)
    for extname in extensions:
        source.create_extension(extname=extname, dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(),
        target_conn_info=target.conn_info(),
        verbose=True,
        excluded_extensions=",".join(extensions),
    )
    assert set(pg_mig.target.excluded_extensions) == extensions

    pg_mig.validate()


@pytest.mark.parametrize("createdb", [True, False])
def test_extension_superuser(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    extname = "aiven_extras"

    source.create_db(dbname=dbname)
    source.create_extension(extname=extname, dbname=dbname)
    if not createdb:
        # create existing db to target
        target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.super_conn_info(),
        target_conn_info=target.super_conn_info(),
        createdb=createdb,
        verbose=True
    )

    pg_mig.validate()
    pg_mig._dump_schema(db=PGDatabase(dbname=dbname, tables=set()))  # pylint: disable=protected-access

    # reset databases so that they and installed extensions get queried from server again
    setattr(pg_mig.target, "_databases", {})

    for ext1 in pg_mig.source.databases[dbname].pg_ext:
        ext2 = next(e for e in pg_mig.target.databases[dbname].pg_ext if e.name == ext1.name)
        assert Version(ext2.version) >= Version(ext1.version)


@pytest.mark.parametrize("createdb", [True, False])
def test_extension_whitelist(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbnames = {random_string() for _ in range(3)}
    extnames = {"btree_gist", "pgcrypto", "postgis"}

    for dbname in dbnames:
        source.create_db(dbname=dbname)
        for extname in extnames:
            source.create_extension(extname=extname, dbname=dbname)
        if not createdb:
            # create existing db to target
            target.create_db(dbname=dbname)

    # whitelist extensions in target
    target.make_conf(**{"extwlist.extensions": "'{}'".format(",".join(extnames))}).reload()

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    pg_mig.validate()
    for dbname in dbnames:
        pg_mig._dump_schema(db=PGDatabase(dbname=dbname, tables=set()))  # pylint: disable=protected-access

    # reset databases so that they and installed extensions get queried from server again
    setattr(pg_mig.target, "_databases", {})

    for dbname in dbnames:
        for ext1 in pg_mig.source.databases[dbname].pg_ext:
            ext2 = next(e for e in pg_mig.target.databases[dbname].pg_ext if e.name == ext1.name)
            assert Version(ext2.version) >= Version(ext1.version)


@pytest.mark.parametrize("createdb", [True, False])
def test_extension_not_available(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    extname = "this_extension_is_not_available_in_target"

    if not createdb:
        # create existing db to target
        target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    # mock source databases
    setattr(
        pg_mig.source, "_databases",
        {dbname: PGDatabase(dbname=dbname, tables=set(), pg_ext=[PGExtension(name=extname, version="1.2.3")])}
    )

    with pytest.raises(PGMigrateValidationFailedError) as err:
        pg_mig.validate()
    assert str(err.value) == f"Extension '{extname}' is not available for installation in target"


@pytest.mark.parametrize("createdb", [True, False])
def test_extension_available_older_version(pg_source_and_target: Tuple[PGRunner, PGRunner], createdb: bool):
    source, target = pg_source_and_target
    dbname = random_string()
    extname = "pgcrypto"

    if not createdb:
        # create existing db to target
        target.create_db(dbname=dbname)

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=createdb, verbose=True
    )

    # mock source databases
    setattr(
        pg_mig.source, "_databases",
        {dbname: PGDatabase(dbname=dbname, tables=set(), pg_ext=[PGExtension(name=extname, version="999999")])}
    )

    with pytest.raises(PGMigrateValidationFailedError) as err:
        pg_mig.validate()
    assert f"Extension '{extname}' version available for installation in target is too old" in str(err.value)


def test_extension_installed_older_version(pg_source_and_target: Tuple[PGRunner, PGRunner]):
    source, target = pg_source_and_target
    dbname = random_string()
    extname = "some_cool_extension_name"
    source_ver = "999999"
    target_ver = "999998"

    pg_mig = PGMigrate(
        source_conn_info=source.conn_info(), target_conn_info=target.conn_info(), createdb=False, verbose=True
    )

    # mock source and target databases
    setattr(
        pg_mig.source, "_databases",
        {dbname: PGDatabase(dbname=dbname, tables=set(), pg_ext=[PGExtension(name=extname, version=source_ver)])}
    )
    setattr(
        pg_mig.target, "_databases",
        {dbname: PGDatabase(dbname=dbname, tables=set(), pg_ext=[PGExtension(name=extname, version=target_ver)])}
    )

    with pytest.raises(PGMigrateValidationFailedError) as err:
        pg_mig.validate()
    assert str(err.value) == (
        f"Installed extension '{extname}' in target database '{dbname}' is older than in source, "
        f"target version: {target_ver}, source version: {source_ver}"
    )
