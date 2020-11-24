# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.migrate.pgmigrate import PGMigrate, PGMigrateResult
from test.conftest import PGRunner
from test.utils import random_string, Timer
from typing import Any, Dict, Optional

import psycopg2
import pytest


class PGMigrateTest:
    """Mixin for pg migration tests"""

    source: PGRunner
    target: PGRunner

    @staticmethod
    def assert_result(
        *,
        result: Dict[str, Any],
        dbname: str,
        method: Optional[str],
        message: str = None,
        error: str = None,
        status: str = "done"
    ):
        assert message or error
        assert result["dbname"] == dbname
        if message:
            assert result["message"] == message
        else:
            assert error in result["message"]
        assert result["method"] == method
        assert result["status"] == status
        for field in result:
            assert field in {"dbname", "message", "method", "status"}

    @staticmethod
    def wait_until_data_migrated(*, pg_mig: PGMigrate, dbname: str, tblname: str, count: int, timeout: float = 10):
        exists = pg_mig.target.c(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name= %s",
            args=(
                "public",
                tblname,
            ),
            dbname=dbname
        )
        assert exists
        timer = Timer(timeout=timeout, what=f"all data replicated to {dbname} table {tblname}")
        while timer.loop():
            result = pg_mig.target.c(f"SELECT count(*) FROM {tblname}", dbname=dbname, return_rows=1)[0]
            if int(result["count"]) == count:
                break

    def _test_migrate(self, *, createdb: bool, expected_method: str, superuser: bool = False, number_of_dbs: int = 3):
        dbnames = [f"test_migrate_db_{i + 1}" for i in range(number_of_dbs)]
        for dbname in dbnames:
            # create db and table with some data in source
            self.source.create_db(dbname=dbname)
            with self.source.cursor(dbname=dbname) as cur:
                cur.execute(f"CREATE TABLE {dbname}_tbl (something INT)")
                cur.execute(f"INSERT INTO {dbname}_tbl VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")

            if not createdb:
                # create existing db to target
                self.target.create_db(dbname=dbname)

            if expected_method == "replication" and not superuser:
                # we need existing db for installing aiven-extras
                assert not createdb
                # have aiven-extras in both source and target
                if not self.source.have_aiven_extras(dbname=dbname) or not self.target.have_aiven_extras(dbname=dbname):
                    pytest.skip("aiven-extras not available")

        if superuser:
            source_conn_info = self.source.super_conn_info()
            target_conn_info = self.target.super_conn_info()
        else:
            source_conn_info = self.source.conn_info()
            target_conn_info = self.target.conn_info()

        pg_mig = PGMigrate(
            source_conn_info=source_conn_info,
            target_conn_info=target_conn_info,
            createdb=createdb,
            max_replication_lag=0,
            stop_replication=True,
            verbose=True
        )

        result: PGMigrateResult = pg_mig.migrate()

        for dbname, r in result.pg_databases.items():
            if dbname == "postgres":
                # default db
                self.assert_result(result=r, dbname=dbname, method=expected_method, message="migrated to existing database")
            else:
                dbnames.remove(dbname)
                self.assert_result(
                    result=r,
                    dbname=dbname,
                    method=expected_method,
                    message="created and migrated database" if createdb else "migrated to existing database"
                )
                self.wait_until_data_migrated(pg_mig=pg_mig, dbname=dbname, tblname=f"{dbname}_tbl", count=10)

        assert not dbnames


@pytest.mark.usefixtures("pg_source_and_target")
class Test_PGMigrate(PGMigrateTest):

    # pylint: disable=no-member

    @pytest.mark.parametrize("createdb", [True, False])
    def test_migrate(self, createdb: bool):
        return self._test_migrate(createdb=createdb, expected_method="dump")

    def test_migrate_no_db(self):
        result: PGMigrateResult = PGMigrate(
            source_conn_info=self.source.conn_info(), target_conn_info=self.target.conn_info()
        ).migrate()
        assert len(result.pg_databases) == 1
        # default db
        assert "postgres" in result.pg_databases

    def test_migrate_db_does_not_exist(self):
        dbname = "this_db_does_not_exist"
        for source_conn_info, target_conn_info in (
            (self.source.conn_info(dbname=dbname), self.target.conn_info()),
            (self.source.conn_info(), self.target.conn_info(dbname=dbname)),
        ):
            with pytest.raises(PGMigrateValidationFailedError) as err:
                PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
            assert f'database "{dbname}" does not exist' in str(err.value)

    def test_migrate_invalid_conn_str(self):
        for source_conn_info, target_conn_info in (
            (None, self.target.conn_info()),
            (self.source.conn_info(), None),
        ):
            assert not (source_conn_info and target_conn_info)
            for conn_info in (
                "postgres://",
                "foo=bar",
            ):
                if source_conn_info is None:
                    source_conn_info = conn_info
                else:
                    target_conn_info = conn_info
                with pytest.raises(PGMigrateValidationFailedError) as err:
                    PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
                assert str(err.value) == "Invalid source or target connection string"

    def test_migrate_same_server(self):
        source_conn_info = target_conn_info = self.target.conn_info()
        with pytest.raises(PGMigrateValidationFailedError) as err:
            PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
        assert str(err.value) == "Migrating to the same server is not supported"

    def test_migrate_missing_languages(self):
        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(), target_conn_info=self.target.conn_info(), createdb=True, verbose=True
        )
        # mock source/target languages
        setattr(pg_mig.source, "_pg_lang", [
            {"lanname": "c"},
            {"lanname": "plpgsql"},
            {"lanname": "sql"},
            {"lanname": "foo"},
            {"lanname": "bar"},
        ])  # yapf: disable
        setattr(pg_mig.target, "_pg_lang", [
            {"lanname": "c"},
            {"lanname": "plpgsql"},
            {"lanname": "sql"},
        ])  # yapf: disable
        with pytest.raises(PGMigrateValidationFailedError) as err:
            pg_mig.migrate()
        assert str(err.value) == "Languages not installed in target: bar, foo"

    def test_migrate_source_connection_rejected(self):
        dbname0 = "postgres"
        dbname1 = random_string()
        dbname2 = random_string()
        # create db's in source
        self.source.create_db(dbname=dbname1)
        self.source.create_db(dbname=dbname2)

        user = self.source.testuser
        self.source.make_hba_conf(dbname=dbname1, user=user, auth="reject").reload()
        # verify that user is rejected for dbname1
        error = f'pg_hba.conf rejects connection for host "[local]", user "{user}", database "{dbname1}"'
        with pytest.raises(psycopg2.OperationalError) as err:
            with self.source.cursor(dbname=dbname1):
                pass
        assert error in str(err.value)

        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(), target_conn_info=self.target.conn_info(), createdb=True, verbose=True
        )

        result: PGMigrateResult = pg_mig.migrate()
        assert len(result.pg_databases) == 3
        self.assert_result(
            result=result.pg_databases[dbname0], dbname=dbname0, method="dump", message="migrated to existing database"
        )
        self.assert_result(result=result.pg_databases[dbname1], dbname=dbname1, method=None, error=error, status="failed")
        self.assert_result(
            result=result.pg_databases[dbname2], dbname=dbname2, method="dump", message="created and migrated database"
        )

    def test_migrate_target_connection_rejected(self):
        dbname0 = "postgres"
        dbname1 = random_string()
        dbname2 = random_string()
        # create db's in source
        self.source.create_db(dbname=dbname1)
        self.source.create_db(dbname=dbname2)
        # create db in target
        self.target.create_db(dbname=dbname1)

        user = self.target.testuser
        self.target.make_hba_conf(dbname=dbname1, user=user, auth="reject").reload()
        # verify that user is rejected for dbname1
        error = f'pg_hba.conf rejects connection for host "[local]", user "{user}", database "{dbname1}"'
        with pytest.raises(psycopg2.OperationalError) as err:
            with self.target.cursor(dbname=dbname1):
                pass
        assert error in str(err.value)

        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(), target_conn_info=self.target.conn_info(), createdb=True, verbose=True
        )

        result: PGMigrateResult = pg_mig.migrate()
        assert len(result.pg_databases) == 3
        self.assert_result(
            result=result.pg_databases[dbname0], dbname=dbname0, method="dump", message="migrated to existing database"
        )
        self.assert_result(result=result.pg_databases[dbname1], dbname=dbname1, method=None, error=error, status="failed")
        self.assert_result(
            result=result.pg_databases[dbname2], dbname=dbname2, method="dump", message="created and migrated database"
        )


@pytest.mark.usefixtures("pg_source_and_target_replication")
class Test_PGMigrate_Replication(PGMigrateTest):

    # pylint: disable=no-member

    def test_migrate_with_aiven_extras(self):
        # default db
        dbname = "postgres"
        if not self.source.have_aiven_extras(dbname=dbname) or not self.target.have_aiven_extras(dbname=dbname):
            pytest.skip("aiven-extras not available")
        self.source.add_cleanup(lambda: self.source.drop_aiven_extras(dbname=dbname))
        self.target.add_cleanup(lambda: self.target.drop_aiven_extras(dbname=dbname))
        # we need to have existing db for installing aiven-extras
        return self._test_migrate(createdb=False, expected_method="replication")

    @pytest.mark.parametrize("createdb", [True, False])
    def test_migrate_with_superuser(self, createdb: bool):
        return self._test_migrate(createdb=createdb, expected_method="replication", superuser=True)

    @pytest.mark.parametrize("createdb", [True])
    def test_migrate_source_aiven_extras(self, createdb: bool):
        dbname = random_string()
        tblname = f"{dbname}_tbl"
        # create db in source
        self.source.create_db(dbname=dbname)
        # have aiven-extras in source
        if not self.source.have_aiven_extras(dbname=dbname):
            pytest.skip("aiven-extras not available in source")
        # create some data in db
        with self.source.cursor(dbname=dbname) as cur:
            cur.execute(f"CREATE TABLE {tblname} (something INT)")
            cur.execute(f"INSERT INTO {tblname} VALUES (1), (2), (3)")

        # whitelist aiven-extras in target
        extnames = {"aiven_extras", "dblink"}
        self.target.make_conf(**{"extwlist.extensions": "'{}'".format(",".join(extnames))}).reload()

        if not createdb:
            # create existing db to target, since we are also doing this automatically and failing silently in the tool
            # the expected method will always be replication
            self.target.create_db(dbname=dbname)

        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(),
            target_conn_info=self.target.conn_info(),
            createdb=createdb,
            max_replication_lag=0,
            stop_replication=True,
            verbose=True
        )

        result: PGMigrateResult = pg_mig.migrate()

        assert len(result.pg_databases) == 2
        self.assert_result(
            result=result.pg_databases[dbname],
            dbname=dbname,
            method="replication",
            message="created and migrated database" if createdb else "migrated to existing database"
        )
        self.wait_until_data_migrated(pg_mig=pg_mig, dbname=dbname, tblname=tblname, count=3)
        # default db
        self.assert_result(
            result=result.pg_databases["postgres"],
            dbname="postgres",
            method="dump",
            message="migrated to existing database"
        )

        # verify that there's no leftovers
        assert not self.source.list_pubs(dbname=dbname)
        assert not self.source.list_slots()
        assert not self.target.list_subs()

    def test_migrate_target_aiven_extras(self):
        dbname = random_string()
        tblname = f"{dbname}_tbl"
        # create existing db to target
        self.target.create_db(dbname=dbname)
        # have aiven-extras in target
        if not self.target.have_aiven_extras(dbname=dbname):
            pytest.skip("aiven-extras not available in target")
        # create db in source
        self.source.create_db(dbname=dbname)
        # create some data in db
        with self.source.cursor(dbname=dbname) as cur:
            cur.execute(f"CREATE TABLE {tblname} (something INT)")
            cur.execute(f"INSERT INTO {tblname} VALUES (1), (2), (3)")

        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(),
            target_conn_info=self.target.conn_info(),
            createdb=False,
            max_replication_lag=0,
            stop_replication=True,
            verbose=True
        )

        result: PGMigrateResult = pg_mig.migrate()

        assert len(result.pg_databases) == 2
        self.assert_result(
            result=result.pg_databases[dbname], dbname=dbname, method="dump", message="migrated to existing database"
        )
        self.wait_until_data_migrated(pg_mig=pg_mig, dbname=dbname, tblname=tblname, count=3)
        # default db
        self.assert_result(
            result=result.pg_databases["postgres"],
            dbname="postgres",
            method="dump",
            message="migrated to existing database"
        )

        # verify that there's no leftovers
        assert not self.source.list_pubs(dbname=dbname)
        assert not self.source.list_slots()
        assert not self.target.list_subs()
