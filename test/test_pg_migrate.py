# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.errors import PGMigrateValidationFailedError
from aiven_db_migrate.migrate.models import DBMigrateResult, PGMigrateMethod, PGMigrateResult, PGMigrateStatus
from aiven_db_migrate.migrate.pgmigrate import PGMigrate
from test.utils import PGRunner, random_string, Timer

import os
import psycopg2
import pytest
import time


class PGMigrateTest:
    """Mixin for pg migration tests"""

    source: PGRunner
    target: PGRunner

    @staticmethod
    def assert_result(
        *,
        result: DBMigrateResult,
        status: PGMigrateStatus = PGMigrateStatus.done,
        migrate_method: PGMigrateMethod = PGMigrateMethod.replication_with_dump_fallback,
        has_error: bool = False,
        has_source_db_error: bool = False,
        has_target_db_error: bool = False,
        dump_schema_status: PGMigrateStatus | None = PGMigrateStatus.done,
        dump_data_status: PGMigrateStatus | None = PGMigrateStatus.done,
        replication_setup_status: PGMigrateStatus | None = PGMigrateStatus.done,
    ):
        # import pprint
        # pprint.pprint(result)

        assert result.status == status
        assert result.migrate_method == migrate_method
        assert result.error if has_error else not result.error
        assert result.source_db_error if has_source_db_error else not result.source_db_error
        assert result.target_db_error if has_target_db_error else not result.target_db_error

        if dump_schema_status is None:
            assert result.dump_schema_result is None
        else:
            assert result.dump_schema_result is not None
            assert result.dump_schema_result.status == dump_schema_status
            if result.dump_schema_result.status == PGMigrateStatus.failed:
                assert result.dump_schema_result.error
        if dump_data_status is None:
            assert result.dump_data_result is None
        else:
            assert result.dump_data_result is not None
            assert result.dump_data_result.status == dump_data_status
            if result.dump_data_result.status == PGMigrateStatus.failed:
                assert result.dump_data_result.error
        if replication_setup_status is None:
            assert result.replication_setup_result is None
        else:
            assert result.replication_setup_result is not None
            assert result.replication_setup_result.status == replication_setup_status
            if result.replication_setup_result.status == PGMigrateStatus.failed:
                assert result.replication_setup_result.error

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
            result = pg_mig.target.c(f"SELECT count(*) FROM public.{tblname}", dbname=dbname, return_rows=1)[0]
            if int(result["count"]) == count:
                break

    def _test_migrate(
        self, *, createdb: bool, migration_method: PGMigrateMethod, superuser: bool = False, number_of_dbs: int = 3
    ):
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

            if migration_method == PGMigrateMethod.replication and not superuser:
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
            verbose=True,
            migrate_method=migration_method,
        )

        result = pg_mig.migrate()
        for db_migrate_result in result.db_migrate_results:
            if migration_method == PGMigrateMethod.replication:
                dump_data_status = None
                replication_setup_status = PGMigrateStatus.done
            elif migration_method == PGMigrateMethod.dump:
                dump_data_status = PGMigrateStatus.done
                replication_setup_status = None
            elif migration_method == PGMigrateMethod.schema_only:
                dump_data_status = None
                replication_setup_status = None
            else:
                raise NotImplementedError()
            self.assert_result(
                result=db_migrate_result,
                migrate_method=migration_method,
                dump_data_status=dump_data_status,
                replication_setup_status=replication_setup_status
            )

            if db_migrate_result.dbname != "postgres":
                rows = 0 if migration_method == PGMigrateMethod.schema_only else 10
                self.wait_until_data_migrated(
                    pg_mig=pg_mig, dbname=db_migrate_result.dbname, tblname=f"{db_migrate_result.dbname}_tbl", count=rows
                )


@pytest.mark.usefixtures("pg_source_and_target")
class Test_PGMigrate(PGMigrateTest):

    # pylint: disable=no-member

    @pytest.mark.parametrize("createdb", [True, False])
    def test_migrate(self, createdb: bool):
        return self._test_migrate(createdb=createdb, migration_method=PGMigrateMethod.dump)

    @pytest.mark.parametrize("createdb", [True, False])
    def test_migrate_schema_only(self, createdb: bool):
        return self._test_migrate(createdb=createdb, migration_method=PGMigrateMethod.schema_only)

    def test_migrate_no_db(self):
        result = PGMigrate(source_conn_info=self.source.conn_info(), target_conn_info=self.target.conn_info()).migrate()
        assert len(result.db_migrate_results) == 1
        db_migrate_result = result.db_migrate_results[0]
        assert db_migrate_result.status == PGMigrateStatus.done
        assert db_migrate_result.error is None
        assert db_migrate_result.dbname == "postgres"

    def test_migrate_db_does_not_exist(self):
        dbname = "this_db_does_not_exist"
        for source_conn_info, target_conn_info in (
            (self.source.conn_info(dbname=dbname), self.target.conn_info()),
            (self.source.conn_info(), self.target.conn_info(dbname=dbname)),
        ):
            result = PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
            assert result.validation.status == PGMigrateStatus.failed
            assert type(result.validation.error) is PGMigrateValidationFailedError
            assert f'database "{dbname}" does not exist' in str(result.validation.error)

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

                result = PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
                assert result.validation.status == PGMigrateStatus.failed
                assert type(result.validation.error) is PGMigrateValidationFailedError
                assert str(result.validation.error) == f"Invalid source or target connection string"

    def test_migrate_connect_timeout_parameter(self):
        for source_conn_info in ("host=example.org connect_timeout=1", "postgresql://example.org?connect_timeout=1"):
            start_time = time.monotonic()
            result = PGMigrate(source_conn_info=source_conn_info, target_conn_info=self.target.conn_info()).migrate()
            assert result.validation.status == PGMigrateStatus.failed
            assert type(result.validation.error) is TimeoutError
            end_time = time.monotonic()
            assert end_time - start_time < 2

    def test_migrate_connect_timeout_environment(self):
        start_time = time.monotonic()
        original_timeout = os.environ.get("PGCONNECT_TIMEOUT")
        try:
            os.environ["PGCONNECT_TIMEOUT"] = "1"
            result = PGMigrate(source_conn_info="host=example.org", target_conn_info=self.target.conn_info()).migrate()
            assert result.validation.status == PGMigrateStatus.failed
            assert type(result.validation.error) is TimeoutError
            end_time = time.monotonic()
            assert end_time - start_time < 2
        finally:
            if original_timeout is not None:
                os.environ["PGCONNECT_TIMEOUT"] = original_timeout

    def test_migrate_same_server(self):
        source_conn_info = target_conn_info = self.target.conn_info()
        result = PGMigrate(source_conn_info=source_conn_info, target_conn_info=target_conn_info).migrate()
        assert result.validation.status == PGMigrateStatus.failed
        assert type(result.validation.error) is PGMigrateValidationFailedError
        assert str(result.validation.error) == "Migrating to the same server is not supported"

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
        result = pg_mig.migrate()
        assert result.validation.status == PGMigrateStatus.failed
        assert type(result.validation.error) is PGMigrateValidationFailedError
        assert str(result.validation.error) == "Languages not installed in target: bar, foo"

    def test_migrate_source_connection_rejected(self):
        dbname0 = "postgres"
        dbname1 = "db1_" + random_string()
        dbname2 = "db2_" + random_string()
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
        assert len(result.db_migrate_results) == 3

        for db_migrate_result in result.db_migrate_results:
            if db_migrate_result.dbname == dbname0:
                self.assert_result(
                    result=db_migrate_result, status=PGMigrateStatus.done, replication_setup_status=PGMigrateStatus.failed
                )
            elif db_migrate_result.dbname == dbname1:
                self.assert_result(
                    result=db_migrate_result,
                    status=PGMigrateStatus.failed,
                    has_source_db_error=True,
                    dump_data_status=None,
                    dump_schema_status=None,
                    replication_setup_status=None
                )
            elif db_migrate_result.dbname == dbname2:
                self.assert_result(
                    result=db_migrate_result, status=PGMigrateStatus.done, replication_setup_status=PGMigrateStatus.failed
                )
            else:
                raise ValueError(f"Unexpected dbname {db_migrate_result.dbname} in results")

    def test_migrate_target_connection_rejected(self):
        dbname0 = "postgres"
        dbname1 = "db1_" + random_string()
        dbname2 = "db2_" + random_string()
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
        result = pg_mig.migrate()
        assert len(result.db_migrate_results) == 3
        for db_migrate_result in result.db_migrate_results:
            if db_migrate_result.dbname == dbname0:
                self.assert_result(
                    result=db_migrate_result, status=PGMigrateStatus.done, replication_setup_status=PGMigrateStatus.failed
                )
            elif db_migrate_result.dbname == dbname1:
                self.assert_result(
                    result=db_migrate_result,
                    status=PGMigrateStatus.failed,
                    has_target_db_error=True,
                    dump_data_status=None,
                    dump_schema_status=None,
                    replication_setup_status=None
                )
            elif db_migrate_result.dbname == dbname2:
                self.assert_result(
                    result=db_migrate_result, status=PGMigrateStatus.done, replication_setup_status=PGMigrateStatus.failed
                )
            else:
                raise ValueError(f"Unexpected dbname {db_migrate_result.dbname} in results")

    def test_migrate_filtered_db_sql_injection(self):
        dbname1 = random_string()
        dbname2 = random_string()

        self.source.create_db(dbname=dbname1)
        self.source.create_db(dbname=dbname2)

        pg_mig = PGMigrate(
            source_conn_info=self.source.conn_info(),
            target_conn_info=self.target.conn_info(),
            createdb=True,
            verbose=True,
            filtered_db=f"{dbname1},') OR ('a' = 'a",
        )
        result: PGMigrateResult = pg_mig.migrate()
        assert len(result.db_migrate_results) == 2


@pytest.mark.usefixtures("pg_source_and_target")
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
        return self._test_migrate(createdb=False, migration_method=PGMigrateMethod.replication)

    @pytest.mark.parametrize("createdb", [True, False])
    def test_migrate_with_superuser(self, createdb: bool):
        return self._test_migrate(createdb=createdb, migration_method=PGMigrateMethod.replication, superuser=True)

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

        result = pg_mig.migrate()
        assert len(result.db_migrate_results) == 2
        for db_migrate_result in result.db_migrate_results:
            if db_migrate_result.dbname == dbname:
                self.assert_result(
                    result=db_migrate_result,
                    dump_data_status=None,
                    replication_setup_status=PGMigrateStatus.done,
                )
            else:
                self.assert_result(
                    result=db_migrate_result,
                    replication_setup_status=PGMigrateStatus.failed,
                )

        self.wait_until_data_migrated(pg_mig=pg_mig, dbname=dbname, tblname=tblname, count=3)

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

        result = pg_mig.migrate()
        assert len(result.db_migrate_results) == 2
        for db_migrate_result in result.db_migrate_results:
            self.assert_result(
                result=db_migrate_result,
                replication_setup_status=PGMigrateStatus.failed,
            )
        self.wait_until_data_migrated(pg_mig=pg_mig, dbname=dbname, tblname=tblname, count=3)

        # verify that there's no leftovers
        assert not self.source.list_pubs(dbname=dbname)
        assert not self.source.list_slots()
        assert not self.target.list_subs()
