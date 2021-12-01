# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.pgmigrate import PGCluster
from distutils.version import LooseVersion
from multiprocessing import Process
from test.conftest import PGRunner

import os
import pytest
import signal
import time


def test_interruptible_queries(pg_cluster: PGRunner):
    def wait_and_interrupt():
        time.sleep(1)
        os.kill(os.getppid(), signal.SIGINT)

    cluster = PGCluster(conn_info=pg_cluster.conn_info())
    interuptor = Process(target=wait_and_interrupt)
    interuptor.start()
    start_time = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        cluster.c("select pg_sleep(100)")
    assert time.monotonic() - start_time < 2
    interuptor.join()


def test_trusted_extensions(pg_cluster: PGRunner):
    # A small sample of built-in contrib extensions, no need to be exhaustive
    known_trusted = {"btree_gin", "btree_gist", "hstore", "intarray", "pgcrypto", "plpgsql", "unaccent"}
    known_untrusted = {"pg_buffercache", "pg_freespacemap", "pg_prewarm", "pg_stat_statements"}
    cluster = PGCluster(conn_info=pg_cluster.conn_info())
    if cluster.version >= LooseVersion("13"):
        for extension in cluster.pg_ext:
            assert isinstance(extension.trusted, bool)
            if extension.name in known_trusted:
                assert extension.trusted
            if extension.name in known_untrusted:
                assert not extension.trusted
    else:
        for extension in cluster.pg_ext:
            assert extension.trusted is None
