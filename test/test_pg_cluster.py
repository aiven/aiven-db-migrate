# Copyright (c) 2021 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.pgmigrate import PGCluster
from multiprocessing import Process
from test.conftest import PGRunner
from typing import Tuple

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
