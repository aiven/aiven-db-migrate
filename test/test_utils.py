# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven_db_migrate.migrate.pgutils import find_pgbin_dir, validate_pg_identifier_length
from pathlib import Path
from test.utils import random_string

import pytest
import tempfile


def test_pgbin_dir_exists_for_supported_versions():
    for pgversion in ("13", "14", "15", "16", "17"):
        find_pgbin_dir(pgversion)
    for pgversion in ("12345", "12345.6"):
        with pytest.raises(ValueError, match=f"Couldn't find bin dir for pg version '{pgversion}'.*"):
            find_pgbin_dir(pgversion)


@pytest.mark.parametrize("pg_dir", ["pgsql-12", "pgsql-12.4", "lib/postgresql/12", "lib/postgresql/12.4"])
def test_find_pgbin_dir(pg_dir):
    with tempfile.TemporaryDirectory() as temp_dir:
        usr_dir = Path(temp_dir)
        pgbin_12 = usr_dir / pg_dir / "bin"
        pgbin_12.mkdir(parents=True)
        with pytest.raises(ValueError):
            find_pgbin_dir("10", usr_dir=usr_dir)
        with pytest.raises(ValueError):
            find_pgbin_dir("10", max_pgversion="11", usr_dir=usr_dir)
        assert find_pgbin_dir("10", max_pgversion="12", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("10", max_pgversion="13", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("12", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("12", max_pgversion="13", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("12.2", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("12.2", max_pgversion="13", usr_dir=usr_dir) == pgbin_12
        with pytest.raises(ValueError):
            assert find_pgbin_dir("13", usr_dir=usr_dir)
        with pytest.raises(ValueError):
            assert find_pgbin_dir("13", max_pgversion="14", usr_dir=usr_dir)


def test_find_pgbin_dir_prefers_oldest():
    with tempfile.TemporaryDirectory() as temp_dir:
        usr_dir = Path(temp_dir)
        pgbin_12 = usr_dir / "pgsql-12/bin"
        pgbin_12.mkdir(parents=True)
        pgbin_13 = usr_dir / "pgsql-13/bin"
        pgbin_13.mkdir(parents=True)
        assert find_pgbin_dir("10", max_pgversion="13", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("11", max_pgversion="13", usr_dir=usr_dir) == pgbin_12
        assert find_pgbin_dir("12", max_pgversion="13", usr_dir=usr_dir) == pgbin_12


def test_validate_pg_identifier_length():
    validate_pg_identifier_length(random_string(length=63))
    ident = random_string(length=64)
    with pytest.raises(ValueError) as err:
        validate_pg_identifier_length(ident)
    assert str(err.value) == f"PostgreSQL max identifier length is 63, len('{ident}') = 64"
