# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/

from aiven.migrate.pgutils import find_pgbin_dir, validate_pg_identifier_length
from test.utils import random_string

import pytest


def test_find_pgbin_dir():
    for pgversion in ("9.5", "9.6", "10", "10.4", "11", "11.7", "12", "12.2"):
        find_pgbin_dir(pgversion)
    for pgversion in ("12345", "12345.6"):
        with pytest.raises(ValueError) as err:
            find_pgbin_dir(pgversion)
        assert f"Couldn't find pg version '{pgversion}' bin dir" in str(err)


def test_validate_pg_identifier_length():
    validate_pg_identifier_length(random_string(length=63))
    ident = random_string(length=64)
    with pytest.raises(ValueError) as err:
        validate_pg_identifier_length(ident)
    assert str(err.value) == f"PostgreSQL max identifier length is 63, len('{ident}') = 64"
