# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from distutils.version import LooseVersion
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

import psycopg2
import re
import select
import time


def find_pgbin_dir(pgversion: str, *, max_pgversion: Optional[str] = None, usr_dir: Path = Path("/usr")) -> Path:
    """
    Returns an existing pgbin directory with a version equal to `pgversion`.

    If `max_pgversion` is specified, returns the oldest existing pgbin directory with a version between
    `pgversion` and `max_pgversion` included.

    Versions equal or above 10 only check the major version number: 10, 11, 12, 13...
    """
    min_version = LooseVersion(pgversion).version
    max_version = min_version if max_pgversion is None else LooseVersion(max_pgversion).version
    max_version = max(max_version, min_version)
    max_parts = 1
    candidates = []
    search_scopes = [(usr_dir, r"pgsql-([0-9]+(\.[0-9]+)*)"), (usr_dir / "lib/postgresql", r"([0-9]+(\.[0-9]+)*)")]
    for base_dir, pattern in search_scopes:
        if base_dir.is_dir():
            for path in base_dir.iterdir():
                match = re.search(pattern, path.name)
                bin_path = path / "bin"
                if match and bin_path.is_dir():
                    candidate_version = LooseVersion(match.group(1)).version
                    if min_version[:max_parts] <= candidate_version[:max_parts] <= max_version[:max_parts]:
                        candidates.append((candidate_version, bin_path))
    candidates.sort()
    if candidates:
        return candidates[0][1]
    search_scope_description = [str(search_scope[0] / search_scope[1] / "bin") for search_scope in search_scopes]
    if max_pgversion is not None:
        raise ValueError(
            "Couldn't find bin dir for any pg version between {!r} and {!r}, tried {!r}".format(
                pgversion, max_pgversion, search_scope_description
            )
        )
    else:
        raise ValueError("Couldn't find bin dir for pg version {!r}, tried {!r}".format(pgversion, search_scope_description))


def validate_pg_identifier_length(ident: str):
    length = len(ident)
    if length > 63:
        raise ValueError(f"PostgreSQL max identifier length is 63, len({ident!r}) = {length}")


def create_connection_string(conn_info: Dict[str, Any]) -> str:
    return " ".join("{}='{}'".format(k, str(v).replace("'", "\\'")) for k, v in sorted(conn_info.items()) if v)


def get_connection_info(info) -> Dict[str, Any]:
    """
    Turn a connection info into a dict or return it if it was a dict already.
    Supports both the traditional libpq format and postgres:// uri format.
    """
    if isinstance(info, dict):
        return info.copy()
    elif info.startswith("postgres://") or info.startswith("postgresql://"):
        return parse_connection_string_url(info)
    else:
        return parse_connection_string_libpq(info)


def parse_connection_string_libpq(connection_string: str) -> Dict[str, Any]:
    """
    Parse a postgresql connection string as defined in
    http://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
    """
    fields = {}
    while True:
        connection_string = connection_string.strip()
        if not connection_string:
            break
        if "=" not in connection_string:
            raise ValueError("expecting key=value format in connection_string fragment {!r}".format(connection_string))
        key, rem = connection_string.split("=", 1)
        if rem.startswith("'"):
            asis, value = False, ""
            for i in range(1, len(rem)):
                if asis:
                    value += rem[i]
                    asis = False
                elif rem[i] == "'":
                    break  # end of entry
                elif rem[i] == "\\":
                    asis = True
                else:
                    value += rem[i]
            else:
                raise ValueError("invalid connection_string fragment {!r}".format(rem))
            connection_string = rem[i + 1:]  # pylint: disable=undefined-loop-variable
        else:
            res = rem.split(None, 1)
            if len(res) > 1:
                value, connection_string = res
            else:
                value, connection_string = rem, ""
        fields[key] = value
    return fields


def parse_connection_string_url(url: str) -> Dict[str, str]:
    if "://" not in url:
        url = f"http://{url}"
    p = urlparse(url)
    fields = {}
    if p.hostname:
        fields["host"] = p.hostname
    if p.port:
        fields["port"] = str(p.port)
    if p.username:
        fields["user"] = p.username
    if p.password is not None:
        fields["password"] = p.password
    if p.path and p.path != "/":
        fields["dbname"] = p.path[1:]
    for k, v in parse_qs(p.query).items():
        fields[k] = v[-1]
    return fields


# This enables interruptible queries with an approach similar to
# https://www.psycopg.org/docs/faq.html#faq-interrupt-query
# However, to handle timeouts we can't use psycopg2.extensions.set_wait_callback :
# https://github.com/psycopg/psycopg2/issues/944
# Instead we rely on manually calling wait_select after connection and queries.
# Since it's not a wait callback, we do not capture and transform KeyboardInterupt here.
def wait_select(conn, timeout=None):
    start_time = time.monotonic()
    poll = select.poll()
    while True:
        if timeout is not None and timeout > 0:
            time_left = start_time + timeout - time.monotonic()
            if time_left <= 0:
                raise TimeoutError("wait_select: timeout after {} seconds".format(timeout))
        else:
            time_left = 1
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            return
        elif state == psycopg2.extensions.POLL_READ:
            poll.register(conn.fileno(), select.POLLIN)
        elif state == psycopg2.extensions.POLL_WRITE:
            poll.register(conn.fileno(), select.POLLOUT)
        else:
            raise conn.OperationalError("wait_select: invalid poll state")
        try:
            # When the remote address does not exist at all, poll.poll() waits its full timeout without any event.
            # However, in the same conditions, conn.poll() raises a psycopg2 exception almost immediately.
            # It is better to fail quickly instead of waiting the full timeout, so we keep our poll.poll() below 1sec.
            poll.poll(min(1.0, time_left) * 1000)
        finally:
            poll.unregister(conn.fileno())
