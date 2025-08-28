# Copyright (c) 2020 Aiven, Helsinki, Finland. https://aiven.io/
from aiven_db_migrate.migrate.errors import PGDumpError, PGRestoreError
from aiven_db_migrate.migrate.models import DumpTaskResult, DumpType, PGMigrateStatus
from collections import deque
from packaging.version import Version
from pathlib import Path
from typing import Any, Deque, Dict, IO, Optional
from urllib.parse import parse_qs, urlparse

import logging
import psycopg2
import re
import select
import subprocess
import threading
import time

logger = logging.getLogger("PGMigrateUtils")


def find_pgbin_dir(pgversion: str, *, max_pgversion: Optional[str] = None, usr_dir: Path = Path("/usr")) -> Path:
    """
    Returns an existing pgbin directory with a version equal to `pgversion`.

    If `max_pgversion` is specified, returns the oldest existing pgbin directory with a version between
    `pgversion` and `max_pgversion` included.

    Versions equal or above 10 only check the major version number: 10, 11, 12, 13...
    """
    min_version = list(Version(pgversion).release)
    max_version = min_version if max_pgversion is None else list(Version(max_pgversion).release)
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
                    candidate_version = list(Version(match.group(1)).release)
                    if min_version[:max_parts] <= candidate_version[:max_parts] <= max_version[:max_parts]:
                        candidates.append((candidate_version, bin_path))
    candidates.sort()
    if candidates:
        return candidates[0][1]
    search_scope_description = [str(search_scope[0] / search_scope[1] / "bin") for search_scope in search_scopes]
    if max_pgversion is not None:
        raise ValueError(
            f"Couldn't find bin dir for any pg version between {pgversion!r} and {max_pgversion!r}, "
            f"tried {search_scope_description!r}"
        )
    raise ValueError(f"Couldn't find bin dir for pg version {pgversion!r}, tried {search_scope_description!r}")


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
            raise ValueError(f"expecting key=value format in connection_string fragment {connection_string!r}")
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
                raise ValueError(f"invalid connection_string fragment {rem!r}")
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
                raise TimeoutError(f"wait_select: timeout after {timeout} seconds")
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


def log_stream(
    logger_obj: logging.Logger, stream: IO[bytes] | None, label: str, log_level: int, buffer: deque | None = None
):
    if stream is None:
        logger_obj.warning("log_stream called with None stream, label: %s", label)
        return
    msg_template = f"{label} %s"
    for line in stream:
        decoded_line = line.decode(errors="replace").strip()
        if decoded_line:
            logger_obj.log(log_level, msg_template, decoded_line)
            if buffer is not None:
                buffer.append(decoded_line)
    stream.close()


def run_pg_dump_pg_restore(
    *, pg_dump_cmd: list[str], pg_restore_cmd: list[str], dbname: str, pg_dump_type: DumpType
) -> DumpTaskResult:
    pg_dump = subprocess.Popen(pg_dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pg_restore = subprocess.Popen(pg_restore_cmd, stdin=pg_dump.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # allow pg_dump to receive a SIGPIPE if pg_restore exists
    if pg_dump.stdout:
        pg_dump.stdout.close()

    pg_dump_log_label = f"[pg_dump({pg_dump.pid})][{pg_dump_type.value}][{dbname}]"
    pg_restore_log_label = f"[pg_restore({pg_restore.pid})][{pg_dump_type.value}][{dbname}]"

    # We need to detect a log entry in pg_restore indicating that the restore has completed but with errors.
    # It writes this at the end, so we're saving its last log entries.
    pg_restore_log_buffer: Deque[str] = deque(maxlen=10)
    threads = [
        threading.Thread(
            target=log_stream, daemon=True, args=(logger, pg_dump.stderr, pg_dump_log_label, logging.WARNING, None)
        ),
        threading.Thread(
            target=log_stream,
            daemon=True,
            args=(logger, pg_restore.stderr, pg_restore_log_label, logging.WARNING, pg_restore_log_buffer)
        ),
        threading.Thread(
            target=log_stream, daemon=True, args=(logger, pg_restore.stdout, pg_restore_log_label, logging.INFO, None)
        )
    ]
    for t in threads:
        t.start()

    tracked_proc = {pg_dump_log_label: pg_dump, pg_restore_log_label: pg_restore}
    while tracked_proc:
        for proc_log_label, proc in list(tracked_proc.items()):
            try:
                retcode = proc.wait(timeout=120)
                logger.info("Process %s exited with code %s", proc_log_label, retcode)
                tracked_proc.pop(proc_log_label)
            except subprocess.TimeoutExpired:
                logger.info("Process %s is still running...", proc_log_label)

    for t in threads:
        t.join(timeout=10)
        if t.is_alive():
            logger.warning("Logs reading thread %s did not finish in time, it may still be running", t.name)

    pg_restore_errors_warning = None
    for log_line in pg_restore_log_buffer:
        if 'errors ignored on restore' in log_line:
            pg_restore_errors_warning = log_line

    error: Exception | None = None
    if pg_dump.returncode != 0:
        status = PGMigrateStatus.failed
        error = PGDumpError(f"{pg_dump_log_label} failed. Check the logs for details.")
    else:
        if pg_restore.returncode == 0:
            status = PGMigrateStatus.done
        elif pg_restore.returncode != 0 and pg_restore_errors_warning:
            status = PGMigrateStatus.done
        else:
            status = PGMigrateStatus.failed
            error = PGRestoreError(f"{pg_restore_log_label} failed. Check the logs for details.")

    return DumpTaskResult(
        status=status,
        type=pg_dump_type,
        error=error,
        pg_dump_returncode=pg_dump.returncode,
        pg_restore_returncode=pg_restore.returncode,
        pg_restore_warnings=pg_restore_errors_warning,
    )


def build_pg_restore_cmd(pgbin: Path, conn_str: str, createdb: bool = False, verbose: bool = False) -> list[str]:
    pg_restore_cmd = [
        str(pgbin / "pg_restore"),
        "-d",
        conn_str,
    ]
    if verbose:
        pg_restore_cmd.append("--verbose")
    if createdb:
        pg_restore_cmd.append("--create")
    return pg_restore_cmd


def build_pg_dump_cmd(
    pgbin: Path,
    conn_str: str,
    extensions: list[str] | None = None,
    tables: list[str] | None = None,
    schema_only: bool = False,
    data_only: bool = False,
    no_owner: bool = False,
    verbose: bool = False,
) -> list[str]:
    pg_dump_cmd = [
        str(pgbin / "pg_dump"),
        "-Fc",
        conn_str,
    ]
    if verbose:
        pg_dump_cmd.append("--verbose")
    if no_owner:
        pg_dump_cmd.append("--no-owner")
    if schema_only:
        pg_dump_cmd.append("--schema-only")
    if data_only:
        pg_dump_cmd.append("--data-only")
    if extensions:
        for ext in extensions:
            pg_dump_cmd.extend(["-e", ext])
    if tables:
        for table in tables:
            pg_dump_cmd.extend(["-t", table])
    return pg_dump_cmd
