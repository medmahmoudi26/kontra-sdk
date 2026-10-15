"""Helpers for ``tests/e2e``: the ``kontra dev`` stack as the tests see it, parties logged
in to it, sellers as ``kontra serve`` processes, and scans of everything Kontra stores.

Nothing here talks to Kontra in a way a seller or buyer could not: parties log in with the
dev login, sellers are the ``kontra`` command, buyers are :class:`kontra.hire.Kontra`. The
scans are the operator's view -- the bucket through its S3 API with the stack's own
credentials, Postgres through ``psql`` inside its container -- because they look for what
the parties must not be able to leave behind.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

from kontra.auth import Session, login
from kontra.hire import Kontra
from kontra.profile import Profile

T = TypeVar("T")

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
#: Offerings written for these tests only; each test copies one into a temporary directory
#: and writes its offering.yaml there.
OFFERINGS = Path(__file__).resolve().parent / "offerings"
PROJECT = "kontra-dev"
#: Environment variables a party's processes keep from ours (every other KONTRA_* is the
#: stack's or the test run's, not the party's).
_KEEP = frozenset({"KONTRA_TEMPORAL_CLI"})


def kontra_argv(*args: str) -> list[str]:
    """The ``kontra`` command, from this interpreter's installation."""
    return [sys.executable, "-m", "kontra.cli", *args]


async def run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str],
    cwd: Path | None = None,
    within: float = 300,
) -> subprocess.CompletedProcess[str]:
    """A command to completion, on a thread (the test's event loop keeps running); its
    output captured as text."""
    return await asyncio.to_thread(
        subprocess.run,
        list(argv),
        env=dict(env),
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=within,
    )


async def run_kontra(
    *args: str, env: Mapping[str, str], cwd: Path | None = None, within: float = 300
) -> subprocess.CompletedProcess[str]:
    """``kontra ARGS`` to completion (:func:`run`)."""
    return await run(kontra_argv(*args), env=env, cwd=cwd, within=within)


def handle(prefix: str) -> str:
    """A party handle no other test (or earlier run on the same stack) has used."""
    return f"{prefix}-{secrets.token_hex(3)}"


def marker(label: str) -> str:
    """A string that appears nowhere unless a test put it there."""
    return f"KONTRA-E2E-{label.upper()}-{secrets.token_hex(12).upper()}"


async def until(
    probe: Callable[[], Awaitable[T]],
    ok: Callable[[T], bool],
    *,
    within: float,
    interval: float = 1.0,
    what: str = "the condition",
) -> T:
    """Poll ``probe`` until ``ok`` holds; AssertionError with the last value after ``within``
    seconds."""
    loop = asyncio.get_running_loop()
    end = loop.time() + within
    value = await probe()
    while not ok(value):
        if loop.time() > end:
            raise AssertionError(f"{what} did not hold within {within:.0f}s; last: {value!r}")
        await asyncio.sleep(interval)
        value = await probe()
    return value


def read_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines (``dev.env``); comments and blank lines skipped."""
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").split("\n"):
        name, sep, value = line.partition("=")
        if sep and not name.lstrip().startswith("#"):
            out[name.strip()] = value
    return out


# --------------------------------------------------------------------------- the stack


def compose_container(service: str) -> str:
    """The id of the running container of ``service`` in the dev project."""
    out = subprocess.run(
        [
            "docker",
            "ps",
            "-q",
            "--filter",
            f"label=com.docker.compose.project={PROJECT}",
            "--filter",
            f"label=com.docker.compose.service={service}",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    ).stdout.split()
    assert len(out) == 1, f"expected one running {service} container in {PROJECT}, found {out}"
    return out[0]


@dataclass(frozen=True)
class ScanReport:
    """What a scan read and where it found any needle (name -> the needles' indexes)."""

    scanned: dict[str, int]  # place -> rows (Postgres) or objects (bucket)
    bytes: int
    hits: dict[str, list[int]]


@dataclass(frozen=True)
class Stack:
    """The running ``kontra dev`` stack: where its services answer and its credentials
    (``$KONTRA_HOME/dev.env``)."""

    home: Path
    gate_url: str
    temporal: str
    s3_endpoint: str
    s3_bucket: str
    s3_region: str
    s3_access_key: str
    s3_secret_key: str
    pg_user: str
    parties: Path
    #: How long the session's ``kontra dev up`` took (the stack from nothing when the
    #: session started from ``down --volumes``); acceptance 1 counts it.
    up_seconds: float = 0.0
    #: The gate's ledger adapter for this session (``KONTRA_LEDGER``; acceptance 9 runs
    #: the suite once per adapter).
    ledger: str = "postgres"

    # ------------------------------------------------------------------ processes

    def party_env(self, home: Path, **extra: str) -> dict[str, str]:
        """A party's process environment: its own KONTRA_HOME, none of the stack's
        settings, output unbuffered (the tests read the log as it is written)."""
        env = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_") or k in _KEEP}
        env.update(KONTRA_HOME=str(home), PYTHONUNBUFFERED="1", **extra)
        return env

    def dev_env(self, home: Path) -> dict[str, str]:
        """For ``kontra dev`` from another home: every stack setting of ours (ports, images)
        but that home."""
        return {**os.environ, "KONTRA_HOME": str(home), "PYTHONUNBUFFERED": "1"}

    # ------------------------------------------------------------------ parties

    async def party(self, prefix: str) -> Party:
        """A fresh party (dev login: 100.00 USD of play money) with its own home."""
        return await self.login(handle(prefix))

    async def login(self, name: str) -> Party:
        home = self.parties / name
        profile = await login(self.gate_url, handle=name, dev=True, home=home, open_browser=False)
        return Party(name, home, profile, self)

    # ------------------------------------------------------------------ Postgres

    def psql(self, database: str, sql: str, *, timeout: float = 600) -> str:
        """Run ``sql`` with psql in the Postgres container (unaligned, tuples only)."""
        proc = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                compose_container("postgres"),
                "psql",
                "-U",
                self.pg_user,
                "-d",
                database,
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-f",
                "-",
            ],
            input=sql,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        assert proc.returncode == 0, f"psql on {database} failed: {proc.stderr.strip()}"
        return proc.stdout

    def databases(self) -> list[str]:
        out = self.psql(
            "postgres",
            "SELECT datname FROM pg_database WHERE NOT datistemplate AND datname <> 'postgres' "
            "ORDER BY 1;",
        )
        return [line for line in out.split("\n") if line]

    def scan_postgres(self, needles: Sequence[bytes]) -> ScanReport:
        """Every text-like and binary column of every table of every database in the stack's
        Postgres -- the gate's own, and Temporal's persistence and visibility stores -- for
        any of ``needles``. SQL sees values detoasted (decompressed), so a compressed value
        is searched as stored, not as TOAST's bytes."""
        cols = (
            "SELECT c.table_schema, c.table_name, c.column_name, c.data_type "
            "FROM information_schema.columns c JOIN information_schema.tables t "
            "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
            "WHERE t.table_type = 'BASE TABLE' "
            "AND c.table_schema NOT IN ('pg_catalog', 'information_schema') "
            "AND c.data_type IN ('bytea', 'text', 'character varying', 'character', "
            "'json', 'jsonb') ORDER BY 1, 2, 3;"
        )
        literals = [f"'\\x{n.hex()}'::bytea" for n in needles]
        scanned: dict[str, int] = {}
        hits: dict[str, list[int]] = {}
        total = 0
        for db in self.databases():
            selects: list[str] = []
            for line in self.psql(db, cols).split("\n"):
                if not line:
                    continue
                schema, table, column, kind = line.split("|")
                ident = f'"{column.replace(chr(34), chr(34) * 2)}"'
                expr = ident if kind == "bytea" else f"convert_to({ident}::text, 'UTF8')"
                place = f"{db}.{schema}.{table}.{column}"
                counts = ", ".join(
                    f"count(*) FILTER (WHERE position({lit} IN {expr}) > 0)" for lit in literals
                )
                qualified = f'"{schema.replace(chr(34), chr(34) * 2)}"."{table.replace(chr(34), chr(34) * 2)}"'
                selects.append(
                    f"SELECT '{place}', count(*), coalesce(sum(octet_length({expr})), 0), "
                    f"{counts} FROM {qualified};"
                )
            if not selects:
                continue
            for row in self.psql(db, "\n".join(selects)).split("\n"):
                if not row:
                    continue
                place, rows, size, *found = row.split("|")
                scanned[place] = int(rows)
                total += int(size)
                found_at = [i for i, n in enumerate(found) if int(n) > 0]
                if found_at:
                    hits[place] = found_at
        return ScanReport(scanned, total, hits)

    # ------------------------------------------------------------------ the bucket

    def s3(self) -> Any:
        import boto3
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            region_name=self.s3_region,
            aws_access_key_id=self.s3_access_key,
            aws_secret_access_key=self.s3_secret_key,
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
        )

    def objects(self, prefix: str = "") -> dict[str, int]:
        """Every object in the bucket (key -> size)."""
        s3 = self.s3()
        out: dict[str, int] = {}
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=self.s3_bucket, Prefix=prefix
        ):
            for obj in page.get("Contents", []):
                out[obj["Key"]] = int(obj["Size"])
        return out

    def uploads(self) -> list[str]:
        """Keys of the multipart uploads still open in the bucket."""
        answer = self.s3().list_multipart_uploads(Bucket=self.s3_bucket)
        return [u["Key"] for u in answer.get("Uploads", [])]

    def scan_bucket(self, needles: Sequence[bytes]) -> ScanReport:
        """Every object in the bucket, read whole through the S3 API (what anyone with the
        bucket's credentials gets), for any of ``needles``."""
        s3 = self.s3()
        keep = max(len(n) for n in needles) - 1
        scanned: dict[str, int] = {}
        hits: dict[str, list[int]] = {}
        total = 0
        for key in self.objects():
            body = s3.get_object(Bucket=self.s3_bucket, Key=key)["Body"]
            tail = b""
            found: set[int] = set()
            for chunk in body.iter_chunks(1 << 20):
                total += len(chunk)
                buf = tail + chunk
                found.update(i for i, n in enumerate(needles) if n in buf)
                tail = buf[-keep:]
            scanned[key] = 1
            if found:
                hits[key] = sorted(found)
        return ScanReport(scanned, total, hits)

    # ------------------------------------------------------------------ the disks

    def postgres_data_dir(self) -> str:
        """Where this Postgres keeps its cluster, asked rather than remembered: the
        official image moved it from /var/lib/postgresql/data to
        /var/lib/postgresql/<major>/docker in 18, and a privacy scan that greps a path
        that no longer exists is a scan that finds nothing and says so loudly."""
        found = self.psql("postgres", "SHOW data_directory;").strip()
        assert found.startswith("/"), f"postgres gave no data directory: {found!r}"
        return found

    def scan_files(self, service: str, directory: str, needles: Sequence[bytes]) -> list[str]:
        """The files under ``directory`` in ``service``'s container that hold any of
        ``needles`` byte for byte: what is on disk rather than what the service serves --
        Postgres's write-ahead log and dead rows, SeaweedFS's deleted needles. (A value a
        store compressed is not seen here; the scans through the services see it.)"""
        argv = ["docker", "exec", compose_container(service), "grep", "-rlF"]
        for n in needles:
            argv += ["-e", n.decode("ascii")]
        proc = subprocess.run(
            [*argv, directory], capture_output=True, text=True, check=False, timeout=1800
        )
        assert proc.returncode in (0, 1), f"grep in {service} failed: {proc.stderr.strip()}"
        return sorted(line for line in proc.stdout.split("\n") if line)


def needles(*markers: str) -> list[bytes]:
    """Each marker as stored plainly, hex-encoded, and base64-encoded at every alignment
    (standard and URL-safe alphabets): plaintext that something base64-encoded before
    storing it must not slip past a scan for the raw bytes. Only the base64 characters that
    the marker alone determines are searched."""
    out: list[bytes] = []
    for text in markers:
        raw = text.encode()
        out += [raw, raw.hex().encode(), raw.hex().upper().encode()]
        for pad in range(3):
            enc = base64.b64encode(b"\0" * pad + raw)
            start = 4 if pad else 0
            end = len(enc) - (4 if (pad + len(raw)) % 3 else 0)
            core = enc[start:end]
            out += [core, core.replace(b"+", b"-").replace(b"/", b"_")]
    return list(dict.fromkeys(out))


# --------------------------------------------------------------------------- parties


@dataclass
class Party:
    """A party logged in to the dev gate, with its own ``KONTRA_HOME``."""

    handle: str
    home: Path
    profile: Profile
    stack: Stack

    def env(self, **extra: str) -> dict[str, str]:
        return self.stack.party_env(self.home, **extra)

    def kontra(self, **kwargs: Any) -> Kontra:
        return Kontra(home=self.home, **kwargs)

    @contextlib.asynccontextmanager
    async def temporal(self) -> AsyncIterator[Any]:
        """A Temporal client on the party's own namespace, as its worker token allows: what
        the party itself can see (no codec: payloads stay as stored)."""
        session = Session(Profile.load(self.home), home=self.home)
        try:
            client = await session.connect_temporal("worker")
            try:
                yield client
            finally:
                await session.release(client)
        finally:
            await session.aclose()

    async def history_payloads(self, workflow_id: str) -> list[tuple[str, str, bytes]]:
        """Every payload in the history of ``workflow_id`` in this party's namespace, read
        with no codec (as Temporal stores it): ``(where, encoding, data)``, ``where`` the
        field path in its event (a header's key in brackets)."""
        from google.protobuf.message import Message
        from temporalio.api.common.v1 import Payload

        found: list[tuple[str, str, bytes]] = []

        def walk(msg: Message, where: str) -> None:
            if isinstance(msg, Payload):
                enc = msg.metadata.get("encoding", b"").decode(errors="replace")
                found.append((where, enc, msg.data))
                return
            for fd, value in msg.ListFields():
                if fd.message_type is None:
                    continue
                if isinstance(value, Message):
                    walk(value, f"{where}.{fd.name}")
                elif fd.message_type.GetOptions().map_entry:  # a map: its values by key
                    for key, item in value.items():
                        if isinstance(item, Message):
                            walk(item, f"{where}.{fd.name}[{key}]")
                else:  # a repeated field
                    for i, item in enumerate(value):
                        walk(item, f"{where}.{fd.name}[{i}]")

        async with self.temporal() as client:
            history = await client.get_workflow_handle(workflow_id).fetch_history()
        for event in history.events:
            walk(event, f"event {event.event_id}")
        return found

    def point_gate_at(self, url: str) -> None:
        """Make this party's processes reach the gate at ``url`` (a :class:`Tap`)."""
        profile = Profile.load(self.home)
        profile.gate_url = url
        profile.save(self.home)


# --------------------------------------------------------------------------- projects


def set_fields(yaml_text: str, **fields: str) -> str:
    """Replace top-level ``key: value`` lines of an offering.yaml (adding missing ones)."""
    for key, value in fields.items():
        line = f"{key}: {value}"
        pattern = re.compile(rf"(?m)^{re.escape(key)}:.*$")
        yaml_text, n = pattern.subn(line, yaml_text)
        if n == 0:
            yaml_text = yaml_text.rstrip("\n") + f"\n{line}\n"
    return yaml_text


def copy_project(src: Path, dest: Path, **fields: str) -> Path:
    """Copy an offering project (an example, or one under ``offerings/``) to ``dest`` and set
    ``fields`` in its offering.yaml there. The source is never written."""
    shutil.copytree(src, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "gen"))
    path = dest / "offering.yaml"
    if fields:
        path.write_text(set_fields(path.read_text(encoding="utf-8"), **fields), encoding="utf-8")
    return dest


def offering_yaml(name: str, **fields: str) -> str:
    """An offering.yaml for a test offering: these defaults, then ``fields``."""
    base = {
        "name": name,
        "version": "1.0.0",
        "description": "An end-to-end test offering",
        "max_duration": "10m",
        "review_window": "1h",
        "max_output_bytes": "10MB",
        "artifact_retention": "1d",
    }
    base.update(fields)
    return "".join(f"{k}: {v}\n" for k, v in base.items())


def own_project(kind: str, dest: Path, name: str, **fields: str) -> Path:
    """``offerings/<kind>`` copied to ``dest`` with an offering.yaml for ``name``."""
    shutil.copytree(OFFERINGS / kind, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (dest / "offering.yaml").write_text(offering_yaml(name, **fields), encoding="utf-8")
    return dest


async def scaffold(party: Party, where: Path, name: str = "wc") -> tuple[Path, str]:
    """``kontra init NAME`` for ``party`` in ``where`` (a directory that exists): the
    project directory and its offering's key."""
    done = await run_kontra("init", name, "--party", party.handle, env=party.env(), cwd=where)
    assert done.returncode == 0, done.stderr
    return where / name, f"{party.handle}/{name}@0.1.0"


# --------------------------------------------------------------------------- sellers


@dataclass
class Serve:
    """A seller's worker as its own process: ``kontra serve --dir`` unless ``argv`` says
    otherwise, with the party's environment; output to ``log``."""

    directory: Path
    party: Party
    log: Path
    argv: list[str] | None = None
    extra_env: Mapping[str, str] = field(default_factory=dict[str, str])
    proc: subprocess.Popen[bytes] | None = None

    def start(self) -> None:
        argv = self.argv or kontra_argv("serve", "--dir", str(self.directory))
        with self.log.open("ab") as out:
            self.proc = subprocess.Popen(
                argv,
                cwd=self.directory,
                env=self.party.env(**self.extra_env),
                stdout=out,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )

    @property
    def pid(self) -> int:
        assert self.proc is not None
        return self.proc.pid

    def text(self) -> str:
        return self.log.read_text(encoding="utf-8", errors="replace") if self.log.exists() else ""

    def tail(self, lines: int = 80) -> str:
        return "\n".join(self.text().split("\n")[-lines:])

    async def ready(self, keys: Sequence[str], within: float = 120.0) -> None:
        """Until the worker logs that it serves every one of ``keys``."""
        loop = asyncio.get_running_loop()
        end = loop.time() + within
        while True:
            text = self.text()
            if all(f"serving {k} " in text for k in keys):
                return
            assert self.proc is not None
            if self.proc.poll() is not None:
                raise AssertionError(
                    f"the seller exited ({self.proc.returncode}) before serving:\n{self.tail()}"
                )
            if loop.time() > end:
                raise AssertionError(f"the seller is not serving {keys} yet:\n{self.tail()}")
            await asyncio.sleep(0.5)

    async def wait(self, within: float = 120.0) -> int:
        """Its exit status, once it exits by itself."""
        assert self.proc is not None
        loop = asyncio.get_running_loop()
        end = loop.time() + within
        while (code := self.proc.poll()) is None:
            if loop.time() > end:
                raise AssertionError(f"the process is still running:\n{self.tail()}")
            await asyncio.sleep(0.2)
        return code

    async def stop(self) -> None:
        """Ctrl-C (a graceful stop), a second one after a while, then SIGKILL."""
        proc = self.proc
        if proc is None or proc.poll() is not None:
            return
        for sig, grace in ((signal.SIGINT, 5.0), (signal.SIGINT, 20.0), (signal.SIGKILL, 10.0)):
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, sig)
            loop = asyncio.get_running_loop()
            end = loop.time() + grace
            while True:
                if proc.poll() is not None:
                    return
                if loop.time() >= end:
                    break
                await asyncio.sleep(0.2)


@contextlib.asynccontextmanager
async def serving(
    directory: Path,
    party: Party,
    keys: Sequence[str],
    *,
    log: Path,
    argv: list[str] | None = None,
    env: Mapping[str, str] | None = None,
) -> AsyncIterator[Serve]:
    """A seller serving ``keys`` for the block, stopped after it."""
    seller = Serve(directory, party, log, argv=argv, extra_env=dict(env or {}))
    seller.start()
    try:
        await seller.ready(keys)
        yield seller
    finally:
        await seller.stop()


# --------------------------------------------------------------------------- watching


class PeakRss:
    """Samples a process's resident set every 5 ms on a thread: its RSS when sampling
    started (``baseline``) and the highest seen (``peak``)."""

    def __init__(self, pid: int | None = None) -> None:
        import psutil

        self._proc = psutil.Process(pid)
        self.baseline = self._proc.memory_info().rss
        self.peak = self.baseline
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        import psutil

        while not self._stop.is_set():
            with contextlib.suppress(psutil.Error):
                self.peak = max(self.peak, self._proc.memory_info().rss)
            time.sleep(0.005)

    def __enter__(self) -> PeakRss:
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._thread.join()

    @property
    def growth(self) -> int:
        return self.peak - self.baseline


class Tap:
    """A TCP relay in front of the gate that records every chunk its client sends: point a
    seller's profile at it, and the seller's every contact with the gate is on record with
    a timestamp (``time.monotonic``). It runs on its own thread and event loop."""

    def __init__(self, target_host: str, target_port: int) -> None:
        self._target = (target_host, target_port)
        self._lock = threading.Lock()
        self._sent: list[tuple[float, int, bytes]] = []
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._server: asyncio.Server | None = None
        self._thread = threading.Thread(target=self._run, name="gate-tap", daemon=True)
        self.port = 0

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def sent(
        self, since: float = 0.0, until: float = float("inf")
    ) -> list[tuple[float, int, bytes]]:
        """``(time, bytes, first bytes)`` of every chunk the client sent within the window."""
        with self._lock:
            return [e for e in self._sent if since <= e[0] <= until]

    def __enter__(self) -> Tap:
        self._thread.start()
        if not self._ready.wait(10):
            raise RuntimeError("the gate tap did not start")
        return self

    def __exit__(self, *exc: object) -> None:
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        self._thread.join(timeout=10)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop

        async def pump(src: asyncio.StreamReader, dst: asyncio.StreamWriter, record: bool) -> None:
            try:
                while data := await src.read(65536):
                    if record:
                        with self._lock:
                            self._sent.append((time.monotonic(), len(data), data[:160]))
                    dst.write(data)
                    await dst.drain()
            except (ConnectionError, OSError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    dst.close()

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                up_reader, up_writer = await asyncio.open_connection(*self._target)
            except OSError:
                writer.close()
                return
            await asyncio.gather(pump(reader, up_writer, True), pump(up_reader, writer, False))

        async def start() -> None:
            self._server = await asyncio.start_server(handle, "127.0.0.1", 0)
            self.port = int(self._server.sockets[0].getsockname()[1])

        loop.run_until_complete(start())
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            if self._server is not None:
                self._server.close()
            loop.close()


def free_disk(path: Path) -> int:
    return shutil.disk_usage(path).free
