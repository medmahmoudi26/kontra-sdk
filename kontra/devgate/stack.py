"""``DevStack``: the dev gate and its Temporal, on the running event loop, loopback only.

One host process (``python -m kontra.devgate``) runs :class:`~kontra.devgate.gate.DevGate` under
uvicorn on its main loop and a :class:`~kontra.devgate.temporal.TemporalChild` as its child. An
ASGI wrapper checks the ``Host`` header on every request (a page that rebinds a name to 127.0.0.1
is refused) and answers control routes outside the seam ② table -- ``GET /_devgate/ready``,
``POST /_devgate/config``, ``POST /_devgate/stop`` -- which need the bearer token (stdin only,
constant-time compare) and no ``Origin``. ``GET /_devgate/hello?nonce=N`` needs no token: it is
how the host proves it holds one (:func:`~kontra.devgate.state.hello_proof`) before the CLI
sends it to a port that a stale record names. The gate stands in for the platform and never
holds a party's key, so the codec server stays its own process (started by the CLI, not here).
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import re
import socket
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs

import uvicorn

from kontra.devgate import store
from kontra.devgate.gate import DevGate, platform_defaults
from kontra.devgate.state import hello_proof
from kontra.devgate.temporal import TemporalChild
from kontra.gate_client import models as m

if TYPE_CHECKING:
    from kontra._control import Control
    from kontra.devgate.state import Ports

__all__ = ["CONTROL_PREFIX", "PERSIST_EVERY", "DevStack", "StackConfig"]

CONTROL_PREFIX = "/_devgate/"
#: Seconds between ``--persist`` passes (a crash loses at most this much, §5.4).
PERSIST_EVERY = 10.0
_OPEN_STATES = frozenset({m.JobState.SUBMITTED, m.JobState.WORKING})
_NONCE = re.compile(r"[0-9a-f]{32,128}", re.ASCII)
_access = logging.getLogger("kontra.devgate.access")
_log = logging.getLogger("kontra.devgate")


@dataclass(frozen=True, slots=True)
class StackConfig:
    """What the host needs to stand the stack up. ``home``'s data is in ``home/dev/data`` and
    logs in ``home/dev/logs``; ``cli`` is a verified Temporal CLI."""

    home: Path
    ports: Ports
    cli: Path
    ui: bool = False
    metrics: bool = False
    persist: bool = False


class DevStack:
    """The dev gate and its Temporal child. Use it as an async context manager::

    async with DevStack(config, token=token) as stack:
        ...                       # serving; the ready fd is written by the host
        await stack.wait_closed()
    """

    def __init__(self, config: StackConfig, *, token: str) -> None:
        self.config = config
        self._token = token
        self._ui = config.ui
        self._metrics = config.metrics
        self._persist = config.persist
        p = config.ports
        self._hosts = frozenset({f"127.0.0.1:{p.gate}", f"localhost:{p.gate}"})
        self.gate_url = f"http://127.0.0.1:{p.gate}"
        self.temporal_address = f"127.0.0.1:{p.temporal}"
        self._started = time.monotonic()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._temporal: TemporalChild | None = None
        self._control: Control | None = None
        self._server: uvicorn.Server | None = None
        self._serving: asyncio.Task[None] | None = None
        self._persist_task: asyncio.Task[None] | None = None
        self._persist_stop: asyncio.Event | None = None
        self._last_digest: str | None = None
        self._blobs: store.BlobFiles | None = None
        self.gate: DevGate = None  # type: ignore[assignment]  # set in __aenter__

    @property
    def temporal_http(self) -> str:
        assert self._temporal is not None
        return self._temporal.http_base

    @property
    def temporal_http_port(self) -> int:
        """The Temporal HTTP API's kernel-chosen port (what ``stack.json`` records)."""
        assert self._temporal is not None
        return self._temporal.http_port

    # ------------------------------------------------------------------ lifecycle

    async def __aenter__(self) -> DevStack:
        """Start Temporal, then the gate. Whatever fails after the Temporal child started (the
        connect, a refused ``--persist`` state, a taken gate port) stops it before the error
        propagates: ``__aexit__`` does not run when ``__aenter__`` raises, and an orphaned
        child would keep the Temporal port."""
        from kontra.devgate import state

        self._loop = asyncio.get_running_loop()
        p = self.config.ports
        http_port = _free_port()
        self._temporal = TemporalChild(
            cli=self.config.cli,
            port=p.temporal,
            http_port=http_port,
            db=state.data_dir(self.config.home) / "temporal.db",
            log_path=state.logs_dir(self.config.home) / "temporal.log",
            ui=self._ui,
            ui_port=p.ui,
            codec_port=p.codec,
            metrics_port=p.temporal_metrics if self._metrics else None,
        )
        await self._temporal.start()
        try:
            await self._open()
        except BaseException:
            await self._shutdown(quiet=True)
            raise
        return self

    async def _open(self) -> None:
        from temporalio.client import Client

        from kontra._control import Control

        assert self._temporal is not None
        client = await Client.connect(self.temporal_address)
        self._control = Control(client, self._temporal.http_base)
        self.gate = DevGate(
            self._control,
            base_url=self.gate_url,
            temporal_address=self.temporal_address,
            dev=True,
            **platform_defaults(),  # type: ignore[arg-type]
        )
        if self._persist:
            self._restore()
            # At once, so the kept pair (gate.json and temporal.db) is whole from the start: a
            # host killed before its first periodic snapshot leaves a state to restore.
            self._snapshot_now()
        await self._start_serving()
        if self._persist:
            self._start_persisting()

    async def __aexit__(self, *exc: object) -> None:
        try:
            await self._stop_persisting()
            if self._persist:
                self._snapshot_now()
        finally:
            await self._shutdown(quiet=False)

    async def _shutdown(self, *, quiet: bool) -> None:
        """Stop serving, then the Temporal child, then the control client -- each even when an
        earlier step fails. ``quiet``: a server that never started cleanly is not re-raised
        (the caller is already raising the startup error)."""
        try:
            if self._server is not None:
                self._server.should_exit = True
            if self._serving is not None:
                if quiet:
                    with contextlib.suppress(BaseException):
                        await self._serving
                else:
                    await self._serving
        finally:
            try:
                if self._temporal is not None:
                    await self._temporal.stop()
            finally:
                if self._control is not None:
                    await self._control.aclose()

    async def wait_closed(self) -> None:
        if self._serving is not None:
            await self._serving

    def request_stop(self) -> None:
        """Ask uvicorn to stop gracefully (SIGTERM/SIGINT handler, or ``POST /_devgate/stop``)."""
        if self._server is not None:
            self._server.should_exit = True

    async def _start_serving(self) -> None:
        p = self.config.ports
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", p.gate))
        config = uvicorn.Config(
            self._asgi,
            interface="asgi3",
            log_level="warning",
            access_log=False,
            lifespan="on",
            timeout_graceful_shutdown=3,
        )
        self._server = uvicorn.Server(config)
        assert self._loop is not None
        self._serving = self._loop.create_task(
            self._server.serve(sockets=[sock]), name="devgate-uvicorn"
        )
        while not self._server.started:
            if self._serving.done():
                await self._serving  # surfaces a startup error
                return
            await asyncio.sleep(0.02)

    # ------------------------------------------------------------------ reconfigure

    async def reconfigure(self, *, ui: bool, metrics: bool, persist: bool) -> dict[str, Any]:
        """Apply a ``POST /_devgate/config``: restart the Temporal child only when ``ui`` or
        ``metrics`` changed; snapshot at once when ``persist`` turns on. Turned off, nothing is
        written again (the loop is stopped) and the snapshot is deleted: the state is in memory
        from now, and a crash cannot bring back what was kept before."""
        assert self._temporal is not None
        restarted = False
        if ui != self._ui or metrics != self._metrics:
            self._ui, self._metrics = ui, metrics
            self._temporal.ui = ui
            self._temporal.metrics_port = self.config.ports.temporal_metrics if metrics else None
            await self._temporal.restart()
            restarted = True
        if persist and not self._persist:
            self._persist = True
            self._snapshot_now()
            self._start_persisting()
        elif not persist and self._persist:
            self._persist = False
            await self._stop_persisting()
            self._forget_snapshot()
        return {
            "ui": self._ui,
            "metrics": self._metrics,
            "persist": self._persist,
            "restarted": restarted,
        }

    # ------------------------------------------------------------------ persistence

    def _store_path(self) -> Path:
        from kontra.devgate import state

        return state.data_dir(self.config.home) / "gate.json"

    def _blob_files(self) -> store.BlobFiles:
        if self._blobs is None:
            self._blobs = store.BlobFiles(self._store_path().parent / "blobs")
        return self._blobs

    def _restore(self) -> None:
        doc = store.read_doc(self._store_path())  # 0600 and ours, or refused
        if doc is not None:
            store.restore(self.gate, doc, self._blob_files())

    def _snapshot_now(self) -> None:
        self._last_digest = store.write_snapshot(self.gate, self._store_path(), self._blob_files())

    def _forget_snapshot(self) -> None:
        self._store_path().unlink(missing_ok=True)
        self._blob_files().discard()
        self._last_digest = None

    def _start_persisting(self) -> None:
        if self._persist_task is None and self._loop is not None:
            self._persist_stop = asyncio.Event()
            self._persist_task = self._loop.create_task(
                self._persist_loop(self._persist_stop), name="devgate-persist"
            )

    async def _stop_persisting(self) -> None:
        """Stop the loop and wait for it: a pass in flight finishes (never half a write)."""
        task, self._persist_task = self._persist_task, None
        if task is None:
            return
        if self._persist_stop is not None:
            self._persist_stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    async def _persist_loop(self, stop: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), PERSIST_EVERY)
                return  # asked to stop
            except TimeoutError:
                pass
            if not self._persist:
                return
            try:
                await self._persist_pass()
            except Exception:  # keep snapshotting: the next pass may succeed, `down` retries
                _log.exception("a --persist snapshot failed")

    async def _persist_pass(self) -> None:
        """One pass: the snapshot on the gate's loop (big byte strings by digest, hashed once
        each), the JSON dump and the writes off it; written only when its digest changed."""
        files = self._blob_files()
        collected = files.collect()
        doc = await self.gate.run(lambda: _wrap(store.snapshot(self.gate, collected)))
        raw = await asyncio.to_thread(json.dumps, doc)
        files.remember(collected)
        digest = store.digest(raw)
        if digest == self._last_digest or not self._persist:
            return
        await asyncio.to_thread(files.commit, self._store_path(), raw, collected.refs)
        self._last_digest = digest

    # ------------------------------------------------------------------ the ASGI wrapper

    async def _asgi(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] == "lifespan":
            await self.gate.app(scope, receive, send)
            return
        if scope["type"] != "http":
            await _plain(send, 403, b"no websockets on the dev gate")
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        if headers.get("host") not in self._hosts:
            await _plain(send, 403, b"unexpected Host (the dev gate is loopback only)")
            return
        path: str = scope["path"]
        if path.startswith(CONTROL_PREFIX):
            if path == f"{CONTROL_PREFIX}hello" and "origin" not in headers:
                await self._hello(scope, send)
                return
            given = headers.get("authorization", "").encode()
            ok = "origin" not in headers and hmac.compare_digest(
                given, f"Bearer {self._token}".encode()
            )
            if not ok:
                await _plain(send, 403, b"not this host's control token")
                return
            await self._control_route(path[len(CONTROL_PREFIX) :], scope, receive, send)
            return
        await self._logged(scope, receive, send)

    async def _hello(self, scope: dict[str, Any], send: Any) -> None:
        """``GET /_devgate/hello?nonce=<32-128 hex>``: ``{"proof": hello_proof(token, nonce)}``
        and nothing else -- no pid, no state. The one control route without the token: it
        proves this host holds it, to a CLI that will not send the token before it knows."""
        if scope["method"] != "GET":
            await _plain(send, 404, b"no such control route")
            return
        query = parse_qs(scope.get("query_string", b"").decode("latin-1"))
        nonce = query.get("nonce", [""])
        if len(nonce) != 1 or _NONCE.fullmatch(nonce[0]) is None:
            await _plain(send, 400, b"a nonce of 32-128 lowercase hex digits is expected")
            return
        body = json.dumps({"proof": hello_proof(self._token, nonce[0])}).encode()
        await _plain(send, 200, body, b"application/json")

    async def _logged(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Pass through to the gate, logging one access line with the query string dropped
        (``scope['path']`` carries no query; a ``/_blob/`` signature never reaches the log)."""
        status = {"code": 0}

        async def capture(message: Any) -> None:
            if message["type"] == "http.response.start":
                status["code"] = int(message["status"])
            await send(message)

        await self.gate.app(scope, receive, capture)
        _access.info("%s %s %s", scope["method"], scope["path"], status["code"])

    # ------------------------------------------------------------------ control routes

    async def _control_route(
        self, name: str, scope: dict[str, Any], receive: Any, send: Any
    ) -> None:
        method = scope["method"]
        if name == "ready" and method == "GET":
            await _plain(send, 200, json.dumps(self._ready_doc()).encode(), b"application/json")
            return
        if name == "stop" and method == "POST":
            await _plain(send, 204, b"")
            self.request_stop()
            return
        if name == "config" and method == "POST":
            want = await _read_json(receive)
            result = await self.reconfigure(
                ui=bool(want.get("ui", self._ui)),
                metrics=bool(want.get("metrics", self._metrics)),
                persist=bool(want.get("persist", self._persist)),
            )
            await _plain(send, 200, json.dumps(result).encode(), b"application/json")
            return
        await _plain(send, 404, b"no such control route")

    def _ready_doc(self) -> dict[str, Any]:
        import os

        return {
            "pid": os.getpid(),
            "temporal_pid": self._temporal.pid if self._temporal else None,
            "temporal_http": self._temporal.http_port if self._temporal else None,
            "ui": self._ui,
            "metrics": self._metrics,
            "persist": self._persist,
            "uptime": round(time.monotonic() - self._started, 1),
            "counts": self._counts(),
        }

    def _counts(self) -> dict[str, int]:
        parties = len(getattr(self.gate, "parties", {}))
        offerings = len(getattr(self.gate, "offerings", {}))
        open_jobs = 0
        try:
            for job in self.gate.jobs.values():
                if job.record.state in _OPEN_STATES:
                    open_jobs += 1
        except Exception:
            open_jobs = len(getattr(self.gate, "jobs", {}))
        return {"parties": parties, "offerings": offerings, "open_jobs": open_jobs}


# --------------------------------------------------------------------------- ASGI helpers


async def _plain(send: Any, status: int, body: bytes, ctype: bytes = b"text/plain") -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", ctype),
                (b"content-length", str(len(body)).encode()),
                (b"cache-control", b"no-store"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _read_json(receive: Any) -> dict[str, Any]:
    raw = b""
    while True:
        message = await receive()
        raw += message.get("body", b"")
        if not message.get("more_body"):
            break
    try:
        doc = json.loads(raw or b"{}")
    except ValueError:
        return {}
    return doc if isinstance(doc, dict) else {}


def _free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


async def _wrap(value: Any) -> Any:
    return value
