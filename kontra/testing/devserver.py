"""A local Temporal dev server with standalone Nexus operations, for tests and examples.

Standalone Nexus operations (how a buyer's script or the gate starts ``execute`` without a
worker, decisions I4) need Temporal server 1.32+ with two dynamic-config switches; the dev
server in Temporal CLI 1.9.1 is server 1.32.0. :func:`start_dev_server` starts it through
``temporalio.testing.WorkflowEnvironment.start_local`` -- the binary at
``$KONTRA_TEMPORAL_CLI`` if set, else CLI 1.9.1 downloaded once into the temp directory --
with its HTTP API on a free port (the gate relays quotes over it).

The returned :class:`DevServer` is the *internal* frontend: no authorizer, every API open.
Namespaces are created through :class:`kontra._control.Control`, as the gate does.

Test suites share one server per process with :func:`acquire_shared_dev_server` /
:func:`release_shared_dev_server`: it lives on a private event-loop thread, so pytest's
per-test event loops can come and go while it keeps running (a ``temporalio`` client is
usable from any loop).
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
from collections.abc import Coroutine, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any, TypeVar

import httpx
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment

from kontra._control import Control

__all__ = [
    "CLI_ENV",
    "DEV_SERVER_VERSION",
    "DYNAMIC_CONFIG",
    "DevServer",
    "acquire_shared_dev_server",
    "free_port",
    "release_shared_dev_server",
    "start_dev_server",
]

#: Overrides the downloaded CLI with a binary on disk (no network needed).
CLI_ENV = "KONTRA_TEMPORAL_CLI"
DEV_SERVER_VERSION = "v1.9.1"
#: What standalone Nexus operations need on server 1.32 (decisions I4).
DYNAMIC_CONFIG = ("nexusoperation.enableStandalone=true", "history.enableChasm=true")

_T = TypeVar("_T")


def free_port(host: str = "127.0.0.1") -> int:
    """A TCP port nothing listens on right now (the OS's choice)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return int(s.getsockname()[1])


@dataclass
class DevServer:
    """A running dev server. ``grpc_address`` is ``host:port`` for ``Client.connect``;
    ``http_base`` is its HTTP API (``http://host:port``)."""

    env: WorkflowEnvironment
    grpc_address: str
    http_base: str
    _stopped: bool = field(default=False, repr=False)

    @property
    def client(self) -> Client:
        """A client on the ``default`` namespace. Calls that name their namespace (all of
        :class:`~kontra._control.Control`'s) may use it for any namespace."""
        return self.env.client

    async def connect(self, namespace: str = "default", **kwargs: Any) -> Client:
        """A new client for ``namespace`` (a worker needs its namespace on the client)."""
        return await Client.connect(self.grpc_address, namespace=namespace, **kwargs)

    def client_for(self, namespace: str) -> Client:
        """A client for ``namespace`` sharing this server's connection (no new dial)."""
        config = self.env.client.config()
        config["namespace"] = namespace
        return Client(**config)

    def control(self, *, http: httpx.AsyncClient | None = None) -> Control:
        """The control plane on this server, as the gate would hold it."""
        return Control(self.env.client, self.http_base, http=http)

    async def stop(self) -> None:
        if not self._stopped:
            self._stopped = True
            await self.env.shutdown()

    async def __aenter__(self) -> DevServer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()


async def start_dev_server(
    *,
    cli_path: str | None = None,
    version: str = DEV_SERVER_VERSION,
    http_port: int | None = None,
    log_level: str = "error",
    extra_args: Sequence[str] = (),
) -> DevServer:
    """Start a dev server with standalone Nexus enabled and its HTTP API on ``http_port``
    (a free port by default). ``cli_path`` defaults to ``$KONTRA_TEMPORAL_CLI``; without
    either, CLI ``version`` is downloaded (cached in the temp directory)."""
    cli = cli_path or os.environ.get(CLI_ENV) or None
    port = http_port or free_port()
    args: list[str] = []
    for value in DYNAMIC_CONFIG:
        args += ["--dynamic-config-value", value]
    args += ["--http-port", str(port), *extra_args]
    if cli:
        env = await WorkflowEnvironment.start_local(
            dev_server_existing_path=cli,
            dev_server_log_level=log_level,
            dev_server_extra_args=args,
        )
    else:
        env = await WorkflowEnvironment.start_local(
            dev_server_download_version=version,
            dev_server_log_level=log_level,
            dev_server_extra_args=args,
        )
    target = env.client.service_client.config.target_host
    host = target.rsplit(":", 1)[0]
    return DevServer(env=env, grpc_address=target, http_base=f"http://{host}:{port}")


# --------------------------------------------------------------------------- one per process


class _Owner:
    """An event loop on a daemon thread that owns the shared server's lifetime."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self.loop.run_forever, name="kontra-dev-server", daemon=True
        )
        self.thread.start()

    def run(self, coro: Coroutine[Any, Any, _T]) -> _T:
        fut: Future[_T] = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return fut.result()

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=10)
        if not self.thread.is_alive():
            self.loop.close()


_lock = threading.Lock()
_shared: DevServer | None = None
_owner: _Owner | None = None
_refs = 0


def acquire_shared_dev_server() -> DevServer:
    """The process's shared dev server, started on first use. Pair every call with
    :func:`release_shared_dev_server`; the last release stops it. Synchronous, for
    session-scoped pytest fixtures (which run outside any event loop)."""
    global _shared, _owner, _refs
    with _lock:
        if _shared is None:
            owner = _Owner()
            try:
                _shared = owner.run(start_dev_server())
            except BaseException:
                owner.close()
                raise
            _owner = owner
        _refs += 1
        return _shared


def release_shared_dev_server() -> None:
    global _shared, _owner, _refs
    with _lock:
        if _refs == 0:
            return
        _refs -= 1
        if _refs or _shared is None or _owner is None:
            return
        server, owner = _shared, _owner
        _shared = _owner = None
        try:
            owner.run(server.stop())
        finally:
            owner.close()
