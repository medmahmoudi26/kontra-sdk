"""Fixtures for the over-the-wire isolation tests (seam ①, PRD §15 Phase 0).

These start the real ``kontra-temporal`` binary on free ports with SQLite in memory, serve
a local JWKS that the server's default claim mapper fetches, and mint ES256 JWTs the way
the gate will. The seller namespaces, buyer namespace and the Nexus endpoint are created
through the INTERNAL frontend (the control plane); every assertion that follows is made on
the PUBLIC frontend, over the wire with real ``temporalio`` clients.

Run with ``.venv/bin/pytest -m temporal tests/temporal`` (the marker is deselected by the
default addopts). ``KONTRA_TEMPORAL_BIN`` overrides the binary under test.

Starting the server is made robust against a busy machine (FIXUPS #12: the suite once errored
25 times under concurrent load). Its ports are picked at random *below* the kernel's ephemeral
range, where neither outgoing connections nor other suites binding port 0 take them, and are
held bound until just before the server starts; a server that still loses a port to someone
(``address already in use``) is restarted on fresh ports. Readiness is the gRPC health check
of both frontends, polled against a deadline, and a server that exits or never gets healthy
fails the fixture with the tail of its own output instead of a bare timeout. A frontend that
answers is not proof it is ours (a port lost to another server answers health checks too):
on Linux the server must also hold every one of its listeners itself, read from ``/proc``.
A server that dies while the tests run fails the session's teardown with its output.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import socket
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import jwt
import pytest
import temporal_isolation_app as app
from cryptography.hazmat.primitives.asymmetric import ec
from google.protobuf.duration_pb2 import Duration
from jwt.algorithms import ECAlgorithm
from temporalio.api.nexus.v1 import EndpointSpec, EndpointTarget
from temporalio.api.operatorservice.v1 import CreateNexusEndpointRequest
from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest
from temporalio.client import Client

AUDIENCE = "kontra:temporal"


#: The server's listeners (KONTRA_PORT_BASE only sets defaults, all overridden here).
_PORT_VARS = (
    "KONTRA_PORT_BASE",
    "KONTRA_FRONTEND_PORT",
    "KONTRA_INTERNAL_FRONTEND_PORT",
    "KONTRA_INTERNAL_FRONTEND_HTTP_PORT",
    "KONTRA_HISTORY_PORT",
    "KONTRA_MATCHING_PORT",
    "KONTRA_WORKER_PORT",
)
#: The variables naming a port the server listens on (KONTRA_PORT_BASE only sets defaults).
_LISTENER_VARS = _PORT_VARS[1:]
_START_ATTEMPTS = 4
_START_TIMEOUT_S = 90.0
_LOG_TAIL = 6000
#: Below 10000 sit the ports other stacks on this machine use (7233, 8080, 9000, ...).
_LOWEST_PORT = 10_000


def _ephemeral_floor() -> int:
    """The bottom of the kernel's ephemeral port range (Linux), else the IANA default."""
    try:
        return int(Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split()[0])
    except (OSError, ValueError, IndexError):
        return 49152


def _reserve_ports(count: int) -> list[socket.socket]:
    """``count`` distinct free ports on 127.0.0.1, each held by a bound socket until the
    caller closes it (just before the server binds it). Picked at random below the ephemeral
    range: the kernel hands ephemeral ports to every outgoing connection and to every bind
    to port 0, so under load a port found free there is often taken again before the server
    starts. Falls back to port 0 if that range is unusable."""
    held: list[socket.socket] = []
    top = _ephemeral_floor()
    rng = random.SystemRandom()
    tries = 0
    try:
        while len(held) < count:
            tries += 1
            port = rng.randrange(_LOWEST_PORT, top) if top > _LOWEST_PORT + 1000 else 0
            if tries > 2000:
                port = 0
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                sock.close()
                continue
            held.append(sock)
    except BaseException:
        for sock in held:
            sock.close()
        raise
    return held


class _PortTaken(Exception):
    """The server could not bind a port someone took after we released it: retry."""


def _tail(log: Path) -> str:
    try:
        data = log.read_bytes()
    except OSError as e:
        return f"(no server output: {e})"
    return data[-_LOG_TAIL:].decode("utf-8", "replace") or "(the server printed nothing)"


def _stop(proc: subprocess.Popen[bytes]) -> None:
    """Stop the server we started, by its PID."""
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


async def _healthy(address: str) -> bool:
    """The frontend at ``address`` answers the gRPC health check (no token needed)."""
    try:
        client = await asyncio.wait_for(
            Client.connect(address, namespace="temporal-system"), timeout=5
        )
        return await asyncio.wait_for(client.service_client.check_health(), timeout=5)
    except Exception:
        return False


def _listening_ports(pid: int) -> set[int] | None:
    """The TCP ports process ``pid`` itself listens on, from Linux's ``/proc`` (its socket
    inodes, matched against the LISTEN entries of ``/proc/net/tcp``); None where that cannot
    be read, e.g. not on Linux."""
    try:
        inodes: set[str] = set()
        for fd in os.listdir(f"/proc/{pid}/fd"):
            with contextlib.suppress(OSError):
                target = os.readlink(f"/proc/{pid}/fd/{fd}")
                if target.startswith("socket:[") and target.endswith("]"):
                    inodes.add(target[len("socket:[") : -1])
        table = Path("/proc/net/tcp").read_text().splitlines()[1:]
    except OSError:
        return None
    ports: set[int] = set()
    for line in table:
        fields = line.split()
        # sl local_address rem_address st ... inode: st 0A is LISTEN
        if len(fields) > 9 and fields[3] == "0A" and fields[9] in inodes:
            ports.add(int(fields[1].rsplit(":", 1)[1], 16))
    return ports


async def _wait_healthy(
    proc: subprocess.Popen[bytes],
    addresses: list[str],
    listeners: list[int],
    log: Path,
    within: float,
) -> None:
    """Until both frontends answer the health check *and* the server holds every listener
    port itself (a frontend port someone took after we released it answers too, until our
    server gives up on it)."""
    deadline = time.monotonic() + within
    pending = list(addresses)
    while True:
        code = proc.poll()
        if code is not None:
            output = _tail(log)
            if "address already in use" in output.lower():
                raise _PortTaken(output)
            raise RuntimeError(
                f"kontra-temporal exited with status {code} while starting:\n{output}"
            )
        pending = [a for a in pending if not await _healthy(a)]
        unheld: list[int] = []
        if not pending:
            held = _listening_ports(proc.pid)
            unheld = [] if held is None else [p for p in listeners if p not in held]
            if not unheld:
                return
        if time.monotonic() >= deadline:
            waiting = pending or [f"port {p} (not held by the server)" for p in unheld]
            raise TimeoutError(
                f"kontra-temporal did not report healthy on {', '.join(waiting)} within "
                f"{within:.0f}s:\n{_tail(log)}"
            )
        await asyncio.sleep(0.25)


@contextlib.contextmanager
def _server(binary: str, env: dict[str, str]) -> Any:
    """Run ``binary serve`` until the block exits; yields the port of each listener."""
    last: Exception | None = None
    for _ in range(_START_ATTEMPTS):
        held = _reserve_ports(len(_PORT_VARS))
        ports = {var: sock.getsockname()[1] for var, sock in zip(_PORT_VARS, held, strict=True)}
        fd, name = tempfile.mkstemp(prefix="kontra-temporal-", suffix=".log")
        log = Path(name)
        try:
            for sock in held:
                sock.close()
            with os.fdopen(fd, "wb") as out:
                proc = subprocess.Popen(
                    [binary, "serve"],
                    env={**env, **{k: str(v) for k, v in ports.items()}},
                    stdout=out,
                    stderr=subprocess.STDOUT,
                )
            try:
                addresses = [
                    f"127.0.0.1:{ports['KONTRA_FRONTEND_PORT']}",
                    f"127.0.0.1:{ports['KONTRA_INTERNAL_FRONTEND_PORT']}",
                ]
                listeners = [ports[v] for v in _LISTENER_VARS]
                loop = asyncio.new_event_loop()
                try:
                    loop.run_until_complete(
                        _wait_healthy(proc, addresses, listeners, log, _START_TIMEOUT_S)
                    )
                finally:
                    loop.close()
            except _PortTaken as e:
                _stop(proc)
                last = e
                continue
            except BaseException:
                _stop(proc)
                raise
            try:
                yield ports
            finally:
                died = proc.poll()
                output = _tail(log) if died is not None else ""
                _stop(proc)
            if died is not None:
                # Every test after this point failed against a server that was not there.
                raise RuntimeError(
                    f"kontra-temporal exited with status {died} while the tests ran:\n{output}"
                )
            return
        finally:
            log.unlink(missing_ok=True)
    raise RuntimeError(
        f"kontra-temporal lost a port to another process {_START_ATTEMPTS} times in a row; "
        f"last output:\n{last}"
    )


@dataclass
class Signer:
    """Mints the ES256 tokens the gate would (and the broken tokens an attacker would)."""

    key: ec.EllipticCurvePrivateKey
    kid: str

    def jwt(
        self,
        permissions: list[str],
        *,
        subject: str = "party",
        audience: str | None = AUDIENCE,
        exp: datetime | None = None,
        kid: str | None = ...,  # type: ignore[assignment]
        key: Any | None = None,
        algorithm: str = "ES256",
    ) -> str:
        claims: dict[str, Any] = {
            "sub": subject,
            "permissions": permissions,
            "exp": exp or datetime.now(UTC) + timedelta(hours=1),
            "iat": datetime.now(UTC),
        }
        if audience is not None:
            claims["aud"] = audience
        headers = {}
        header_kid = self.kid if kid is ... else kid
        if header_kid is not None:
            headers["kid"] = header_kid
        return jwt.encode(
            claims, self.key if key is None else key, algorithm=algorithm, headers=headers
        )

    def api_key(self, namespace: str, **kw: Any) -> str:
        return self.jwt([f"{namespace}:worker"], subject=f"{namespace}-worker", **kw)


class _JWKS:
    """A JWKS the server fetches over http:// — a stand-in for the gate's /.well-known."""

    def __init__(self) -> None:
        self.key = ec.generate_private_key(ec.SECP256R1())
        self.kid = "kontra-test-" + uuid.uuid4().hex[:8]
        pub = json.loads(ECAlgorithm.to_jwk(self.key.public_key()))
        pub.update(kid=self.kid, use="sig", alg="ES256")
        self._body = json.dumps({"keys": [pub]}).encode()
        jwks = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(jwks._body)

            def log_message(self, *args: Any) -> None:
                pass

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_port
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}/jwks.json"

    def __enter__(self) -> _JWKS:
        self._thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self._server.shutdown()


@dataclass
class Cluster:
    public_address: str
    internal_address: str
    signer: Signer
    endpoint_name: str
    endpoint_id: str
    other_endpoint_name: str  # another seller's offering (namespace other)


@pytest.fixture(scope="session")
def _binary() -> str:
    env = os.environ.get("KONTRA_TEMPORAL_BIN")
    if env:
        return env
    built = Path(__file__).resolve().parents[2] / "temporal/authorizer/bin/kontra-temporal"
    if not built.exists():
        pytest.skip(
            f"{built} not built; run temporal/authorizer/build.sh or set KONTRA_TEMPORAL_BIN"
        )
    return str(built)


@pytest.fixture(scope="session")
def cluster(_binary: str) -> Any:
    with _JWKS() as jwks:
        env = {
            **os.environ,
            "KONTRA_DB": "sqlite",
            "KONTRA_JWKS_URLS": jwks.url,
            "KONTRA_JWKS_REFRESH": "2s",
            "KONTRA_JWT_AUDIENCE": AUDIENCE,
            "KONTRA_LOG_LEVEL": "error",
        }
        with _server(_binary, env) as ports:
            public = f"127.0.0.1:{ports['KONTRA_FRONTEND_PORT']}"
            internal = f"127.0.0.1:{ports['KONTRA_INTERNAL_FRONTEND_PORT']}"
            endpoints = _provision(internal)
            yield Cluster(
                public_address=public,
                internal_address=internal,
                signer=Signer(jwks.key, jwks.kid),
                endpoint_name=endpoints[0][0],
                endpoint_id=endpoints[0][1],
                other_endpoint_name=endpoints[1][0],
            )


def _provision(internal_address: str) -> list[tuple[str, str]]:
    """Create the namespaces and the Nexus endpoints through the internal frontend — the
    control plane. The gate does exactly this (decisions I3): the endpoint name is a fresh
    opaque ``k...`` string, its target is the seller's namespace and task queue. Returns
    (name, id) of acme's endpoint, then of other's."""

    async def go() -> list[tuple[str, str]]:
        control = await Client.connect(internal_address, namespace="temporal-system")
        for ns in (app.SELLER_NAMESPACE, app.BUYER_NAMESPACE, app.OTHER_NAMESPACE):
            await control.workflow_service.register_namespace(
                RegisterNamespaceRequest(
                    namespace=ns,
                    workflow_execution_retention_period=Duration(seconds=24 * 3600),
                )
            )
        # Let the namespace registry pick the new namespaces up before any public call.
        await asyncio.sleep(2)
        endpoints = []
        for namespace, task_queue in (
            (app.SELLER_NAMESPACE, app.SELLER_TASK_QUEUE),
            (app.OTHER_NAMESPACE, app.OTHER_TASK_QUEUE),
        ):
            name = "k" + uuid.uuid4().hex
            resp = await control.operator_service.create_nexus_endpoint(
                CreateNexusEndpointRequest(
                    spec=EndpointSpec(
                        name=name,
                        target=EndpointTarget(
                            worker=EndpointTarget.Worker(namespace=namespace, task_queue=task_queue)
                        ),
                    )
                )
            )
            endpoints.append((name, resp.endpoint.id))
        return endpoints

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(go())
    finally:
        loop.close()


@pytest.fixture
async def control(cluster: Cluster) -> Any:
    """A client on the INTERNAL frontend (no token). The gate reaches Temporal this way."""
    return await Client.connect(cluster.internal_address, namespace="temporal-system")


async def connect_public(
    cluster: Cluster, namespace: str, *, api_key: str | None = None, **client_kwargs: Any
) -> Client:
    """A client on the PUBLIC frontend, authenticating with a worker JWT when given one.

    ``api_key`` becomes the ``Authorization: Bearer`` header on every call. Raw
    ``client.workflow_service`` / ``client.operator_service`` calls carry their own
    namespace in the request, so the ``namespace`` here is only the high-level default and
    a namespace-less call can be made whatever it is set to.

    ``tls=False`` is forced: the SDK turns TLS on when an api_key is present, but the dev
    server is plaintext (in production the TLS proxy terminates in front of it, PRD §10.2).
    """
    return await Client.connect(
        cluster.public_address, namespace=namespace, api_key=api_key, tls=False, **client_kwargs
    )
