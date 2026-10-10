"""Fixtures for the over-the-wire isolation tests (seam ①, PRD §15 Phase 0).

These start the real ``kontra-temporal`` binary on free ports with SQLite in memory, serve
a local JWKS that the server's default claim mapper fetches, and mint ES256 JWTs the way
the gate will. The seller namespaces, buyer namespace and the Nexus endpoint are created
through the INTERNAL frontend (the control plane); every assertion that follows is made on
the PUBLIC frontend, over the wire with real ``temporalio`` clients.

Run with ``.venv/bin/pytest -m temporal tests/temporal`` (the marker is deselected by the
default addopts). ``KONTRA_TEMPORAL_BIN`` overrides the binary under test.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_port(port: int, deadline: float) -> None:
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.25)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.1)
    raise TimeoutError(f"nothing listening on 127.0.0.1:{port}")


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
        base = _free_port()
        ports = {
            "KONTRA_PORT_BASE": str(base),
            "KONTRA_FRONTEND_PORT": str(_free_port()),
            "KONTRA_INTERNAL_FRONTEND_PORT": str(_free_port()),
            "KONTRA_INTERNAL_FRONTEND_HTTP_PORT": str(_free_port()),
            "KONTRA_HISTORY_PORT": str(_free_port()),
            "KONTRA_MATCHING_PORT": str(_free_port()),
            "KONTRA_WORKER_PORT": str(_free_port()),
        }
        public_port = int(ports["KONTRA_FRONTEND_PORT"])
        internal_port = int(ports["KONTRA_INTERNAL_FRONTEND_PORT"])
        env = {
            **os.environ,
            **ports,
            "KONTRA_DB": "sqlite",
            "KONTRA_JWKS_URLS": jwks.url,
            "KONTRA_JWKS_REFRESH": "2s",
            "KONTRA_JWT_AUDIENCE": AUDIENCE,
            "KONTRA_LOG_LEVEL": "error",
        }
        proc = subprocess.Popen([_binary, "serve"], env=env)
        try:
            deadline = time.monotonic() + 90
            _wait_port(public_port, deadline)
            _wait_port(internal_port, deadline)
            signer = Signer(jwks.key, jwks.kid)
            endpoints = _provision(f"127.0.0.1:{internal_port}")
            yield Cluster(
                public_address=f"127.0.0.1:{public_port}",
                internal_address=f"127.0.0.1:{internal_port}",
                signer=signer,
                endpoint_name=endpoints[0][0],
                endpoint_id=endpoints[0][1],
                other_endpoint_name=endpoints[1][0],
            )
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                proc.kill()


def _provision(internal_address: str) -> list[tuple[str, str]]:
    """Create the namespaces and the Nexus endpoints through the internal frontend — the
    control plane. The gate does exactly this (decisions I3): the endpoint name is a fresh
    opaque ``k...`` string, its target is the seller's namespace and task queue. Returns
    (name, id) of acme's endpoint, then of other's."""
    import asyncio

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

    return asyncio.new_event_loop().run_until_complete(go())


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
