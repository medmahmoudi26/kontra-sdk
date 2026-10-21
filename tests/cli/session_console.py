"""The dev console as ``kontra dev up --ui`` wires it, for the tests that read an agent's session
over HTTP (decisions I36): the codec server and its console in a thread, holding a party's key
and its job keys, and the page's own requests. ``tests/cli/test_session_lifecycle.py`` and
``tests/examples/test_recon_harness.py`` read the same pane through it."""

from __future__ import annotations

import threading
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any

import httpx
from temporalio.client import Client

from kontra.cli._codec_server import CodecServer
from kontra.cli._console import Connected, Console
from kontra.codec import KontraCodec
from kontra.profile import Profile
from kontra.testing import Marketplace, TestParty

__all__ = ["PAGE", "Consoles", "codec_for", "consoles", "get", "row", "rows"]

#: The header the page sends on its own requests.
PAGE = {"X-Kontra-Console": "1"}


def codec_for(party: TestParty) -> Callable[[], KontraCodec]:
    """What ``kontra dev``'s codec server holds: this machine's key and its job keys."""

    def make() -> KontraCodec:
        return KontraCodec(
            party.identity,
            [party.public_key],
            role="seller",
            extra_identities=lambda: tuple(Profile.job_identities(party.home)),
        )

    return make


class Consoles:
    """Consoles served on loopback for the block, each a :class:`CodecServer` in a thread;
    all closed with it."""

    def __init__(self) -> None:
        self._started: list[tuple[CodecServer, Console, threading.Thread]] = []

    def open(self, mp: Marketplace, party: TestParty) -> CodecServer:
        grpc, ns = mp.dev_server.grpc_address, party.namespace

        async def connect() -> Connected:
            return Connected(await Client.connect(grpc, namespace=ns), ns)

        codec = codec_for(party)
        console = Console(party.home, codec, connect=connect, temporal_ui="http://localhost:8233")
        srv = CodecServer(
            0, codec=codec, origins=["http://localhost:8233"], token="t", console=console
        )
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        self._started.append((srv, console, thread))
        return srv

    def close(self) -> None:
        started, self._started = self._started, []
        for srv, console, thread in started:
            console.close()
            srv.shutdown()
            srv.server_close()
            thread.join(5)


@contextmanager
def consoles() -> Generator[Consoles]:
    opened = Consoles()
    try:
        yield opened
    finally:
        opened.close()


def get(srv: CodecServer, path: str, **headers: str) -> httpx.Response:
    with httpx.Client(trust_env=False, timeout=60) as http:
        return http.get(f"http://127.0.0.1:{srv.port}{path}", headers=headers)


def rows(session: dict[str, Any]) -> list[dict[str, Any]]:
    """Every turn's rows, in order (nested ones stay under their handoff)."""
    return [r for t in session["turns"] for r in t["rows"]]


def row(session: dict[str, Any], label: str, tool: str | None = None) -> dict[str, Any]:
    """The first row of ``label`` (of ``tool``, when given)."""
    return next(
        r
        for r in rows(session)
        if r["label"] == label and (tool is None or r.get("tool_name") == tool)
    )
