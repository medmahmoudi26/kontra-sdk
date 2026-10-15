"""Fixtures for the fake-gate tests.

Most tests run the gate in-process (``httpx.ASGITransport``) on a :class:`ScriptedControl`,
with the sweeper driven by hand (``await world.gate.sweep()``), so every rule is exercised
deterministically and fast. Tests marked ``temporal`` use a real dev server (one per test
process, shared with ``tests/control``) and a served gate."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from kontra.auth import Session
from kontra.gate_client import GateClient
from kontra.gate_client import models as m
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)
from kontra.testing.fake_gate import FakeGate

from .support import ScriptedControl, handle, logged_in, offering_doc


@pytest.fixture(scope="session")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


@dataclass
class World:
    """An in-process fake gate on a scripted control plane, and helpers to act on it."""

    gate: FakeGate
    control: ScriptedControl
    transport: httpx.ASGITransport
    root: Path
    sessions: list[Session] = field(default_factory=list[Session])

    async def party(self, name: str | None = None) -> Session:
        h = name or handle()
        session = await logged_in(self.gate.base_url, self.root / h, h, self.transport)
        self.sessions.append(session)
        return session

    def anon(self) -> GateClient:
        return GateClient(self.gate.base_url, transport=self.transport)

    def bearer(self, token: str) -> GateClient:
        async def provider() -> str:
            return token

        return GateClient(self.gate.base_url, token=provider, transport=self.transport)

    async def publish(self, seller: Session, **facts: Any) -> m.CatalogEntry:
        return await seller.gate.publish(offering_doc(seller.profile.party, **facts))

    async def close(self) -> None:
        for s in self.sessions:
            await s.aclose()


@pytest.fixture
def scripted() -> ScriptedControl:
    return ScriptedControl()


@pytest.fixture
async def world(scripted: ScriptedControl, tmp_path: Path) -> AsyncIterator[World]:
    gate = FakeGate(scripted, sweep_interval=0, resolvers=("judge",))
    async with gate:
        w = World(gate, scripted, httpx.ASGITransport(app=gate.app), tmp_path)
        try:
            yield w
        finally:
            await w.close()
