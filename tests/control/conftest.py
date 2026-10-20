"""Fixtures for the control-plane tests: one Temporal dev server per test process
(shared with ``tests/testing``), a :class:`~kontra._control.Control` per test.

Without ``KONTRA_TEMPORAL_CLI`` the dev server is CLI 1.9.1, downloaded once and
checksum-verified (``kontra.devgate.temporal_cli``); with neither the binary nor the network,
the tests are skipped."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator

import pytest

from kontra._control import Control
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)


@pytest.fixture(scope="session")
def dev_server() -> Iterator[DevServer]:
    try:
        server = acquire_shared_dev_server()
    except Exception as e:
        if os.environ.get(CLI_ENV):
            raise  # an explicit binary must work
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    try:
        yield server
    finally:
        release_shared_dev_server()


@pytest.fixture
async def control(dev_server: DevServer) -> AsyncIterator[Control]:
    c = dev_server.control()
    try:
        yield c
    finally:
        await c.aclose()
