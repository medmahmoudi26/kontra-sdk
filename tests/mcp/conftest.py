"""Fixtures for ``kontra.mcp``: one Temporal dev server per test process (shared with the
other suites). Without ``KONTRA_TEMPORAL_CLI`` the dev server is CLI 1.9.1, downloaded once
and checksum-verified (``kontra.devgate.temporal_cli``); with neither the binary nor the
network these tests are skipped."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

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
