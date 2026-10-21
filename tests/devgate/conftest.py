"""Fixtures for ``kontra.devgate``: the shared Temporal dev server (one per test process, as in
the other suites) for the tests marked ``temporal``. Without ``KONTRA_TEMPORAL_CLI`` the CLI is
the pinned 1.9.1 (``kontra.devgate.temporal_cli.ensure``); with neither it nor the network, those
tests are skipped."""

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
