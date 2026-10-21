"""Fixtures for the engine tests: one Temporal dev server per test process (shared with
``tests/control`` and ``tests/testing``) for the Temporal adapter's contract run."""

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
