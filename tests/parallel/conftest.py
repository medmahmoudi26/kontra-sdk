"""Fixtures for ``execute_activities_in_parallel``: the process's shared dev server, and a
running worker on a fresh task queue with the pydantic converter and Kontra's sandbox
passthrough -- what ``kontra.serve.Worker`` gives a seller, minus the marketplace.

Without ``KONTRA_TEMPORAL_CLI`` the dev server is CLI 1.9.1, downloaded once into the temp
directory; with neither the binary nor the network, these tests are skipped."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from kontra._parallel import _Batch
from kontra.serve.worker import workflow_runner
from kontra.testing.devserver import (
    CLI_ENV,
    DevServer,
    acquire_shared_dev_server,
    release_shared_dev_server,
)

from . import acts, configured
from .flows import Parent

ACTIVITIES = [
    acts.double,
    acts.shout,
    acts.offsets,
    acts.tokens,
    acts.stuck,
    acts.slow,
    acts.fragile,
    acts.probe,
    configured.scale,
]


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


def pydantic_client(client: Client) -> Client:
    config = client.config()
    config["data_converter"] = pydantic_data_converter
    return Client(**config)


@asynccontextmanager
async def serving(client: Client, **kwargs: Any) -> AsyncIterator[tuple[Client, str]]:
    """A worker for :class:`Parent` and the batch child on a new task queue."""
    client = pydantic_client(client)
    task_queue = f"batch-{uuid.uuid4().hex[:8]}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[Parent, _Batch],
        activities=ACTIVITIES,
        workflow_runner=workflow_runner(),
        **kwargs,
    ):
        yield client, task_queue
