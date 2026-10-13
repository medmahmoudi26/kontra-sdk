"""``Control.history_summary`` follows the chain escrow pinned (decisions I15, PRD §8.4).

Once a job's run is pinned (``JobRecord.run_id``), its buyer's progress is read from that
chain only -- through its continue-as-new runs -- and never from another run of
``job-<id>``, such as one the seller starts itself after the buyer's chain closed. Against
the real dev server, with plain workflows: the summary reads history metadata alone."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

import pytest
import pytest_asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from kontra._control import Control, HistorySummary
from kontra.escrow import ObservedStatus
from kontra.testing.devserver import DevServer

from .flows import Chain, shout

pytestmark = [pytest.mark.temporal]

_T = TypeVar("_T")


@dataclass
class Seller:
    namespace: str
    client: Client
    task_queue: str


@pytest_asyncio.fixture
async def control(dev_server: DevServer) -> AsyncIterator[Control]:
    c = dev_server.control()
    try:
        yield c
    finally:
        await c.aclose()


@pytest_asyncio.fixture
async def seller(dev_server: DevServer, control: Control) -> AsyncIterator[Seller]:
    namespace = f"seller-{uuid.uuid4().hex[:8]}"
    await control.ensure_namespace(namespace)
    client = dev_server.client_for(namespace)
    task_queue = f"chain-{uuid.uuid4().hex[:8]}"
    async with Worker(client, task_queue=task_queue, workflows=[Chain], activities=[shout]):
        yield Seller(namespace, client, task_queue)


async def until(probe: Callable[[], Awaitable[_T]], ok: Callable[[_T], bool]) -> _T:
    for _ in range(300):
        value = await probe()
        if ok(value):
            return value
        await asyncio.sleep(0.05)
    raise AssertionError(f"never got there; last: {value!r}")


def _done(count: int) -> Callable[[HistorySummary | None], bool]:
    """A summary of a waiting run with ``count`` activities completed along its chain.

    The pending activities and the history are two reads, so right as the last activity
    completes a summary can count it and still list it as running: wait for both to agree."""
    return lambda s: (
        s is not None
        and s.activities_completed == count
        and s.last_event == "ActivityTaskCompleted"
        and not s.activities_running
    )


async def test_a_pinned_chain_is_summarised_through_its_continue_as_new(
    control: Control, seller: Seller
) -> None:
    wf = f"job-{uuid.uuid4().hex}"
    handle = await seller.client.start_workflow(
        Chain.run, args=[1, 2, True], id=wf, task_queue=seller.task_queue
    )
    root = handle.first_execution_run_id
    assert root is not None
    pinned = await until(lambda: control.history_summary(seller.namespace, wf, root), _done(4))
    assert pinned == HistorySummary(
        state=ObservedStatus.RUNNING,
        last_event="ActivityTaskCompleted",
        activities_completed=4,
        activities_running=(),
        continue_as_new_count=1,
    )
    # The latest run of the id is the pinned chain's: the same answer, pinned or not.
    assert await control.history_summary(seller.namespace, wf) == pinned
    await seller.client.get_workflow_handle(wf).terminate("done")


async def test_another_run_of_the_id_never_feeds_the_pinned_summary(
    control: Control, seller: Seller
) -> None:
    """The buyer's chain (two runs, two activities each) is terminated by the seller, who
    then starts its own run of the id with three activities. Unpinned, the summary is that
    new run's; pinned, it is still the buyer's chain, closed."""
    wf = f"job-{uuid.uuid4().hex}"
    buyers = await seller.client.start_workflow(
        Chain.run, args=[1, 2, True], id=wf, task_queue=seller.task_queue
    )
    root = buyers.first_execution_run_id
    assert root is not None
    await until(lambda: control.history_summary(seller.namespace, wf, root), _done(4))
    await seller.client.get_workflow_handle(wf).terminate("the seller's doing")
    await seller.client.start_workflow(
        Chain.run, args=[0, 3, True], id=wf, task_queue=seller.task_queue
    )

    latest = await until(lambda: control.history_summary(seller.namespace, wf), _done(3))
    assert (latest.state, latest.continue_as_new_count) == (ObservedStatus.RUNNING, 0)
    pinned = await control.history_summary(seller.namespace, wf, root)
    assert pinned == HistorySummary(
        state=ObservedStatus.TERMINATED,
        last_event="WorkflowExecutionTerminated",
        activities_completed=4,
        activities_running=(),
        continue_as_new_count=1,
    )
    # Asked again (the walk is remembered), and after the seller's run moves on.
    assert await control.history_summary(seller.namespace, wf, root) == pinned
    # A pin that names no run of the id (past retention, say) summarises nothing.
    assert await control.history_summary(seller.namespace, wf, str(uuid.uuid4())) is None
    await seller.client.get_workflow_handle(wf).terminate("done")
