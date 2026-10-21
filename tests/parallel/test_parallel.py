"""``kontra.contrib.parallel.execute_activities_in_parallel`` and its ``kontra.batch.v1`` child,
on a real dev server with :class:`~kontra.contrib.parallel.ParallelPlugin` on a plain
temporalio worker: both sources (a page of the helper's and one of the seller's own), the
fold, backpressure, failure, refusals at call time, replay, and a batch that continues as new
mid-page on a server that suggests it early."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio
from temporalio import activity as temporal_activity
from temporalio import workflow as temporal_workflow
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError
from temporalio.worker import Replayer
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner

from kontra.codec import KontraCodec
from kontra.codec.age import generate_identity
from kontra.contrib.parallel import (
    BATCH_WORKFLOW,
    Page,
    ParallelPlugin,
    _Batch,
    _Cursor,
    _Options,
    _page,
    _Ref,
    _Spec,
    execute_activities_in_parallel,
)
from kontra.serve.worker import seller_data_converter
from kontra.testing.devserver import DevServer, start_dev_server

from . import acts
from .conftest import serving
from .flows import Parent
from .models import Hit, Tally, Words

_T = timedelta(seconds=30)
#: The server suggests continue-as-new past this many history events (default: thousands).
_SUGGEST_CAN_AT = 60


async def _start(
    client: Client, task_queue: str, case: str, n: int = 0
) -> WorkflowHandle[Any, Any]:
    return await client.start_workflow(
        Parent.run,
        args=[case, n],
        id=f"parent-{case}-{uuid.uuid4().hex[:8]}",
        task_queue=task_queue,
        execution_timeout=timedelta(seconds=60),
    )


async def _children(client: Client, parent: WorkflowHandle[Any, Any]) -> list[tuple[str, str]]:
    """(workflow id, first run id) of every batch child the parent started."""
    found: list[tuple[str, str]] = []
    async for e in client.get_workflow_handle(parent.id).fetch_history_events():
        if e.event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:
            a = e.child_workflow_execution_started_event_attributes
            assert a.workflow_type.name == BATCH_WORKFLOW
            found.append((a.workflow_execution.workflow_id, a.workflow_execution.run_id))
    return found


async def _runs(client: Client, workflow_id: str, run_id: str) -> list[str]:
    """The run ids of a continue-as-new chain, first to last."""
    runs = [run_id]
    while True:
        handle = client.get_workflow_handle(workflow_id, run_id=runs[-1])
        last = None
        async for e in handle.fetch_history_events():
            last = e
        assert last is not None
        if last.event_type != EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
            return runs
        runs.append(last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id)


async def _replay(client: Client, workflow_id: str, run_id: str | None = None) -> None:
    history = await client.get_workflow_handle(workflow_id, run_id=run_id).fetch_history()
    await Replayer(
        workflows=[Parent], plugins=[ParallelPlugin()], data_converter=pydantic_data_converter
    ).replay_workflow(history)


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def worker(dev_server: DevServer) -> AsyncIterator[tuple[Client, str]]:
    async with serving(dev_server.client) as w:
        yield w


pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="module")]


# --------------------------------------------------------------------------- sources and folds


async def test_items_are_returned_as_a_list(worker: tuple[Client, str]) -> None:
    client, tq = worker
    handle = await _start(client, tq, "items", 12)
    out = await handle.result()
    assert sorted(out) == [2 * i for i in range(12)]
    [(child, run)] = await _children(client, handle)
    assert child.startswith(f"{handle.id}/batch/double/")
    assert await _runs(client, child, run) == [run]  # far below any suggestion: one run
    await _replay(client, handle.id)
    await _replay(client, child)


async def test_an_int_cursor_pages_and_reduce_folds(worker: tuple[Client, str]) -> None:
    client, tq = worker
    assert await (await _start(client, tq, "offsets")).result() == 2 * sum(range(30))


async def test_a_str_cursor_pages_through_an_empty_page(worker: tuple[Client, str]) -> None:
    """``tokens`` returns the seller's own model (``Words``), not the helper's ``Page``."""
    client, tq = worker
    out = await (await _start(client, tq, "tokens")).result()
    assert sorted(out) == ["ALPHA", "BETA", "GAMMA"]


async def test_a_pager_that_returns_its_own_cursor_empty_fails_the_batch(
    worker: tuple[Client, str],
) -> None:
    client, tq = worker
    with pytest.raises(WorkflowFailureError) as e:
        await (await _start(client, tq, "stuck")).result()
    child = e.value.cause
    assert isinstance(child, ChildWorkflowError)
    assert isinstance(child.cause, ApplicationError)
    assert child.cause.type == "KontraBatchStuck"


async def test_one_parent_batches_the_same_activity_twice(worker: tuple[Client, str]) -> None:
    client, tq = worker
    handle = await _start(client, tq, "twice")
    assert await handle.result() == [[2, 4], [20]]
    ids = [c for c, _ in await _children(client, handle)]
    assert len(set(ids)) == 2


async def test_the_item_activity_module_is_passed_through_to_the_child(
    worker: tuple[Client, str],
) -> None:
    """The child finds the activity's module as the parent's code imported it, passed
    through the sandbox: imported afresh inside it, a module that reads ``os.environ`` as it
    loads (``configured``) failed the child's every workflow task, and the call never
    returned."""
    client, tq = worker
    handle = await _start(client, tq, "configured", 4)
    assert sorted(await asyncio.wait_for(handle.result(), 30)) == [0, 3, 6, 9]
    [(child, _)] = await _children(client, handle)
    await _replay(client, child)


async def test_models_reach_reduce_as_models(worker: tuple[Client, str]) -> None:
    client, tq = worker
    out = await (await _start(client, tq, "tally", 10)).result()
    assert Tally.model_validate(out) == Tally(seen=10, open=["host-0", "host-5"])


# --------------------------------------------------------------------------- backpressure, failure


async def test_max_concurrent_bounds_the_activities_in_flight(worker: tuple[Client, str]) -> None:
    client, tq = worker
    acts.LIVE.update(now=0, max=0)
    out = await (await _start(client, tq, "bounded", 20)).result()
    assert sorted(out) == list(range(20))
    assert acts.LIVE["max"] == 3


async def test_an_item_that_fails_for_good_fails_the_call(worker: tuple[Client, str]) -> None:
    client, tq = worker
    handle = await _start(client, tq, "fragile", 6)
    with pytest.raises(WorkflowFailureError) as e:
        await handle.result()
    # Parent <- ChildWorkflowError <- ActivityError <- the activity's own ApplicationError.
    child = e.value.cause
    assert isinstance(child, ChildWorkflowError)
    assert isinstance(child.cause, ActivityError)
    assert isinstance(child.cause.cause, ApplicationError)
    assert (child.cause.cause.type, child.cause.cause.message) == ("BadItem", "item 3 is bad")
    # Nothing was left running unasked: every other item activity completed or was asked to
    # cancel (TRY_CANCEL, the default, does not wait for an activity that never heartbeats).
    [(child_id, _)] = await _children(client, handle)
    scheduled = settled = 0
    async for ev in client.get_workflow_handle(child_id).fetch_history_events():
        t = ev.event_type
        scheduled += t == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
        settled += t in (
            EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED,
            EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
        )
    assert scheduled == 6
    assert settled == 6
    await _replay(client, child_id)


# --------------------------------------------------------------------------- continue-as-new


@pytest_asyncio.fixture(loop_scope="module", scope="module")
async def eager_can() -> AsyncIterator[DevServer]:
    """A dev server of its own that suggests continue-as-new after a few dozen events."""
    try:
        server = await start_dev_server(
            extra_args=[
                "--dynamic-config-value",
                f"limit.historyCount.suggestContinueAsNew={_SUGGEST_CAN_AT}",
            ]
        )
    except Exception as e:
        pytest.skip(f"no Temporal CLI 1.9.1 dev server: {e}")
    async with server:
        yield server


async def test_a_batch_continues_as_new_and_loses_nothing(eager_can: DevServer) -> None:
    """120 items as one in-memory page, with the server suggesting continue-as-new every
    ~60 events: the child rolls over mid-page several times, carrying the rest of the page
    and a pydantic accumulator, and the result is complete and typed."""
    async with serving(eager_can.client) as (client, tq):
        handle = await _start(client, tq, "tally", 120)
        out = Tally.model_validate(await handle.result())
        assert out.seen == 120
        assert sorted(out.open) == sorted(
            f"host-{i}" for i in range(120) if str(i).endswith(("0", "5"))
        )
        [(child, first)] = await _children(client, handle)
        runs = await _runs(client, child, first)
        assert len(runs) >= 3, runs
        for run in runs:
            await _replay(client, child, run)
        await _replay(client, handle.id)

        # pages=: the next page's cursor crosses continue-as-new too.
        handle = await _start(client, tq, "offsets")
        assert await handle.result() == 2 * sum(range(30))
        [(child, first)] = await _children(client, handle)
        assert len(await _runs(client, child, first)) >= 2


# --------------------------------------------------------------------------- refused at call time


def _nested_reduce() -> Any:
    def inner(acc: int, r: int) -> int:
        return acc + r

    return inner


@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({}, ValueError, "exactly one of pages= or items="),
        ({"items": [1], "pages": acts.offsets}, ValueError, "exactly one"),
        ({"items": [1], "reduce": lambda a, r: a + r, "init": 0}, TypeError, "module-level"),
        ({"items": [1], "reduce": _nested_reduce(), "init": 0}, TypeError, "module-level"),
        ({"items": [1], "init": 0}, TypeError, "init= is the starting value"),
        ({"items": [1], "reduce": acts.double, "init": 0}, TypeError, r"\(acc, result\)"),
        ({"items": [1], "max_concurrent": 0}, ValueError, "at least 1"),
        ({"items": "abc"}, TypeError, "not a string"),
        ({"items": [1], "task_queue": "elsewhere"}, TypeError, "parent's"),
        ({"items": [1], "activity_id": "x"}, TypeError, "unexpected keyword"),
        ({"items": [1], "start_to_close_timeout": None}, ValueError, "is required"),
        ({"pages": acts.add}, TypeError, r"@activity\.defn"),
    ],
)
async def test_misuse_is_refused_when_the_call_is_made(
    kwargs: dict[str, Any], error: type[Exception], match: str
) -> None:
    kwargs = {"start_to_close_timeout": _T, **kwargs}
    with pytest.raises(error, match=match):
        await execute_activities_in_parallel(acts.double, **kwargs)


async def test_an_undecorated_or_nested_activity_is_refused() -> None:
    with pytest.raises(TypeError, match=r"@activity\.defn"):
        await execute_activities_in_parallel(acts.add, items=[1], start_to_close_timeout=_T)

    @temporal_activity.defn(name="local_double")
    async def local(x: int) -> int:
        return x

    with pytest.raises(TypeError, match="module-level"):
        await execute_activities_in_parallel(local, items=[1], start_to_close_timeout=_T)


# --------------------------------------------------------------------------- the payloads


async def test_spec_and_cursor_round_trip_through_the_seller_converter() -> None:
    """timedelta, RetryPolicy, Priority and the enum inside the options come back as
    themselves through the converter a Kontra worker's client uses -- typed options, not a
    dict, are what make that true."""
    identity, recipient = generate_identity()
    codec = KontraCodec(identity, [recipient], role="seller")
    converter = seller_data_converter(codec).payload_converter
    spec = _Spec(
        activity="probe",
        activity_ref=_Ref("tests.parallel.acts", "probe"),
        pages="tokens",
        pages_ref=_Ref("tests.parallel.acts", "tokens"),
        items=None,
        max_concurrent=7,
        options=_Options(
            start_to_close_timeout=timedelta(seconds=3),
            heartbeat_timeout=timedelta(milliseconds=1500),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=2),
                maximum_attempts=4,
                non_retryable_error_types=["BadItem"],
            ),
            cancellation_type=temporal_workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        ),
        reduce=_Ref("tests.parallel.acts", "tally"),
    )
    cursor = _Cursor(next="t2", acc={"seen": 1}, started=True, rest=["a", "b"])
    payloads = converter.to_payloads([spec, cursor])
    assert converter.from_payloads(payloads, [_Spec, _Cursor]) == [spec, cursor]
    page = converter.to_payloads([Page(items=[Hit(target="h", open=True)], next=3)])
    assert converter.from_payloads(page, [Page[Hit]]) == [
        Page(items=[Hit(target="h", open=True)], next=3)
    ]


# --------------------------------------------------------------------------- the plugin


async def test_the_plugin_registers_the_child_and_passes_the_helper_through() -> None:
    """``ParallelPlugin`` is all a worker needs: ``kontra.batch.v1`` after the seller's
    workflows, and the helper's module passed through a sandbox (the seller's own included);
    a runner that is not Temporal's sandbox is the seller's choice, kept as it is."""
    plugin = ParallelPlugin()
    assert plugin.name() == "kontra.ParallelPlugin"
    mine = SandboxedWorkflowRunner()
    config: Any = plugin.configure_worker(
        {"workflows": [Parent], "workflow_runner": mine}  # type: ignore[typeddict-item]
    )
    assert config["workflows"] == [Parent, _Batch]
    runner = config["workflow_runner"]
    assert isinstance(runner, SandboxedWorkflowRunner)
    assert "kontra.contrib.parallel" in runner.restrictions.passthrough_modules
    assert "kontra.contrib.parallel" not in mine.restrictions.passthrough_modules  # a copy
    other = object()
    config = plugin.configure_worker({"workflows": [], "workflow_runner": other})  # type: ignore[typeddict-item]
    assert config["workflow_runner"] is other


@pytest.mark.parametrize(
    "page",
    [
        Page(items=[1, 2], next=3),
        Words(items=["a"], next=None),
        {"items": ["x"], "next": "n"},
    ],
)
async def test_a_page_is_anything_with_a_list_of_items_and_a_next(page: Any) -> None:
    items, following = _page("pager", page)
    assert isinstance(items, list)
    assert following == (page["next"] if isinstance(page, dict) else page.next)


@pytest.mark.parametrize("page", [[1, 2], {"items": [1]}, Hit(target="h", open=True), None])
async def test_anything_else_from_a_pager_is_its_bug(page: Any) -> None:
    with pytest.raises(TypeError, match=r"pager returned \w+, not a page"):
        _page("pager", page)
