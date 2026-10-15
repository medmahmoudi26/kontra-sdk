"""kontra._control against a real Temporal dev server (CLI 1.9.1, server 1.32): every call
the two gates make on the internal frontend, with the namespace named in each request."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.converter import DataConverter, PayloadCodec
from temporalio.worker import Worker

from kontra._control import (
    DEFAULT_RETENTION,
    RETENTION_MARGIN,
    BuyerCall,
    Control,
    EndpointConflict,
    HistorySummary,
    NexusSyncKind,
    TemporalError,
)
from kontra.contract.names import new_endpoint_name
from kontra.contract.offering import MAX_DURATION_RANGE
from kontra.escrow import DEFAULT_GRACE, ObservedStatus, Provenance
from kontra.gate_client import models as m
from kontra.testing.devserver import DevServer, free_port
from kontra.workers import verdict

from .flows import Job, Plan, hang, step
from .handlers import Calls, make_handler

pytestmark = pytest.mark.temporal


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def json_payload(value: object) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode())


#: A buyer's call no workflow here was started by (provenance has its own tests).
NO_CALL = BuyerCall("nobody", new_endpoint_name(), "x.y", frozenset())


@dataclass
class Seller:
    """A namespace with a worker serving :mod:`.handlers` and :mod:`.flows`."""

    namespace: str
    task_queue: str
    service: str
    client: Client
    calls: Calls


@pytest.fixture
async def seller(dev_server: DevServer, control: Control) -> AsyncIterator[Seller]:
    ns = unique("seller")
    await control.ensure_namespace(ns)
    s = Seller(ns, f"{ns}/svc@1.0.0", f"{ns}.svc", dev_server.client_for(ns), Calls())
    async with Worker(
        s.client,
        task_queue=s.task_queue,
        workflows=[Job],
        activities=[step, hang],
        nexus_service_handlers=[make_handler(s.service, s.calls)],
    ):
        yield s


async def wait_status(
    client: Client, workflow_id: str, want: Sequence[WorkflowExecutionStatus]
) -> None:
    for _ in range(200):
        try:
            desc = await client.get_workflow_handle(workflow_id).describe()
        except Exception:
            desc = None
        if desc is not None and desc.status in want:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"{workflow_id} never reached {want}")


# --------------------------------------------------------------------------- namespaces


async def test_ensure_namespace_registers_once_and_is_usable_at_once(
    dev_server: DevServer, control: Control
) -> None:
    ns = unique("party")
    await asyncio.gather(*(control.ensure_namespace(ns) for _ in range(3)))
    await control.ensure_namespace(ns, timedelta(days=1))  # idempotent; retention unchanged
    client = dev_server.client_for(ns)
    handle = await client.start_workflow(Job.run, Plan("slow"), id="probe", task_queue="nobody")
    assert (await handle.describe()).status is WorkflowExecutionStatus.RUNNING
    await handle.terminate("done")


def test_default_retention_outlasts_the_longest_hold() -> None:
    """A hold token can start ``job-<id>`` until max_duration after the hold; only the
    first run's record refuses a second start, so it must outlive every token (FIXUPS
    #19). Tied to the contract's cap, so raising the cap raises retention with it."""
    longest_token, retention = MAX_DURATION_RANGE[1], DEFAULT_RETENTION
    assert retention == longest_token + RETENTION_MARGIN
    assert retention >= longest_token + DEFAULT_GRACE + timedelta(days=1)


async def _retention(control: Control, namespace: str) -> timedelta:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

    d = await control.client.workflow_service.describe_namespace(
        DescribeNamespaceRequest(namespace=namespace)
    )
    return d.config.workflow_execution_retention_ttl.ToTimedelta()


async def test_ensure_namespace_raises_a_short_retention_and_never_lowers_it(
    control: Control,
) -> None:
    ns = unique("party")
    await control.ensure_namespace(ns, timedelta(days=1))  # as an older gate registered it
    assert await _retention(control, ns) == timedelta(days=1)
    await control.ensure_namespace(ns)
    assert await _retention(control, ns) == DEFAULT_RETENTION
    await control.ensure_namespace(ns, timedelta(days=2))
    assert await _retention(control, ns) == DEFAULT_RETENTION  # never shortened
    longer = DEFAULT_RETENTION + timedelta(days=10)
    await control.ensure_namespace(ns, longer)
    assert await _retention(control, ns) == longer
    await control.ensure_namespace(ns)
    assert await _retention(control, ns) == longer
    fresh = unique("party")
    await control.ensure_namespace(fresh)
    assert await _retention(control, fresh) == DEFAULT_RETENTION
    with pytest.raises(ValueError, match="retention"):
        await control.ensure_namespace(fresh, timedelta(0))


async def test_ensure_namespace_refuses_an_empty_name(control: Control) -> None:
    with pytest.raises(ValueError, match="empty"):
        await control.ensure_namespace("")


# --------------------------------------------------------------------------- endpoints


async def test_create_endpoint_is_idempotent_and_never_repoints(control: Control) -> None:
    ns = unique("ep")
    await control.ensure_namespace(ns)
    name = new_endpoint_name()
    assert await control.find_endpoint(name) is None
    created = await control.create_endpoint(name, ns, "acme/enrich@1.2.0")
    assert (created.name, created.namespace, created.task_queue) == (name, ns, "acme/enrich@1.2.0")
    assert created.id
    again = await control.create_endpoint(name, ns, "acme/enrich@1.2.0")
    assert again == created
    assert await control.find_endpoint(name) == created
    with pytest.raises(EndpointConflict):
        await control.create_endpoint(name, ns, "acme/enrich@9.9.9")
    with pytest.raises(EndpointConflict):
        await control.create_endpoint(name, "default", "acme/enrich@1.2.0")


@pytest.mark.parametrize(
    "name", ["acme-enrich", "k" + "A" * 32, "k" + "0" * 31, "", "k" + "0" * 32 + "\n"]
)
async def test_create_endpoint_takes_only_catalog_names(control: Control, name: str) -> None:
    with pytest.raises(ValueError, match="new_endpoint_name"):
        await control.create_endpoint(name, "default", "q")


async def test_create_endpoint_in_a_missing_namespace_fails(control: Control) -> None:
    with pytest.raises(TemporalError):
        await control.create_endpoint(new_endpoint_name(), unique("missing"), "q")


# --------------------------------------------------------------------------- the quote relay


async def test_sync_ok_relays_payload_and_headers(control: Control, seller: Seller) -> None:
    res = await control.nexus_sync(
        seller.namespace,
        seller.task_queue,
        seller.service,
        "echo",
        json_payload({"text": "hello"}),
        {"Kontra-Hold": "tok.en"},
        10.0,
    )
    assert res.kind is NexusSyncKind.OK, res
    assert res.payload is not None
    assert res.payload.metadata["encoding"] == b"json/plain"
    assert json.loads(res.payload.data) == {"text": "hello", "hold": "tok.en"}


@pytest.mark.parametrize(
    ("operation", "needle"),
    [
        ("boom", "quote exploded"),
        ("bad", "bad input"),
        ("refuse", "operation failed: refused on purpose"),
        ("missing", "missing"),
    ],
)
async def test_sync_handler_failures(
    control: Control, seller: Seller, operation: str, needle: str
) -> None:
    res = await control.nexus_sync(
        seller.namespace,
        seller.task_queue,
        seller.service,
        operation,
        json_payload({"text": "x"}),
        {},
        10.0,
    )
    assert res.kind is NexusSyncKind.HANDLER_ERROR, res
    assert needle in res.message
    assert res.payload is None


async def test_sync_unknown_service_is_a_handler_error(control: Control, seller: Seller) -> None:
    res = await control.nexus_sync(
        seller.namespace,
        seller.task_queue,
        "nobody.svc",
        "echo",
        json_payload({"text": "x"}),
        {},
        10.0,
    )
    assert res.kind is NexusSyncKind.HANDLER_ERROR
    assert "nobody.svc" in res.message


async def test_sync_slow_handler_times_out(control: Control, seller: Seller) -> None:
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    res = await control.nexus_sync(
        seller.namespace,
        seller.task_queue,
        seller.service,
        "slow",
        json_payload({"text": "x"}),
        {},
        3.5,
    )
    assert res.kind is NexusSyncKind.TIMEOUT, res
    assert 3.0 <= loop.time() - t0 < 7  # the handler had the whole timeout


async def test_sync_without_a_worker_is_unavailable(control: Control) -> None:
    ns = unique("idle")
    await control.ensure_namespace(ns)
    res = await control.nexus_sync(
        ns, "nobody/home@1.0.0", "x.y", "quote", json_payload({}), {}, 3.0
    )
    assert res.kind is NexusSyncKind.UNAVAILABLE, res
    assert "no worker" in res.message


async def test_sync_in_a_missing_namespace_is_unavailable(control: Control) -> None:
    res = await control.nexus_sync(unique("ghost"), "q", "x.y", "quote", json_payload({}), {}, 3.0)
    assert res.kind is NexusSyncKind.UNAVAILABLE, res


async def test_sync_with_the_frontend_down_is_unavailable(dev_server: DevServer) -> None:
    down = Control(dev_server.client, f"http://127.0.0.1:{free_port()}")
    try:
        res = await down.nexus_sync("default", "q", "x.y", "quote", json_payload({}), {}, 3.0)
    finally:
        await down.aclose()
    assert res.kind is NexusSyncKind.UNAVAILABLE
    assert "unreachable" in res.message


class Wrap(PayloadCodec):
    """Turns every payload into an opaque ``binary/test`` one, as an encrypting codec would."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(metadata={"encoding": b"binary/test"}, data=p.SerializeToString())
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            if p.metadata.get("encoding") == b"binary/test":
                inner = Payload()
                inner.ParseFromString(p.data)
                out.append(inner)
            else:
                out.append(p)
        return out


async def test_sync_non_json_results_come_back_as_payload_protos(
    dev_server: DevServer, control: Control
) -> None:
    ns = unique("codec")
    await control.ensure_namespace(ns)
    tq, svc = f"{ns}/svc@1.0.0", f"{ns}.svc"
    client = dev_server.client_for(ns)
    config = client.config()
    config["data_converter"] = DataConverter(payload_codec=Wrap())
    wrapped = Client(**config)
    async with Worker(wrapped, task_queue=tq, nexus_service_handlers=[make_handler(svc)]):
        res = await control.nexus_sync(ns, tq, svc, "echo", json_payload({"text": "x"}), {}, 10.0)
    assert res.kind is NexusSyncKind.OK, res
    assert res.payload is not None
    assert res.payload.metadata["encoding"] == b"binary/test"
    [inner] = await Wrap().decode([res.payload])
    assert json.loads(inner.data) == {"text": "x", "hold": None}


@pytest.mark.parametrize(
    "headers",
    [
        {"Content-Type": "text/plain"},
        {"Request-Timeout": "1ms"},
        {"nexus-callback-url": "http://evil"},
        {"temporal-x": "1"},
        {"bad header": "1"},
        {"ok": "line\r\nInjected: 1"},
    ],
)
async def test_sync_refuses_headers_it_owns_or_cannot_send(
    control: Control, headers: dict[str, str]
) -> None:
    with pytest.raises(ValueError, match="header"):
        await control.nexus_sync("default", "q", "x.y", "quote", json_payload({}), headers, 3.0)


async def test_sync_works_from_another_event_loop(control: Control, seller: Seller) -> None:
    args = (seller.namespace, seller.task_queue, seller.service, "echo")
    first = await control.nexus_sync(*args, json_payload({"text": "a"}), {}, 10.0)

    def elsewhere() -> NexusSyncKind:
        res = asyncio.run(control.nexus_sync(*args, json_payload({"text": "b"}), {}, 10.0))
        return res.kind

    assert first.kind is NexusSyncKind.OK
    assert await asyncio.to_thread(elsewhere) is NexusSyncKind.OK


async def test_sync_refuses_a_nonsense_timeout(control: Control) -> None:
    for bad in (0.0, -1.0, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="timeout"):
            await control.nexus_sync("default", "q", "x.y", "quote", json_payload({}), {}, bad)


# --------------------------------------------------------------------------- observation


async def test_observe_reports_any_run_but_only_as_the_buyers_once_verified(
    control: Control, seller: Seller
) -> None:
    """A run nobody's Nexus call started is reported with its status and times, as not the
    buyer's, and its result is not even read: it can never be a delivery. (A run a buyer's
    call started: ``test_provenance``.)"""
    obs, result = await control.observe(seller.namespace, "job-missing", NO_CALL, None)
    assert (obs.status, result) == (ObservedStatus.NOT_FOUND, None)

    handle = await seller.client.start_workflow(
        Job.run, Plan("ok"), id=unique("job"), task_queue=seller.task_queue
    )
    await handle.result()
    obs, result = await control.observe(seller.namespace, handle.id, NO_CALL, None)
    assert obs.status is ObservedStatus.COMPLETED
    assert obs.provenance is Provenance.NOT_FROM_BUYER
    assert obs.run_id == handle.result_run_id
    assert obs.started_at is not None
    assert obs.closed_at is not None
    assert obs.started_at <= obs.closed_at
    assert (result, obs.result_sha256, obs.result_size) == (None, None, None)


@pytest.mark.parametrize(
    ("how", "want"),
    [
        ("fail", ObservedStatus.FAILED),
        ("cancel", ObservedStatus.CANCELED),
        ("terminate", ObservedStatus.TERMINATED),
        ("timeout", ObservedStatus.TIMED_OUT),
    ],
)
async def test_observe_unsuccessful_closes(
    control: Control, seller: Seller, how: str, want: ObservedStatus
) -> None:
    wf = unique("job")
    plan = Plan("fail" if how == "fail" else "slow")
    timeout = timedelta(seconds=1) if how == "timeout" else None
    await seller.client.start_workflow(
        Job.run, plan, id=wf, task_queue=seller.task_queue, execution_timeout=timeout
    )
    if how != "fail":
        await wait_status(seller.client, wf, [WorkflowExecutionStatus.RUNNING])
        obs, _ = await control.observe(seller.namespace, wf, NO_CALL, None)
        assert obs.status is ObservedStatus.RUNNING
        assert obs.started_at is not None
        assert obs.closed_at is None
    if how == "cancel":
        await control.cancel_workflow(seller.namespace, wf, "buyer canceled")
    elif how == "terminate":
        await control.terminate_workflow(seller.namespace, wf, "max_duration exceeded")
    closed = {
        WorkflowExecutionStatus.FAILED,
        WorkflowExecutionStatus.CANCELED,
        WorkflowExecutionStatus.TERMINATED,
        WorkflowExecutionStatus.TIMED_OUT,
    }
    await wait_status(seller.client, wf, list(closed))
    obs, result = await control.observe(seller.namespace, wf, NO_CALL, None)
    assert obs.status is want
    assert obs.provenance is Provenance.NOT_FROM_BUYER
    assert result is None
    assert obs.closed_at is not None
    # stopping what is already closed, or gone, is not an error
    await control.cancel_workflow(seller.namespace, wf, "again")
    await control.terminate_workflow(seller.namespace, wf, "again")
    await control.cancel_workflow(seller.namespace, "job-nope", "gone")
    await control.terminate_workflow(seller.namespace, "job-nope", "gone")


async def test_a_missing_namespace_is_a_platform_fault_not_a_job_fact(control: Control) -> None:
    ghost = unique("ghost")
    with pytest.raises(TemporalError):
        await control.terminate_workflow(ghost, "wf", "x")
    with pytest.raises(TemporalError):
        await control.cancel_workflow(ghost, "wf", "x")
    with pytest.raises(TemporalError):
        await control.observe(ghost, "wf", NO_CALL, None)
    with pytest.raises(TemporalError):
        await control.history_summary(ghost, "wf")


# --------------------------------------------------------------------------- history summary


async def test_history_summary_counts_the_whole_chain(control: Control, seller: Seller) -> None:
    assert await control.history_summary(seller.namespace, "job-missing") is None
    handle = await seller.client.start_workflow(
        Job.run, Plan("can2"), id=unique("job"), task_queue=seller.task_queue
    )
    await seller.client.get_workflow_handle(handle.id).result()
    summary = await control.history_summary(seller.namespace, handle.id)
    assert summary == HistorySummary(
        state=ObservedStatus.COMPLETED,
        last_event="WorkflowExecutionCompleted",
        activities_completed=6,
        activities_running=(),
        continue_as_new_count=2,
    )
    # the closed runs are remembered, and the answer does not change
    assert await control.history_summary(seller.namespace, handle.id) == summary


async def test_history_summary_names_running_activities(control: Control, seller: Seller) -> None:
    wf = unique("job")
    await seller.client.start_workflow(Job.run, Plan("hang"), id=wf, task_queue=seller.task_queue)
    summary: HistorySummary | None = None
    for _ in range(100):
        summary = await control.history_summary(seller.namespace, wf)
        if summary is not None and summary.activities_running:
            break
        await asyncio.sleep(0.05)
    assert summary is not None
    assert summary.state is ObservedStatus.RUNNING
    assert summary.activities_running == ("hang",)
    assert summary.activities_completed == 0
    assert summary.last_event == "ActivityTaskScheduled"
    await control.terminate_workflow(seller.namespace, wf, "done")


async def test_terminate_with_a_first_run_id_stays_inside_that_chain(
    control: Control, seller: Seller
) -> None:
    """A cancel terminates only the job's pinned chain: naming another chain's first run
    leaves the open run alone (and is no error); naming its own ends it."""
    wf = unique("job")
    handle = await seller.client.start_workflow(
        Job.run, Plan("hang"), id=wf, task_queue=seller.task_queue
    )
    run_id = handle.result_run_id
    assert run_id
    await control.terminate_workflow(seller.namespace, wf, "not this chain", str(uuid.uuid4()))
    await asyncio.sleep(0.2)
    assert (await handle.describe()).status is WorkflowExecutionStatus.RUNNING
    await control.terminate_workflow(seller.namespace, wf, "buyer canceled", run_id)
    await wait_status(seller.client, wf, [WorkflowExecutionStatus.TERMINATED])


# --------------------------------------------------------------------------- standalone operations


async def test_start_operation_in_a_buyers_namespace_is_idempotent(
    control: Control, seller: Seller
) -> None:
    buyer = unique("buyer")
    await control.ensure_namespace(buyer)
    endpoint = new_endpoint_name()
    await control.create_endpoint(endpoint, seller.namespace, seller.task_queue)
    wf = unique("job")
    op_id = unique("op")
    args = (
        buyer,
        op_id,
        endpoint,
        seller.service,
        "run",
        json_payload({"text": "ok"}),
        {"x-workflow-id": wf, "kontra-hold": "tok"},
        timedelta(minutes=5),
    )
    await control.start_operation(*args)
    await control.start_operation(*args)  # an existing operation is success
    await wait_status(seller.client, wf, [WorkflowExecutionStatus.COMPLETED])
    assert await seller.client.get_workflow_handle(wf).result() == "OK"
    await control.start_operation(*args)  # even once it has completed
    assert seller.calls.run == [wf]


async def test_start_operation_validates_before_calling(control: Control) -> None:
    with pytest.raises(ValueError, match="schedule_to_close"):
        await control.start_operation(
            "default",
            "op",
            new_endpoint_name(),
            "x.y",
            "execute",
            json_payload({}),
            {},
            timedelta(0),
        )
    with pytest.raises(ValueError, match="header"):
        await control.start_operation(
            "default",
            "op",
            new_endpoint_name(),
            "x.y",
            "execute",
            json_payload({}),
            {"a b": "c"},
            timedelta(1),
        )


async def test_start_operation_in_a_missing_namespace_is_a_temporal_error(control: Control) -> None:
    with pytest.raises(TemporalError):
        await control.start_operation(
            unique("ghost"),
            "op",
            new_endpoint_name(),
            "x.y",
            "execute",
            json_payload({}),
            {},
            timedelta(minutes=1),
        )


async def test_history_summary_reads_an_unchanged_history_once(
    control: Control, seller: Seller, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The sweeper asks for every open job's summary on every pass; a seller's history can
    reach Temporal's 50 MB per run, so it is read again only when it grew."""
    reads: list[str] = []
    real = control._events  # pyright: ignore[reportPrivateUsage]

    async def counting(namespace: str, workflow_id: str, run_id: str, **kw: bool) -> list[object]:
        reads.append(run_id)
        return await real(namespace, workflow_id, run_id, **kw)  # type: ignore[return-value]

    monkeypatch.setattr(control, "_events", counting)
    wf = unique("job")
    await seller.client.start_workflow(Job.run, Plan("hang"), id=wf, task_queue=seller.task_queue)
    summary: HistorySummary | None = None
    for _ in range(100):
        summary = await control.history_summary(seller.namespace, wf)
        if summary is not None and summary.activities_running:
            break
        await asyncio.sleep(0.05)
    assert summary is not None
    reads.clear()
    for _ in range(3):  # the activity heartbeats, but heartbeats are not history events
        assert await control.history_summary(seller.namespace, wf) == summary
    assert reads == []
    await control.terminate_workflow(seller.namespace, wf, "done")
    await wait_status(seller.client, wf, [WorkflowExecutionStatus.TERMINATED])
    closed = await control.history_summary(seller.namespace, wf)
    assert closed is not None
    assert (closed.state, closed.last_event) == (
        ObservedStatus.TERMINATED,
        "WorkflowExecutionTerminated",
    )
    assert len(reads) == 1  # it grew: read again, once


async def test_a_long_chain_is_counted_within_a_read_budget(
    control: Control, seller: Seller, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A seller can continue-as-new thousands of times: one summary reads at most
    CHAIN_READS earlier histories, the count converges over calls, and once counted the
    chain costs nothing more."""
    import kontra._control as control_module

    monkeypatch.setattr(control_module, "CHAIN_READS", 1)
    reads: list[str] = []
    real = control._events  # pyright: ignore[reportPrivateUsage]

    async def counting(namespace: str, workflow_id: str, run_id: str, **kw: bool) -> list[object]:
        reads.append(run_id)
        return await real(namespace, workflow_id, run_id, **kw)  # type: ignore[return-value]

    monkeypatch.setattr(control, "_events", counting)
    handle = await seller.client.start_workflow(
        Job.run, Plan("can3"), id=unique("job"), task_queue=seller.task_queue
    )
    await seller.client.get_workflow_handle(handle.id).result()
    counts: list[tuple[int, int]] = []
    for _ in range(4):
        reads.clear()
        s = await control.history_summary(seller.namespace, handle.id)
        assert s is not None
        counts.append((s.continue_as_new_count, s.activities_completed))
        assert len(reads) <= 2  # the current run (first call only) + one earlier run
    assert counts == [(1, 4), (2, 6), (3, 8), (3, 8)]
    assert reads == []


async def test_aclose_from_another_loop_closes_the_relay_client(
    control: Control, seller: Seller
) -> None:
    """The relay client is made on the first caller's loop; closing from a gate's thread
    must not leave it to the garbage collector."""
    args = (seller.namespace, seller.task_queue, seller.service, "echo")
    assert (await control.nexus_sync(*args, json_payload({"text": "a"}), {}, 10.0)).kind is (
        NexusSyncKind.OK
    )
    own = control._own_http  # pyright: ignore[reportPrivateUsage]
    assert own is not None
    await asyncio.to_thread(lambda: asyncio.run(control.aclose()))
    for _ in range(50):
        if own.is_closed:
            break
        await asyncio.sleep(0.01)
    assert own.is_closed


# --------------------------------------------------------------------------- task queues


async def test_describe_task_queue_lists_the_worker_under_each_type(
    control: Control, seller: Seller
) -> None:
    """The seller's worker polls all three types of its task queue; Temporal lists it under
    each, with the build id its SDK sent (an unversioned Python worker's: a hash of its
    code), and reports each type's stats."""
    described: dict[m.TaskQueueType, m.TaskQueueTypeInfo] = {}
    for _ in range(100):
        described = await control.describe_task_queue(seller.namespace, seller.task_queue)
        if all(info.pollers for info in described.values()):
            break
        await asyncio.sleep(0.1)
    assert set(described) == set(m.TaskQueueType)
    for info in described.values():
        [poller] = info.pollers
        assert poller.identity == seller.client.identity
        assert poller.build_id
        assert poller.rate_per_second > 0
        assert abs(datetime.now(UTC) - poller.last_access_time) < timedelta(minutes=2)
        assert info.stats.approximate_backlog_count == 0
    assert verdict(described) == "idle, 1 poller"


async def test_describe_task_queue_reports_a_backlog_nobody_polls(
    control: Control, seller: Seller
) -> None:
    queue = unique("unpolled")
    for _ in range(2):
        await seller.client.start_workflow(Job.run, Plan("ok"), id=unique("job"), task_queue=queue)
    stats = m.TaskQueueStats()
    described: dict[m.TaskQueueType, m.TaskQueueTypeInfo] = {}
    for _ in range(150):  # Temporal answers from a cache it refreshes every 5 s
        described = await control.describe_task_queue(seller.namespace, queue)
        stats = described[m.TaskQueueType.WORKFLOW].stats
        if stats.approximate_backlog_count == 2 and stats.approximate_backlog_age > 0:
            break
        await asyncio.sleep(0.2)
    assert stats.approximate_backlog_count == 2
    assert stats.tasks_dispatch_rate == 0
    assert stats.backlog_increase_rate == stats.tasks_add_rate
    assert not any(info.pollers for info in described.values())
    line = verdict(described)
    assert line.startswith("workflow backlog of 2 for "), line
    assert line.endswith(", no workflow pollers -- start a worker"), line


async def test_describe_task_queue_in_a_missing_namespace_is_a_temporal_error(
    control: Control,
) -> None:
    with pytest.raises(TemporalError, match="describe the workflow task queue"):
        await control.describe_task_queue(unique("nowhere"), "q")
