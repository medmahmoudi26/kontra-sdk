"""The ``kontra-job`` header: plain JSON, validated on the way in, never trusted otherwise;
and which heartbeats reach the buyer as progress."""

from __future__ import annotations

import dataclasses
from typing import Any

import relay_bound
from temporalio.api.common.v1 import Payload
from temporalio.testing import ActivityEnvironment

from kontra.codec import stream as s
from kontra.contract.names import new_job_id
from kontra.serve import JOB_HEADER, interceptors
from kontra.serve.interceptors import _ActivityInbound, job_from_headers, job_header
from kontra.serve.turn import TURN_ACTIVITY


def test_the_header_round_trips() -> None:
    job = new_job_id()
    p = job_header(job)
    assert p.metadata["encoding"] == b"json/plain"
    assert job_from_headers({JOB_HEADER: p}) == job


def test_anything_else_is_ignored() -> None:
    def plain(data: bytes) -> Payload:
        return Payload(metadata={"encoding": b"json/plain"}, data=data)

    for headers in (
        {},
        {JOB_HEADER: plain(b'"job-123"')},
        {JOB_HEADER: plain(b'"' + b"a" * 31 + b'G"')},
        {JOB_HEADER: plain(b'"' + b"a" * 32 + b'\\n"')},
        {JOB_HEADER: plain(b"12")},
        {JOB_HEADER: plain(b"{")},
        {JOB_HEADER: plain(b'"' + b"a" * 100 + b'"')},
        {JOB_HEADER: Payload(metadata={"encoding": b"binary/kontra-age"}, data=b"x")},
        {"other": job_header(new_job_id())},
    ):
        assert job_from_headers(headers) is None, headers


class _Sink:
    def __init__(self) -> None:
        self.offered: list[tuple[str, str, tuple[Any, ...]]] = []

    def offer(self, job_id: str, activity: str, details: tuple[Any, ...]) -> None:
        self.offered.append((job_id, activity, details))

    async def settle(self, job_id: str) -> None:
        pass


def _offered(activity_type: str) -> list[tuple[str, str, tuple[Any, ...]]]:
    """What a heartbeat of an activity of ``activity_type`` working for a job offers the
    progress forwarder."""
    sink = _Sink()
    inbound = _ActivityInbound(None, sink)  # type: ignore[arg-type]  # next is not called
    inbound.job_id = new_job_id()
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, activity_type=activity_type)
    env.run(inbound.offer, ({"percent": 50},))
    return sink.offered


def test_a_sellers_heartbeat_is_offered_as_progress() -> None:
    [(_, kind, details)] = _offered("enrich")
    assert (kind, details) == ("enrich", ({"percent": 50},))


def test_the_turn_activitys_heartbeat_never_reaches_the_buyer() -> None:
    """``kontra.turn.v1.deliver`` heartbeats for liveness (kontra.serve.turn): Kontra's own,
    not the seller's progress, so the forwarder never sees it."""
    assert _offered(TURN_ACTIVITY) == []


def test_the_guard_is_half_the_pinned_limits() -> None:
    p = relay_bound.pinned()
    assert p["limit.historyCount.error"] // 2 == interceptors.STREAM_HISTORY_EVENTS
    assert p["limit.historySize.error"] // 2 == interceptors.STREAM_HISTORY_BYTES
    assert p["limit.blobSize.warn"] >= s.ENVELOPE_MAX
    # The run's poll budget is half its updates; the other half stays the seller's own.
    assert p["history.maxTotalUpdates"] // 2 == interceptors.STREAM_POLLS_MAX == s.STREAM_POLLS_MAX
    # Polls in flight leave the seller four of the run's update slots.
    assert p["history.maxInFlightUpdates"] - 4 == interceptors.STREAM_PARKED_MAX
    assert interceptors.STREAM_FULL == "KontraStreamFull"
    assert interceptors.STREAM_BUSY == "KontraStreamBusy"


def test_the_relays_share_of_a_run_is_bounded_under_the_limits() -> None:
    """Decisions I39's formula, from the pinned files: whoever holds a relay token and
    whatever offsets it polls from, it adds at most 5,010 events and 10,752,828 bytes to a
    run, plus one workflow-task triple beside each of the seller's own; a run whose own
    history stays under the rest survives it. The bytes guard, not the poll count, binds for
    requests padded to the Authorizer's cap."""
    p = relay_bound.pinned()
    b = relay_bound.bound()
    assert b.per_poll_bytes == 26_502
    assert b.events == 5_010
    assert b.bytes == 10_752_828
    events_left = p["limit.historyCount.error"] - b.events
    bytes_left = p["limit.historySize.error"] - b.bytes
    assert (events_left, bytes_left) == (15_470, 10_218_692)  # what the seller's own may use
    assert b.events < p["limit.historyCount.error"] // 4 + 1  # 24.5%
    assert b.bytes < p["limit.historySize.error"] * 52 // 100  # 51%
    # Counting each of the seller's tasks twice: a job whose own history is under about
    # 7.7k events survives on the events side.
    own = 7_734
    assert own + b.events_with(own // 3) < p["limit.historyCount.error"]
    # The poll count would allow more bytes than the guard: the guard binds.
    assert s.STREAM_POLLS_MAX * b.per_poll_bytes > interceptors.STREAM_HISTORY_BYTES
    # Each run of a continue-as-new chain is bounded alike: the counts are per run.


def test_the_relays_rate_and_shape_are_pinned_in_the_authorizer() -> None:
    """The Authorizer holds a relay subject to ``RelayRate`` requests a second, a twentieth
    of its namespace's frontend rate, and gives the reason ``kontra._relay`` waits on."""
    p = relay_bound.pinned()
    rate = int(relay_bound.authorizer_constant("RelayRate"))
    burst = int(relay_bound.authorizer_constant("RelayBurst"))
    assert 0 < rate * 20 <= p["frontend.namespaceRPS"]
    assert burst >= 2 * rate
    from kontra import _relay

    assert relay_bound.authorizer_constant("RelayRateReason") == _relay.RELAY_RATE_REASON


def test_the_answer_budget_leaves_room_in_a_workflow_tasks_reply() -> None:
    """What a root answers in one workflow task (``STREAM_ANSWER_BUDGET``) holds every
    answer in flight (references: at most ``ENVELOPE_MAX`` each); and with the rest of the
    worker's reply to that task -- the update requests it echoes (at most
    ``history.maxInFlightUpdates`` in flight, each a relay's at most ``RelayRequestMax``), the
    answers with no items, one payload of the seller's at ``limit.blobSize.error`` -- it stays
    under gRPC's 4 MiB message, which a reply must fit or Temporal ends the run."""
    p = relay_bound.pinned()
    in_flight = p["history.maxInFlightUpdates"]
    request = int(relay_bound.authorizer_constant("RelayRequestMax"))
    held = s.envelope_bound(0)
    assert in_flight * s.ENVELOPE_MAX <= interceptors.STREAM_ANSWER_BUDGET  # never reached
    rest = p["limit.blobSize.error"] + in_flight * (request + held)
    assert interceptors.STREAM_ANSWER_BUDGET + rest < 4 * 1024 * 1024


def test_the_hosted_stream_is_the_contribs_own_class() -> None:
    """``WorkflowStream`` reads its caller's frame name: Kontra's subclass must be built from
    an ``__init__``, and must stay the contrib's implementation."""
    import inspect

    from temporalio.contrib.workflow_streams import WorkflowStream

    assert issubclass(interceptors._HostedStream, WorkflowStream)  # pyright: ignore[reportPrivateUsage]
    source = inspect.getsource(WorkflowStream.__init__)
    assert 'caller_name != "__init__"' in source
    assert set(vars(interceptors._HostedStream)) <= {"__init__", "__module__", "__doc__", "__qualname__", "__firstlineno__", "__static_attributes__"}  # pyright: ignore[reportPrivateUsage]  # fmt: skip


def test_an_answer_is_cut_at_the_item_cap() -> None:
    """A poll's answer is cut at ``STREAM_ITEMS_MAX`` items -- a count, the same on any worker
    and on replay -- with the contrib's own truncation protocol, so every answer has a fixed
    upper size (decisions I39)."""
    from temporalio.contrib.workflow_streams import PollResult
    from temporalio.contrib.workflow_streams._types import (  # pyright: ignore[reportPrivateUsage]
        _WorkflowStreamWireItem,  # pyright: ignore[reportPrivateUsage]
    )

    from kontra.serve.interceptors import KontraInterceptor

    items = [_WorkflowStreamWireItem("events", "A" * 4_000, 7 + i) for i in range(400)]
    inbound = KontraInterceptor(None)._inbound(None)  # type: ignore[arg-type]  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    cut = inbound._trim(PollResult(items=items, next_offset=407))  # pyright: ignore[reportPrivateUsage]
    assert len(cut.items) == s.STREAM_ITEMS_MAX
    assert (cut.more_ready, cut.next_offset) == (True, 7 + s.STREAM_ITEMS_MAX)
    exact = PollResult(items=items[: s.STREAM_ITEMS_MAX], next_offset=99, more_ready=False)
    assert inbound._trim(exact) is exact  # pyright: ignore[reportPrivateUsage]
