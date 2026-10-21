"""The ``kontra-job`` header: plain JSON, validated on the way in, never trusted otherwise;
and which heartbeats reach the buyer as progress."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
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
    # The run's poll budget is an eighth of its updates; the rest stays the seller's own.
    assert p["history.maxTotalUpdates"] // 8 == interceptors.STREAM_POLLS_MAX == s.STREAM_POLLS_MAX
    # Polls in flight leave the seller four of the run's update slots.
    assert p["history.maxInFlightUpdates"] - 4 == interceptors.STREAM_PARKED_MAX
    assert interceptors.STREAM_FULL == "KontraStreamFull"
    assert interceptors.STREAM_BUSY == "KontraStreamBusy"


def test_the_relays_share_of_a_run_is_bounded_under_the_limits() -> None:
    """Decisions I39's formula, from the pinned files: whoever holds a relay token and
    whatever offsets it polls from, it adds at most 1,260 events and 6,633,948 bytes to a
    run, plus one workflow-task triple beside each of the seller's own; a run whose own
    history stays under the rest survives it. The poll count binds before the bytes guard,
    even for requests padded to the Authorizer's cap."""
    p = relay_bound.pinned()
    b = relay_bound.bound()
    assert b.per_poll_bytes == 26_502
    assert b.events == 1_260
    assert b.bytes == 6_633_948
    # The drain: a timer and two workflow tasks, each triple at most TASK_BYTES_MAX.
    assert relay_bound.DRAIN_BYTES >= 2 * relay_bound.TASK_BYTES_MAX
    events_left = p["limit.historyCount.error"] - b.events
    bytes_left = p["limit.historySize.error"] - b.bytes
    assert (events_left, bytes_left) == (19_220, 14_337_572)  # what the seller's own may use
    assert b.events < p["limit.historyCount.error"] * 7 // 100  # 6.2%
    assert b.bytes < p["limit.historySize.error"] * 32 // 100  # 31.6%
    # Counting each of the seller's tasks twice: a job whose own history is under about
    # 9.6k events survives on the events side.
    own = 9_609
    assert own + b.events_with(own // 3) < p["limit.historyCount.error"]
    # The poll count binds before the guard: the guard binds only on a history the seller
    # grew itself.
    assert s.STREAM_POLLS_MAX * b.per_poll_bytes < interceptors.STREAM_HISTORY_BYTES
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


# --------------------------------------------------------------------------- bounded work


class _Counted(list[Any]):
    """A log that counts the entries read from it."""

    reads = 0

    def __getitem__(self, index: Any) -> Any:
        type(self).reads += 1
        return super().__getitem__(index)


def _log(topics: list[str], pad: int = 8) -> list[Any]:
    from temporalio.contrib.workflow_streams import WorkflowStreamItem
    from temporalio.converter import default

    convert = default().payload_converter
    return [
        WorkflowStreamItem(topic=t, data=convert.to_payloads([f"{i}-" + "p" * pad])[0])
        for i, t in enumerate(topics)
    ]


async def _contrib(log: list[Any], base: int, poll: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The contrib's own ``_on_poll`` on ``log``, its wait over, cut as Kontra cuts it."""
    from temporalio.contrib.workflow_streams import WorkflowStream
    from temporalio.contrib.workflow_streams import _stream as contrib  # pyright: ignore[reportPrivateUsage]

    async def over(*_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(contrib.workflow, "wait_condition", over)
    stream = object.__new__(WorkflowStream)
    stream._log, stream._base_offset, stream._detaching = log, base, False  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
    inbound = interceptors.KontraInterceptor(None)._inbound(None)  # type: ignore[arg-type]  # pyright: ignore[reportPrivateUsage, reportArgumentType]
    return inbound._trim(await stream._on_poll(poll))  # pyright: ignore[reportPrivateUsage]


async def test_kontras_answer_is_the_contribs_cut_at_the_item_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Where the scan bound does not bind, Kontra's answer (``_answer``) is exactly the
    contrib's own, cut at ``STREAM_ITEMS_MAX``: every offset, every topic filter, a truncated
    log, the ~1 MB cap (decisions I39)."""
    import random

    from temporalio.contrib.workflow_streams import PollInput

    rng = random.Random(5)
    for case in range(300):
        n = rng.choice([0, 1, 5, 63, 64, 65, 130, 400])
        log = _log([rng.choice(["a", "b", "c"]) for _ in range(n)], pad=rng.choice([0, 8, 40_000]))
        base = rng.choice([0, 0, 7])
        offset = rng.choice([0, base, base + n // 2, base + max(n - 1, 0), 3])
        topics = rng.choice([[], ["a"], ["a", "b"], ["z"]])
        poll = PollInput(topics=topics, from_offset=offset)
        try:
            want = await _contrib(list(log), base, poll, monkeypatch)
        except Exception as e:
            with pytest.raises(type(e)):
                interceptors._answer(log, base, poll, encode=True)  # pyright: ignore[reportPrivateUsage]
            continue
        got = interceptors._answer(log, base, poll, encode=True)  # pyright: ignore[reportPrivateUsage]
        assert got == want, case
        replayed = interceptors._answer(log, base, poll, encode=False)  # pyright: ignore[reportPrivateUsage]
        assert [i.offset for i in replayed.items] == [i.offset for i in want.items]
        assert (replayed.next_offset, replayed.more_ready) == (want.next_offset, want.more_ready)
        assert all(i.data == "" for i in replayed.items)


def test_a_poll_costs_its_answer_never_the_log(monkeypatch: pytest.MonkeyPatch) -> None:
    """However long the log and whatever topics a poll names, an answer looks at no more than
    ``STREAM_SCAN_MAX`` entries and encodes no more than ``STREAM_ITEMS_MAX`` items; a poll
    the scan stops is answered ``more_ready`` at the first entry not looked at, so a poller
    reads on; on replay nothing is encoded (decisions I39)."""
    from temporalio.contrib.workflow_streams import PollInput

    encoded: list[int] = []
    real = interceptors._encode_payload  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(interceptors, "_encode_payload", lambda p: encoded.append(1) or real(p))  # pyright: ignore[reportUnknownLambdaType]
    log = _Counted(_log(["small"] * 20_000))
    _Counted.reads = 0
    every = interceptors._answer(log, 0, PollInput(topics=[], from_offset=0), encode=True)  # pyright: ignore[reportPrivateUsage]
    assert len(every.items) == s.STREAM_ITEMS_MAX
    assert (every.next_offset, every.more_ready) == (s.STREAM_ITEMS_MAX, True)
    assert _Counted.reads <= s.STREAM_ITEMS_MAX + 1
    assert len(encoded) == s.STREAM_ITEMS_MAX
    # A topic the log does not hold: a bounded scan, nothing encoded, read on from there.
    _Counted.reads, encoded[:] = 0, []
    none = interceptors._answer(log, 0, PollInput(topics=["other"], from_offset=0), encode=True)  # pyright: ignore[reportPrivateUsage]
    assert none.items == []
    assert (none.next_offset, none.more_ready) == (interceptors.STREAM_SCAN_MAX, True)
    assert _Counted.reads == interceptors.STREAM_SCAN_MAX
    assert encoded == []
    # The last window: the log's end, not more_ready.
    tail = interceptors._answer(
        log, 0, PollInput(topics=["other"], from_offset=19_990), encode=True
    )  # pyright: ignore[reportPrivateUsage]
    assert (tail.items, tail.next_offset, tail.more_ready) == ([], 20_000, False)
    # Replayed: the same answer's shape, nothing encoded.
    _Counted.reads = 0
    replayed = interceptors._answer(log, 0, PollInput(topics=[], from_offset=0), encode=False)  # pyright: ignore[reportPrivateUsage]
    assert len(replayed.items) == s.STREAM_ITEMS_MAX
    assert encoded == []
    assert interceptors.STREAM_SCAN_MAX == 16 * s.STREAM_ITEMS_MAX


def test_a_replayed_answer_is_a_constant_the_codec_passes() -> None:
    """On replay a poll's answer is already in the history and is never sent: the constant
    the interceptor answers then is plaintext ``null``, the same every time, and holds no
    item."""
    a, b = s.replayed_payload(), s.replayed_payload()
    assert a == b
    assert a.data == b"null"
    assert a.metadata[s.STREAM_MARK] == s.REPLAYED_MARK
