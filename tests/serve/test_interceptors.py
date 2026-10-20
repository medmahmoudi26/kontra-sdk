"""The ``kontra-job`` header: plain JSON, validated on the way in, never trusted otherwise;
and which heartbeats reach the buyer as progress."""

from __future__ import annotations

import dataclasses
from typing import Any

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
    from pathlib import Path

    import yaml

    pinned = yaml.safe_load(
        (
            Path(__file__).resolve().parents[2] / "temporal/authorizer/dynamicconfig/kontra.yaml"
        ).read_text()
    )
    assert pinned["limit.historyCount.error"][0]["value"] // 2 == interceptors.STREAM_HISTORY_EVENTS
    assert pinned["limit.historySize.error"][0]["value"] // 2 == interceptors.STREAM_HISTORY_BYTES
    assert pinned["limit.blobSize.warn"][0]["value"] >= s.ENVELOPE_MAX


def test_the_hosted_stream_is_the_contribs_own_class() -> None:
    """``WorkflowStream`` reads its caller's frame name: Kontra's subclass must be built from
    an ``__init__``, and must stay the contrib's implementation."""
    import inspect

    from temporalio.contrib.workflow_streams import WorkflowStream

    assert issubclass(interceptors._HostedStream, WorkflowStream)  # pyright: ignore[reportPrivateUsage]
    source = inspect.getsource(WorkflowStream.__init__)
    assert 'caller_name != "__init__"' in source
    assert set(vars(interceptors._HostedStream)) <= {"__init__", "__module__", "__doc__", "__qualname__", "__firstlineno__", "__static_attributes__"}  # pyright: ignore[reportPrivateUsage]  # fmt: skip
