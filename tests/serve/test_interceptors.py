"""The ``kontra-job`` header: plain JSON, validated on the way in, never trusted otherwise;
and which heartbeats reach the buyer as progress."""

from __future__ import annotations

import dataclasses
from typing import Any

from temporalio.api.common.v1 import Payload
from temporalio.testing import ActivityEnvironment

from kontra.contract.names import new_job_id
from kontra.serve import JOB_HEADER
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
