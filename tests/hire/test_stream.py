"""``job.stream(topic, type=T)`` (decisions I31): one topic of a job's workflow stream,
relayed by the gate, opened with the job's key, its sealed topic and offset checked,
validated as the buyer's own type, deduplicated by cursor and by offset.

First the iterator against a scripted gate (each rule on its own), then a real hire: a
seller that publishes with temporalio's own API on a topic its offering lists and on one it
does not, its heartbeats as progress, and a buyer reading both."""

from __future__ import annotations

import asyncio
import base64
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict
from temporalio.api.common.v1 import Payload

from kontra.codec.age import encrypt, generate_identity
from kontra.codec.stream import SEALED_JOB_KEY, SEALED_OFFSET_KEY, SEALED_TOPIC_KEY
from kontra.gate_client import models as m
from kontra.hire import JobStream, StreamInterrupted, StreamItemInvalid
from kontra.hire._stream import open_event
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

from .models import FeedEvent
from .sellers import feed_worker
from .support import handle

JOB = "0123456789abcdef0123456789abcdef"
ME = generate_identity()
OTHER = generate_identity()
NOW = datetime(2026, 10, 20, tzinfo=UTC)


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")

    n: int


def sealed(
    topic: str, offset: int, data: bytes, *, to: str = ME[1], job: str = JOB, **meta: bytes
) -> str:
    """An item's ciphertext as the seller's codec seals it (topic, offset and job inside)."""
    p = Payload(
        metadata={
            "encoding": b"json/plain",
            SEALED_TOPIC_KEY: topic.encode(),
            SEALED_OFFSET_KEY: str(offset).encode(),
            SEALED_JOB_KEY: job.encode(),
            **meta,
        },
        data=data,
    )
    return base64.b64encode(encrypt(p.SerializeToString(), [to])).decode()


def item(
    cursor: int, offset: int, data: bytes, *, topic: str = "events", **kw: Any
) -> m.StreamEvent:
    ct = kw.pop("ct", None) or sealed(
        kw.pop("sealed_topic", topic),
        kw.pop("sealed_offset", offset),
        data,
        job=kw.pop("sealed_job", JOB),
        **kw,
    )
    return m.StreamEvent(
        cursor=cursor, kind=m.StreamEventKind.ITEM, at=NOW, topic=topic, offset=offset, ciphertext_b64=ct
    )  # fmt: skip


def gap(
    cursor: int, reason: str, offset: int | None = None, topic: str = "events"
) -> m.StreamEvent:
    return m.StreamEvent.model_validate(
        {
            "cursor": cursor,
            "kind": "gap",
            "at": NOW,
            "reason": reason,
            **({"topic": topic, "offset": offset, "size": 300_000} if offset is not None else {}),
        }
    )


@dataclass
class StubGate:
    events: list[m.StreamEvent]
    asked: list[dict[str, Any]] = field(default_factory=list[dict[str, Any]])
    closed: int = 0

    def stream(self, job_id: str, **kw: Any) -> AsyncIterator[m.StreamEvent]:
        self.asked.append(kw)
        return self._events(kw.get("after", 0))

    async def _events(self, after: int) -> AsyncIterator[m.StreamEvent]:
        try:
            for e in self.events:
                if e.cursor > after:
                    yield e
        finally:
            self.closed += 1


@dataclass
class StubKontra:
    gate: StubGate
    identities: tuple[str, ...] = (ME[0],)
    ensured: list[str] = field(default_factory=list[str])

    async def _ensure_job_key(self, job_id: str, *, required: bool = True) -> None:
        self.ensured.append(job_id)


def stream(
    events: list[m.StreamEvent], type_: Any = Any, after: int = 0
) -> tuple[JobStream[Any], StubKontra]:
    from pydantic import TypeAdapter

    k = StubKontra(StubGate(events))
    return JobStream(k, JOB, "events", TypeAdapter(type_), after), k  # type: ignore[arg-type]  # fmt: skip


async def read(s: JobStream[Any]) -> list[Any]:
    return [v async for v in s]


async def test_items_are_opened_validated_and_resumable() -> None:
    s, k = stream([item(1, 0, b'{"n": 0}'), item(2, 3, b'{"n": 3}')], Event)
    assert await read(s) == [Event(n=0), Event(n=3)]
    assert s.cursor == 2
    assert k.ensured == [JOB]
    assert k.gate.asked == [{"topics": ["events"], "history": False, "after": 0}]
    assert k.gate.closed == 1
    resumed, k = stream([item(1, 0, b'{"n": 0}'), item(2, 3, b'{"n": 3}')], Event, after=1)
    assert await read(resumed) == [Event(n=3)]
    assert k.gate.asked[0]["after"] == 1


async def test_without_a_type_the_json_as_it_is() -> None:
    s, _ = stream([item(1, 0, b'{"any": [1, "x"]}'), item(2, 1, b"null")])
    assert await read(s) == [{"any": [1, "x"]}, None]


async def test_duplicates_by_cursor_and_by_offset_are_skipped() -> None:
    events = [item(1, 0, b'{"n": 0}'), item(1, 0, b'{"n": 0}'), item(2, 0, b'{"n": 0}'), item(3, 1, b'{"n": 1}')]  # fmt: skip
    s, _ = stream(events, Event)
    assert await read(s) == [Event(n=0), Event(n=1)]  # a replay overlap; a re-read poll


async def test_an_item_not_sealed_for_us_relabelled_or_another_jobs_is_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    events = [
        item(1, 0, b'{"n": 0}', to=OTHER[1]),  # a topic the worker does not list
        item(2, 1, b'{"n": 1}', sealed_topic="private"),  # relabelled by a gate
        item(3, 2, b'{"n": 2}', sealed_offset=7),  # moved by a gate
        item(4, 3, b'{"n": 3}', ct="bm90IGFnZQ=="),  # not an age file at all
        # Another job's item, the same topic and offset, sealed to a key we hold too: a gate
        # splicing one job's stream into another's.
        item(5, 4, b'{"n": 99}', sealed_job="f" * 32),
        item(6, 4, b'{"n": 4}'),
    ]
    s, _ = stream(events, Event)
    with caplog.at_level(logging.WARNING, "kontra.hire"):
        assert await read(s) == [Event(n=4)]
    assert "not sealed for us" in caplog.text
    assert "relabelled" in caplog.text


async def test_an_item_that_is_not_the_type_raises() -> None:
    s, _ = stream([item(1, 0, b'{"n": "zero", "secret": "x"}')], Event)
    with pytest.raises(StreamItemInvalid) as e:
        await read(s)
    assert (e.value.topic, e.value.offset, e.value.job_id) == ("events", 0, JOB)
    assert "zero" not in str(e.value)  # where and what, never the value
    other = item(1, 0, b"\x00", **{"encoding": b"binary/protobuf"})
    s, _ = stream([other], dict)
    with pytest.raises(StreamItemInvalid, match="only JSON"):
        await read(s)
    lenient, _ = stream([item(1, 0, b'{"n": "zero"}')], dict)
    assert await read(lenient) == [{"n": "zero"}]


async def test_gaps_are_logged_and_a_stopped_relay_raises(caplog: pytest.LogCaptureFixture) -> None:
    events = [item(1, 0, b'{"n": 0}'), gap(2, "too_large", offset=1), item(3, 2, b'{"n": 2}'), gap(4, "history_budget")]  # fmt: skip
    s, k = stream(events, Event)
    got: list[Any] = []

    async def follow() -> None:
        async for v in s:
            got.append(v)

    with caplog.at_level(logging.INFO, "kontra.hire"), pytest.raises(StreamInterrupted) as e:
        await follow()
    assert got == [Event(n=0), Event(n=2)]
    assert e.value.reason == "history_budget"
    assert "too_large" in caplog.text
    assert k.gate.closed == 1  # closed when it raised


async def test_history_events_and_other_topics_are_not_items() -> None:
    events = [
        m.StreamEvent(cursor=1, kind=m.StreamEventKind.HISTORY, at=NOW, state=m.JobState.WORKING),
        item(2, 0, b'{"n": 0}', topic="kontra.progress"),
        item(3, 1, b'{"n": 1}'),
    ]
    s, _ = stream(events, Event)
    assert await read(s) == [Event(n=1)]


def test_open_event_wants_an_item() -> None:
    history = m.StreamEvent(cursor=1, kind=m.StreamEventKind.HISTORY, at=NOW, state=m.JobState.WORKING)  # fmt: skip
    assert open_event(history, [ME[0]], JOB) is None


async def test_a_topic_name_is_checked_before_anything_is_sent() -> None:
    from kontra.hire._client import Job

    job = Job(StubKontra(StubGate([])), job_id=JOB, offering="acme/x@1.0.0", amount_minor=1, currency="USD", operation_id="o")  # type: ignore[arg-type]  # fmt: skip
    for bad in ("bad topic", "", "x" * 65):
        with pytest.raises(ValueError, match="topic"):
            job.stream(bad)
    assert isinstance(job.stream("events", type=Event, after=3), JobStream)
    with pytest.raises(ValueError, match="cursor"):
        job.stream("events", after=-1)


# --------------------------------------------------------------------------- a real hire


@pytest.mark.temporal
async def test_a_buyer_reads_a_listed_topic_and_its_progress(
    dev_server: DevServer, tmp_path: Path
) -> None:
    worker, bound = feed_worker(tmp_path)
    async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
        acme = await mp.party("acme")
        async with mp.serve(worker, acme):
            party = await mp.party(handle(), balance="10.00")
            k = party.kontra(client=mp.client(party.namespace))
            try:
                entry = await k.offering(bound.key)
                assert entry.streams == ["events"]
                job = await (await k.quote(bound.key, count=20, note="hello")).hire(max_price=1)
                progress = asyncio.create_task(_collect(job.progress()))
                events = [e async for e in job.stream("events", type=FeedEvent)]
                beats = [p for p in await asyncio.wait_for(progress, 60) if p.source == "heartbeat"]
                result = await job.result()
            finally:
                await k.aclose()
    assert result.published == 20
    assert events == [FeedEvent(n=n, note="hello") for n in range(20)]
    assert beats  # latest wins at the interval: at least the last one
    assert beats[-1].percent == 100.0
    assert beats[-1].activity == "feed"


async def _collect(it: AsyncIterator[Any]) -> list[Any]:
    return [v async for v in it]
