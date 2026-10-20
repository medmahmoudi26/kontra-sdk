"""Deliberately broken engines: each is :class:`~kontra.testing.FakeEngine` with one bug a real
adapter could plausibly have. The engine contract must reject every one of them (seam ⑥, as
``tests/ledger/broken_ledgers.py`` is for seam ④)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Collection
from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from typing import Literal

from temporalio.api.common.v1 import Payload

from kontra.engine import (
    AGE_MAGIC,
    EngineError,
    Fenced,
    JobHandle,
    Observed,
    Pin,
    Sealed,
    SealedPayload,
    Stop,
    StreamBatch,
    StreamItem,
)
from kontra.escrow import Observation, ObservedStatus
from kontra.testing.engine import FakeEngine, _parse, _Run  # pyright: ignore[reportPrivateUsage]


def _forged(data: bytes) -> SealedPayload:
    """A SealedPayload that skipped its own check: what a buggy adapter could build."""
    bad = object.__new__(SealedPayload)
    object.__setattr__(bad, "data", data)
    return bad


class DoubleStart(FakeEngine):
    """A start is not idempotent by job: every start begins a lineage of its own."""

    async def start(
        self, handle: JobHandle, input: bytes, *, hold: str, deadline: datetime
    ) -> None:
        job = self._job(_parse(handle))
        earlier, job.runs, job.gate_started = job.runs, [], False
        try:
            await super().start(handle, input, hold=hold, deadline=deadline)
        finally:
            job.runs = earlier + job.runs


class SoftEnd(FakeEngine):
    """END only asks: a run that ignores it, or honours it as a cancel, is not ended."""

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        await super().cancel(handle, stop=Stop.ASK, pin=pin, reason=reason)


class RevivableEnd(FakeEngine):
    """The seller can bring an ended run back (Temporal: a reset the authorizer allowed)."""

    def seller_revive(self, handle: JobHandle) -> bool:
        job = self._jobs.get(_parse(handle)["job"])
        for run in job.runs if job is not None else ():
            if run.ended:
                run.ended = False
                run.status = ObservedStatus.COMPLETED
                run.result = Payload(
                    metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"},
                    data=AGE_MAGIC + b"revived",
                ).SerializeToString(deterministic=True)
                return True
        return False


class TrustsAnyRun(FakeEngine):
    """Provenance from the run itself: any run of the job is taken as the buyer's."""

    def _verified(self, run: _Run, holds: Collection[str]) -> bool:
        return True


class Leaky(FakeEngine):
    """Hands the gate a completion whose result is not sealed, as if it were a delivery."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        if seen.observation.status is not ObservedStatus.UNSEALED:
            return seen
        job = self._jobs[_parse(handle)["job"]]
        raw = job.runs[-1].result or b""
        o = replace(
            seen.observation,
            status=ObservedStatus.COMPLETED,
            result_sha256=sha256(raw).hexdigest(),
            result_size=len(raw),
        )
        return Observed(o, _forged(raw))


class NoFence(FakeEngine):
    """Says CLOSED and closes nothing."""

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        _parse(handle)
        self._up()
        return Fenced.CLOSED


class FenceStops(FakeEngine):
    """A fence of a started job ends its run (it would refund a genuine run against the
    seller before it was pinned)."""

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        found = await super().fence(handle, reason=reason)
        if found is Fenced.STARTED:
            await self.cancel(handle, stop=Stop.END, pin=None, reason=reason)
        return found


class DriftingPin(FakeEngine):
    """A restart within the lineage names it anew: the pin escrow holds stops matching."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        o = seen.observation
        job = self._jobs.get(_parse(handle)["job"])
        run = next((r for r in job.runs if r.pin == o.pin), None) if job is not None else None
        if run is None or not run.restarts:
            return seen
        return Observed(replace(o, pin=f"{o.pin}-{run.restarts}"), seen.result)


class StrangerReaches(FakeEngine):
    """A credential reaches every job, its party's or not."""

    def reaches(self, credential: str, handle: JobHandle) -> bool:
        return credential in self.credentials


class GuessesOnError(FakeEngine):
    """When it cannot ask, it answers NOT_FOUND: a fact it does not have."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        try:
            return await super().observe(handle, pin=pin, holds=holds)
        except EngineError:
            return Observed(Observation(ObservedStatus.NOT_FOUND))


class LaxHandles(FakeEngine):
    """Reads what it can of a handle instead of refusing what it did not mint."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        try:
            return await super().observe(handle, pin=pin, holds=holds)
        except ValueError:
            self._up()
            return Observed(Observation(ObservedStatus.NOT_FOUND))


class IOBeforeParse(FakeEngine):
    """Asks the engine before it checked the handle (a tampered one reaches I/O)."""

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        self._up()
        return await super().fence(handle, reason=reason)


class UnlistedTopics(FakeEngine):
    """Relays every topic of the job's stream, not only those asked for."""

    async def stream(
        self,
        handle: JobHandle,
        *,
        pin: Pin,
        topics: Collection[str],
        after: int | Literal["tail"],
        closed: Callable[[], Awaitable[bool]],
        session: str,
    ) -> AsyncGenerator[StreamBatch, None]:
        job = self._jobs[_parse(handle)["job"]]
        everything = {topic for run in job.runs for topic, _ in run.log} | set(topics)
        async for batch in super().stream(
            handle, pin=pin, topics=everything, after=after, closed=closed, session=session
        ):
            yield batch


class RewindsOnBuyerWord(FakeEngine):
    """Relays from the start whatever offset it was asked for (a buyer's resume moves it)."""

    async def stream(
        self,
        handle: JobHandle,
        *,
        pin: Pin,
        topics: Collection[str],
        after: int | Literal["tail"],
        closed: Callable[[], Awaitable[bool]],
        session: str,
    ) -> AsyncGenerator[StreamBatch, None]:
        async for batch in super().stream(
            handle, pin=pin, topics=topics, after=0, closed=closed, session=session
        ):
            yield batch


class PlaintextItems(FakeEngine):
    """Hands over a relayed item that is not an age file (by forging the type)."""

    async def stream(
        self,
        handle: JobHandle,
        *,
        pin: Pin,
        topics: Collection[str],
        after: int | Literal["tail"],
        closed: Callable[[], Awaitable[bool]],
        session: str,
    ) -> AsyncGenerator[StreamBatch, None]:
        async for batch in super().stream(
            handle, pin=pin, topics=topics, after=after, closed=closed, session=session
        ):
            items: list[StreamItem] = []
            for item in batch.items:
                bad = object.__new__(Sealed)
                object.__setattr__(bad, "data", b"plaintext " + bytes(item.offset))
                items.append(replace(item, sealed=bad))
            yield replace(batch, items=tuple(items))


BROKEN: tuple[type[FakeEngine], ...] = (
    DoubleStart,
    SoftEnd,
    RevivableEnd,
    TrustsAnyRun,
    Leaky,
    NoFence,
    FenceStops,
    DriftingPin,
    StrangerReaches,
    GuessesOnError,
    LaxHandles,
    IOBeforeParse,
    UnlistedTopics,
    RewindsOnBuyerWord,
    PlaintextItems,
)
