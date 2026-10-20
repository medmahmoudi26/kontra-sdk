"""Deliberately broken engines: each is :class:`~kontra.testing.FakeEngine` with one bug a real
adapter could plausibly have. The engine contract must reject every one of them, each on the
rule its bug breaks, and every rule rejects at least one (seam ⑥, as
``tests/ledger/broken_ledgers.py`` is for seam ④): a later edit that weakens a check is
caught here, in this repository and in the copy of the contract the platform runs against its
own adapter."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable, Collection
from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from typing import Literal, cast

from temporalio.api.common.v1 import Payload

from kontra.codec import age
from kontra.contract.names import OfferingId
from kontra.engine import (
    BuyerStart,
    EngineError,
    EngineName,
    Fenced,
    Isolation,
    JobHandle,
    Observed,
    Pin,
    QuoteAnswer,
    QuoteKind,
    Reach,
    Sealed,
    SealedPayload,
    Stop,
    StreamBatch,
    StreamItem,
)
from kontra.escrow import Observation, ObservedStatus
from kontra.testing.engine import FakeEngine, _Job, _parse, _Run  # pyright: ignore[reportPrivateUsage]


def _forged(data: bytes) -> SealedPayload:
    """A SealedPayload that skipped its own check: what a buggy adapter could build."""
    bad = object.__new__(SealedPayload)
    object.__setattr__(bad, "data", data)
    return bad


def _forged_observed(
    observation: Observation,
    result: SealedPayload | None,
    reach: Reach | None = None,
    buyer_start: BuyerStart | None = None,
) -> Observed:
    """An Observed that skipped its own check."""
    bad = object.__new__(Observed)
    object.__setattr__(bad, "observation", observation)
    object.__setattr__(bad, "result", result)
    object.__setattr__(bad, "reach", reach)
    object.__setattr__(bad, "buyer_start", buyer_start)
    return bad


class UnknownName(FakeEngine):
    """Calls itself by a name no gate dispatches by."""

    @property
    def name(self) -> EngineName:
        return cast(EngineName, "golem")


class OneCredentialPerParty(FakeEngine):
    """Mints one credential per party, whatever the session: revoking one session's leaves
    another session's working."""

    async def isolate(self, party: str, *, session: str | None = None) -> Isolation:
        iso = await super().isolate(party, session=session)
        if iso.credential is None:
            return iso
        mine = next((c for c, p in self.credentials.items() if p == party), iso.credential)
        return replace(iso, credential=mine)


class RunningBeforeStart(FakeEngine):
    """Reports a job nothing started as running (a buyer could then cancel it as one)."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        if seen.observation.status is ObservedStatus.NOT_FOUND:
            return Observed(replace(seen.observation, status=ObservedStatus.RUNNING))
        return seen


class BytelessDelivery(FakeEngine):
    """Hands over a verified completion's digest but not its bytes (by forging the type):
    escrow would pay for a delivery the gate does not hold."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        if seen.result is None:
            return seen
        return _forged_observed(seen.observation, None)


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
                    data=age.encrypt(b"revived", [age.generate_identity()[1]]),
                ).SerializeToString(deterministic=True)
                return True
        return False


class CancelIgnoresPin(FakeEngine):
    """A stop with a pin stops every open run of the job, not only the pinned lineage."""

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        await super().cancel(handle, stop=stop, pin=None, reason=reason)


class EndOverwritesDelivery(FakeEngine):
    """END after a completion turns the delivery into a termination."""

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        await super().cancel(handle, stop=stop, pin=pin, reason=reason)
        job = self._jobs.get(_parse(handle)["job"])
        for run in job.runs if job is not None and stop is Stop.END else ():
            if pin is None or run.pin == pin:
                run.status, run.result = ObservedStatus.TERMINATED, None


class AskIsEnd(FakeEngine):
    """ASK ends the run: a seller that would have completed is cut off."""

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        await super().cancel(handle, stop=Stop.END, pin=pin, reason=reason)


class VerifiesSellerRuns(FakeEngine):
    """A run that carries no hold at all -- the seller's own -- is taken as the buyer's."""

    def _verified(self, run: _Run, holds: Collection[str]) -> bool:
        return run.hold is None or super()._verified(run, holds)


class BuyerNeverCancels(FakeEngine):
    """Never reports the buyer's withdrawal of its own start: a buyer who withdrew is
    refunded against the seller."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        return Observed(replace(seen.observation, buyer_canceled=False), seen.result)


class RaisesOnRefusal(FakeEngine):
    """Raises when the seller's quote fails, as if the engine could not be asked."""

    async def quote(
        self,
        offering: OfferingId,
        input: bytes,
        *,
        timeout: float,  # noqa: ASYNC109 -- the call's own timeout
    ) -> QuoteAnswer:
        answer = await super().quote(offering, input, timeout=timeout)
        if answer.kind is QuoteKind.REFUSED:
            raise EngineError(answer.message)
        return answer


class RepointsRoutes(FakeEngine):
    """A route exposed again for another offering is repointed at it."""

    async def expose(self, offering: OfferingId, route: str) -> None:
        self.routes.pop(route, None)
        await super().expose(offering, route)


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


class BuyerReaches(FakeEngine):
    """The buyer's credential reaches the seller's side of the job (its run), not only its
    own start."""

    def reaches(self, credential: str, handle: JobHandle) -> bool:
        plan = _parse(handle)
        return self.credentials.get(credential) in (plan["seller"], plan["buyer"])


class OutsiderStops(FakeEngine):
    """Any party's credential can stop a job's run, the seller's or not."""

    def stops(self, credential: str, handle: JobHandle) -> bool:
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


class ReachFromAnyRun(FakeEngine):
    """Reads how far the job's start got from the job's runs, not from the buyer's start: a run
    the seller started itself is reported STARTED, and the seller is blamed for a job its
    buyer never started (a buyer colluding with a third party could produce such a run)."""

    def _reach(
        self, job_id: str, job: _Job | None, *, verified: bool, pinned: bool
    ) -> tuple[Reach | None, BuyerStart | None]:
        if job is not None and job.runs:
            return Reach.STARTED, None
        return super()._reach(job_id, job, verified=verified, pinned=pinned)


class BlamesWhenUnsure(FakeEngine):
    """With no start of the buyer's seen, defaults to a reach that blames the seller: the
    start went unanswered."""

    def _reach(
        self, job_id: str, job: _Job | None, *, verified: bool, pinned: bool
    ) -> tuple[Reach | None, BuyerStart | None]:
        reach, start = super()._reach(job_id, job, verified=verified, pinned=pinned)
        if reach is Reach.NEVER_DISPATCHED:
            return Reach.UNANSWERED, BuyerStart()
        return reach, start


class ForgetsTheStart(FakeEngine):
    """Reports a verified lineage as never handed to the seller (by forging the type): the
    buyer's own start began it."""

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        seen = await super().observe(handle, pin=pin, holds=holds)
        if seen.reach is not Reach.STARTED:
            return seen
        return _forged_observed(seen.observation, seen.result, Reach.NEVER_DISPATCHED)


BROKEN: tuple[type[FakeEngine], ...] = (
    UnknownName,
    OneCredentialPerParty,
    RunningBeforeStart,
    BytelessDelivery,
    CancelIgnoresPin,
    EndOverwritesDelivery,
    AskIsEnd,
    VerifiesSellerRuns,
    BuyerNeverCancels,
    RaisesOnRefusal,
    RepointsRoutes,
    BuyerReaches,
    OutsiderStops,
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
    ReachFromAnyRun,
    BlamesWhenUnsure,
    ForgetsTheStart,
)
