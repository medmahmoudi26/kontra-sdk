"""``FakeEngine``: the in-memory engine of seam ⑥ (decisions I32), as :func:`fake_ledger` is
seam ④'s.

It plays an engine faithfully enough for :mod:`kontra.testing.engine_contract` -- which it
passes, as every adapter must -- and for a :class:`~kontra.testing.FakeGate` to run a whole job
lifecycle (hire, deliver, review, paid; a cancel; a deadline refund; the provenance attacks)
with no Temporal at all::

    engine = FakeEngine()
    gate = FakeGate(engines={"temporal": engine})
    engine.serve(offering, program)            # the seller's code comes online

The seller's code is a coroutine per offering (:data:`Program`): it gets a :class:`RunContext`
and the buyer's serialized input, and returns the job's result -- a sealed payload's bytes, or
anything else, which the fake then reports :attr:`~kontra.escrow.ObservedStatus.UNSEALED` as an
adapter must. The test controls the seller's side, tricks included: :meth:`FakeEngine.serve`,
:meth:`~FakeEngine.serve_quote`, :meth:`~FakeEngine.offline`, :meth:`~FakeEngine.seller_start`
(a run the buyer's start did not begin, I15), :meth:`~FakeEngine.seller_revive` (always
refused after END), :meth:`~FakeEngine.buyer_withdraws` (I16) and :meth:`~FakeEngine.reaches`
(the isolation probe). ``unreachable = True`` makes every async member raise
:class:`~kontra.engine.EngineError`.

What it models, each a rule of the contract: a lineage per buyer start, with a random pin that
a restart within it keeps; provenance by the hold the gate issued (only a run the gate's start
began, with one of ``holds``, is verified); a start idempotent by job, refused by a fence and by
a run the seller started first; ASK as a request the code may ignore, END as final; deliveries
only as :class:`~kontra.engine.SealedPayload`; per-run stream logs of age files; credentials
confined to their party's jobs.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncGenerator, Awaitable, Callable, Collection, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Literal, cast

from kontra.contract.names import (
    NameError_,
    OfferingId,
    is_endpoint_name,
    is_handle,
    is_job_id,
)
from kontra.engine import (
    HANDLE_MAX,
    EngineError,
    EngineName,
    Fenced,
    InvalidHandle,
    Isolation,
    JobHandle,
    JobPlan,
    Liveness,
    NotSealed,
    Observed,
    Pin,
    QuoteAnswer,
    QuoteKind,
    RouteConflict,
    Sealed,
    SealedPayload,
    Stop,
    StreamBatch,
    StreamItem,
    hold_digest,
)
from kontra.escrow import Observation, ObservedStatus, Provenance

__all__ = ["HANDLE_PREFIX", "FakeEngine", "Program", "QuoteProgram", "RunCanceled", "RunContext"]

#: What every handle the fake mints starts with.
HANDLE_PREFIX = b"fake/1\n"
_KEYS = ("buyer", "job", "offering", "route", "seller")
_CREDENTIAL_TTL = timedelta(minutes=15)
_OPEN = ObservedStatus.RUNNING


class RunCanceled(Exception):
    """Raised by a :data:`Program` that honours an ASK: the run ends CANCELED."""


class RunContext:
    """What the seller's code sees of its run."""

    def __init__(self, run: _Run) -> None:
        self._run = run

    @property
    def cancel_requested(self) -> bool:
        """An ASK reached the run (:meth:`FakeEngine.cancel` with :attr:`Stop.ASK`, or the
        buyer withdrew its start)."""
        return self._run.asked.is_set()

    async def cancelled(self) -> None:
        """Wait until an ASK reaches the run."""
        await self._run.asked.wait()

    def step(self, name: str) -> None:
        """One step of the run done (liveness: ``steps_done``, ``last_step``)."""
        self._run.steps_done += 1
        self._run.last_step = name

    def restart(self) -> None:
        """Continue within the lineage (Temporal: continue-as-new): the pin stays."""
        self._run.restarts += 1

    def publish(self, topic: str, item: bytes) -> None:
        """Append an item to the run's stream. The seller side seals: the fake refuses
        anything that is not an age file, as a codec would never write one."""
        Sealed(item)
        self._run.log.append((topic, item))


#: The seller's code for an offering: ``(ctx, input) -> result``.
Program = Callable[[RunContext, bytes], Awaitable[bytes]]
#: The seller's quote for an offering: ``input -> the KontraQuote document``.
QuoteProgram = Callable[[bytes], Awaitable[bytes]]


@dataclass(eq=False)
class _Run:
    pin: str
    by_gate: bool
    hold: str | None
    started_at: datetime
    status: ObservedStatus = ObservedStatus.RUNNING
    result: bytes | None = None
    closed_at: datetime | None = None
    asked: asyncio.Event = field(default_factory=asyncio.Event)
    ended: bool = False
    buyer_withdrew: bool = False
    steps_done: int = 0
    last_step: str | None = None
    restarts: int = 0
    log: list[tuple[str, bytes]] = field(default_factory=list[tuple[str, bytes]])
    task: asyncio.Task[None] | None = None


@dataclass(eq=False)
class _Job:
    seller: str
    buyer: str
    offering: str
    runs: list[_Run] = field(default_factory=list[_Run])
    #: The gate's start was made (or refused): a job begins at most one lineage that way.
    gate_started: bool = False
    #: Closed to starts by a fence; the placeholder's pin.
    fence_pin: str | None = None


def _canonical(fields: Mapping[str, str]) -> bytes:
    return HANDLE_PREFIX + json.dumps(
        dict(sorted(fields.items())), separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")


def _parse(raw: object) -> dict[str, str]:
    """The plan inside a handle the fake minted, or :class:`InvalidHandle` (before any I/O,
    as an adapter must: prefix, size, exact keys, fields, construct-and-compare)."""
    if not isinstance(raw, bytes) or len(raw) > HANDLE_MAX or not raw.startswith(HANDLE_PREFIX):
        raise InvalidHandle("not a fake/1 handle")
    try:
        loaded: object = json.loads(raw[len(HANDLE_PREFIX) :])
    except (ValueError, UnicodeDecodeError):
        raise InvalidHandle("not a fake/1 handle") from None
    if not isinstance(loaded, dict):
        raise InvalidHandle("not a fake/1 handle")
    fields = cast(dict[object, object], loaded)
    if tuple(sorted(str(k) for k in fields)) != _KEYS or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in fields.items()
    ):
        raise InvalidHandle("not a fake/1 handle")
    plan = {str(k): str(v) for k, v in fields.items()}
    if _canonical(plan) != raw:
        raise InvalidHandle("not a canonical fake/1 handle")
    try:
        offering = OfferingId.parse(plan["offering"])
    except NameError_:
        raise InvalidHandle("not a fake/1 handle") from None
    if not (
        offering.key == plan["offering"]
        and offering.party == plan["seller"]
        and is_handle(plan["buyer"])
        and is_job_id(plan["job"])
        and is_endpoint_name(plan["route"])
    ):
        raise InvalidHandle("not a fake/1 handle")
    return plan


def _utcnow() -> datetime:
    return datetime.now(UTC)


class FakeEngine:
    """An in-memory :class:`kontra.engine.Engine` (see the module docstring).

    ``name``: the engine it stands in for (what a FakeGate dispatches by). ``clock``: the
    times it records (default: the machine's UTC clock; pass the gate's ``now`` to move with
    ``FakeGate.advance``). ``address``: what :meth:`isolate` tells a party its worker dials."""

    def __init__(
        self,
        *,
        name: EngineName = "temporal",
        clock: Callable[[], datetime] | None = None,
        address: str = "fake-engine.invalid:7233",
    ) -> None:
        self._name: EngineName = name
        self._clock = clock or _utcnow
        self.address = address
        #: Every async member raises EngineError while True.
        self.unreachable = False
        #: Parties with a home here.
        self.homes: set[str] = set()
        #: credential -> party
        self.credentials: dict[str, str] = {}
        #: route -> offering key
        self.routes: dict[str, str] = {}
        self._programs: dict[str, Program] = {}
        self._quotes: dict[str, QuoteProgram] = {}
        self._jobs: dict[str, _Job] = {}

    @property
    def name(self) -> EngineName:
        return self._name

    # ------------------------------------------------------------------ the seller's side

    def serve(self, offering: OfferingId, program: Program) -> None:
        """The seller's worker for ``offering`` comes online, running ``program``."""
        self._programs[offering.key] = program

    def serve_quote(self, offering: OfferingId, quote: QuoteProgram) -> None:
        """The seller's ``quote`` for ``offering`` comes online."""
        self._quotes[offering.key] = quote

    def offline(self, offering: OfferingId) -> None:
        """Nothing serves ``offering`` any more (its runs in flight go on)."""
        self._programs.pop(offering.key, None)
        self._quotes.pop(offering.key, None)

    def seller_start(self, handle: JobHandle, result: bytes) -> bool:
        """The seller starts the job's run itself and completes it with ``result`` (I15's
        attack): never the buyer's lineage. False when a run of the job is open (an engine
        holds one open run per job at most)."""
        job = self._job(_parse(handle))
        if any(r.status is _OPEN for r in job.runs):
            return False
        now = self._clock()
        run = _Run(_new_pin(), by_gate=False, hold=None, started_at=now)
        run.status, run.result, run.closed_at = ObservedStatus.COMPLETED, result, now
        job.runs.append(run)
        return True

    def seller_revive(self, handle: JobHandle) -> bool:
        """Everything a seller may try to revive an ended run (Temporal: reset). END is final
        here as on every engine: always False."""
        _parse(handle)
        return False

    def buyer_withdraws(self, handle: JobHandle) -> None:
        """The buyer cancels its own start (Temporal: its Nexus operation, I16): its run is
        asked to stop, and a CANCELED end is the buyer's doing."""
        job = self._jobs.get(_parse(handle)["job"])
        for run in job.runs if job is not None else ():
            if run.by_gate and run.status is _OPEN:
                run.buyer_withdrew = True
                run.asked.set()

    def reaches(self, credential: str, handle: JobHandle) -> bool:
        """Whether ``credential`` reaches the job: only its seller's or its buyer's do."""
        plan = _parse(handle)
        party = self.credentials.get(credential)
        return party is not None and party in (plan["seller"], plan["buyer"])

    def runs(self, handle: JobHandle) -> int:
        """How many runs the job has had (for tests: one start, one lineage)."""
        job = self._jobs.get(_parse(handle)["job"])
        return 0 if job is None else len(job.runs)

    # ------------------------------------------------------------------ internals

    def _up(self) -> None:
        if self.unreachable:
            raise EngineError("the fake engine is unreachable")

    def _job(self, plan: Mapping[str, str]) -> _Job:
        job = self._jobs.get(plan["job"])
        if job is None:
            job = _Job(plan["seller"], plan["buyer"], plan["offering"])
            self._jobs[plan["job"]] = job
        return job

    def _lineage(self, job: _Job, pin: Pin | None) -> _Run | None:
        if pin is None:
            return job.runs[-1] if job.runs else None
        return next((r for r in job.runs if r.pin == pin), None)

    def _verified(self, run: _Run, holds: Collection[str]) -> bool:
        """Provenance (I15): only a run the gate's start began, carrying one of ``holds``."""
        return run.by_gate and run.hold is not None and hold_digest(run.hold) in set(holds)

    async def _run(self, run: _Run, program: Program, data: bytes) -> None:
        try:
            out = await program(RunContext(run), data)
        except asyncio.CancelledError:
            if not run.ended:
                run.status, run.closed_at = ObservedStatus.CANCELED, self._clock()
            raise
        except RunCanceled:
            if not run.ended:
                run.status, run.closed_at = ObservedStatus.CANCELED, self._clock()
            return
        except Exception:
            if not run.ended:
                run.status, run.closed_at = ObservedStatus.FAILED, self._clock()
            return
        if run.ended:
            return  # END is final: a late completion delivers nothing
        run.status, run.result, run.closed_at = ObservedStatus.COMPLETED, out, self._clock()

    # ------------------------------------------------------------------ parties

    async def isolate(self, party: str, *, session: str | None = None) -> Isolation:
        self._up()
        if not is_handle(party):
            raise ValueError("not a party handle")
        self.homes.add(party)
        if session is None:
            return Isolation(home=party, address=self.address)
        credential = "fake." + secrets.token_urlsafe(24)
        self.credentials[credential] = party
        expires = self._clock() + _CREDENTIAL_TTL
        return Isolation(party, self.address, credential=credential, expires_at=expires)

    # ------------------------------------------------------------------ offerings

    async def expose(self, offering: OfferingId, route: str) -> None:
        self._up()
        if not is_endpoint_name(route):
            raise ValueError("not a route this catalog assigns")
        if offering.party not in self.homes:
            raise EngineError(f"{offering.party} has no home in this engine")
        if self.routes.get(route, offering.key) != offering.key:
            raise RouteConflict(f"{route} targets another offering")
        self.routes[route] = offering.key

    async def quote(
        self,
        offering: OfferingId,
        input: bytes,
        *,
        timeout: float,  # noqa: ASYNC109 -- the call's own timeout, as on an engine
    ) -> QuoteAnswer:
        self._up()
        quote = self._quotes.get(offering.key)
        if quote is None:
            return QuoteAnswer(QuoteKind.UNAVAILABLE, message="nothing serves the offering")
        try:
            async with asyncio.timeout(timeout):
                document = await quote(input)
        except TimeoutError:
            return QuoteAnswer(QuoteKind.TIMEOUT, message="no answer in time")
        except Exception as e:
            return QuoteAnswer(
                QuoteKind.REFUSED, message=f"the seller's quote raised {type(e).__name__}"
            )
        return QuoteAnswer(QuoteKind.OK, document)

    # ------------------------------------------------------------------ jobs

    def bind(self, plan: JobPlan) -> JobHandle:
        raw = _canonical(
            {
                "buyer": plan.buyer,
                "job": plan.job_id,
                "offering": plan.offering.key,
                "route": plan.route,
                "seller": plan.seller,
            }
        )
        _parse(raw)
        return JobHandle(raw)

    async def start(
        self, handle: JobHandle, input: bytes, *, hold: str, deadline: datetime
    ) -> None:
        plan = _parse(handle)
        self._up()
        job = self._job(plan)
        if job.gate_started or job.fence_pin is not None or job.runs:
            # Idempotent by job; a fenced job, or one whose id a run took first, starts none.
            job.gate_started = True
            return
        job.gate_started = True
        run = _Run(_new_pin(), by_gate=True, hold=hold, started_at=self._clock())
        job.runs.append(run)
        program = self._programs.get(plan["offering"])
        if program is None:
            return  # nobody serves it: it stays running until a stop ends it
        run.task = asyncio.get_running_loop().create_task(
            self._run(run, program, input), name=f"fake-engine-run-{plan['job']}"
        )

    async def observe(
        self, handle: JobHandle, *, pin: Pin | None, holds: Collection[str]
    ) -> Observed:
        plan = _parse(handle)
        self._up()
        job = self._jobs.get(plan["job"])
        run = self._lineage(job, pin) if job is not None else None
        if run is None:
            if job is not None and job.fence_pin is not None and pin is None:
                return Observed(
                    Observation(
                        ObservedStatus.TERMINATED,
                        provenance=Provenance.OCCUPIED,
                        pin=job.fence_pin,
                    )
                )
            return Observed(Observation(ObservedStatus.NOT_FOUND, pin=pin))
        verified = self._verified(run, holds)
        obs = Observation(
            status=run.status,
            started_at=run.started_at,
            closed_at=run.closed_at,
            provenance=Provenance.VERIFIED if verified else Provenance.NOT_FROM_BUYER,
            pin=run.pin,
            buyer_canceled=verified
            and run.status is ObservedStatus.CANCELED
            and run.buyer_withdrew,
        )
        if run.status is not ObservedStatus.COMPLETED or not verified:
            return Observed(obs)
        try:
            sealed = SealedPayload(run.result if run.result is not None else b"")
        except NotSealed:
            return Observed(replace(obs, status=ObservedStatus.UNSEALED))
        digest = sha256(sealed.data).hexdigest()
        return Observed(replace(obs, result_sha256=digest, result_size=len(sealed.data)), sealed)

    async def liveness(self, handle: JobHandle, *, pin: Pin) -> Liveness | None:
        plan = _parse(handle)
        self._up()
        job = self._jobs.get(plan["job"])
        run = self._lineage(job, pin) if job is not None else None
        if run is None:
            return None
        return Liveness(run.status, run.last_step, run.steps_done, (), run.restarts)

    async def cancel(self, handle: JobHandle, *, stop: Stop, pin: Pin | None, reason: str) -> None:
        plan = _parse(handle)
        self._up()
        job = self._jobs.get(plan["job"])
        for run in job.runs if job is not None else ():
            if (pin is not None and run.pin != pin) or run.status is not _OPEN:
                continue  # another lineage, or closed already: a delivery stands
            if stop is Stop.ASK:
                run.asked.set()
                continue
            run.ended = True
            run.status, run.closed_at = ObservedStatus.TERMINATED, self._clock()
            if run.task is not None:
                run.task.cancel()

    async def fence(self, handle: JobHandle, *, reason: str) -> Fenced:
        plan = _parse(handle)
        self._up()
        job = self._job(plan)
        if job.runs:
            return Fenced.STARTED
        if job.fence_pin is None:
            job.fence_pin = _new_pin()
        return Fenced.CLOSED

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
        plan = _parse(handle)
        self._up()
        wanted = frozenset(topics)
        job = self._jobs.get(plan["job"])
        run = self._lineage(job, pin) if job is not None else None
        if run is None:
            return
        offset = len(run.log) if after == "tail" else after
        if isinstance(offset, bool) or offset < 0:
            raise ValueError("after is an offset >= 0 or 'tail'")
        while True:
            self._up()
            log = run.log
            if offset < len(log):
                items: list[StreamItem] = []
                dropped = 0
                for i in range(offset, len(log)):
                    topic, data = log[i]
                    if topic not in wanted:
                        dropped += 1
                        continue
                    items.append(StreamItem(topic, i, Sealed(data), len(data)))
                offset = len(log)
                yield StreamBatch(tuple(items), offset, dropped)
                continue
            if await closed():
                return
            await asyncio.sleep(0.01)


def _new_pin() -> str:
    return "pin-" + secrets.token_hex(12)
