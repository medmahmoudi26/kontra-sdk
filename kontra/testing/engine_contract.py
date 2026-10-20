"""The engine contract (seam ⑥, decisions I32): one suite every :class:`~kontra.engine.Engine`
adapter passes unchanged, as every ledger passes :mod:`kontra.testing.ledger_contract`.

:class:`~kontra.testing.FakeEngine` is proven against it in this repository, and
:class:`~kontra.engine.temporal.TemporalEngine` on a Temporal dev server (isolation over the
wire on Kontra's own server, ``tests/temporal``); the platform runs the same coroutine
against its gate's adapter on its compose Temporal::

    from kontra.testing.engine_contract import run_engine_contract

    async def test_the_gates_engine(harness):
        report = await run_engine_contract(lambda: harness)

``make`` returns an :class:`EngineHarness`: the engine, three parties (a seller, a buyer, a
stranger), the seller's offering and its route, and the probes an adapter's own test rig
supplies -- the seller's code speaking the :data:`Behaviour` vocabulary, a hold the seller's
code accepts, the key that opens what the seller sealed, and the tricks a seller or a buyer
may try. Every content value the seller's code produces carries a marker the suite chose
(``MARKER-55-<hex>``), inside ciphertext only; every value any member returns is collected
and walked, and the walk must find the marker nowhere (E12).

The checks (each raises :class:`EngineContractViolation` naming the rule):

* E1 conformance and the guardrail: an :class:`~kontra.engine.Engine`, the pinned
  :data:`~kontra.engine.MEMBERS`, a known name.
* E2 isolation, locally: one home per party (concurrent provisioning converges), no
  credential without a session, a distinct credential per party and per session.
* E3 handles: pure and deterministic (in a second instance too), distinct per job, at most
  :data:`~kontra.engine.HANDLE_MAX` bytes; a tampered handle is :class:`InvalidHandle` from
  every member, before any I/O (on an engine that cannot be reached).
* E4 a start is idempotent by job (concurrent, late, after completion: one lineage); the
  verified completion is a sealed delivery whose digest and size are of exactly its bytes,
  that opens to the seller's output, stable under its pin and in a second instance.
* E5 before any start: NOT_FOUND, never an exception, never RUNNING.
* E6 END on the pinned lineage: TERMINATED, no result, for good -- whatever the seller tries
  after (a revival, a run of its own); and a stop with a pin stops that lineage only (a run
  of the seller's own, open meanwhile, is untouched by END or ASK of the pin).
* E7 a delivery stands: END or ASK after the completion changes nothing.
* E8 ASK is only a request: a run that ignores it completes, one that honours it is CANCELED.
* E9 provenance: a run the seller started itself, or one observed with holds the gate did
  not issue, is never verified and never a result.
* E10 the fence: before any run it closes the job (a later start never delivers); after a
  start it says STARTED and stops nothing; raced against starts it is one or the other.
* E11 restarts within a lineage keep its pin; liveness counts them, and an unknown pin has
  none.
* E12 ciphertext only: a completion that is not sealed is UNSEALED and withheld; the marker
  is in nothing any member returned.
* E13 the buyer's withdrawal of its own start is ``buyer_canceled`` (where buyers have a
  presence in the engine).
* E14 streams (where the adapter relays them): only the topics asked for, increasing offsets,
  ``next_offset`` past every item, nothing before ``after``, "tail" only what comes next,
  every item opens to what the seller published; the relay of a closed job ends.
* E15 quotes: OK with the seller's document, TIMEOUT, REFUSED, UNAVAILABLE -- never raised
  for the seller's own failure.
* E16 expose: idempotent; a route is never repointed (:class:`RouteConflict`).
* E17 errors are not facts: an engine that cannot be reached raises
  :class:`~kontra.engine.EngineError` from every member; it never answers NOT_FOUND.
* E18 isolation over the wire (only where the harness enforces it): the seller's credential
  reaches its own job's run, neither the buyer's (whose side is its own start) nor a
  stranger's does, neither of theirs can stop it or fence the job (start under its id), and
  no party's can provision a home or expose a route. (The seller's own runs of its job are
  E9's: never the buyer's delivery.)
* E19 reach (decisions I40), how far the buyer's start got, which a gate's reputation reads:
  STARTED in every observation of a verified or pinned lineage the suite made, and for the
  buyer's start that began one, before and after its end; NEVER_DISPATCHED before any start,
  and while only a run of the seller's own exists (it is no start of the buyer's); and for the
  gate's start of a job such a run took first, or of a fenced job, a reach that blames no one
  (refused, or never handed over).

:func:`run_engine_contract` returns one line per check, ``ok`` or ``SKIPPED`` with why.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import secrets
import typing
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterable
from datetime import UTC, datetime, timedelta
from enum import Enum
from hashlib import sha256
from typing import Any, Literal, Protocol, cast

from kontra.contract.names import OfferingId, new_job_id
from kontra.engine import (
    ENGINE_NAMES,
    HANDLE_MAX,
    MEMBERS,
    BuyerStart,
    Engine,
    EngineError,
    Fenced,
    InvalidHandle,
    JobHandle,
    JobPlan,
    Observed,
    Pin,
    QuoteAnswer,
    QuoteKind,
    Reach,
    RouteConflict,
    Sealed,
    SealedPayload,
    Stop,
    StreamBatch,
    StreamStopped,
    hold_digest,
)
from kontra.escrow import ObservedStatus, Provenance

__all__ = [
    "CHECKS",
    "LISTED_TOPIC",
    "UNLISTED_TOPIC",
    "Behaviour",
    "EngineContractViolation",
    "EngineHarness",
    "QuoteBehaviour",
    "protocol_members",
    "run_engine_contract",
]

#: What the seller's code does for a job, told by the job's input:
#:
#: * ``ok``: completes with ``DONE <marker>``, sealed;
#: * ``hang``: runs until stopped; an ASK ends it CANCELED;
#: * ``ignore_ask``: runs until an ASK, then completes as ``ok`` does;
#: * ``plain``: completes with ``PLAIN <marker>`` NOT sealed (a seller that bypassed the codec);
#: * ``can2``: restarts within its lineage twice (Temporal: continue-as-new), then as ``ok``;
#: * ``publish``: publishes ``ITEM <marker> <offset>`` sealed, on :data:`LISTED_TOPIC` at
#:   offsets 0 and 2 and on :data:`UNLISTED_TOPIC` at 1; on an ASK, three more the same way
#:   (3, 5 listed; 4 not), then completes as ``ok``.
Behaviour = Literal["ok", "hang", "ignore_ask", "plain", "can2", "publish"]
#: What the seller's quote does: answers its document, raises (naming no content), or takes
#: longer than any timeout the suite passes.
QuoteBehaviour = Literal["ok", "fail", "slow"]

#: The topic a ``publish`` job's buyer may read, and one it may not.
LISTED_TOPIC = "events"
UNLISTED_TOPIC = "secret"

#: Every check, in the order the suite runs them (E12's walk is last: it covers them all).
CHECKS = (
    "E1",
    "E2",
    "E3",
    "E5",
    "E4",
    "E7",
    "E6",
    "E8",
    "E9",
    "E10",
    "E11",
    "E13",
    "E14",
    "E15",
    "E16",
    "E17",
    "E18",
    "E19",
    "E12",
)

_DEADLINE = timedelta(minutes=10)
_WAIT = 30.0
_POLL = 0.05
_QUOTE_TIMEOUT = 3.0
_RACES = 6
#: What the gate's start of a job a run of the seller's own took first, or of a fenced job, may
#: reach (E19): refused, or never handed over -- nothing that could cost the seller.
_UNBLAMED = frozenset({Reach.NEVER_DISPATCHED, Reach.REFUSED})


class EngineContractViolation(AssertionError):
    """A rule of the engine contract was broken; the message names it."""


class EngineHarness(Protocol):
    """An adapter's test rig (see the module docstring). The parties have no home yet; the
    seller's offering is exposed under ``route`` and served by code speaking
    :data:`Behaviour`, and ``other_offering`` (the seller's too) is exposed nowhere and served
    by nothing."""

    @property
    def engine(self) -> Engine: ...

    @property
    def seller(self) -> str: ...

    @property
    def buyer(self) -> str: ...

    @property
    def stranger(self) -> str: ...

    @property
    def offering(self) -> OfferingId: ...

    @property
    def other_offering(self) -> OfferingId: ...

    @property
    def route(self) -> str: ...

    #: Credentials are judged by the engine (Temporal: an authorizer); else E18 is skipped.
    @property
    def enforces_isolation(self) -> bool: ...

    #: A buyer can withdraw its own start (Temporal yes, Golem no); else E13 is skipped.
    @property
    def buyer_presence(self) -> bool: ...

    #: The adapter relays streams and the seller's code publishes them; else E14 is skipped.
    @property
    def streams(self) -> bool: ...

    def input(self, behaviour: Behaviour, marker: str) -> bytes:
        """The buyer's serialized payload telling the seller's code what to do."""
        ...

    def quote_input(self, behaviour: QuoteBehaviour) -> bytes:
        """The buyer's serialized payload for a quote."""
        ...

    def quote_document(self) -> bytes:
        """What the seller's quote answers to ``quote_input("ok")``."""
        ...

    def hold(self, job_id: str) -> str:
        """A hold for ``job_id`` the seller's code accepts (re-issued: the same bytes)."""
        ...

    def open(self, result: SealedPayload) -> str:
        """The seller's output inside a delivery, opened with the job's key."""
        ...

    def open_item(self, item: Sealed) -> str:
        """What the seller published inside a relayed stream item."""
        ...

    async def seller_start(
        self, handle: JobHandle, job_id: str, marker: str, behaviour: Behaviour = "ok"
    ) -> None:
        """The seller runs the job itself, as its own credential allows (I15's attack), its
        code doing ``behaviour`` (``ok``; ``hang``: until a stop ends it). A run of the job
        that is open already wins: the call then starts nothing."""
        ...

    async def seller_revive(self, handle: JobHandle, job_id: str) -> None:
        """Everything a seller's credential may try to bring an ended run back."""
        ...

    async def buyer_withdraw(self, handle: JobHandle, job_id: str) -> None:
        """The buyer withdraws its own start (only called with ``buyer_presence``)."""
        ...

    async def reach(self, credential: str, handle: JobHandle) -> bool:
        """Whether ``credential`` reaches the job's run where it runs, in the seller's home
        (Temporal: describes ``job-<id>`` in the seller's namespace; only called with
        ``enforces_isolation``)."""
        ...

    async def stops(self, credential: str, handle: JobHandle) -> bool:
        """Whether ``credential`` can stop the job's run -- end it, ask it to stop -- or fence
        the job, as only the gate may (Temporal: terminate, request-cancel, or start under
        ``job-<id>`` in the seller's namespace; only called with ``enforces_isolation``, for
        the buyer's and a stranger's)."""
        ...

    async def controls(self, credential: str) -> bool:
        """Whether ``credential`` can do anything only the gate may: provision a home,
        expose a route (only called with ``enforces_isolation``)."""
        ...

    async def fresh_engine(self) -> Engine:
        """A second instance of the adapter on the same engine: a gate restart."""
        ...

    async def unreachable_engine(self) -> Engine:
        """An instance of the adapter that cannot reach its engine."""
        ...


def protocol_members(protocol: type) -> frozenset[str]:
    """The members of a Protocol class, as ``typing`` counts them (``__protocol_attrs__``
    from Python 3.12; the same computation before it)."""
    attrs: object = getattr(protocol, "__protocol_attrs__", None)
    if attrs is None:  # pragma: no cover - Python 3.11
        attrs = typing._get_protocol_attrs(protocol)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]
    return frozenset(cast(Iterable[str], attrs))


def _violation(rule: str, message: str) -> EngineContractViolation:
    return EngineContractViolation(f"{rule}: {message}")


class _Seen:
    """Every value a member returned, flattened to bytes, for the marker walk."""

    def __init__(self) -> None:
        self.blobs: list[bytes] = []

    def add(self, *values: object) -> None:
        for v in values:
            if v is None or isinstance(v, bool | int | float):
                continue
            if isinstance(v, bytes):
                self.blobs.append(v)
            elif isinstance(v, str):
                self.blobs.append(v.encode("utf-8", "surrogatepass"))
            elif isinstance(v, Enum):
                self.blobs.append(str(v.value).encode())
            elif dataclasses.is_dataclass(v) and not isinstance(v, type):
                self.add(*(getattr(v, f.name) for f in dataclasses.fields(v)))
            elif isinstance(v, tuple | list | set | frozenset):
                self.add(*cast(Iterable[object], v))
            else:
                self.blobs.append(repr(v).encode())


class _Run:
    """One run of the suite: the harness, the marker, what was seen."""

    def __init__(self, h: EngineHarness, settle: float, wait: float) -> None:
        self.h = h
        #: How long to wait for something that must happen (a completion, a close).
        self.wait = wait
        #: How long to wait for something that must NOT happen (a second lineage, a
        #: delivery after a fence) before concluding it did not.
        self.settle = settle
        self.e = h.engine
        self.marker = "MARKER-55-" + secrets.token_hex(8)
        self.seen = _Seen()
        self.deadline = datetime.now(UTC) + _DEADLINE
        self.credentials: dict[str, str] = {}
        #: Every observation the suite made, with the pin it asked for (E19 reads them all).
        self.observed: list[tuple[Observed, Pin | None]] = []
        #: E4's delivered job: (job id, handle, holds, observation).
        self.delivered: tuple[str, JobHandle, frozenset[str], Observed] | None = None

    def plan(self, job_id: str) -> JobPlan:
        h = self.h
        return JobPlan(job_id, h.offering, h.route, h.buyer, h.seller)

    def new(self) -> tuple[str, JobHandle, frozenset[str]]:
        job_id = new_job_id()
        handle = self.e.bind(self.plan(job_id))
        self.seen.add(handle)
        return job_id, handle, frozenset({hold_digest(self.h.hold(job_id))})

    async def start(self, job_id: str, handle: JobHandle, behaviour: Behaviour) -> None:
        await self.e.start(
            handle,
            self.h.input(behaviour, self.marker),
            hold=self.h.hold(job_id),
            deadline=self.deadline,
        )

    async def observe(
        self, handle: JobHandle, holds: Collection[str], pin: Pin | None = None
    ) -> Observed:
        seen = await self.e.observe(handle, pin=pin, holds=holds)
        self.seen.add(seen)
        self.observed.append((seen, pin))
        return seen

    async def until(
        self,
        rule: str,
        what: str,
        handle: JobHandle,
        holds: Collection[str],
        ok: Callable[[Observed], bool],
        *,
        pin: Pin | None = None,
    ) -> Observed:
        loop = asyncio.get_running_loop()
        end = loop.time() + self.wait
        last: Observed | None = None
        while loop.time() < end:
            last = await self.observe(handle, holds, pin)
            if ok(last):
                return last
            await asyncio.sleep(_POLL)
        raise _violation(rule, f"{what} never happened (last: {last})")

    def check_delivery(self, rule: str, seen: Observed) -> SealedPayload:
        o, result = seen.observation, seen.result
        if (
            o.status is not ObservedStatus.COMPLETED
            or o.provenance is not Provenance.VERIFIED
            or not o.pin
            or result is None
        ):
            raise _violation(rule, f"the buyer's start did not deliver: {seen}")
        try:
            SealedPayload(bytes(result.data))  # again: a type can be forged
        except ValueError:
            raise _violation(rule, "the delivery handed over is not sealed") from None
        if (o.result_sha256, o.result_size) != (sha256(result.data).hexdigest(), len(result.data)):
            raise _violation(rule, "the digest and size are not of the bytes handed over")
        return result


def _running_verified(seen: Observed) -> bool:
    o = seen.observation
    return (
        o.status is ObservedStatus.RUNNING and o.provenance is Provenance.VERIFIED and bool(o.pin)
    )


def _closed(seen: Observed) -> bool:
    return seen.observation.status not in (ObservedStatus.RUNNING, ObservedStatus.NOT_FOUND)


def _pin_of(rule: str, seen: Observed) -> Pin:
    if not seen.observation.pin:
        raise _violation(rule, f"a verified observation names no pin: {seen}")
    return Pin(seen.observation.pin)


# ----------------------------------------------------------------------------- the checks


async def _e1(r: _Run) -> str:
    if not isinstance(r.e, Engine):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise _violation("E1", f"{type(r.e).__name__} is not a kontra.engine.Engine")
    members = protocol_members(Engine)
    if members != MEMBERS:
        raise _violation("E1", f"the Protocol's members changed: {sorted(members ^ MEMBERS)}")
    if r.e.name not in ENGINE_NAMES:
        raise _violation("E1", f"{r.e.name!r} is not an engine name")
    return "conformance; the member set is pinned"


async def _e2(r: _Run) -> str:
    h, e = r.h, r.e
    homes = await asyncio.gather(*(e.isolate(h.seller) for _ in range(3)))
    r.seen.add(*homes)
    if len({x.home for x in homes}) != 1:
        raise _violation("E2", "concurrent provisioning gave a party two homes")
    if any(x.credential is not None for x in homes):
        raise _violation("E2", "isolate without a session minted a credential")
    a1 = await e.isolate(h.seller, session=uuid.uuid4().hex)
    a2 = await e.isolate(h.seller, session=uuid.uuid4().hex)
    b = await e.isolate(h.buyer, session=uuid.uuid4().hex)
    s = await e.isolate(h.stranger, session=uuid.uuid4().hex)
    r.seen.add(a1, a2, b, s)
    if a1.home != homes[0].home or len({a1.home, b.home, s.home}) != 3:
        raise _violation("E2", "homes: one per party, the same every time")
    creds = [a1.credential, a2.credential, b.credential, s.credential]
    if any(c is None for c in creds) or len(set(creds)) != len(creds):
        raise _violation("E2", "credentials: one per party and per session")
    try:
        await e.isolate("Not a party!")
    except ValueError:
        pass
    else:
        raise _violation("E2", "a home was provisioned for what is not a party handle")
    r.credentials = {h.seller: str(a1.credential), h.buyer: str(b.credential)}
    r.credentials[h.stranger] = str(s.credential)
    return "isolate: one stable home per party, a credential per party and session"


def _tampered(handle: JobHandle) -> list[bytes]:
    first, sep, rest = handle.partition(b"\n")
    return [
        b"",
        handle[:-1],
        handle + b" ",
        b" " + handle,
        handle * 2,
        handle.swapcase(),
        b"golem/1" + sep + rest if first != b"golem/1" else b"temporal/9" + sep + rest,
        handle.replace(b",", b", ", 1),
        handle.replace(b'"', b"'"),
        b"x" * (HANDLE_MAX + 1),
    ]


async def _first(it: AsyncIterator[StreamBatch]) -> StreamBatch | None:
    try:
        return await anext(it)
    except StopAsyncIteration:
        return None
    finally:
        close: Any = getattr(it, "aclose", None)
        if close is not None:
            await close()


def _member_calls(
    e: Engine, handle: JobHandle, holds: frozenset[str], deadline: datetime
) -> list[tuple[str, Callable[[], Awaitable[object]]]]:
    pin = Pin("pin-" + "0" * 8)

    async def never() -> bool:
        return True

    return [
        ("start", lambda: e.start(handle, b"", hold="h", deadline=deadline)),
        ("observe", lambda: e.observe(handle, pin=None, holds=holds)),
        ("liveness", lambda: e.liveness(handle, pin=pin)),
        ("cancel", lambda: e.cancel(handle, stop=Stop.END, pin=None, reason="x")),
        ("fence", lambda: e.fence(handle, reason="x")),
        (
            "stream",
            lambda: _first(
                e.stream(handle, pin=pin, topics=[LISTED_TOPIC], after=0, closed=never, session="s")
            ),
        ),
    ]


async def _e3(r: _Run) -> str:
    e = r.e
    job_id = new_job_id()
    handle = e.bind(r.plan(job_id))
    r.seen.add(handle)
    if not isinstance(handle, bytes) or len(handle) > HANDLE_MAX:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise _violation("E3", "a handle is bytes, at most HANDLE_MAX of them")
    if (
        e.bind(r.plan(job_id)) != handle
        or (await r.h.fresh_engine()).bind(r.plan(job_id)) != handle
    ):
        raise _violation("E3", "bind is not deterministic (the same plan, other bytes)")
    if e.bind(r.plan(new_job_id())) == handle:
        raise _violation("E3", "two jobs, one handle")
    holds = frozenset({hold_digest(r.h.hold(job_id))})
    dead = await r.h.unreachable_engine()
    for bad in _tampered(handle):
        for name, call in _member_calls(dead, JobHandle(bad), holds, r.deadline):
            try:
                await call()
            except InvalidHandle:
                continue
            except Exception as x:
                raise _violation(
                    "E3", f"{name} of a tampered handle {bad[:40]!r} was not refused first: {x!r}"
                ) from None
            raise _violation("E3", f"{name} acted on a tampered handle {bad[:40]!r}")
    try:
        await dead.observe(cast(JobHandle, handle.decode("latin-1")), pin=None, holds=holds)
    except InvalidHandle:
        pass
    except Exception as x:
        raise _violation("E3", f"a handle that is not bytes was not refused first: {x!r}") from None
    else:
        raise _violation("E3", "a handle that is not bytes was acted on")
    return "bind: pure, deterministic, distinct; tampered handles refused before any I/O"


async def _e5(r: _Run) -> str:
    _job_id, handle, holds = r.new()
    seen = await r.observe(handle, holds)
    if seen.observation.status is not ObservedStatus.NOT_FOUND or seen.result is not None:
        raise _violation("E5", f"before any start: {seen}")
    unknown = await r.e.liveness(handle, pin=Pin(str(uuid.uuid4())))
    if unknown is not None:
        raise _violation("E5", f"liveness of a job that never ran: {unknown}")
    return "observe before any start: NOT_FOUND"


async def _e4(r: _Run) -> str:
    h = r.h
    job_id, handle, holds = r.new()
    await asyncio.gather(*(r.start(job_id, handle, "ok") for _ in range(3)))
    await r.start(job_id, handle, "ok")
    done = await r.until("E4", "a completion", handle, holds, _closed)
    result = r.check_delivery("E4", done)
    pin = _pin_of("E4", done)
    await r.start(job_id, handle, "ok")  # after the run closed: no second lineage
    await asyncio.sleep(r.settle)
    latest = await r.observe(handle, holds)
    if latest.observation.pin != pin or latest.result != result:
        raise _violation("E4", "start is not idempotent by job: the latest run is another lineage")
    if h.open(result) != f"DONE {r.marker}":
        raise _violation("E4", "the delivery does not open to the seller's output")
    again = await r.observe(handle, holds, pin)
    if again.observation.pin != pin or again.result != result:
        raise _violation("E4", f"observe(pin) is not stable: {again}")
    fresh = await h.fresh_engine()
    restarted = await fresh.observe(handle, pin=pin, holds=holds)
    r.seen.add(restarted)
    r.observed.append((restarted, pin))
    if restarted.result != result:
        raise _violation("E4", "a second instance of the adapter reads another delivery")
    foreign = await r.observe(handle, frozenset({"0" * 64}))
    if foreign.observation.provenance is Provenance.VERIFIED or foreign.result is not None:
        raise _violation("E4", "a run verified against holds the gate did not issue")
    r.delivered = (job_id, handle, holds, done)
    return "start idempotent by job (3 concurrent, 2 late: one lineage); a sealed delivery"


async def _e7(r: _Run) -> str:
    if r.delivered is None:
        return "SKIPPED (needs E4)"
    _job_id, handle, holds, done = r.delivered
    pin = _pin_of("E7", done)
    for stop in (Stop.END, Stop.ASK):
        await r.e.cancel(handle, stop=stop, pin=pin, reason="too late")
        await r.e.cancel(handle, stop=stop, pin=None, reason="too late")
        after = await r.observe(handle, holds, pin)
        if (
            after.observation.status is not ObservedStatus.COMPLETED
            or after.result != done.result
            or after.observation.result_sha256 != done.observation.result_sha256
        ):
            raise _violation("E7", f"{stop.value} after the completion changed it: {after}")
    return "END and ASK after a completion: the delivery stands, the same bytes"


async def _e6(r: _Run) -> str:
    e = r.e
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    run = await r.until("E6", "a verified run", handle, holds, _running_verified)
    pin = _pin_of("E6", run)
    await e.cancel(handle, stop=Stop.END, pin=pin, reason="the buyer canceled")
    await e.cancel(handle, stop=Stop.END, pin=pin, reason="again")
    ended = await r.until("E6", "the end", handle, holds, _closed, pin=pin)
    if ended.observation.status is not ObservedStatus.TERMINATED or ended.result is not None:
        raise _violation("E6", f"END did not end the lineage: {ended}")
    await r.h.seller_revive(handle, job_id)
    await r.h.seller_start(handle, job_id, r.marker)
    for _ in range(2):
        later = await r.observe(handle, holds, pin)
        if later.observation.status is not ObservedStatus.TERMINATED or later.result is not None:
            raise _violation(
                "E6", f"the ended lineage came back, or another run spoke for it: {later}"
            )
        await asyncio.sleep(r.settle)
    # The pin bounds a stop (I17): a run outside the lineage -- the seller's own, open now --
    # is not touched by END or ASK of the pinned one.
    await r.until(
        "E6",
        "the seller's own runs to close",
        handle,
        holds,
        lambda o: o.observation.pin != pin and _closed(o),
    )
    await r.h.seller_start(handle, job_id, r.marker, "hang")
    other = await r.until(
        "E6",
        "a run of the seller's own",
        handle,
        holds,
        lambda o: (
            o.observation.status is ObservedStatus.RUNNING and o.observation.pin not in (None, pin)
        ),
    )
    for stop in (Stop.END, Stop.ASK):
        await e.cancel(handle, stop=stop, pin=pin, reason="the pinned lineage only")
    await asyncio.sleep(r.settle)
    still = await r.observe(handle, holds)
    if (
        still.observation.status is not ObservedStatus.RUNNING
        or still.observation.pin != other.observation.pin
    ):
        raise _violation("E6", f"a stop with a pin reached a run outside its lineage: {still}")
    await e.cancel(handle, stop=Stop.END, pin=None, reason="done")
    return (
        "END on the pinned lineage: TERMINATED, no result, whatever the seller tries; "
        "a pinned stop stops nothing else"
    )


async def _e8(r: _Run) -> str:
    e = r.e
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "ignore_ask")
    pin = _pin_of("E8", await r.until("E8", "a verified run", handle, holds, _running_verified))
    await e.cancel(handle, stop=Stop.ASK, pin=pin, reason="please stop")
    r.check_delivery("E8", await r.until("E8", "the close", handle, holds, _closed, pin=pin))
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    pin = _pin_of("E8", await r.until("E8", "a verified run", handle, holds, _running_verified))
    await e.cancel(handle, stop=Stop.ASK, pin=pin, reason="please stop")
    closed = await r.until("E8", "the close", handle, holds, _closed, pin=pin)
    o = closed.observation
    if o.status is not ObservedStatus.CANCELED or o.buyer_canceled or closed.result is not None:
        raise _violation(
            "E8", f"an honoured ASK is a CANCELED run the buyer did not ask for: {closed}"
        )
    return "ASK is a request: ignored it completes, honoured it is CANCELED"


async def _e9(r: _Run) -> str:
    job_id, handle, holds = r.new()
    await r.h.seller_start(handle, job_id, r.marker)
    forged = await r.until(
        "E9",
        "the seller's own run",
        handle,
        holds,
        lambda o: o.observation.status is not ObservedStatus.NOT_FOUND,
    )
    if forged.observation.provenance is Provenance.VERIFIED or forged.result is not None:
        raise _violation("E9", f"a run the seller started itself verified: {forged}")
    with contextlib.suppress(EngineError):  # refusing a start a run beat is correct
        await r.start(job_id, handle, "ok")
    await asyncio.sleep(r.settle * 1.5)
    later = await r.observe(handle, holds)
    if later.observation.provenance is Provenance.VERIFIED or later.result is not None:
        raise _violation("E9", f"a job the seller's run took first delivered: {later}")
    return "a run the seller started itself is never verified, never a result"


async def _e10(r: _Run) -> str:
    e = r.e
    job_id, handle, holds = r.new()
    if await e.fence(handle, reason="a cancel before any run") is not Fenced.CLOSED:
        raise _violation("E10", "the fence of a job with no run did not close it")
    if await e.fence(handle, reason="again") is not Fenced.CLOSED:
        raise _violation("E10", "the fence is not idempotent")
    with contextlib.suppress(EngineError):  # refusing a fenced job's start is correct
        await r.start(job_id, handle, "ok")
    await asyncio.sleep(r.settle * 1.5)
    fenced = await r.observe(handle, holds)
    if fenced.observation.provenance is Provenance.VERIFIED or fenced.result is not None:
        raise _violation("E10", f"a fenced job delivered: {fenced}")
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    pin = _pin_of("E10", await r.until("E10", "a verified run", handle, holds, _running_verified))
    if await e.fence(handle, reason="late") is not Fenced.STARTED:
        raise _violation("E10", "the fence of a started job did not say STARTED")
    await asyncio.sleep(r.settle / 2)
    untouched = await r.observe(handle, holds, pin)
    if not _running_verified(untouched):
        raise _violation("E10", f"the fence stopped a run: {untouched}")
    await e.cancel(handle, stop=Stop.END, pin=pin, reason="done")
    for _ in range(_RACES):
        job_id, handle, holds = r.new()
        fence, _started = await asyncio.gather(
            e.fence(handle, reason="race"), r.start(job_id, handle, "ok"), return_exceptions=True
        )
        if isinstance(fence, BaseException):
            raise _violation("E10", f"a fence raced against a start raised {fence!r}")
        if fence is Fenced.CLOSED:
            await asyncio.sleep(r.settle)
            seen = await r.observe(handle, holds)
            if seen.observation.provenance is Provenance.VERIFIED or seen.result is not None:
                raise _violation("E10", f"a race the fence won delivered: {seen}")
        else:
            await r.until(
                "E10",
                "the run a fence found",
                handle,
                holds,
                lambda o: o.observation.status is not ObservedStatus.NOT_FOUND,
            )
    return f"fence: CLOSED before any run, STARTED after (untouched); {_RACES} races consistent"


async def _e11(r: _Run) -> str:
    e = r.e
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "can2")
    first = await r.until(
        "E11",
        "a verified run",
        handle,
        holds,
        lambda o: o.observation.provenance is Provenance.VERIFIED,
    )
    pin = _pin_of("E11", first)
    last = await r.until(
        "E11",
        "the lineage's completion",
        handle,
        holds,
        lambda o: o.observation.status is ObservedStatus.COMPLETED,
        pin=pin,
    )
    r.check_delivery("E11", last)
    if last.observation.pin != pin:
        raise _violation("E11", f"the pin moved across a restart within the lineage: {last}")
    live = await e.liveness(handle, pin=pin)
    r.seen.add(live)
    if live is None or live.restarts < 2:
        raise _violation("E11", f"liveness lost the lineage's restarts: {live}")
    if await e.liveness(handle, pin=Pin(str(uuid.uuid4()))) is not None:
        raise _violation("E11", "liveness reported a lineage that does not exist")
    return "restarts within the lineage keep its pin; liveness counts them"


async def _e13(r: _Run) -> str:
    if not r.h.buyer_presence:
        return "SKIPPED (buyers have no presence in this engine)"
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    pin = _pin_of("E13", await r.until("E13", "a verified run", handle, holds, _running_verified))
    await r.h.buyer_withdraw(handle, job_id)
    closed = await r.until("E13", "the close", handle, holds, _closed, pin=pin)
    if (
        closed.observation.status is not ObservedStatus.CANCELED
        or not closed.observation.buyer_canceled
    ):
        raise _violation("E13", f"the buyer's withdrawal is not buyer_canceled: {closed}")
    return "the buyer's withdrawal of its own start is buyer_canceled"


async def _collect(
    it: AsyncIterator[StreamBatch], want: int | None, within: float
) -> list[StreamBatch]:
    """Batches until ``want`` items came (None: until the stream ends)."""
    got: list[StreamBatch] = []

    async def go() -> None:
        async for batch in it:
            got.append(batch)
            if want is not None and sum(len(b.items) for b in got) >= want:
                return

    try:
        async with asyncio.timeout(within):
            await go()
    except TimeoutError:
        raise _violation("E14", f"the stream stalled after {got}") from None
    finally:
        close: Any = getattr(it, "aclose", None)
        if close is not None:
            await close()
    return got


async def _e14(r: _Run) -> str:
    if not r.h.streams:
        return "SKIPPED (this harness relays no streams)"
    h, e = r.h, r.e
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "publish")
    pin = _pin_of("E14", await r.until("E14", "a verified run", handle, holds, _running_verified))
    job_closed = False

    async def closed() -> bool:
        return job_closed

    def stream(after: int | Literal["tail"]) -> AsyncIterator[StreamBatch]:
        return e.stream(
            handle, pin=pin, topics=[LISTED_TOPIC], after=after, closed=closed, session="contract"
        )

    def check(batches: list[StreamBatch], after: int) -> list[int]:
        r.seen.add(*batches)
        offsets: list[int] = []
        for b in batches:
            for item in b.items:
                if item.topic != LISTED_TOPIC:
                    raise _violation("E14", f"an item of a topic not asked for: {item.topic!r}")
                if item.offset < after or (offsets and item.offset <= offsets[-1]):
                    raise _violation("E14", f"offsets out of order or before after: {item.offset}")
                if item.sealed is None:
                    raise _violation("E14", "a small item was not relayed")
                try:
                    Sealed(bytes(item.sealed.data))  # again: a type can be forged
                except ValueError:
                    raise _violation("E14", "a relayed item is not an age file") from None
                if h.open_item(item.sealed) != f"ITEM {r.marker} {item.offset}":
                    raise _violation("E14", "an item does not open to what the seller published")
                if b.next_offset <= item.offset:
                    raise _violation("E14", "next_offset is not past every item")
                offsets.append(item.offset)
        return offsets

    first = check(await _collect(stream(0), 2, r.wait), 0)
    if first != [0, 2]:
        raise _violation("E14", f"from 0, the listed items are offsets {first}, not [0, 2]")
    resumed = check(await _collect(stream(2), 1, r.wait), 2)
    if resumed != [2]:
        raise _violation("E14", f"from 2, the listed items are offsets {resumed}, not [2]")
    tail_batches: list[StreamBatch] = []

    async def follow_tail() -> None:
        tail_batches.extend(await _collect(stream("tail"), 2, r.wait))

    tail = asyncio.create_task(follow_tail())
    await asyncio.sleep(r.settle * 2)  # the tail is attached at the stream's end (3)
    await e.cancel(handle, stop=Stop.ASK, pin=pin, reason="publish the rest")
    done = await r.until("E14", "the completion", handle, holds, _closed, pin=pin)
    r.check_delivery("E14", done)
    await tail
    later = check(tail_batches, 3)
    if later != [3, 5]:
        raise _violation("E14", f"'tail' yielded offsets {later}, not [3, 5]")
    job_closed = True
    check(await _collect(stream(0), None, r.wait), 0)  # a closed job's relay ends
    return "streams: topics asked for only, offsets in order, after and 'tail' kept; it ends"


async def _e15(r: _Run) -> str:
    h = r.h

    async def quote(offering: OfferingId, input: bytes, within: float) -> QuoteAnswer:
        try:
            answer = await r.e.quote(offering, input, timeout=within)
        except Exception as x:  # the seller's own failure is an answer, never raised
            raise _violation("E15", f"a quote raised instead of answering: {x!r}") from None
        r.seen.add(answer)
        return answer

    ok = await quote(h.offering, h.quote_input("ok"), _QUOTE_TIMEOUT * 3)
    if ok.kind is not QuoteKind.OK or ok.document != h.quote_document():
        raise _violation("E15", f"the seller's quote document was not relayed as it is: {ok}")
    loop = asyncio.get_running_loop()
    began = loop.time()
    slow = await quote(h.offering, h.quote_input("slow"), _QUOTE_TIMEOUT)
    took = loop.time() - began
    if slow.kind is not QuoteKind.TIMEOUT or took > _QUOTE_TIMEOUT + 3.0:
        raise _violation(
            "E15", f"a slow quote is TIMEOUT within its timeout: {slow} in {took:.1f}s"
        )
    failed = await quote(h.offering, h.quote_input("fail"), _QUOTE_TIMEOUT * 3)
    if failed.kind is not QuoteKind.REFUSED or failed.document is not None:
        raise _violation("E15", f"a failing quote is REFUSED: {failed}")
    nobody = await quote(h.other_offering, h.quote_input("ok"), _QUOTE_TIMEOUT)
    if nobody.kind is not QuoteKind.UNAVAILABLE:
        raise _violation("E15", f"a quote nothing serves is UNAVAILABLE: {nobody}")
    return "quote: OK with the seller's document, TIMEOUT, REFUSED, UNAVAILABLE"


async def _e16(r: _Run) -> str:
    h, e = r.h, r.e
    await e.expose(h.offering, h.route)
    await e.expose(h.offering, h.route)
    try:
        await e.expose(h.other_offering, h.route)
    except RouteConflict:
        pass
    else:
        raise _violation("E16", "a route was repointed at another offering")
    return "expose: idempotent; a route is never repointed"


async def _e17(r: _Run) -> str:
    h = r.h
    dead = await h.unreachable_engine()
    job_id = new_job_id()
    handle = dead.bind(r.plan(job_id))
    holds = frozenset({hold_digest(h.hold(job_id))})
    calls: list[tuple[str, Callable[[], Awaitable[object]]]] = [
        ("isolate", lambda: dead.isolate(h.seller)),
        ("expose", lambda: dead.expose(h.offering, h.route)),
        ("quote", lambda: dead.quote(h.offering, h.quote_input("ok"), timeout=1.0)),
        *_member_calls(dead, handle, holds, r.deadline),
    ]
    for name, call in calls:
        try:
            got = await call()
        except EngineError:
            continue
        except StreamStopped:
            if name == "stream":
                continue
            raise
        raise _violation("E17", f"{name} answered {got!r} although the engine cannot be reached")
    return "an engine that cannot be reached: EngineError, never a fact"


async def _e18(r: _Run) -> str:
    h = r.h
    if not h.enforces_isolation:
        return "SKIPPED (this harness enforces no isolation)"
    if not r.credentials:
        raise _violation("E18", "needs E2's credentials")
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    pin = _pin_of("E18", await r.until("E18", "a verified run", handle, holds, _running_verified))
    try:
        if not await h.reach(r.credentials[h.seller], handle):
            raise _violation("E18", "the seller's credential does not reach its own job")
        if await h.reach(r.credentials[h.buyer], handle):
            raise _violation("E18", "the buyer's credential reached the seller's side of its job")
        if await h.reach(r.credentials[h.stranger], handle):
            raise _violation("E18", "a stranger's credential reached another party's job")
        for party in (h.buyer, h.stranger):
            if await h.stops(r.credentials[party], handle):
                raise _violation("E18", f"{party}'s credential can stop or fence the job")
        for party, credential in r.credentials.items():
            if await h.controls(credential):
                raise _violation("E18", f"{party}'s credential holds control power")
        untouched = await r.observe(handle, holds, pin)
        if not _running_verified(untouched):
            raise _violation("E18", f"a party's probe stopped the job's run: {untouched}")
    finally:
        await r.e.cancel(handle, stop=Stop.END, pin=pin, reason="done")
    return (
        "isolation: the seller's credential reaches its job's run, the buyer's and a "
        "stranger's neither reach, stop nor fence it, and none holds control power"
    )


def _reach_of(rule: str, what: str, seen: Observed, want: Collection[Reach | None]) -> None:
    """``seen``'s reach is one of ``want``, and a start's record comes only with a reach."""
    record = seen.buyer_start
    if seen.reach not in want or (record is not None and seen.reach is None):
        names = " or ".join(sorted("None" if x is None else x.name for x in want))
        raise _violation(rule, f"{what}: the reach is {seen.reach}, not {names}: {seen}")
    if record is not None and not isinstance(record, BuyerStart):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise _violation(rule, f"{what}: a start's record that is not a BuyerStart: {record!r}")


async def _e19(r: _Run) -> str:
    e = r.e
    for seen, pin in r.observed:  # again: a type can be forged
        o = seen.observation
        if o.provenance is Provenance.VERIFIED or pin is not None:
            _reach_of("E19", "a verified or pinned lineage", seen, {Reach.STARTED, None})
    job_id, handle, holds = r.new()
    _reach_of("E19", "before any start", await r.observe(handle, holds), {Reach.NEVER_DISPATCHED})
    await r.h.seller_start(handle, job_id, r.marker)
    own = await r.until(
        "E19",
        "the seller's own run",
        handle,
        holds,
        lambda o: o.observation.status is not ObservedStatus.NOT_FOUND,
    )
    _reach_of("E19", "a run the seller started itself", own, {Reach.NEVER_DISPATCHED})
    with contextlib.suppress(EngineError):  # refusing a start a run beat is correct
        await r.start(job_id, handle, "ok")
    await asyncio.sleep(r.settle * 1.5)
    taken = await r.observe(handle, holds)
    _reach_of("E19", "the gate's start of a job the seller's run took first", taken, _UNBLAMED)
    job_id, handle, holds = r.new()
    await e.fence(handle, reason="a cancel before any run")
    with contextlib.suppress(EngineError):  # refusing a fenced job's start is correct
        await r.start(job_id, handle, "ok")
    await asyncio.sleep(r.settle * 1.5)
    _reach_of("E19", "the gate's start of a fenced job", await r.observe(handle, holds), _UNBLAMED)
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "hang")
    began = await r.until("E19", "a verified run", handle, holds, _running_verified)
    _reach_of("E19", "the buyer's start that began a lineage", began, {Reach.STARTED})
    pin = _pin_of("E19", began)
    await e.cancel(handle, stop=Stop.END, pin=pin, reason="done")
    ended = await r.until("E19", "the end", handle, holds, _closed, pin=pin)
    _reach_of("E19", "an ended lineage, pinned", ended, {Reach.STARTED})
    _reach_of("E19", "an ended lineage", await r.observe(handle, holds), {Reach.STARTED})
    return (
        "reach: STARTED for every verified lineage; NEVER_DISPATCHED before a start and for a "
        "run of the seller's own; a start a run beat, or a fence, blames no one"
    )


async def _e12(r: _Run) -> str:
    job_id, handle, holds = r.new()
    await r.start(job_id, handle, "plain")
    closed = await r.until("E12", "the close", handle, holds, _closed)
    if closed.observation.status is not ObservedStatus.UNSEALED or closed.result is not None:
        raise _violation("E12", f"an unsealed completion was not withheld: {closed}")
    marker = r.marker.encode()
    hits = [b for b in r.seen.blobs if marker in b]
    if hits:
        raise _violation("E12", f"plaintext in what the adapter handed over: {hits[0][:80]!r}")
    return f"ciphertext only: UNSEALED withheld; {len(r.seen.blobs)} values walked, 0 markers"


_RUNNERS: dict[str, Callable[[_Run], Awaitable[str]]] = {
    "E1": _e1,
    "E2": _e2,
    "E3": _e3,
    "E4": _e4,
    "E5": _e5,
    "E6": _e6,
    "E7": _e7,
    "E8": _e8,
    "E9": _e9,
    "E10": _e10,
    "E11": _e11,
    "E12": _e12,
    "E13": _e13,
    "E14": _e14,
    "E15": _e15,
    "E16": _e16,
    "E17": _e17,
    "E18": _e18,
    "E19": _e19,
}


async def run_engine_contract(
    make: Callable[[], Awaitable[EngineHarness]],
    *,
    only: Collection[str] | None = None,
    settle: float = 1.0,
    wait: float = _WAIT,
) -> list[str]:
    """Run the contract against the adapter ``make`` rigs (called once). ``only``: the check
    ids to run (e.g. ``{"E1", "E2", "E3", "E18"}`` over the wire); E2 runs whenever E18
    does, for its credentials. ``settle``: seconds to wait for what must not happen before
    concluding it did not (an in-memory engine needs far less than a server); ``wait``:
    seconds to wait for what must happen (a completion) before calling it a violation. Returns one
    line per check run: ``"E4 ok: ..."`` or ``"E18 SKIPPED (...)"``. Raises
    :class:`EngineContractViolation`."""
    chosen = set(CHECKS if only is None else only)
    unknown = chosen - set(CHECKS)
    if unknown:
        raise ValueError(f"no such checks: {sorted(unknown)}")
    if "E18" in chosen:
        chosen.add("E2")
    run = _Run(await make(), settle, wait)
    report: list[str] = []
    for check in CHECKS:
        if check not in chosen:
            continue
        line = await _RUNNERS[check](run)
        report.append(f"{check} {line}" if line.startswith("SKIPPED") else f"{check} ok: {line}")
    return report
