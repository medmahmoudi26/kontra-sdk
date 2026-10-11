"""The ledger contract (PRD §9.3): one suite every :class:`~kontra.ledger.Ledger` adapter
passes unchanged (seam ④).

The in-memory ledger is proven against it in this repository; the platform runs the same
coroutine against its Postgres adapter::

    from kontra.testing.ledger_contract import run_ledger_contract

    async def test_postgres_ledger(database_url):
        await run_ledger_contract(lambda: connect_ledger(database_url), audit_every=500)

``make_ledger`` is called once (an adapter may be slow to create), and the run works on a
ledger that already holds data: parties, job ids and refs carry a per-run tag, and every
total is checked relative to what was there before.

Three passes over that one ledger:

1. :func:`check_rules` -- every rule once, in a fixed order, on fresh ids:

   * a first call returns a ``pending`` or ``confirmed`` handle keyed by its job id (its
     ref for deposits and withdrawals) and records exactly one event. An event *is* its
     postings (one ``event_id``, legs summing to zero, all of one kind), so even a
     dispute, which moves no money, posts a zero leg; the fee is a leg of the release or
     resolve event, credited to the platform with kind ``fee``;
   * a repeat of a completed call returns the original handle (same ref), even after
     later transitions, and records nothing; the same key with different arguments --
     currency included -- is ``HoldConflict`` (``expires_at`` is not compared: a retried
     hire has a new deadline);
   * every transition outside :func:`~kontra.ledger.next_state` is ``IllegalTransition``
     naming the job, its state and the op; a call on a job with no hold is ``UnknownHold``;
   * refusals record nothing, and come in this order: argument checks (``InvalidParty``,
     ``InvalidAmount`` -- which includes an amount above :data:`MAX_MINOR` --,
     ``CurrencyMismatch``), ``UnknownHold``, the repeat rule, ``IllegalTransition``,
     ``InvalidSplit``, ``InsufficientFunds``;
   * the world account is never a party to a call, the platform is never a buyer or a
     seller and only withdraws; ``buyer == seller`` is allowed (a seller trying their own
     offering under ``kontra dev``; it can only cost them the fee);
   * fees: ``fee_for(amount, fee_bps)`` to the platform at release, ``fee_for(seller's
     share)`` at resolve, the rest to the seller; currencies never mix.

2. :func:`check_interleavings` -- ``interleavings`` random batches. A batch is a few jobs
   (new ones, and open ones from earlier batches), deposits, withdrawals and malformed
   calls, all running as concurrent asyncio tasks that yield a random number of times
   before each call. Within a job, steps run in order and the calls of a step run
   concurrently: retries, conflicting terms, racing transitions, splits that do not add
   up, amounts at the edge of the buyer's balance. After every batch the outcomes must be
   explained by *some* order of each step's calls under the rules above; handles, holds
   and every balance must equal what the successful calls imply; no balance may be
   negative; a refusal for funds must be possible in some order; and (every
   ``audit_every`` batches) the new postings are audited: each event sums to zero, a
   job's postings touch only its buyer, its seller and the platform (fee legs), each
   successful call recorded exactly one event, ``sum(postings) == balances`` for every
   (party, currency, account), and the money inside equals deposits minus withdrawals.

3. :func:`assert_invariants` -- the same audit from scratch over everything recorded, plus
   the holds against their history and the held balances against the open holds.

A failure raises :class:`LedgerContractViolation`. For an interleaving its message has the
seed, the index, and the op log (every call with its yields and outcome). Op generation
depends only on the seed and the ledger's earlier answers, so against the in-memory
ledger ``run_ledger_contract(make, seed=S, interleavings=i + 1)`` replays it exactly.

Postgres note: the audit reads ``postings()`` in full, so its cost grows with history.
With ``audit_every=1`` (the default) 10,000 interleavings take about half a minute on the
in-memory ledger; against a database pass a larger ``audit_every`` -- everything except
the posting audit is still checked after every batch, and the final pass audits it all.
"""

from __future__ import annotations

import asyncio
import itertools
import random
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TypeVar

from kontra.ledger import (
    CURRENCIES,
    PLATFORM_PARTY,
    WORLD_PARTY,
    Account,
    Balance,
    CurrencyMismatch,
    Handle,
    HandleState,
    Hold,
    HoldConflict,
    HoldState,
    IllegalTransition,
    InsufficientFunds,
    InvalidAmount,
    InvalidParty,
    InvalidSplit,
    Ledger,
    LedgerError,
    Money,
    Op,
    Posting,
    Split,
    UnknownHold,
    fee_for,
    next_state,
)

__all__ = [
    "MAX_MINOR",
    "LedgerContractViolation",
    "assert_invariants",
    "check_interleavings",
    "check_rules",
    "run_ledger_contract",
]

#: The largest ``amount_minor`` a call may carry: §9.1 stores amounts as a Postgres
#: ``bigint``. A price is the seller's to choose (its quote), so an adapter refuses anything
#: larger with ``InvalidAmount`` rather than failing in its database driver, and every
#: adapter refuses the same amounts (acceptance criterion 9).
MAX_MINOR = 2**63 - 1

#: The deadline of every lock in the interleavings. A constant, not ``now() + 1h``: the op
#: log prints it, and replaying a seed must print the same log (no ledger rule reads it).
_DEADLINE = datetime(2100, 1, 1, tzinfo=UTC)

_LIVE = (HandleState.PENDING, HandleState.CONFIRMED)
_OPEN = (HoldState.HELD, HoldState.DISPUTED)
_TRANSITIONS = (Op.RELEASE, Op.REFUND, Op.DISPUTE, Op.RESOLVE)
_AV, _HELD = Account.AVAILABLE, Account.HELD


class LedgerContractViolation(AssertionError):
    """A rule of the ledger contract was broken. The message names the rule and, for a
    random interleaving, carries the seed and op log needed to replay it."""


async def run_ledger_contract(
    make_ledger: Callable[[], Awaitable[Ledger]],
    *,
    interleavings: int = 10_000,
    seed: int = 0,
    fee_bps: int = 500,
    currencies: Sequence[str] = ("USD",),
    audit_every: int = 1,
) -> None:
    """The whole §9.3 suite against one ledger from ``make_ledger``.

    ``fee_bps`` must be the adapter's configured fee. ``currencies`` are the ones it
    settles (the suite spreads jobs across them and expects ``CurrencyMismatch`` for the
    rest of :data:`~kontra.ledger.CURRENCIES`)."""
    ledger = await make_ledger()
    if not isinstance(ledger, Ledger):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise LedgerContractViolation(f"make_ledger returned {ledger!r}, not a Ledger")
    await check_rules(ledger, fee_bps=fee_bps, currencies=currencies)
    await check_interleavings(
        ledger,
        interleavings=interleavings,
        seed=seed,
        fee_bps=fee_bps,
        currencies=currencies,
        audit_every=audit_every,
    )
    await assert_invariants(ledger, fee_bps=fee_bps)


# =========================================================================== shared helpers


def _v(message: str) -> LedgerContractViolation:
    return LedgerContractViolation(message)


def _new_tag() -> str:
    """A prefix for this run's ids: the ledger may hold data from earlier runs."""
    return "t" + uuid.uuid4().hex[:7]


def _show(x: object) -> str:
    if isinstance(x, Handle):
        return f"handle({x.op.value}, ref={x.ref}, {x.state.value})"
    if isinstance(x, BaseException):
        return f"{type(x).__name__}({x})"
    return repr(x)


def _net(legs: Sequence[Posting]) -> dict[tuple[str, Account], int]:
    """What an event does to each (party, account), zeros dropped: comparing this rather
    than legs leaves an adapter free to split or order its legs as it likes."""
    out: dict[tuple[str, Account], int] = {}
    for p in legs:
        k = (p.party_id, p.account)
        out[k] = out.get(k, 0) + p.amount_minor
    return {k: v for k, v in out.items() if v}


def _fmt_net(net: dict[tuple[str, Account], int]) -> str:
    if not net:
        return "nothing"
    return ", ".join(f"{p}.{a.value} {v:+d}" for (p, a), v in sorted(net.items()))


def _fmt_legs(legs: Sequence[Posting]) -> str:
    return "; ".join(
        f"{p.kind} {p.party_id}.{p.account.value} {p.amount_minor:+d} {p.currency}" for p in legs
    )


def _check_rows(rows: Sequence[Balance]) -> None:
    """Balance rows alone: no duplicates, nothing negative, every currency sums to zero."""
    totals: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for r in rows:
        key = (r.party_id, r.currency)
        if key in seen:
            raise _v(f"balances() lists {r.party_id}/{r.currency} twice")
        seen.add(key)
        if r.party_id == WORLD_PARTY:
            if r.held_minor != 0 or r.available_minor > 0:
                raise _v(f"the world account must hold nothing and stay <= 0, it is {r}")
        elif r.available_minor < 0 or r.held_minor < 0:
            raise _v(
                f"{r.party_id} has a negative {r.currency} balance (available "
                f"{r.available_minor}, held {r.held_minor}): money was spent twice"
            )
        totals[r.currency] = totals.get(r.currency, 0) + r.available_minor + r.held_minor
    for cur, total in totals.items():
        if total:
            raise _v(f"{cur} balances sum to {total:+d}, not 0: money was created or destroyed")


@dataclass(frozen=True, slots=True)
class _Terms:
    """What the audit needs to know about a hold."""

    buyer: str
    seller: str
    amount: Money


def _terms(hold: Hold | None) -> _Terms | None:
    return None if hold is None else _Terms(hold.buyer, hold.seller, hold.amount)


# =========================================================================== the audit


class _Auditor:
    """Folds postings, in order, into the facts the rules are checked against. Fed
    incrementally during the interleavings and once from scratch by assert_invariants."""

    def __init__(self, fee_bps: int | None) -> None:
        self.fee_bps = fee_bps
        self.sums: dict[tuple[str, str, Account], int] = {}
        self.job_ops: dict[str, list[Op]] = {}
        self.job_state: dict[str, HoldState] = {}
        self.moved: Counter[tuple[Op, str]] = Counter()
        self.move_events: Counter[Op] = Counter()
        self.seen = 0
        self.last: Posting | None = None
        self.last_event: int | None = None

    def feed(self, postings: Sequence[Posting], terms: Callable[[str], _Terms | None]) -> None:
        """Audit ``postings[self.seen:]``. History must only grow: the boundary row is
        compared to the one seen last time (a full re-check is assert_invariants' job)."""
        n = len(postings)
        if n < self.seen:
            raise _v(
                f"postings() returned {n} rows after returning {self.seen}: history must only grow"
            )
        if self.seen and postings[self.seen - 1] != self.last:
            raise _v(
                f"posting #{self.seen - 1} changed after it was recorded: was {self.last}, "
                f"is {postings[self.seen - 1]}"
            )
        i = self.seen
        while i < n:
            j = i + 1
            event_id = postings[i].event_id
            while j < n and postings[j].event_id == event_id:
                j += 1
            self._event(postings[i:j], terms)
            i = j
        self.seen = n
        self.last = postings[-1] if n else None

    def _event(self, legs: Sequence[Posting], terms: Callable[[str], _Terms | None]) -> None:
        first = legs[0]
        eid = first.event_id
        if self.last_event is not None and eid <= self.last_event:
            raise _v(
                f"event {eid} follows event {self.last_event}: postings() must be in event "
                "order with each event's legs together"
            )
        self.last_event = eid
        cur, job = first.currency, first.job_id
        if cur not in CURRENCIES or any(p.currency != cur or p.job_id != job for p in legs):
            raise _v(f"event {eid} mixes currencies or jobs: {_fmt_legs(legs)}")
        total = sum(p.amount_minor for p in legs)
        if total:
            raise _v(f"event {eid} sums to {total:+d} {cur}, not 0: {_fmt_legs(legs)}")
        kinds = {p.kind for p in legs if p.party_id != PLATFORM_PARTY}
        if len(kinds) != 1:
            raise _v(f"event {eid} must have one kind on its non-platform legs: {_fmt_legs(legs)}")
        kind = kinds.pop()
        try:
            op = Op(kind)
        except ValueError:
            raise _v(f"event {eid} has unknown kind {kind!r}") from None
        if any(p.party_id == PLATFORM_PARTY and p.kind not in ("fee", kind) for p in legs):
            raise _v(
                f"event {eid}: a platform leg must be the fee or the event's kind: {_fmt_legs(legs)}"
            )
        net = _net(legs)
        if job is None:
            self._movement(eid, op, cur, net, legs)
        else:
            self._job_event(eid, job, op, cur, net, legs, terms)
        for p in legs:
            k = (p.party_id, p.currency, p.account)
            self.sums[k] = self.sums.get(k, 0) + p.amount_minor

    def _movement(
        self,
        eid: int,
        op: Op,
        cur: str,
        net: dict[tuple[str, Account], int],
        legs: Sequence[Posting],
    ) -> None:
        if op not in (Op.DEPOSIT, Op.WITHDRAW):
            raise _v(f"event {eid} is a {op.value} with no job id: {_fmt_legs(legs)}")
        others = {k: v for k, v in net.items() if k[0] != WORLD_PARTY}
        world = net.get((WORLD_PARTY, _AV), 0)
        if len(others) != 1 or len(net) != 2:
            raise _v(
                f"{op.value} event {eid} must move money between the world and one party: {_fmt_legs(legs)}"
            )
        ((party, account), amount), *_ = others.items()
        inward = op is Op.DEPOSIT
        if (
            account is not _AV
            or (amount > 0) != inward
            or world != -amount
            or (inward and party == PLATFORM_PARTY)
        ):
            raise _v(f"{op.value} event {eid} moves the wrong money: {_fmt_legs(legs)}")
        self.moved[(op, cur)] += abs(amount)
        self.move_events[op] += 1

    def _job_event(
        self,
        eid: int,
        job: str,
        op: Op,
        cur: str,
        net: dict[tuple[str, Account], int],
        legs: Sequence[Posting],
        terms: Callable[[str], _Terms | None],
    ) -> None:
        t = terms(job)
        if t is None:
            raise _v(f"event {eid} posts for job {job}, which has no hold")
        if cur != t.amount.currency:
            raise _v(f"event {eid} of job {job} is in {cur}, the hold is in {t.amount.currency}")
        stray = sorted({p.party_id for p in legs} - {t.buyer, t.seller, PLATFORM_PARTY})
        if stray:
            raise _v(
                f"{op.value} event {eid} of job {job} touches {stray}, neither its buyer "
                f"{t.buyer}, its seller {t.seller}, nor the platform: {_fmt_legs(legs)}"
            )
        if any(
            p.party_id == PLATFORM_PARTY and (p.account is not _AV or p.amount_minor < 0)
            for p in legs
        ):
            raise _v(f"event {eid}: the platform can only be credited a fee: {_fmt_legs(legs)}")
        state = self.job_state.get(job)
        try:
            self.job_state[job] = next_state(state, op)
        except IllegalTransition:
            history = [o.value for o in self.job_ops.get(job, [])]
            raise _v(
                f"job {job} recorded {op.value} after {history}: not a legal transition"
            ) from None
        self.job_ops.setdefault(job, []).append(op)
        want = self._expected(eid, op, t, net)
        if want != net:
            raise _v(
                f"{op.value} event {eid} of job {job} ({t.buyer} -> {t.seller}, "
                f"{t.amount.display()}) moves {_fmt_net(net)}; the rules move {_fmt_net(want)}"
            )

    def _expected(
        self, eid: int, op: Op, t: _Terms, net: dict[tuple[str, Account], int]
    ) -> dict[tuple[str, Account], int]:
        a, b, s, p = t.amount.amount_minor, t.buyer, t.seller, PLATFORM_PARTY
        fee = net.get((p, _AV), 0)
        want: dict[tuple[str, Account], int]
        if op is Op.LOCK:
            want = {(b, _AV): -a, (b, _HELD): a}
        elif op is Op.REFUND:
            want = {(b, _HELD): -a, (b, _AV): a}
        elif op is Op.DISPUTE:
            want = {}
        elif op is Op.RELEASE:
            self._check_fee(eid, fee, a, exact=True)
            want = {(b, _HELD): -a, (s, _AV): a - fee, (p, _AV): fee}
        elif op is Op.RESOLVE and b != s:
            buyer_part = net.get((b, _AV), 0)
            seller_part = net.get((s, _AV), 0) + fee
            if buyer_part < 0 or seller_part < 0:
                raise _v(f"resolve event {eid} takes money from a party: {_fmt_net(net)}")
            self._check_fee(eid, fee, seller_part, exact=True)
            want = {
                (b, _HELD): -a,
                (b, _AV): buyer_part,
                (s, _AV): seller_part - fee,
                (p, _AV): fee,
            }
        elif op is Op.RESOLVE:
            # buyer == seller: the two shares land on one account and cannot be told apart,
            # so the fee can only be bounded (fee_for steps by at most 1, so any value in
            # 0..fee_for(a) is the fee of some share).
            self._check_fee(eid, fee, a, exact=False)
            want = {(b, _HELD): -a, (b, _AV): a - fee, (p, _AV): fee}
        else:
            raise _v(f"event {eid} is a {op.value} with a job id")
        return {k: v for k, v in want.items() if v}

    def _check_fee(self, eid: int, fee: int, base: int, *, exact: bool) -> None:
        if fee < 0 or fee > base:
            raise _v(f"event {eid} charges a fee of {fee} on {base}")
        if self.fee_bps is None:
            return
        want = fee_for(base, self.fee_bps)
        if fee != want if exact else fee > want:
            raise _v(
                f"event {eid} charges a fee of {fee} on {base}; at {self.fee_bps} bps it is "
                f"fee_for({base}) = {want}"
            )

    def compare(self, rows: Sequence[Balance]) -> None:
        """sum(postings) == balances for every (party, currency, account), and the money
        inside the ledger == deposits - withdrawals."""
        actual: dict[tuple[str, str, Account], int] = {}
        for r in rows:
            if r.available_minor:
                actual[(r.party_id, r.currency, _AV)] = r.available_minor
            if r.held_minor:
                actual[(r.party_id, r.currency, _HELD)] = r.held_minor
        want = {k: v for k, v in self.sums.items() if v}
        if actual != want:
            diffs = [
                f"{p}/{c}/{a.value}: postings {want.get((p, c, a), 0)}, balance {actual.get((p, c, a), 0)}"
                for (p, c, a) in sorted(set(actual) | set(want))
                if actual.get((p, c, a), 0) != want.get((p, c, a), 0)
            ]
            raise _v("sum(postings) != balances: " + "; ".join(diffs[:10]))
        for cur in {c for _, c, _ in actual} | {c for _, c in self.moved}:
            inside = sum(v for (p, c, _), v in actual.items() if c == cur and p != WORLD_PARTY)
            net_in = self.moved[(Op.DEPOSIT, cur)] - self.moved[(Op.WITHDRAW, cur)]
            if inside != net_in:
                raise _v(
                    f"{inside} {cur} is inside the ledger, but deposits - withdrawals = {net_in}"
                )


async def assert_invariants(ledger: Ledger, *, fee_bps: int | None = None) -> None:
    """Audit everything the ledger has recorded, from scratch: every event sums to zero
    and is a legal step of its job; a job's postings touch only its buyer, its seller and
    the platform; ``sum(postings) == balances`` per (party, currency, account); no balance
    is negative; the money inside == deposits - withdrawals; each hold's state and handles
    match its events; each party's held balance is its open holds. With ``fee_bps``, every
    fee is checked too. Raises :class:`LedgerContractViolation`."""
    postings = await ledger.postings()
    rows = await ledger.balances()
    holds: dict[str, Hold] = {}
    for job in dict.fromkeys(p.job_id for p in postings if p.job_id is not None):
        hold = await ledger.get_hold(job)
        if hold is not None:
            holds[job] = hold
    audit = _Auditor(fee_bps)
    audit.feed(postings, lambda job: _terms(holds.get(job)))
    audit.compare(rows)
    _check_rows(rows)
    held: dict[tuple[str, str], int] = {}
    for job, hold in holds.items():
        ops = audit.job_ops[job]
        if hold.job_id != job or hold.state is not audit.job_state[job]:
            raise _v(
                f"hold {job} is {hold.state.value}, its events {[o.value for o in ops]} say otherwise"
            )
        if set(hold.handles) != set(ops):
            raise _v(
                f"hold {job} has handles for {sorted(hold.handles)}, its events are {[o.value for o in ops]}"
            )
        for op, h in hold.handles.items():
            if h.job_id != job or h.op is not op or h.state not in _LIVE:
                raise _v(f"hold {job} carries a bad {op.value} handle: {h}")
        if hold.state in _OPEN:
            key = (hold.buyer, hold.amount.currency)
            held[key] = held.get(key, 0) + hold.amount.amount_minor
    actual = {(r.party_id, r.currency): r.held_minor for r in rows if r.held_minor}
    want = {k: v for k, v in held.items() if v}
    if actual != want:
        raise _v(f"held balances {actual} are not the open holds {want}")


# =========================================================================== pass 1: rules


async def check_rules(
    ledger: Ledger, *, fee_bps: int = 500, currencies: Sequence[str] = ("USD",)
) -> None:
    """Every rule once, in a fixed order, on fresh ids; then a full audit."""
    rules = _Rules(ledger, fee_bps, currencies)
    await rules.money_in_and_out()
    await rules.locks()
    await rules.transitions()
    await rules.disputes()
    await rules.unknown_holds()
    await rules.fees()
    await rules.currencies()
    await assert_invariants(ledger, fee_bps=fee_bps)


class _Rules:
    def __init__(self, ledger: Ledger, fee_bps: int, currencies: Sequence[str]) -> None:
        if (
            not currencies
            or len(set(currencies)) != len(currencies)
            or any(c not in CURRENCIES for c in currencies)
        ):
            raise ValueError(
                f"currencies must be a non-empty subset of {sorted(CURRENCIES)}, each named once"
            )
        self.l = ledger
        self.fee_bps = fee_bps
        self.cur = currencies[0]
        self.other = currencies[1] if len(currencies) > 1 else None
        self.unsettled = [c for c in CURRENCIES if c not in currencies]
        self.tag = _new_tag()
        self.n = 0
        self.refs: set[str] = set()
        self.expires = datetime.now(UTC) + timedelta(hours=1)

    def party(self, name: str) -> str:
        return f"{self.tag}-{name}"

    def key(self, name: str) -> str:
        self.n += 1
        return f"{self.tag}-{name}-{self.n}"

    def m(self, minor: int, currency: str | None = None) -> Money:
        return Money(minor, currency or self.cur)

    def lock(self, job: str, buyer: str, seller: str, amount: Money) -> Awaitable[Handle]:
        return self.l.lock(job, buyer, seller, amount, expires_at=self.expires)

    def transition(self, op: Op, job: str) -> Awaitable[Handle]:
        calls = {Op.RELEASE: self.l.release, Op.REFUND: self.l.refund, Op.DISPUTE: self.l.dispute}
        return calls[op](job)

    # ------------------------------------------------------------------ assertions

    async def _events_since(self, mark: int) -> list[list[Posting]]:
        events: list[list[Posting]] = []
        for p in (await self.l.postings())[mark:]:
            if events and events[-1][0].event_id == p.event_id:
                events[-1].append(p)
            else:
                events.append([p])
        return events

    async def first(
        self, rule: str, call: Awaitable[Handle], *, key: str, op: Op
    ) -> tuple[Handle, list[Posting]]:
        """A first call: a fresh live handle keyed by ``key`` and exactly one new event."""
        mark = len(await self.l.postings())
        try:
            h = await call
        except Exception as e:
            raise _v(f"{rule}: must succeed, raised {_show(e)}") from e
        if h.job_id != key or h.op is not op or h.state not in _LIVE:
            raise _v(f"{rule}: want a pending or confirmed {op.value} handle keyed {key}, got {h}")
        if h.ref in self.refs:
            raise _v(f"{rule}: ref {h.ref} was already given to another event")
        self.refs.add(h.ref)
        events = await self._events_since(mark)
        if len(events) != 1:
            raise _v(f"{rule}: recorded {len(events)} events, must be exactly one")
        return h, events[0]

    async def repeat(self, rule: str, call: Awaitable[Handle], original: Handle) -> None:
        """A repeat: the original handle, nothing recorded."""
        mark = len(await self.l.postings())
        try:
            h = await call
        except Exception as e:
            raise _v(f"{rule}: a repeat must return the original handle, raised {_show(e)}") from e
        if (h.job_id, h.op, h.ref) != (
            original.job_id,
            original.op,
            original.ref,
        ) or h.state not in _LIVE:
            raise _v(f"{rule}: a repeat must return the original handle {original}, got {h}")
        if await self._events_since(mark):
            raise _v(f"{rule}: a repeat recorded an event")

    async def refused(
        self, rule: str, call: Awaitable[object], cls: type[LedgerError]
    ) -> LedgerError:
        """A refusal of type ``cls`` that records nothing."""
        mark = len(await self.l.postings())
        try:
            got = await call
        except Exception as e:
            if not isinstance(e, cls):
                raise _v(f"{rule}: want {cls.__name__}, got {_show(e)}") from e
            err = e
        else:
            raise _v(f"{rule}: want {cls.__name__}, got {_show(got)}")
        if await self._events_since(mark):
            raise _v(f"{rule}: a refused call recorded an event")
        return err

    async def illegal(
        self, rule: str, call: Awaitable[Handle], job: str, state: HoldState | None, op: Op
    ) -> None:
        err = await self.refused(rule, call, IllegalTransition)
        assert isinstance(err, IllegalTransition)
        if (err.job_id, err.state, err.op) != (job, state, op):
            raise _v(
                f"{rule}: IllegalTransition must name job {job}, state {state} and op {op.value}; "
                f"it names {err.job_id}, {err.state}, {err.op}"
            )

    async def balance_is(
        self, rule: str, party: str, available: int, held: int, currency: str | None = None
    ) -> None:
        cur = currency or self.cur
        b = await self.l.balance(party, cur)
        if (b.party_id, b.currency, b.available_minor, b.held_minor) != (
            party,
            cur,
            available,
            held,
        ):
            raise _v(
                f"{rule}: {party} must have {available} available and {held} held in {cur}; "
                f"balance() says {b}"
            )

    async def available(self, party: str, currency: str | None = None) -> int:
        return (await self.l.balance(party, currency or self.cur)).available_minor

    async def hold_is(
        self,
        rule: str,
        job: str,
        buyer: str,
        seller: str,
        amount: Money,
        state: HoldState,
        handles: dict[Op, Handle],
    ) -> None:
        h = await self.l.get_hold(job)
        if h is None:
            raise _v(f"{rule}: get_hold({job}) is None")
        got = (h.job_id, h.buyer, h.seller, h.amount, h.state, h.expires_at)
        want = (job, buyer, seller, amount, state, self.expires)
        if got != want:
            raise _v(f"{rule}: get_hold({job}) is {got}, want {want}")
        if {o: (x.job_id, x.op, x.ref) for o, x in h.handles.items()} != {
            o: (x.job_id, x.op, x.ref) for o, x in handles.items()
        }:
            raise _v(f"{rule}: hold {job} has handles {dict(h.handles)}, want {handles}")

    async def fund(self, party: str, minor: int, currency: str | None = None) -> None:
        ref = self.key("fund")
        await self.first(
            f"deposit to {party}",
            self.l.deposit(party, self.m(minor, currency), ref=ref),
            key=ref,
            op=Op.DEPOSIT,
        )

    async def locked(self, job: str, buyer: str, seller: str, amount: Money) -> Handle:
        h, _ = await self.first(
            f"lock {job}", self.lock(job, buyer, seller, amount), key=job, op=Op.LOCK
        )
        return h

    # ------------------------------------------------------------------ the rules

    async def money_in_and_out(self) -> None:
        a, b = self.party("alice"), self.party("bob")
        ref = self.key("deposit")
        h, _ = await self.first(
            "deposit", self.l.deposit(a, self.m(10_000), ref=ref), key=ref, op=Op.DEPOSIT
        )
        await self.balance_is("a deposit credits the party's available balance", a, 10_000, 0)
        await self.repeat(
            "deposit repeated with its ref", self.l.deposit(a, self.m(10_000), ref=ref), h
        )
        await self.balance_is("a repeated deposit credits nothing", a, 10_000, 0)
        await self.refused(
            "deposit ref reused for another amount",
            self.l.deposit(a, self.m(9_999), ref=ref),
            HoldConflict,
        )
        await self.refused(
            "deposit ref reused for another party",
            self.l.deposit(b, self.m(10_000), ref=ref),
            HoldConflict,
        )
        await self.refused(
            "deposit ref reused by a withdrawal",
            self.l.withdraw(a, self.m(10_000), ref=ref),
            HoldConflict,
        )
        for minor, what in ((0, "zero"), (-1, "negative")):
            await self.refused(
                f"{what} deposit",
                self.l.deposit(a, self.m(minor), ref=self.key("d")),
                InvalidAmount,
            )
            await self.refused(
                f"{what} withdrawal",
                self.l.withdraw(a, self.m(minor), ref=self.key("w")),
                InvalidAmount,
            )
        # Beyond §9.1's bigint: the same refusal from every adapter, not a driver error.
        await self.refused(
            "deposit beyond a 64-bit amount",
            self.l.deposit(a, self.m(MAX_MINOR + 1), ref=self.key("d")),
            InvalidAmount,
        )
        await self.refused(
            "withdrawal beyond a 64-bit amount",
            self.l.withdraw(a, self.m(MAX_MINOR + 1), ref=self.key("w")),
            InvalidAmount,
        )
        for p in (WORLD_PARTY, PLATFORM_PARTY, ""):
            await self.refused(
                f"deposit to {p!r}", self.l.deposit(p, self.m(1), ref=self.key("d")), InvalidParty
            )
        for p in (WORLD_PARTY, ""):
            await self.refused(
                f"withdrawal from {p!r}",
                self.l.withdraw(p, self.m(1), ref=self.key("w")),
                InvalidParty,
            )
        ref = self.key("withdraw")
        await self.refused(
            "withdrawal beyond the available balance",
            self.l.withdraw(a, self.m(10_001), ref=ref),
            InsufficientFunds,
        )
        await self.balance_is("a refused withdrawal moves nothing", a, 10_000, 0)
        # A ref refused for funds is not used up: nothing was recorded under it.
        h, _ = await self.first(
            "withdrawal", self.l.withdraw(a, self.m(2_500), ref=ref), key=ref, op=Op.WITHDRAW
        )
        await self.balance_is("a withdrawal debits the available balance", a, 7_500, 0)
        await self.repeat(
            "withdrawal repeated with its ref", self.l.withdraw(a, self.m(2_500), ref=ref), h
        )
        await self.balance_is("a repeated withdrawal debits nothing", a, 7_500, 0)

    async def locks(self) -> None:
        b, s, c = self.party("buyer"), self.party("seller"), self.party("carol")
        await self.fund(b, 5_000)
        job = self.key("job")
        amount = self.m(1_200)
        h, _ = await self.first("lock", self.lock(job, b, s, amount), key=job, op=Op.LOCK)
        await self.balance_is("a lock moves the amount from available to held", b, 3_800, 1_200)
        await self.balance_is("a lock pays the seller nothing yet", s, 0, 0)
        await self.hold_is("get_hold after lock", job, b, s, amount, HoldState.HELD, {Op.LOCK: h})
        await self.repeat("lock repeated", self.lock(job, b, s, amount), h)
        await self.repeat(
            "lock repeated with a later deadline (a retried hire)",
            self.l.lock(job, b, s, amount, expires_at=self.expires + timedelta(minutes=5)),
            h,
        )
        await self.balance_is("a repeated lock holds nothing more", b, 3_800, 1_200)
        await self.hold_is(
            "a repeated lock keeps the first deadline",
            job,
            b,
            s,
            amount,
            HoldState.HELD,
            {Op.LOCK: h},
        )
        await self.refused(
            "lock repeated with another amount", self.lock(job, b, s, self.m(1_201)), HoldConflict
        )
        await self.refused(
            "lock repeated with another seller", self.lock(job, b, c, amount), HoldConflict
        )
        await self.refused(
            "lock repeated with another buyer", self.lock(job, c, s, amount), HoldConflict
        )
        poor = self.key("job")
        await self.refused(
            "lock beyond the available balance",
            self.lock(poor, b, s, self.m(3_801)),
            InsufficientFunds,
        )
        await self.balance_is("a refused lock moves nothing", b, 3_800, 1_200)
        if await self.l.get_hold(poor) is not None:
            raise _v("a lock refused for funds must leave no hold")
        for p in (WORLD_PARTY, PLATFORM_PARTY, ""):
            await self.refused(
                f"lock with buyer {p!r}", self.lock(self.key("job"), p, s, self.m(1)), InvalidParty
            )
            await self.refused(
                f"lock with seller {p!r}", self.lock(self.key("job"), b, p, self.m(1)), InvalidParty
            )
        await self.refused(
            "lock of a negative amount", self.lock(self.key("job"), b, s, self.m(-1)), InvalidAmount
        )
        await self.refused(
            "lock beyond a 64-bit amount",
            self.lock(self.key("job"), b, s, self.m(MAX_MINOR + 1)),
            InvalidAmount,
        )
        # Self-hire: allowed; the party gets its money back less the fee.
        mine = self.key("self")
        await self.first(
            "self-hire lock (buyer == seller)",
            self.lock(mine, b, b, self.m(1_000)),
            key=mine,
            op=Op.LOCK,
        )
        await self.balance_is("a self-hire lock holds like any other", b, 2_800, 2_200)
        await self.first("self-hire release", self.l.release(mine), key=mine, op=Op.RELEASE)
        fee = fee_for(1_000, self.fee_bps)
        await self.balance_is(
            "a self-hire release returns the amount less the fee", b, 3_800 - fee, 1_200
        )
        # A free job goes through escrow like any other.
        free = self.key("free")
        await self.first("zero-amount lock", self.lock(free, b, s, self.m(0)), key=free, op=Op.LOCK)
        await self.first("zero-amount release", self.l.release(free), key=free, op=Op.RELEASE)
        await self.balance_is("a zero-amount job moves nothing", b, 3_800 - fee, 1_200)

    async def transitions(self) -> None:
        b, s = self.party("tbuyer"), self.party("tseller")
        await self.fund(b, 10_000)
        moves: dict[Op, tuple[int, int, int]] = {  # buyer available, buyer held, seller available
            Op.RELEASE: (0, 0, 1_000 - fee_for(1_000, self.fee_bps)),
            Op.REFUND: (1_000, 0, 0),
            Op.DISPUTE: (0, 1_000, 0),
        }
        for op, (back, still_held, paid) in moves.items():
            job = self.key(op.value)
            lock = await self.locked(job, b, s, self.m(1_000))
            b0, s0 = await self.available(b), await self.available(s)
            h, _ = await self.first(
                f"{op.value} of a held hold", self.transition(op, job), key=job, op=op
            )
            state = next_state(HoldState.HELD, op)
            got_b = await self.l.balance(b, self.cur)
            if (got_b.available_minor - b0, got_b.held_minor, await self.available(s) - s0) != (
                back,
                still_held,
                paid,
            ):
                raise _v(
                    f"{op.value} of 1000: buyer available {back:+d}, buyer held {still_held}, seller "
                    f"{paid:+d}; got {got_b.available_minor - b0:+d}, {got_b.held_minor}, "
                    f"{await self.available(s) - s0:+d}"
                )
            await self.repeat(f"{op.value} repeated", self.transition(op, job), h)
            await self.repeat(
                f"lock repeated after {op.value}", self.lock(job, b, s, self.m(1_000)), lock
            )
            await self.hold_is(
                f"get_hold after {op.value}",
                job,
                b,
                s,
                self.m(1_000),
                state,
                {Op.LOCK: lock, op: h},
            )
            for other in moves:
                if other is not op:
                    await self.illegal(
                        f"{other.value} after {op.value}",
                        self.transition(other, job),
                        job,
                        state,
                        other,
                    )
            if op is not Op.DISPUTE:
                await self.illegal(
                    f"resolve after {op.value}",
                    self.l.resolve(job, Split(500, 500)),
                    job,
                    state,
                    Op.RESOLVE,
                )
            if still_held:  # leave nothing held behind
                await self.first(
                    "resolve", self.l.resolve(job, Split(1_000, 0)), key=job, op=Op.RESOLVE
                )
        job = self.key("undisputed")
        await self.locked(job, b, s, self.m(1_000))
        await self.illegal(
            "resolve of an undisputed hold",
            self.l.resolve(job, Split(500, 500)),
            job,
            HoldState.HELD,
            Op.RESOLVE,
        )
        await self.first("refund", self.l.refund(job), key=job, op=Op.REFUND)

    async def disputes(self) -> None:
        b, s = self.party("dbuyer"), self.party("dseller")
        await self.fund(b, 10_000)
        job = self.key("dispute")
        amount = self.m(2_000)
        lock = await self.locked(job, b, s, amount)
        dispute, _ = await self.first("dispute", self.l.dispute(job), key=job, op=Op.DISPUTE)
        await self.balance_is("a dispute keeps the amount held", b, 8_000, 2_000)
        await self.refused(
            "resolve with a split short of the hold",
            self.l.resolve(job, Split(1_000, 999)),
            InvalidSplit,
        )
        await self.refused(
            "resolve with a split beyond the hold",
            self.l.resolve(job, Split(1_000, 1_001)),
            InvalidSplit,
        )
        h, _ = await self.first(
            "resolve", self.l.resolve(job, Split(1_200, 800)), key=job, op=Op.RESOLVE
        )
        await self.balance_is("resolve returns the buyer's share", b, 9_200, 0)
        await self.balance_is(
            "resolve pays the seller's share less the fee", s, 800 - fee_for(800, self.fee_bps), 0
        )
        await self.repeat(
            "resolve repeated with its split", self.l.resolve(job, Split(1_200, 800)), h
        )
        await self.refused(
            "resolve repeated with another split",
            self.l.resolve(job, Split(1_000, 1_000)),
            HoldConflict,
        )
        await self.repeat("dispute repeated after resolve", self.l.dispute(job), dispute)
        for op in (Op.RELEASE, Op.REFUND):
            await self.illegal(
                f"{op.value} after resolve", self.transition(op, job), job, HoldState.RESOLVED, op
            )
        await self.hold_is(
            "get_hold after resolve",
            job,
            b,
            s,
            amount,
            HoldState.RESOLVED,
            {Op.LOCK: lock, Op.DISPUTE: dispute, Op.RESOLVE: h},
        )
        for split in (Split(1_000, 0), Split(0, 1_000)):
            job = self.key("split")
            await self.locked(job, b, s, self.m(1_000))
            await self.first("dispute", self.l.dispute(job), key=job, op=Op.DISPUTE)
            b0, s0 = await self.available(b), await self.available(s)
            await self.first(f"resolve {split}", self.l.resolve(job, split), key=job, op=Op.RESOLVE)
            fee = fee_for(split.seller_minor, self.fee_bps)
            got = (await self.available(b) - b0, await self.available(s) - s0)
            if got != (split.buyer_minor, split.seller_minor - fee):
                raise _v(
                    f"resolve {split}: buyer and seller must get {(split.buyer_minor, split.seller_minor - fee)}, got {got}"
                )

    async def unknown_holds(self) -> None:
        job = self.key("ghost")
        if await self.l.get_hold(job) is not None:
            raise _v(f"get_hold({job}) of a job never locked must be None")
        for op in (Op.RELEASE, Op.REFUND, Op.DISPUTE):
            await self.refused(
                f"{op.value} of a job never locked", self.transition(op, job), UnknownHold
            )
        await self.refused(
            "resolve of a job never locked", self.l.resolve(job, Split(0, 0)), UnknownHold
        )

    async def fees(self) -> None:
        b, s = self.party("fbuyer"), self.party("fseller")
        await self.fund(b, 1_000_000)
        p0 = await self.available(PLATFORM_PARTY)
        collected = 0
        for a in (1, 19, 20, 21, 1_999, 2_000, 123_457):
            job = self.key("fee")
            await self.locked(job, b, s, self.m(a))
            _, legs = await self.first(
                f"release of {a}", self.l.release(job), key=job, op=Op.RELEASE
            )
            fee = fee_for(a, self.fee_bps)
            net = _net(legs)
            if (net.get((PLATFORM_PARTY, _AV), 0), net.get((s, _AV), 0)) != (fee, a - fee):
                raise _v(
                    f"release of {a} at {self.fee_bps} bps: the platform gets {fee} and the seller "
                    f"{a - fee}; the event moves {_fmt_net(net)}"
                )
            collected += fee
        for split in (Split(1_000, 1_999), Split(2_999, 0), Split(0, 2_999)):
            job = self.key("fee")
            await self.locked(job, b, s, self.m(split.buyer_minor + split.seller_minor))
            await self.first("dispute", self.l.dispute(job), key=job, op=Op.DISPUTE)
            _, legs = await self.first(
                f"resolve {split}", self.l.resolve(job, split), key=job, op=Op.RESOLVE
            )
            fee = fee_for(split.seller_minor, self.fee_bps)
            net = _net(legs)
            got = (net.get((PLATFORM_PARTY, _AV), 0), net.get((s, _AV), 0), net.get((b, _AV), 0))
            if got != (fee, split.seller_minor - fee, split.buyer_minor):
                raise _v(
                    f"resolve {split} at {self.fee_bps} bps: the fee is on the seller's share only "
                    f"(platform {fee}, seller {split.seller_minor - fee}, buyer {split.buyer_minor}); "
                    f"the event moves {_fmt_net(net)}"
                )
            collected += fee
        if await self.available(PLATFORM_PARTY) - p0 != collected:
            raise _v(f"the platform must have collected exactly the fees, {collected}")
        if collected:
            ref = self.key("payout")
            await self.first(
                "the platform withdraws its fees",
                self.l.withdraw(PLATFORM_PARTY, self.m(collected), ref=ref),
                key=ref,
                op=Op.WITHDRAW,
            )
            if await self.available(PLATFORM_PARTY) != p0:
                raise _v("a platform withdrawal must debit the platform's available balance")

    async def currencies(self) -> None:
        b, s = self.party("cbuyer"), self.party("cseller")
        await self.fund(b, 1_000)
        if self.other is not None:
            o = self.other
            await self.balance_is(f"a deposit in {self.cur} leaves {o} untouched", b, 0, 0, o)
            await self.refused(
                f"lock in {o} backed only by {self.cur}",
                self.lock(self.key("cur"), b, s, self.m(1, o)),
                InsufficientFunds,
            )
            # The currency is part of the key's terms: 500 USDC is not 500 USD.
            ref = self.key("fund")
            await self.first(
                f"deposit in {o}",
                self.l.deposit(b, self.m(500, o), ref=ref),
                key=ref,
                op=Op.DEPOSIT,
            )
            await self.refused(
                f"deposit ref reused for the same number in {self.cur}",
                self.l.deposit(b, self.m(500), ref=ref),
                HoldConflict,
            )
            job = self.key("cur")
            await self.locked(job, b, s, self.m(500, o))
            await self.refused(
                f"lock repeated with the same number in {self.cur}",
                self.lock(job, b, s, self.m(500)),
                HoldConflict,
            )
            await self.first(f"release in {o}", self.l.release(job), key=job, op=Op.RELEASE)
            await self.balance_is(f"a job in {o} leaves {self.cur} untouched", b, 1_000, 0)
            await self.balance_is(f"a job in {o} pays in {o} only", s, 0, 0)
            await self.balance_is(
                f"a job in {o} pays in {o}", s, 500 - fee_for(500, self.fee_bps), 0, o
            )
        for c in self.unsettled:
            await self.refused(
                f"lock in {c}, which this ledger does not settle",
                self.lock(self.key("cur"), b, s, self.m(1, c)),
                CurrencyMismatch,
            )
            await self.refused(
                f"deposit in {c}",
                self.l.deposit(b, self.m(1, c), ref=self.key("d")),
                CurrencyMismatch,
            )
            await self.refused(
                f"withdrawal in {c}",
                self.l.withdraw(b, self.m(1, c), ref=self.key("w")),
                CurrencyMismatch,
            )


# =========================================================================== pass 2: interleavings


@dataclass(slots=True)
class _Call:
    op: Op
    key: str  # job id, or ref for a deposit / withdrawal
    yields: int
    party: str = ""  # lock: the buyer; deposit / withdrawal: the party
    seller: str = ""
    amount: Money | None = None
    split: Split | None = None
    expires_at: datetime | None = None
    outcome: Handle | Exception | None = None

    @property
    def money(self) -> Money:
        assert self.amount is not None
        return self.amount

    async def run(self, ledger: Ledger) -> None:
        for _ in range(self.yields):
            await asyncio.sleep(0)
        try:
            self.outcome = await self._invoke(ledger)
        except Exception as e:  # an adapter's bug is an outcome too: the checker names it
            self.outcome = e

    def _invoke(self, ledger: Ledger) -> Awaitable[Handle]:
        match self.op:
            case Op.LOCK:
                assert self.expires_at is not None
                return ledger.lock(
                    self.key, self.party, self.seller, self.money, expires_at=self.expires_at
                )
            case Op.RELEASE:
                return ledger.release(self.key)
            case Op.REFUND:
                return ledger.refund(self.key)
            case Op.DISPUTE:
                return ledger.dispute(self.key)
            case Op.RESOLVE:
                assert self.split is not None
                return ledger.resolve(self.key, self.split)
            case Op.DEPOSIT:
                return ledger.deposit(self.party, self.money, ref=self.key)
            case Op.WITHDRAW:
                return ledger.withdraw(self.party, self.money, ref=self.key)

    def describe(self) -> str:
        if self.op is Op.LOCK:
            args = f"{self.party or '<empty>'} -> {self.seller or '<empty>'}, {self.money.amount_minor} {self.money.currency}"
            if self.expires_at is not None:
                args += f", expires {self.expires_at:%H:%M:%S}"
        elif self.op is Op.RESOLVE and self.split is not None:
            args = f"buyer {self.split.buyer_minor}, seller {self.split.seller_minor}"
        elif self.op in (Op.DEPOSIT, Op.WITHDRAW):
            args = f"{self.party or '<empty>'}, {self.money.amount_minor} {self.money.currency}"
        else:
            args = ""
        return f"{self.op.value}({args})"


@dataclass(slots=True)
class _Chain:
    """One key's calls in a batch: steps in order, the calls of a step concurrently."""

    key: str
    is_job: bool
    steps: list[list[_Call]]

    async def run(self, ledger: Ledger) -> None:
        for step in self.steps:
            if len(step) == 1:
                await step[0].run(ledger)
            else:
                await asyncio.gather(*(c.run(ledger) for c in step))


@dataclass(slots=True)
class _MHold:
    """The model of a hold: what the successful calls imply."""

    buyer: str
    seller: str
    amount: Money
    #: The deadlines the hold may carry: retries of one hire race with different
    #: deadlines, and any of them may have been the first to land.
    expires_at: frozenset[datetime | None]
    state: HoldState
    handles: dict[Op, Handle]
    applied: list[Op]
    split: Split | None = None

    def copy(self) -> _MHold:
        return replace(self, handles=dict(self.handles), applied=list(self.applied))


@dataclass(frozen=True, slots=True)
class _MMove:
    op: Op
    party: str
    amount: Money
    handle: Handle


class _Mismatch(Exception):
    """This order of a step's calls does not explain what the ledger returned."""


@dataclass(slots=True)
class _Sim:
    """One candidate order of a step's calls."""

    seen_refs: set[str]
    taken: set[str] = field(default_factory=set[str])
    applied: list[_Call] = field(default_factory=list[_Call])
    refused: list[_Call] = field(default_factory=list[_Call])

    def new(self, call: _Call) -> Handle:
        out = call.outcome
        if not isinstance(out, Handle):
            raise _Mismatch(f"{call.describe()} must succeed, got {_show(out)}")
        if out.job_id != call.key or out.op is not call.op or out.state not in _LIVE:
            raise _Mismatch(
                f"{call.describe()} returned {out}; want a pending or confirmed {call.op.value} handle keyed {call.key}"
            )
        if out.ref in self.seen_refs or out.ref in self.taken:
            raise _Mismatch(
                f"{call.describe()} returned ref {out.ref}, already given to another event"
            )
        self.taken.add(out.ref)
        self.applied.append(call)
        return out


def _again(call: _Call, original: Handle) -> None:
    out = call.outcome
    if not (
        isinstance(out, Handle)
        and (out.job_id, out.op, out.ref) == (original.job_id, original.op, original.ref)
        and out.state in _LIVE
    ):
        raise _Mismatch(
            f"{call.describe()} repeats a completed call: want its handle (ref={original.ref}), got {_show(out)}"
        )


def _err(call: _Call, cls: type[LedgerError]) -> None:
    if not isinstance(call.outcome, cls):
        raise _Mismatch(f"{call.describe()} must raise {cls.__name__}, got {_show(call.outcome)}")


def _bad_args(call: _Call, settled: Sequence[str]) -> type[LedgerError] | None:
    """The state-independent refusal of a call, if any (they come first)."""
    if call.op is Op.LOCK:
        if any(not p or p in (WORLD_PARTY, PLATFORM_PARTY) for p in (call.party, call.seller)):
            return InvalidParty
        if not 0 <= call.money.amount_minor <= MAX_MINOR:
            return InvalidAmount
    elif call.op in (Op.DEPOSIT, Op.WITHDRAW):
        if (
            not call.party
            or call.party == WORLD_PARTY
            or (call.party == PLATFORM_PARTY and call.op is Op.DEPOSIT)
        ):
            return InvalidParty
        if not 0 < call.money.amount_minor <= MAX_MINOR:
            return InvalidAmount
    else:
        return None
    return None if call.money.currency in settled else CurrencyMismatch


_S = TypeVar("_S")


def _explain(
    state: _S,
    step: list[_Call],
    simulate: Callable[[_S, _Call, _Sim], _S],
    copy: Callable[[_S], _S],
    seen_refs: set[str],
) -> tuple[_S, _Sim]:
    """Find an order of the step's concurrent calls under which the rules give exactly
    the outcomes the ledger returned (a linearization), and the state it leads to."""
    reasons: list[str] = []
    for order in itertools.permutations(step):
        sim = _Sim(seen_refs)
        st = copy(state)
        try:
            for call in order:
                st = simulate(st, call, sim)
        except _Mismatch as m:
            reasons.append(str(m))
            continue
        return st, sim
    if len(step) == 1:
        raise _Mismatch(reasons[0])
    raise _Mismatch(
        "no order of these concurrent calls explains their outcomes, e.g. " + reasons[0]
    )


class _Interleaver:
    def __init__(
        self,
        ledger: Ledger,
        *,
        seed: int,
        fee_bps: int,
        currencies: Sequence[str],
        audit_every: int,
    ) -> None:
        if (
            not currencies
            or len(set(currencies)) != len(currencies)
            or any(c not in CURRENCIES for c in currencies)
        ):
            raise ValueError(
                f"currencies must be a non-empty subset of {sorted(CURRENCIES)}, each named once"
            )
        if audit_every < 1:
            raise ValueError("audit_every must be at least 1")
        self.ledger = ledger
        self.seed = seed
        self.fee_bps = fee_bps
        self.settled = tuple(currencies)
        self.unsettled = [c for c in CURRENCIES if c not in currencies]
        self.audit_every = audit_every
        self.tag = _new_tag()
        self.pool = [f"{self.tag}-p{k}" for k in range(5)]
        self.expires = _DEADLINE
        self.refs: set[str] = set()
        self.balances: dict[tuple[str, str], list[int]] = {}
        self.holds: dict[str, _MHold] = {}
        self.moves: dict[str, _MMove] = {}
        self.open: list[str] = []
        self.audit = _Auditor(fee_bps)
        self.pre_terms: dict[str, _Terms] = {}
        self.base_events: Counter[Op] = Counter()
        self.move_count: Counter[Op] = Counter()
        self.touched: set[str] = set()
        self.stats: Counter[str] = Counter()

    # ------------------------------------------------------------------ driving

    async def run(self, interleavings: int) -> Counter[str]:
        postings = await self.ledger.postings()
        for job in dict.fromkeys(p.job_id for p in postings if p.job_id is not None):
            t = _terms(await self.ledger.get_hold(job))
            if t is not None:
                self.pre_terms[job] = t
        rows = await self.ledger.balances()
        self.audit.feed(postings, self.pre_terms.get)
        self.audit.compare(rows)
        _check_rows(rows)
        self.balances = {(r.party_id, r.currency): [r.available_minor, r.held_minor] for r in rows}
        self.base_events = Counter(self.audit.move_events)
        for i in range(interleavings):
            rng = random.Random(f"{self.seed}:{i}")  # noqa: S311 -- reproducible, not secret
            chains = self.plan(rng, i)
            prior = {c.key: self.holds.get(c.key) for c in chains}
            await asyncio.gather(*(c.run(self.ledger) for c in chains))
            audit = (i + 1) % self.audit_every == 0 or i + 1 == interleavings
            try:
                await self.check(chains, audit=audit)
            except (_Mismatch, LedgerContractViolation) as e:
                raise LedgerContractViolation(self.report(i, chains, prior, str(e))) from None
        return self.stats

    def report(
        self, i: int, chains: list[_Chain], prior: dict[str, _MHold | None], what: str
    ) -> str:
        lines = [
            f"ledger contract violated in interleaving {i} (seed={self.seed}): {what}",
            f"replay: await run_ledger_contract(make_ledger, seed={self.seed}, interleavings={i + 1})"
            " -- exact against an in-memory ledger; a database may schedule differently",
            "op log (chains ran concurrently; a chain's steps in order; the calls of a step concurrently):",
        ]
        for ch in chains:
            before = prior.get(ch.key)
            was = (
                f" (was {before.state.value}: {before.buyer} -> {before.seller}, {before.amount.display()})"
                if before is not None
                else ""
            )
            lines.append(f"  {'job' if ch.is_job else 'ref'} {ch.key}{was}")
            for n, step in enumerate(ch.steps, 1):
                for c in step:
                    lines.append(
                        f"    {n}. {c.describe()} [yields {c.yields}] -> {_show(c.outcome)}"
                    )
        return "\n".join(lines)

    # ------------------------------------------------------------------ the model

    def avail(self, party: str, currency: str) -> int:
        b = self.balances.get((party, currency))
        return b[0] if b else 0

    def _add(self, party: str, currency: str, available: int = 0, held: int = 0) -> None:
        b = self.balances.get((party, currency))
        if b is None:
            b = self.balances[(party, currency)] = [0, 0]
        b[0] += available
        b[1] += held

    def _apply(self, call: _Call, hold: _MHold | None) -> None:
        """The balance changes of one successful first call."""
        if call.op in (Op.DEPOSIT, Op.WITHDRAW):
            a = call.money.amount_minor * (1 if call.op is Op.DEPOSIT else -1)
            self._add(call.party, call.money.currency, a)
            self._add(WORLD_PARTY, call.money.currency, -a)
            return
        assert hold is not None
        a, cur, b, s = hold.amount.amount_minor, hold.amount.currency, hold.buyer, hold.seller
        if call.op is Op.LOCK:
            self._add(b, cur, -a, a)
        elif call.op is Op.RELEASE:
            fee = fee_for(a, self.fee_bps)
            self._add(b, cur, held=-a)
            self._add(s, cur, a - fee)
            self._add(PLATFORM_PARTY, cur, fee)
        elif call.op is Op.REFUND:
            self._add(b, cur, a, -a)
        elif call.op is Op.RESOLVE:
            assert call.split is not None
            fee = fee_for(call.split.seller_minor, self.fee_bps)
            self._add(b, cur, call.split.buyer_minor, -a)
            self._add(s, cur, call.split.seller_minor - fee)
            self._add(PLATFORM_PARTY, cur, fee)

    def _sim_job(self, st: _MHold | None, call: _Call, sim: _Sim) -> _MHold | None:
        bad = _bad_args(call, self.settled)
        if bad is not None:
            _err(call, bad)
            return st
        op = call.op
        if op is Op.LOCK:
            if st is not None:
                if (st.buyer, st.seller, st.amount) == (call.party, call.seller, call.amount):
                    _again(call, st.handles[Op.LOCK])
                else:
                    _err(call, HoldConflict)
                return st
            if isinstance(call.outcome, InsufficientFunds):
                sim.refused.append(call)
                return None
            h = sim.new(call)
            return _MHold(
                call.party,
                call.seller,
                call.money,
                frozenset({call.expires_at}),
                HoldState.HELD,
                {Op.LOCK: h},
                [Op.LOCK],
            )
        if st is None:
            _err(call, UnknownHold)
            return None
        done = st.handles.get(op)
        if done is not None:
            if op is Op.RESOLVE and call.split != st.split:
                _err(call, HoldConflict)
            else:
                _again(call, done)
            return st
        try:
            state = next_state(st.state, op)
        except IllegalTransition:
            out = call.outcome
            if not (
                isinstance(out, IllegalTransition)
                and (out.job_id, out.state, out.op) == (call.key, st.state, op)
            ):
                raise _Mismatch(
                    f"{call.describe()} of a {st.state.value} hold must raise "
                    f"IllegalTransition({call.key}, {st.state.value}, {op.value}), got {_show(out)}"
                ) from None
            return st
        if op is Op.RESOLVE:
            assert call.split is not None
            if call.split.buyer_minor + call.split.seller_minor != st.amount.amount_minor:
                _err(call, InvalidSplit)
                return st
            st.split = call.split
        st.handles[op] = sim.new(call)
        st.state = state
        st.applied.append(op)
        return st

    def _sim_move(self, st: _MMove | None, call: _Call, sim: _Sim) -> _MMove | None:
        bad = _bad_args(call, self.settled)
        if bad is not None:
            _err(call, bad)
            return st
        if st is not None:
            if (st.op, st.party, st.amount) == (call.op, call.party, call.amount):
                _again(call, st.handle)
            else:
                _err(call, HoldConflict)
            return st
        if call.op is Op.WITHDRAW and isinstance(call.outcome, InsufficientFunds):
            sim.refused.append(call)
            return None
        return _MMove(call.op, call.party, call.money, sim.new(call))

    # ------------------------------------------------------------------ checking a batch

    async def check(self, chains: list[_Chain], *, audit: bool) -> None:
        start = {k: v[0] for k, v in self.balances.items()}
        applied: list[tuple[_Call, str]] = []
        refused: list[_Call] = []
        for ch in chains:
            ok: set[int] = set()
            if ch.is_job:
                hold = self.holds.get(ch.key)
                for step in ch.steps:
                    hold, sim = _explain(hold, step, self._sim_job, _copy_hold, self.refs)
                    self._absorb(sim, ch.key, applied, refused, ok)
                    if hold is not None and any(c.op is Op.LOCK for c in sim.applied):
                        ref = hold.handles[Op.LOCK].ref
                        hold.expires_at = frozenset(
                            c.expires_at
                            for c in step
                            if c.op is Op.LOCK
                            and isinstance(c.outcome, Handle)
                            and c.outcome.ref == ref
                        )
                if hold is not None:
                    self.holds[ch.key] = hold
                self.touched.add(ch.key)
            else:
                move = self.moves.get(ch.key)
                for step in ch.steps:
                    move, sim = _explain(move, step, self._sim_move, _same, self.refs)
                    self._absorb(sim, ch.key, applied, refused, ok)
                if move is not None:
                    self.moves[ch.key] = move
            for step in ch.steps:
                for c in step:
                    label = (
                        "ok"
                        if id(c) in ok
                        else "repeat"
                        if isinstance(c.outcome, Handle)
                        else type(c.outcome).__name__
                    )
                    self.stats[f"{c.op.value}:{label}"] += 1
        # Money: apply what succeeded, then check every refusal for funds was possible.
        debits: dict[tuple[str, str], list[tuple[str, int]]] = {}
        for call, key in applied:
            self._apply(call, self.holds.get(key))
            if call.op in (Op.LOCK, Op.WITHDRAW):
                debits.setdefault((call.party, call.money.currency), []).append(
                    (key, call.money.amount_minor)
                )
            if call.op in (Op.DEPOSIT, Op.WITHDRAW):
                self.move_count[call.op] += 1
        for call in refused:
            k = (call.party, call.money.currency)
            floor = start.get(k, 0) - sum(a for key, a in debits.get(k, []) if key != call.key)
            if call.money.amount_minor <= floor:
                raise _v(
                    f"{call.describe()} on {call.key} was refused with InsufficientFunds, yet "
                    f"{call.party} had at least {floor} {call.money.currency} available in every "
                    "order of this batch"
                )
        self.open = [
            j
            for j in dict.fromkeys([*self.open, *(c.key for c in chains if c.is_job)])
            if j in self.holds and self.holds[j].state in _OPEN
        ]
        # The ledger's view of every job in the batch.
        for ch in chains:
            if ch.is_job:
                _compare_hold(ch.key, await self.ledger.get_hold(ch.key), self.holds.get(ch.key))
        rows = await self.ledger.balances()
        actual = {(r.party_id, r.currency): (r.available_minor, r.held_minor) for r in rows}
        diffs = [
            f"{p}/{c}: want available {w[0]} held {w[1]}, balance says {actual.get((p, c), (0, 0))}"
            for (p, c), w in sorted(self.balances.items())
            if actual.get((p, c), (0, 0)) != (w[0], w[1])
        ] + [
            f"{p}/{c}: unexpected balance {v}"
            for (p, c), v in sorted(actual.items())
            if (p, c) not in self.balances and v != (0, 0)
        ]
        if diffs:
            raise _v(
                "balances differ from what the successful calls imply: " + "; ".join(diffs[:10])
            )
        _check_rows(rows)
        if audit:
            self.audit.feed(await self.ledger.postings(), self._terms)
            self.audit.compare(rows)
            for job in sorted(self.touched):
                want = self.holds[job].applied if job in self.holds else []
                got = self.audit.job_ops.get(job, [])
                if got != want:
                    raise _v(
                        f"job {job}: the ledger recorded events {[o.value for o in got]}; the "
                        f"successful calls were {[o.value for o in want]} (one event per transition)"
                    )
            for op in (Op.DEPOSIT, Op.WITHDRAW):
                if self.audit.move_events[op] != self.base_events[op] + self.move_count[op]:
                    raise _v(
                        f"{self.audit.move_events[op] - self.base_events[op]} {op.value} events were "
                        f"recorded for {self.move_count[op]} successful {op.value} calls"
                    )
            self.touched.clear()

    def _absorb(
        self,
        sim: _Sim,
        key: str,
        applied: list[tuple[_Call, str]],
        refused: list[_Call],
        ok: set[int],
    ) -> None:
        self.refs |= sim.taken
        applied.extend((c, key) for c in sim.applied)
        refused.extend(sim.refused)
        ok.update(id(c) for c in sim.applied)

    def _terms(self, job: str) -> _Terms | None:
        h = self.holds.get(job)
        return _Terms(h.buyer, h.seller, h.amount) if h is not None else self.pre_terms.get(job)

    # ------------------------------------------------------------------ generating a batch

    def plan(self, rng: random.Random, i: int) -> list[_Chain]:
        def y() -> int:
            return rng.randint(0, 3)

        chains = [self._new_job(rng, f"{self.tag}-j{i}-{n}", y) for n in range(rng.randint(1, 4))]
        if self.open:  # carry open holds forward; more of them when many are open
            k = min(len(self.open), rng.randint(0, 2) + len(self.open) // 6)
            for job in rng.sample(self.open, k):
                hold = self.holds[job]
                steps = [
                    self._step(
                        rng,
                        job,
                        hold.amount.amount_minor,
                        y,
                        disputed=hold.state is HoldState.DISPUTED,
                    )
                    for _ in range(rng.randint(1, 2))
                ]
                chains.append(_Chain(job, True, steps))
        chains += [self._move(rng, f"{self.tag}-m{i}-{n}", y) for n in range(rng.randint(0, 3))]
        if rng.random() < 0.15:
            chains.append(self._malformed(rng, f"{self.tag}-x{i}", y))
        rng.shuffle(chains)
        return chains

    def _new_job(self, rng: random.Random, job: str, y: Callable[[], int]) -> _Chain:
        buyer = rng.choice(self.pool)
        seller = buyer if rng.random() < 0.15 else rng.choice(self.pool)
        cur = rng.choice(self.settled)
        r = rng.random()
        if r < 0.04:
            minor = 0
        elif r < 0.22:  # at the edge of what the buyer has
            minor = max(0, self.avail(buyer, cur) + rng.randint(-25, 25))
        else:
            minor = rng.randint(1, 1_500)
        lock = _Call(
            Op.LOCK,
            job,
            y(),
            party=buyer,
            seller=seller,
            amount=Money(minor, cur),
            expires_at=self.expires,
        )
        first = [lock]
        if rng.random() < 0.3:  # a retried hire: same terms, a later deadline
            first.append(replace(lock, yields=y(), expires_at=self.expires + timedelta(seconds=1)))
        if rng.random() < 0.12:  # the same job id with other terms
            r = rng.random()
            if r < 0.4:
                first.append(replace(lock, yields=y(), amount=Money(minor + 1, cur)))
            elif r < 0.8 or len(self.settled) == 1:
                first.append(
                    replace(
                        lock, yields=y(), seller=rng.choice([p for p in self.pool if p != seller])
                    )
                )
            else:  # the same number in another currency: a different hold
                first.append(replace(lock, yields=y(), amount=Money(minor, self._other(rng, cur))))
        if rng.random() < 0.08:  # a transition racing its own lock
            first.append(
                self._job_call(rng, rng.choice([Op.RELEASE, Op.REFUND, Op.DISPUTE]), job, minor, y)
            )
        return _Chain(
            job, True, [first, *(self._step(rng, job, minor, y) for _ in range(rng.randint(0, 3)))]
        )

    def _step(
        self,
        rng: random.Random,
        job: str,
        minor: int,
        y: Callable[[], int],
        *,
        disputed: bool = False,
    ) -> list[_Call]:
        ops = list(_TRANSITIONS)
        first = self._job_call(
            rng, rng.choices(ops, [1, 1, 1, 6] if disputed else [3, 2, 3, 3])[0], job, minor, y
        )
        step = [first]
        for _ in range(rng.choices([0, 1, 2], [6, 3, 1])[0]):
            if rng.random() < 0.5:  # a concurrent retry
                step.append(replace(first, yields=y()))
            else:  # a race
                step.append(self._job_call(rng, rng.choice(ops), job, minor, y))
        return step

    def _job_call(
        self, rng: random.Random, op: Op, job: str, minor: int, y: Callable[[], int]
    ) -> _Call:
        if op is not Op.RESOLVE:
            return _Call(op, job, y())
        buyer_part = rng.randint(0, minor)
        seller_part = minor - buyer_part + (1 if rng.random() < 0.15 else 0)  # sometimes off by one
        return _Call(op, job, y(), split=Split(buyer_part, seller_part))

    def _move(self, rng: random.Random, ref: str, y: Callable[[], int]) -> _Chain:
        party = rng.choice(self.pool)
        cur = rng.choice(self.settled)
        if rng.random() < 0.6:
            call = _Call(
                Op.DEPOSIT, ref, y(), party=party, amount=Money(rng.randint(200, 4_000), cur)
            )
        else:
            if rng.random() < 0.1:
                party = PLATFORM_PARTY  # the platform pays out its fees
            edge = rng.random() < 0.5
            minor = (
                max(1, self.avail(party, cur) + rng.randint(-10, 10))
                if edge
                else rng.randint(1, 1_500)
            )
            call = _Call(Op.WITHDRAW, ref, y(), party=party, amount=Money(minor, cur))
        step = [call]
        if rng.random() < 0.25:
            step.append(replace(call, yields=y()))
        if rng.random() < 0.1:  # the same ref for another movement
            r = rng.random()
            if r < 0.4:
                step.append(
                    replace(call, yields=y(), amount=Money(call.money.amount_minor + 1, cur))
                )
            elif r < 0.8 or len(self.settled) == 1:
                step.append(
                    replace(
                        call, yields=y(), op=Op.WITHDRAW if call.op is Op.DEPOSIT else Op.DEPOSIT
                    )
                )
            else:  # the same number in another currency
                other = Money(call.money.amount_minor, self._other(rng, cur))
                step.append(replace(call, yields=y(), amount=other))
        return _Chain(ref, False, [step])

    def _other(self, rng: random.Random, currency: str) -> str:
        return rng.choice([c for c in self.settled if c != currency])

    def _malformed(self, rng: random.Random, key: str, y: Callable[[], int]) -> _Chain:
        b, s, cur = rng.choice(self.pool), rng.choice(self.pool), rng.choice(self.settled)
        e = self.expires
        options = [
            _Call(
                Op.LOCK, key, y(), party=WORLD_PARTY, seller=s, amount=Money(100, cur), expires_at=e
            ),
            _Call(
                Op.LOCK,
                key,
                y(),
                party=b,
                seller=PLATFORM_PARTY,
                amount=Money(100, cur),
                expires_at=e,
            ),
            _Call(Op.LOCK, key, y(), party="", seller=s, amount=Money(100, cur), expires_at=e),
            _Call(Op.LOCK, key, y(), party=b, seller=s, amount=Money(-100, cur), expires_at=e),
            _Call(Op.DEPOSIT, key, y(), party=PLATFORM_PARTY, amount=Money(100, cur)),
            _Call(Op.DEPOSIT, key, y(), party=WORLD_PARTY, amount=Money(100, cur)),
            _Call(Op.WITHDRAW, key, y(), party=WORLD_PARTY, amount=Money(100, cur)),
            _Call(Op.DEPOSIT, key, y(), party=b, amount=Money(0, cur)),
            _Call(Op.WITHDRAW, key, y(), party=b, amount=Money(-1, cur)),
            _Call(
                Op.LOCK,
                key,
                y(),
                party=b,
                seller=s,
                amount=Money(MAX_MINOR + 1, cur),
                expires_at=e,
            ),
            _Call(Op.DEPOSIT, key, y(), party=b, amount=Money(MAX_MINOR + 1, cur)),
            _Call(Op.WITHDRAW, key, y(), party=b, amount=Money(MAX_MINOR + 1, cur)),
        ]
        for c in self.unsettled:
            options.append(
                _Call(Op.LOCK, key, y(), party=b, seller=s, amount=Money(100, c), expires_at=e)
            )
            options.append(_Call(Op.DEPOSIT, key, y(), party=b, amount=Money(100, c)))
        call = rng.choice(options)
        if call.op is not Op.LOCK:
            return _Chain(key, False, [[call]])
        return _Chain(
            key, True, [[call], [self._job_call(rng, rng.choice(list(_TRANSITIONS)), key, 100, y)]]
        )


def _copy_hold(h: _MHold | None) -> _MHold | None:
    return h.copy() if h is not None else None


def _same(m: _MMove | None) -> _MMove | None:
    return m


def _compare_hold(job: str, got: Hold | None, want: _MHold | None) -> None:
    if want is None:
        if got is not None:
            raise _v(f"get_hold({job}) returned {got}, but no lock of it succeeded")
        return
    if got is None:
        raise _v(f"get_hold({job}) is None, but it was locked")
    if (got.job_id, got.buyer, got.seller, got.amount, got.state) != (
        job,
        want.buyer,
        want.seller,
        want.amount,
        want.state,
    ):
        raise _v(
            f"get_hold({job}) is {got.buyer} -> {got.seller} {got.amount.display()} {got.state.value}; "
            f"the successful calls imply {want.buyer} -> {want.seller} {want.amount.display()} {want.state.value}"
        )
    if got.expires_at not in want.expires_at:
        raise _v(
            f"get_hold({job}).expires_at is {got.expires_at}; the lock that succeeded said "
            f"{' or '.join(sorted(str(e) for e in want.expires_at))}"
        )
    g = {op: (h.job_id, h.op, h.ref) for op, h in got.handles.items()}
    w = {op: (h.job_id, h.op, h.ref) for op, h in want.handles.items()}
    if g != w:
        raise _v(f"get_hold({job}).handles is {g}; the calls returned {w}")


async def check_interleavings(
    ledger: Ledger,
    *,
    interleavings: int = 10_000,
    seed: int = 0,
    fee_bps: int = 500,
    currencies: Sequence[str] = ("USD",),
    audit_every: int = 1,
) -> Counter[str]:
    """Pass 2 alone (see the module docstring). Returns how often each ``op:outcome``
    happened (``ok``, ``repeat``, or the error's class name), so a test can assert that
    the batches really exercise every path."""
    return await _Interleaver(
        ledger, seed=seed, fee_bps=fee_bps, currencies=currencies, audit_every=audit_every
    ).run(interleavings)
