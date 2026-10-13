"""An in-memory :class:`~kontra.ledger.Ledger`: the reference adapter of seam ④.

``kontra.testing.fake_ledger()`` returns one. The fake gate runs on it, and it is the first
adapter proven against :mod:`kontra.testing.ledger_contract`. The Postgres adapter is this
file with rows instead of dicts: same events, same legs, same refusals in the same order.

Accounting (decisions I9): every call that changes anything records one *event*, a set of
:class:`~kontra.ledger.Posting` legs that sum to zero, and a balance changes only by
having a leg posted to it. So ``sum(postings) == balances`` holds by construction here,
and the contract suite checks it anyway.

========  ==========================================================================
call      legs (``A`` = the held amount, ``fee = fee_for(x, fee_bps)``)
========  ==========================================================================
deposit   world.available -A, party.available +A
withdraw  party.available -A, world.available +A
lock      buyer.available -A, buyer.held +A
release   buyer.held -A, seller.available +(A - fee(A)), platform.available +fee(A)
refund    buyer.held -A, buyer.available +A
dispute   buyer.held 0 (no money moves, but the transition is in the audit trail)
resolve   buyer.held -A, buyer.available +b, seller.available +(s - fee(s)),
          platform.available +fee(s), for ``Split(b, s)`` with ``b + s == A``
========  ==========================================================================

Zero legs are kept (a free job, a fee that floors to 0), so every transition is an event.

Choices the PRD leaves open, made here and enforced on every adapter by the suite:

* **Self-hire is allowed** (``buyer == seller``). Any party can sell and buy (§4), and a
  seller's first test under ``kontra dev`` is hiring their own offering. Nothing can be
  gained by it: money only goes back to the same party, minus the platform fee. Whether a
  self-hire counts towards reputation is the gate's business, not the ledger's.
* **Reserved accounts.** ``WORLD_PARTY`` is only the counter-account of deposits and
  withdrawals; ``PLATFORM_PARTY`` only receives fees and may withdraw them. Neither can
  be a hold's buyer or seller, so every platform credit is a fee and every job posting
  touches the hold's buyer, its seller, or the platform (§9.3) -- nobody else, by
  construction: the only party ids a job call can post to are read from the hold.
* **Repeats.** A call is keyed by ``job_id`` (``ref`` for deposits and withdrawals). The
  same key with the same arguments returns the original :class:`~kontra.ledger.Handle`,
  even after later transitions (a retried ``lock`` of a released job is still a no-op);
  the same key with different arguments is :class:`~kontra.ledger.HoldConflict`.
  ``expires_at`` is not compared: a retried hire computes a new deadline. Deposit and
  withdrawal refs share one namespace, so a ref names exactly one movement.
* **Order of refusals**: argument checks (``InvalidParty``, ``InvalidAmount``,
  ``CurrencyMismatch``), then ``UnknownHold``, then the repeat rule, then
  ``IllegalTransition``, then ``InvalidSplit``, then ``InsufficientFunds``.
* **Amounts fit §9.1's ``bigint``.** A lock, deposit or withdrawal above
  :data:`~kontra.testing.ledger_contract.MAX_MINOR` is ``InvalidAmount``: the price is the
  seller's to choose, and the Postgres adapter could not store it.
* **Currencies.** Balances are per (party, currency) and never mix. A ledger settles a set
  of currencies (default: all of :data:`~kontra.ledger.CURRENCIES`); a call in any other
  is :class:`~kontra.ledger.CurrencyMismatch`, not a silently created account.

Concurrency: each public method runs its body under one :class:`asyncio.Lock`, and the
bodies never await, so every call is atomic with respect to the other tasks on the loop.
The lock keeps that true for a subclass that adds an await (injected latency, a slow
store). It is not thread-safe.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

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
    Money,
    Op,
    Posting,
    Split,
    UnknownHold,
    fee_for,
    next_state,
)
from kontra.testing.ledger_contract import MAX_MINOR

__all__ = ["InMemoryLedger"]

#: One leg before it becomes a Posting: (party, account, signed amount, kind).
_Leg = tuple[str, Account, int, str]


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class _Account:
    available: int = 0
    held: int = 0


@dataclass(slots=True)
class _HoldRow:
    job_id: str
    buyer: str
    seller: str
    amount: Money
    state: HoldState
    created_at: datetime
    expires_at: datetime
    handles: dict[Op, Handle] = field(default_factory=dict[Op, Handle])
    split: Split | None = None


@dataclass(frozen=True, slots=True)
class _Movement:
    """A deposit or withdrawal, remembered by ``ref`` for the repeat rule."""

    op: Op
    party: str
    amount: Money
    handle: Handle


class InMemoryLedger:
    """:class:`~kontra.ledger.Ledger` in a few dicts. See the module docstring for the rules.

    ``fee_bps`` is the platform fee (default 5%, §9.5). ``currencies`` restricts what this
    ledger settles. ``clock`` stamps postings and holds (tests pin it)."""

    def __init__(
        self,
        fee_bps: int = 500,
        *,
        currencies: Iterable[str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        # An int, not 2.5 or "500": a float rate would put floats into every posting.
        if isinstance(fee_bps, bool) or not isinstance(fee_bps, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise InvalidAmount(f"fee_bps must be an int, got {fee_bps!r}")
        if not 0 <= fee_bps <= 10_000:
            raise InvalidAmount(f"fee_bps must be within 0..10000, got {fee_bps}")
        settled = tuple(CURRENCIES) if currencies is None else tuple(currencies)
        unknown = [c for c in settled if c not in CURRENCIES]
        if not settled or unknown:
            raise InvalidAmount(f"currencies must be a non-empty subset of {sorted(CURRENCIES)}")
        self._fee_bps = fee_bps
        self._settled = frozenset(settled)
        self._clock = clock or _utcnow
        self._lock = asyncio.Lock()
        self._holds: dict[str, _HoldRow] = {}
        self._moves: dict[str, _Movement] = {}
        self._accounts: dict[tuple[str, str], _Account] = {}
        self._postings: list[Posting] = []
        self._next_event = 1

    @property
    def fee_bps(self) -> int:
        return self._fee_bps

    @property
    def currencies(self) -> frozenset[str]:
        """The currencies this ledger settles."""
        return self._settled

    # ------------------------------------------------------------------ §9.2

    async def lock(
        self, job_id: str, buyer: str, seller: str, amount: Money, *, expires_at: datetime
    ) -> Handle:
        async with self._lock:
            return self._lock_hold(job_id, buyer, seller, amount, expires_at)

    async def release(self, job_id: str) -> Handle:
        async with self._lock:
            return self._release(job_id)

    async def refund(self, job_id: str) -> Handle:
        async with self._lock:
            return self._refund(job_id)

    async def dispute(self, job_id: str) -> Handle:
        async with self._lock:
            return self._dispute(job_id)

    async def resolve(self, job_id: str, split: Split) -> Handle:
        async with self._lock:
            return self._resolve(job_id, split)

    # ------------------------------------------------------------------ money in and out

    async def deposit(self, party_id: str, amount: Money, *, ref: str) -> Handle:
        async with self._lock:
            return self._move(Op.DEPOSIT, party_id, amount, ref)

    async def withdraw(self, party_id: str, amount: Money, *, ref: str) -> Handle:
        async with self._lock:
            return self._move(Op.WITHDRAW, party_id, amount, ref)

    # ------------------------------------------------------------------ reads

    async def balance(self, party_id: str, currency: str) -> Balance:
        """Zeros for a party or currency with no postings (a read never creates an
        account); only a currency outside :data:`~kontra.ledger.CURRENCIES` is refused."""
        if currency not in CURRENCIES:
            raise InvalidAmount(f"unsupported currency {currency!r}")
        async with self._lock:
            acct = self._accounts.get((party_id, currency))
            if acct is None:
                return Balance(party_id, currency, 0, 0)
            return Balance(party_id, currency, acct.available, acct.held)

    async def get_hold(self, job_id: str) -> Hold | None:
        async with self._lock:
            row = self._holds.get(job_id)
            if row is None:
                return None
            # A copy of the handles: a caller mutating the snapshot cannot reach the ledger.
            return Hold(
                job_id=row.job_id,
                buyer=row.buyer,
                seller=row.seller,
                amount=row.amount,
                state=row.state,
                created_at=row.created_at,
                expires_at=row.expires_at,
                handles=dict(row.handles),
            )

    async def postings(self) -> list[Posting]:
        async with self._lock:
            return list(self._postings)

    async def balances(self) -> list[Balance]:
        async with self._lock:
            return [
                Balance(party, currency, acct.available, acct.held)
                for (party, currency), acct in sorted(self._accounts.items())
            ]

    # ------------------------------------------------------------------ checks

    @staticmethod
    def _check_party(party: str, role: str, *, platform_ok: bool = False) -> None:
        if not party:
            raise InvalidParty(f"{role} must be a party id")
        if party == WORLD_PARTY or (party == PLATFORM_PARTY and not platform_ok):
            raise InvalidParty(f"{role} {party!r} is a reserved account")

    def _check_amount(self, amount: Money, what: str, *, positive: bool) -> None:
        a = amount.amount_minor
        if a < 0 or (positive and a == 0):
            raise InvalidAmount(
                f"a {what} must be positive, got {a}"
                if positive
                else f"cannot hold a negative amount ({a})"
            )
        if a > MAX_MINOR:
            raise InvalidAmount(f"a {what} of {a} does not fit a 64-bit amount")

    def _check_currency(self, currency: str) -> None:
        if currency not in self._settled:
            raise CurrencyMismatch(
                f"this ledger settles {', '.join(sorted(self._settled))}, not {currency}"
            )

    def _check_funds(self, party: str, amount: Money) -> None:
        acct = self._accounts.get((party, amount.currency))
        available = acct.available if acct else 0
        if available < amount.amount_minor:
            raise InsufficientFunds(
                f"{party} has {Money(available, amount.currency).display()} available, "
                f"needs {amount.display()}"
            )

    def _open(self, job_id: str, op: Op) -> tuple[_HoldRow, Handle | None]:
        """The hold and, for a repeat, the original handle; refuses an unknown hold."""
        row = self._holds.get(job_id)
        if row is None:
            raise UnknownHold(f"no hold for job {job_id}")
        return row, row.handles.get(op)

    def _check_transition(self, row: _HoldRow, op: Op) -> HoldState:
        try:
            return next_state(row.state, op)
        except IllegalTransition:
            raise IllegalTransition(row.job_id, row.state, op) from None

    # ------------------------------------------------------------------ the calls

    def _lock_hold(
        self, job_id: str, buyer: str, seller: str, amount: Money, expires_at: datetime
    ) -> Handle:
        self._check_party(buyer, "buyer")
        self._check_party(seller, "seller")
        self._check_amount(amount, "lock", positive=False)
        self._check_currency(amount.currency)
        row = self._holds.get(job_id)
        if row is not None:
            if (row.buyer, row.seller, row.amount) != (buyer, seller, amount):
                raise HoldConflict(
                    f"job {job_id} is already held as {row.buyer} -> {row.seller} "
                    f"{row.amount.display()}"
                )
            return row.handles[Op.LOCK]
        self._check_funds(buyer, amount)
        a = amount.amount_minor
        at = self._clock()
        handle = self._record(
            job_id,
            job_id,
            Op.LOCK,
            amount.currency,
            [(buyer, Account.AVAILABLE, -a, "lock"), (buyer, Account.HELD, a, "lock")],
            at,
        )
        self._holds[job_id] = _HoldRow(
            job_id, buyer, seller, amount, HoldState.HELD, at, expires_at, {Op.LOCK: handle}
        )
        return handle

    def _release(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.RELEASE)
        if done is not None:
            return done
        state = self._check_transition(row, Op.RELEASE)
        a = row.amount.amount_minor
        fee = fee_for(a, self._fee_bps)
        legs: list[_Leg] = [
            (row.buyer, Account.HELD, -a, "release"),
            (row.seller, Account.AVAILABLE, a - fee, "release"),
            (PLATFORM_PARTY, Account.AVAILABLE, fee, "fee"),
        ]
        return self._advance(row, Op.RELEASE, state, legs)

    def _refund(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.REFUND)
        if done is not None:
            return done
        state = self._check_transition(row, Op.REFUND)
        a = row.amount.amount_minor
        legs: list[_Leg] = [
            (row.buyer, Account.HELD, -a, "refund"),
            (row.buyer, Account.AVAILABLE, a, "refund"),
        ]
        return self._advance(row, Op.REFUND, state, legs)

    def _dispute(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.DISPUTE)
        if done is not None:
            return done
        state = self._check_transition(row, Op.DISPUTE)
        return self._advance(row, Op.DISPUTE, state, [(row.buyer, Account.HELD, 0, "dispute")])

    def _resolve(self, job_id: str, split: Split) -> Handle:
        row, done = self._open(job_id, Op.RESOLVE)
        if done is not None:
            if row.split != split:
                raise HoldConflict(f"job {job_id} was already resolved as {row.split}")
            return done
        state = self._check_transition(row, Op.RESOLVE)
        a = row.amount.amount_minor
        if split.buyer_minor + split.seller_minor != a:
            raise InvalidSplit(
                f"split {split.buyer_minor} + {split.seller_minor} != held amount {a}"
            )
        fee = fee_for(split.seller_minor, self._fee_bps)
        legs: list[_Leg] = [
            (row.buyer, Account.HELD, -a, "resolve"),
            (row.buyer, Account.AVAILABLE, split.buyer_minor, "resolve"),
            (row.seller, Account.AVAILABLE, split.seller_minor - fee, "resolve"),
            (PLATFORM_PARTY, Account.AVAILABLE, fee, "fee"),
        ]
        row.split = split
        return self._advance(row, Op.RESOLVE, state, legs)

    def _move(self, op: Op, party: str, amount: Money, ref: str) -> Handle:
        # The platform may withdraw its fees; nothing ever deposits into it.
        self._check_party(party, op.value, platform_ok=op is Op.WITHDRAW)
        self._check_amount(amount, op.value, positive=True)
        self._check_currency(amount.currency)
        done = self._moves.get(ref)
        if done is not None:
            if (done.op, done.party, done.amount) != (op, party, amount):
                raise HoldConflict(
                    f"ref {ref!r} already names a {done.op.value} of {done.amount.display()} "
                    f"for {done.party}"
                )
            return done.handle
        a = amount.amount_minor
        if op is Op.WITHDRAW:
            self._check_funds(party, amount)
            a = -a
        legs: list[_Leg] = [
            (WORLD_PARTY, Account.AVAILABLE, -a, op.value),
            (party, Account.AVAILABLE, a, op.value),
        ]
        handle = self._record(
            None, ref, op, amount.currency, legs, self._clock(), actor=party, ext_ref=ref
        )
        self._moves[ref] = _Movement(op, party, amount, handle)
        return handle

    # ------------------------------------------------------------------ recording

    def _advance(self, row: _HoldRow, op: Op, state: HoldState, legs: list[_Leg]) -> Handle:
        handle = self._record(row.job_id, row.job_id, op, row.amount.currency, legs, self._clock())
        row.state = state
        row.handles[op] = handle
        return handle

    def _record(
        self,
        job_id: str | None,
        key: str,
        op: Op,
        currency: str,
        legs: list[_Leg],
        at: datetime,
        *,
        actor: str = PLATFORM_PARTY,
        ext_ref: str | None = None,
    ) -> Handle:
        """Post one event and apply it to the balances: the only place either changes.

        ``actor`` is the platform for job calls (the escrow rules make every one of them,
        decisions I6) and the party for its own deposits and withdrawals. ``Posting.ref``
        is the caller's ref for those, and the handle's ref otherwise."""
        event_id = self._next_event
        self._next_event += 1
        ref = str(event_id)
        for party, account, amount, kind in legs:
            self._postings.append(
                Posting(
                    event_id=event_id,
                    job_id=job_id,
                    kind=kind,
                    party_id=party,
                    account=account,
                    amount_minor=amount,
                    currency=currency,
                    at=at,
                    actor=actor,
                    ref=ref if ext_ref is None else ext_ref,
                )
            )
            acct = self._accounts.get((party, currency))
            if acct is None:
                acct = self._accounts[(party, currency)] = _Account()
            if account is Account.AVAILABLE:
                acct.available += amount
            else:
                acct.held += amount
        return Handle(job_id=key, op=op, state=HandleState.CONFIRMED, ref=ref)
