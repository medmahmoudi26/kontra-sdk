"""Deliberately broken ledgers: each is the in-memory ledger with one bug a real adapter
could plausibly have. The contract suite must reject every one of them."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime

from kontra.ledger import (
    PLATFORM_PARTY,
    WORLD_PARTY,
    Account,
    Handle,
    HandleState,
    Hold,
    HoldState,
    InsufficientFunds,
    InvalidParty,
    InvalidSplit,
    Money,
    Op,
    Split,
)
from kontra.testing.ledger import InMemoryLedger, _HoldRow
from kontra.testing.ledger_contract import MAX_MINOR


class ForgetsFeePosting(InMemoryLedger):
    """Credits the platform's balance at release but never writes the fee leg."""

    def _release(self, job_id: str) -> Handle:
        h = super()._release(job_id)
        last = self._postings[-1]
        if last.kind == "fee" and str(last.event_id) == h.ref:
            self._postings.pop()
        return h


class NoFeeAtRelease(InMemoryLedger):
    """Pays the seller the whole amount: balanced and consistent, but the fee is lost."""

    def _release(self, job_id: str) -> Handle:
        bps, self._fee_bps = self._fee_bps, 0
        try:
            return super()._release(job_id)
        finally:
            self._fee_bps = bps


class RefundCreditsTwice(InMemoryLedger):
    """Refunds double: the extra comes from the world account, so every event still sums
    to zero and postings still equal balances."""

    def _refund(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.REFUND)
        if done is not None:
            return done
        state = self._check_transition(row, Op.REFUND)
        a = row.amount.amount_minor
        return self._advance(
            row,
            Op.REFUND,
            state,
            [
                (row.buyer, Account.HELD, -a, "refund"),
                (row.buyer, Account.AVAILABLE, 2 * a, "refund"),
                (WORLD_PARTY, Account.AVAILABLE, -a, "refund"),
            ],
        )


class ReleaseNotIdempotent(InMemoryLedger):
    """Forgets the repeat rule for release: a retried release is an IllegalTransition."""

    def _open(self, job_id: str, op: Op) -> tuple[_HoldRow, Handle | None]:
        row, done = super()._open(job_id, op)
        return row, None if op is Op.RELEASE else done


class RepeatGetsNewRef(InMemoryLedger):
    """A repeated deposit moves nothing but answers with a fresh ref."""

    def _move(self, op: Op, party: str, amount: Money, ref: str) -> Handle:
        before = self._next_event
        h = super()._move(op, party, amount, ref)
        return replace(h, ref=f"{h.ref}-again") if self._next_event == before else h


class DepositNotIdempotent(InMemoryLedger):
    """Ignores the deposit ref: every retry credits again."""

    def _move(self, op: Op, party: str, amount: Money, ref: str) -> Handle:
        if op is Op.DEPOSIT:
            self._moves.pop(ref, None)
        return super()._move(op, party, amount, ref)


class RacyLock(InMemoryLedger):
    """Reads the buyer's balance, awaits (as a database round trip would), then debits on
    that stale read: two concurrent locks can both pass the check."""

    _funded: bool | None = None

    async def lock(
        self, job_id: str, buyer: str, seller: str, amount: Money, *, expires_at: datetime
    ) -> Handle:
        acct = self._accounts.get((buyer, amount.currency))
        funded = (acct.available if acct else 0) >= amount.amount_minor
        await asyncio.sleep(0)
        async with self._lock:
            self._funded = funded
            try:
                return self._lock_hold(job_id, buyer, seller, amount, expires_at)
            finally:
                self._funded = None

    def _check_funds(self, party: str, amount: Money) -> None:
        if self._funded is None:
            return super()._check_funds(party, amount)
        if not self._funded:
            raise InsufficientFunds(f"{party} cannot afford {amount.display()}")
        return None


class RacyTransitions(InMemoryLedger):
    """Reads the hold's state, awaits, then transitions on that stale read: a concurrent
    release and refund can both succeed."""

    _stale: HoldState | None = None

    async def _racy(self, job_id: str, op: Op) -> Handle:
        row = self._holds.get(job_id)
        stale = row.state if row is not None else None
        await asyncio.sleep(0)
        async with self._lock:
            self._stale = stale
            return self._release(job_id) if op is Op.RELEASE else self._refund(job_id)

    async def release(self, job_id: str) -> Handle:
        return await self._racy(job_id, Op.RELEASE)

    async def refund(self, job_id: str) -> Handle:
        return await self._racy(job_id, Op.REFUND)

    def _check_transition(self, row: _HoldRow, op: Op) -> HoldState:
        if op in (Op.RELEASE, Op.REFUND) and self._stale is HoldState.HELD:
            return HoldState.RELEASED if op is Op.RELEASE else HoldState.REFUNDED
        return super()._check_transition(row, op)


class RefundAfterDispute(InMemoryLedger):
    """Lets a disputed hold be refunded, bypassing the resolver."""

    def _check_transition(self, row: _HoldRow, op: Op) -> HoldState:
        if op is Op.REFUND and row.state is HoldState.DISPUTED:
            return HoldState.REFUNDED
        return super()._check_transition(row, op)


class PaysStranger(InMemoryLedger):
    """Pays a resolved seller's share to another party."""

    def _resolve(self, job_id: str, split: Split) -> Handle:
        row = self._holds.get(job_id)
        if row is None or Op.RESOLVE in row.handles:
            return super()._resolve(job_id, split)
        seller, row.seller = row.seller, f"{row.seller}-x"
        try:
            return super()._resolve(job_id, split)
        finally:
            row.seller = seller


class LeakyDispute(InMemoryLedger):
    """Hands the held amount back to the buyer at dispute time."""

    def _dispute(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.DISPUTE)
        if done is not None:
            return done
        state = self._check_transition(row, Op.DISPUTE)
        a = row.amount.amount_minor
        return self._advance(
            row,
            Op.DISPUTE,
            state,
            [
                (row.buyer, Account.HELD, -a, "dispute"),
                (row.buyer, Account.AVAILABLE, a, "dispute"),
            ],
        )


class CurrencyBlindFunds(InMemoryLedger):
    """Checks funds across all of a party's currencies, so USD pays for a USDC lock."""

    def _check_funds(self, party: str, amount: Money) -> None:
        total = sum(a.available for (p, _), a in self._accounts.items() if p == party)
        if total < amount.amount_minor:
            raise InsufficientFunds(f"{party} cannot afford {amount.display()}")


class SplitCheckedFirst(InMemoryLedger):
    """Checks the split before the state: refusals in the wrong order."""

    def _resolve(self, job_id: str, split: Split) -> Handle:
        row = self._holds.get(job_id)
        if (
            row is not None
            and Op.RESOLVE not in row.handles
            and split.buyer_minor + split.seller_minor != row.amount.amount_minor
        ):
            raise InvalidSplit("split checked before the state")
        return super()._resolve(job_id, split)


class LockIgnoresTerms(InMemoryLedger):
    """Answers any repeat of a job id with the original handle, whatever the terms."""

    def _lock_hold(
        self, job_id: str, buyer: str, seller: str, amount: Money, expires_at: datetime
    ) -> Handle:
        row = self._holds.get(job_id)
        if row is not None:
            return row.handles[Op.LOCK]
        return super()._lock_hold(job_id, buyer, seller, amount, expires_at)


class RefusesSelfHire(InMemoryLedger):
    """Refuses buyer == seller, which the contract allows."""

    def _lock_hold(
        self, job_id: str, buyer: str, seller: str, amount: Money, expires_at: datetime
    ) -> Handle:
        if buyer == seller:
            raise InvalidParty("self-hire")
        return super()._lock_hold(job_id, buyer, seller, amount, expires_at)


class PlatformCanBuy(InMemoryLedger):
    """Lets the platform account be a buyer, so its credits are no longer all fees."""

    @staticmethod
    def _check_party(party: str, role: str, *, platform_ok: bool = False) -> None:
        InMemoryLedger._check_party(party, role, platform_ok=platform_ok or party == PLATFORM_PARTY)


class LockRepeatIgnoresCurrency(InMemoryLedger):
    """Compares a repeated lock's amount by number only (``amount_minor`` without its
    currency column): a lock of 500 USD answers with the handle of a 500 USDC hold, so the
    gate believes dollars are held while the seller will be paid in USDC."""

    def _lock_hold(
        self, job_id: str, buyer: str, seller: str, amount: Money, expires_at: datetime
    ) -> Handle:
        row = self._holds.get(job_id)
        if row is not None and (row.buyer, row.seller, row.amount.amount_minor) == (
            buyer,
            seller,
            amount.amount_minor,
        ):
            self._check_currency(amount.currency)
            return row.handles[Op.LOCK]
        return super()._lock_hold(job_id, buyer, seller, amount, expires_at)


class RefIgnoresCurrency(InMemoryLedger):
    """Compares a reused deposit / withdrawal ref by number only: a top-up of 500 USD is
    swallowed as a repeat of an earlier 500 USDC one."""

    def _move(self, op: Op, party: str, amount: Money, ref: str) -> Handle:
        done = self._moves.get(ref)
        if done is not None and (done.op, done.party, done.amount.amount_minor) == (
            op,
            party,
            amount.amount_minor,
        ):
            self._check_currency(amount.currency)
            return done.handle
        return super()._move(op, party, amount, ref)


class AcceptsAnyAmount(InMemoryLedger):
    """No upper bound on amounts: a seller's quote above §9.1's bigint is refused for funds
    here and crashes a Postgres driver there, instead of ``InvalidAmount`` everywhere."""

    def _check_amount(self, amount: Money, what: str, *, positive: bool) -> None:
        if amount.amount_minor <= MAX_MINOR:
            super()._check_amount(amount, what, positive=positive)


class DisputeRecordsNothing(InMemoryLedger):
    """Writes no event when no money moves, so a dispute is missing from the audit trail
    (an adapter that skips the insert for an empty set of legs)."""

    def _dispute(self, job_id: str) -> Handle:
        row, done = self._open(job_id, Op.DISPUTE)
        if done is not None:
            return done
        state = self._check_transition(row, Op.DISPUTE)
        handle = Handle(job_id, Op.DISPUTE, HandleState.CONFIRMED, f"dispute-{job_id}")
        row.state, row.handles[Op.DISPUTE] = state, handle
        return handle


class RefusedLockLeavesHold(InMemoryLedger):
    """Inserts the hold row before the funds check and never rolls it back (no
    transaction): a lock refused for funds leaves a hold behind."""

    def __init__(self) -> None:
        super().__init__()
        self._dangling: dict[str, Hold] = {}

    def _lock_hold(
        self, job_id: str, buyer: str, seller: str, amount: Money, expires_at: datetime
    ) -> Handle:
        try:
            return super()._lock_hold(job_id, buyer, seller, amount, expires_at)
        except InsufficientFunds:
            self._dangling.setdefault(
                job_id,
                Hold(job_id, buyer, seller, amount, HoldState.HELD, self._clock(), expires_at),
            )
            raise

    async def get_hold(self, job_id: str) -> Hold | None:
        hold = await super().get_hold(job_id)
        return hold if hold is not None else self._dangling.get(job_id)


class RefIsTheJobId(InMemoryLedger):
    """Answers every call of a job with the job id as its ref, so a retried call and a
    later transition cannot be told apart."""

    def _record(
        self,
        job_id: str | None,
        key: str,
        op: Op,
        currency: str,
        legs: list[tuple[str, Account, int, str]],
        at: datetime,
        *,
        actor: str = PLATFORM_PARTY,
        ext_ref: str | None = None,
    ) -> Handle:
        handle = super()._record(job_id, key, op, currency, legs, at, actor=actor, ext_ref=ext_ref)
        return replace(handle, ref=key)


BROKEN: list[type[InMemoryLedger]] = [
    ForgetsFeePosting,
    NoFeeAtRelease,
    RefundCreditsTwice,
    ReleaseNotIdempotent,
    RepeatGetsNewRef,
    DepositNotIdempotent,
    RacyLock,
    RacyTransitions,
    RefundAfterDispute,
    PaysStranger,
    LeakyDispute,
    CurrencyBlindFunds,
    SplitCheckedFirst,
    LockIgnoresTerms,
    RefusesSelfHire,
    PlatformCanBuy,
    LockRepeatIgnoresCurrency,
    RefIgnoresCurrency,
    AcceptsAnyAmount,
    DisputeRecordsNothing,
    RefusedLockLeavesHold,
    RefIsTheJobId,
]
