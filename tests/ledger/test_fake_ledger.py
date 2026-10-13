"""The in-memory ledger, rule by rule. The contract suite proves the same rules for any
adapter; these pin the reference adapter's exact legs, handles and refusals."""

from __future__ import annotations

import asyncio
import itertools
import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

import pytest

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
    next_state,
)
from kontra.testing import fake_ledger
from kontra.testing.ledger import InMemoryLedger
from kontra.testing.ledger_contract import MAX_MINOR, assert_invariants

AV, HELD = Account.AVAILABLE, Account.HELD
T0 = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)
EXP = T0 + timedelta(hours=1)
OK = HandleState.CONFIRMED


def usd(minor: int) -> Money:
    return Money(minor, "USD")


def fixed_clock() -> datetime:
    return T0


async def funded(minor: int = 10_000, party: str = "buyer", **kw: object) -> InMemoryLedger:
    led = InMemoryLedger(**kw)  # type: ignore[arg-type]
    await led.deposit(party, usd(minor), ref=f"fund-{party}")
    return led


async def bal(led: Ledger, party: str, currency: str = "USD") -> tuple[int, int]:
    b = await led.balance(party, currency)
    return b.available_minor, b.held_minor


def legs(postings: list[Posting], ref: str) -> list[tuple[str, str, Account, int]]:
    return [
        (p.kind, p.party_id, p.account, p.amount_minor) for p in postings if str(p.event_id) == ref
    ]


async def lock(
    led: Ledger, job: str = "job", amount: int = 1_000, buyer: str = "buyer", seller: str = "seller"
) -> Handle:
    return await led.lock(job, buyer, seller, usd(amount), expires_at=EXP)


# --------------------------------------------------------------------------- construction


def test_fake_ledger_is_a_ledger() -> None:
    led: Ledger = fake_ledger()
    assert isinstance(led, Ledger)
    assert isinstance(led, InMemoryLedger)
    assert led.fee_bps == 500
    assert led.currencies == frozenset(CURRENCIES)
    assert fake_ledger(250, currencies=["USD"]).fee_bps == 250


@pytest.mark.parametrize(
    "kwargs", [{"fee_bps": -1}, {"fee_bps": 10_001}, {"currencies": []}, {"currencies": ["EUR"]}]
)
def test_bad_configuration_is_refused(kwargs: dict[str, object]) -> None:
    with pytest.raises(InvalidAmount):
        InMemoryLedger(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize("fee_bps", [2.5, 500.0, "500", True, None])
def test_the_fee_rate_must_be_an_int(fee_bps: object) -> None:
    """A float rate passed the range check and put floats into every fee posting."""
    with pytest.raises(InvalidAmount, match="fee_bps must be an int"):
        InMemoryLedger(fee_bps)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- money in and out


async def test_deposit_credits_once_and_records_one_balanced_event() -> None:
    led = fake_ledger(clock=fixed_clock)
    h = await led.deposit("alice", usd(2_500), ref="stripe-1")
    assert h == Handle("stripe-1", Op.DEPOSIT, OK, "1")
    assert await bal(led, "alice") == (2_500, 0)
    assert await led.balance(WORLD_PARTY, "USD") == Balance(WORLD_PARTY, "USD", -2_500, 0)
    assert [
        (p.event_id, p.job_id, p.kind, p.party_id, p.account, p.amount_minor, p.at, p.actor, p.ref)
        for p in await led.postings()
    ] == [
        (1, None, "deposit", WORLD_PARTY, AV, -2_500, T0, "alice", "stripe-1"),
        (1, None, "deposit", "alice", AV, 2_500, T0, "alice", "stripe-1"),
    ]
    assert await led.deposit("alice", usd(2_500), ref="stripe-1") == h
    assert await bal(led, "alice") == (2_500, 0)
    assert len(await led.postings()) == 2


@pytest.mark.parametrize(
    ("op", "party", "amount"),
    [
        (Op.DEPOSIT, "alice", usd(2_501)),
        (Op.DEPOSIT, "bob", usd(2_500)),
        (Op.WITHDRAW, "alice", usd(2_500)),
        (Op.DEPOSIT, "alice", Money(2_500, "USDC")),  # the same number is not the same money
    ],
)
async def test_a_ref_names_one_movement(op: Op, party: str, amount: Money) -> None:
    led = fake_ledger()
    await led.deposit("alice", usd(2_500), ref="r")
    call = led.deposit if op is Op.DEPOSIT else led.withdraw
    with pytest.raises(HoldConflict, match="ref 'r' already names a deposit"):
        await call(party, amount, ref="r")
    assert await bal(led, "alice") == (2_500, 0)
    assert await bal(led, "alice", "USDC") == (0, 0)


async def test_a_postings_ref_is_the_callers_ref_even_when_empty() -> None:
    """It fell back to the event id for a falsy ref, so the audit trail lost the key."""
    led = fake_ledger()
    h = await led.deposit("alice", usd(100), ref="")
    assert h.job_id == ""
    assert [p.ref for p in await led.postings()] == ["", ""]


async def test_amounts_must_fit_a_bigint() -> None:
    """§9.1 stores amounts as bigint, and a price is the seller's to choose: anything
    larger is InvalidAmount -- before funds are looked at -- not a database error."""
    led = fake_ledger()
    with pytest.raises(InvalidAmount, match="does not fit a 64-bit amount"):
        await lock(led, amount=MAX_MINOR + 1)  # unfunded: the amount is refused first
    with pytest.raises(InvalidAmount, match="does not fit a 64-bit amount"):
        await led.deposit("buyer", usd(MAX_MINOR + 1), ref="d")
    with pytest.raises(InvalidAmount, match="does not fit a 64-bit amount"):
        await led.withdraw("buyer", usd(MAX_MINOR + 1), ref="w")
    assert await led.postings() == []
    await led.deposit("buyer", usd(MAX_MINOR), ref="d")
    await lock(led, amount=MAX_MINOR)
    assert await bal(led, "buyer") == (0, MAX_MINOR)


async def test_withdraw_needs_available_funds_and_is_idempotent() -> None:
    led = await funded(1_000)
    with pytest.raises(
        InsufficientFunds, match=re.escape("has 10.00 USD available, needs 10.01 USD")
    ):
        await led.withdraw("buyer", usd(1_001), ref="payout-1")
    assert await bal(led, "buyer") == (1_000, 0)
    # The refused ref was not used up.
    h = await led.withdraw("buyer", usd(400), ref="payout-1")
    assert h == Handle("payout-1", Op.WITHDRAW, OK, "2")
    assert legs(await led.postings(), h.ref) == [
        ("withdraw", WORLD_PARTY, AV, 400),
        ("withdraw", "buyer", AV, -400),
    ]
    assert await led.withdraw("buyer", usd(400), ref="payout-1") == h
    assert await bal(led, "buyer") == (600, 0)
    assert await bal(led, WORLD_PARTY) == (-600, 0)


async def test_held_money_cannot_be_withdrawn() -> None:
    led = await funded(1_000)
    await lock(led, amount=800)
    with pytest.raises(InsufficientFunds):
        await led.withdraw("buyer", usd(201), ref="w")
    await led.withdraw("buyer", usd(200), ref="w")


async def test_the_platform_withdraws_its_fees() -> None:
    led = await funded()
    await lock(led, amount=2_000)
    await led.release("job")
    assert await bal(led, PLATFORM_PARTY) == (100, 0)
    await led.withdraw(PLATFORM_PARTY, usd(100), ref="fees")
    assert await bal(led, PLATFORM_PARTY) == (0, 0)


_Call = Callable[[InMemoryLedger], Awaitable[Handle]]

_RESERVED: list[_Call] = [
    lambda led: led.deposit(WORLD_PARTY, usd(1), ref="r"),
    lambda led: led.deposit(PLATFORM_PARTY, usd(1), ref="r"),
    lambda led: led.deposit("", usd(1), ref="r"),
    lambda led: led.withdraw(WORLD_PARTY, usd(1), ref="r"),
    lambda led: led.withdraw("", usd(1), ref="r"),
    lambda led: lock(led, buyer=WORLD_PARTY),
    lambda led: lock(led, buyer=PLATFORM_PARTY),
    lambda led: lock(led, buyer=""),
    lambda led: lock(led, seller=WORLD_PARTY),
    lambda led: lock(led, seller=PLATFORM_PARTY),
    lambda led: lock(led, seller=""),
]


@pytest.mark.parametrize("call", _RESERVED)
async def test_reserved_and_empty_parties_are_refused(call: _Call) -> None:
    led = await funded()
    before = await led.postings()
    with pytest.raises(InvalidParty):
        await call(led)
    assert await led.postings() == before


_NON_POSITIVE: list[_Call] = [
    lambda led: led.deposit("buyer", usd(0), ref="r"),
    lambda led: led.deposit("buyer", usd(-1), ref="r"),
    lambda led: led.withdraw("buyer", usd(0), ref="r"),
    lambda led: led.withdraw("buyer", usd(-5), ref="r"),
    lambda led: lock(led, amount=-1),
]


@pytest.mark.parametrize(
    "call",
    _NON_POSITIVE,
    ids=["deposit-0", "deposit-neg", "withdraw-0", "withdraw-neg", "lock-neg"],
)
async def test_non_positive_movements_and_negative_holds_are_refused(call: _Call) -> None:
    led = await funded()
    with pytest.raises(InvalidAmount):
        await call(led)


async def test_a_ledger_refuses_currencies_it_does_not_settle() -> None:
    led = fake_ledger(currencies={"USD"})
    await led.deposit("buyer", usd(1_000), ref="d")
    with pytest.raises(CurrencyMismatch, match="settles USD, not USDC"):
        await led.lock("job", "buyer", "seller", Money(1, "USDC"), expires_at=EXP)
    with pytest.raises(CurrencyMismatch):
        await led.deposit("buyer", Money(1, "USDC"), ref="d2")
    with pytest.raises(CurrencyMismatch):
        await led.withdraw("buyer", Money(1, "USDC"), ref="w")
    assert await bal(led, "buyer", "USDC") == (0, 0)
    with pytest.raises(InvalidAmount):
        await led.balance("buyer", "EUR")


# --------------------------------------------------------------------------- lock


async def test_lock_moves_available_to_held_and_creates_the_hold() -> None:
    led = await funded(clock=fixed_clock)
    h = await lock(led, amount=1_200)
    assert h == Handle("job", Op.LOCK, OK, "2")
    assert await bal(led, "buyer") == (8_800, 1_200)
    assert await bal(led, "seller") == (0, 0)
    assert legs(await led.postings(), h.ref) == [
        ("lock", "buyer", AV, -1_200),
        ("lock", "buyer", HELD, 1_200),
    ]
    assert all(
        p.actor == PLATFORM_PARTY and p.ref == h.ref and p.job_id == "job"
        for p in (await led.postings())[2:]
    )
    assert await led.get_hold("job") == Hold(
        "job", "buyer", "seller", usd(1_200), HoldState.HELD, T0, EXP, {Op.LOCK: h}
    )


async def test_lock_repeat_returns_the_original_handle() -> None:
    led = await funded()
    h = await lock(led)
    # A retried hire computes a new deadline; it is not a conflict, and the first stands.
    assert (
        await led.lock("job", "buyer", "seller", usd(1_000), expires_at=EXP + timedelta(minutes=3))
        == h
    )
    assert (await led.get_hold("job")).expires_at == EXP  # type: ignore[union-attr]
    await led.release("job")
    assert await lock(led) == h  # still a no-op after the hold moved on
    assert len({p.event_id for p in await led.postings()}) == 3


@pytest.mark.parametrize(
    ("buyer", "seller", "amount"),
    [
        ("other", "seller", usd(1_000)),
        ("buyer", "other", usd(1_000)),
        ("buyer", "seller", usd(999)),
        ("buyer", "seller", Money(1_000, "USDC")),
    ],
)
async def test_lock_repeat_with_other_terms_conflicts(
    buyer: str, seller: str, amount: Money
) -> None:
    led = await funded()
    await lock(led)
    with pytest.raises(HoldConflict, match=re.escape("already held as buyer -> seller 10.00 USD")):
        await led.lock("job", buyer, seller, amount, expires_at=EXP)


async def test_lock_needs_available_funds() -> None:
    led = await funded(1_000)
    await lock(led, "a", 700)
    with pytest.raises(InsufficientFunds):
        await lock(led, "b", 301)  # 1000 in total, but 700 of it is held
    assert await led.get_hold("b") is None
    await lock(led, "b", 300)
    assert await bal(led, "buyer") == (0, 1_000)


async def test_zero_amount_jobs_go_through_escrow() -> None:
    led = fake_ledger()
    h = await lock(led, amount=0)  # no funds needed
    release = await led.release("job")
    assert legs(await led.postings(), h.ref) == [
        ("lock", "buyer", AV, 0),
        ("lock", "buyer", HELD, 0),
    ]
    assert legs(await led.postings(), release.ref) == [
        ("release", "buyer", HELD, 0),
        ("release", "seller", AV, 0),
        ("fee", PLATFORM_PARTY, AV, 0),
    ]


async def test_self_hire_is_allowed_and_costs_the_fee() -> None:
    led = await funded()
    await lock(led, amount=2_000, seller="buyer")
    assert await bal(led, "buyer") == (8_000, 2_000)
    await led.release("job")
    assert await bal(led, "buyer") == (9_900, 0)
    assert await bal(led, PLATFORM_PARTY) == (100, 0)


# --------------------------------------------------------------------------- transitions


@pytest.mark.parametrize(
    ("amount", "fee_bps", "fee"),
    [
        (19, 500, 0),
        (20, 500, 1),
        (1_999, 500, 99),
        (2_000, 500, 100),
        (333, 250, 8),
        (10_000, 0, 0),
        (10_000, 10_000, 10_000),
    ],
)
async def test_release_pays_the_seller_less_the_fee(amount: int, fee_bps: int, fee: int) -> None:
    led = await funded(fee_bps=fee_bps)
    await lock(led, amount=amount)
    h = await led.release("job")
    assert h == Handle("job", Op.RELEASE, OK, "3")
    assert legs(await led.postings(), h.ref) == [
        ("release", "buyer", HELD, -amount),
        ("release", "seller", AV, amount - fee),
        ("fee", PLATFORM_PARTY, AV, fee),
    ]
    assert await bal(led, "buyer") == (10_000 - amount, 0)
    assert await bal(led, "seller") == (amount - fee, 0)
    assert await bal(led, PLATFORM_PARTY) == (fee, 0)


async def test_refund_returns_the_amount() -> None:
    led = await funded()
    await lock(led, amount=1_500)
    h = await led.refund("job")
    assert legs(await led.postings(), h.ref) == [
        ("refund", "buyer", HELD, -1_500),
        ("refund", "buyer", AV, 1_500),
    ]
    assert await bal(led, "buyer") == (10_000, 0)
    assert (await led.get_hold("job")).state is HoldState.REFUNDED  # type: ignore[union-attr]


async def test_dispute_moves_nothing_but_is_recorded() -> None:
    led = await funded()
    await lock(led, amount=1_500)
    h = await led.dispute("job")
    assert legs(await led.postings(), h.ref) == [("dispute", "buyer", HELD, 0)]
    assert await bal(led, "buyer") == (8_500, 1_500)


async def test_resolve_splits_the_hold_and_charges_the_fee_on_the_sellers_share() -> None:
    led = await funded()
    await lock(led, amount=2_000)
    dispute = await led.dispute("job")
    with pytest.raises(InvalidSplit, match=r"1200 \+ 799 != held amount 2000"):
        await led.resolve("job", Split(1_200, 799))
    h = await led.resolve("job", Split(1_200, 800))
    assert legs(await led.postings(), h.ref) == [
        ("resolve", "buyer", HELD, -2_000),
        ("resolve", "buyer", AV, 1_200),
        ("resolve", "seller", AV, 760),
        ("fee", PLATFORM_PARTY, AV, 40),
    ]
    assert await bal(led, "buyer") == (9_200, 0)
    assert await bal(led, "seller") == (760, 0)
    assert await led.resolve("job", Split(1_200, 800)) == h
    with pytest.raises(HoldConflict, match="already resolved"):
        await led.resolve("job", Split(2_000, 0))
    assert await led.dispute("job") == dispute
    hold = await led.get_hold("job")
    assert hold is not None
    assert hold.state is HoldState.RESOLVED
    assert set(hold.handles) == {Op.LOCK, Op.DISPUTE, Op.RESOLVE}


async def _hold_in(state: HoldState) -> tuple[InMemoryLedger, dict[Op, Handle]]:
    led = await funded()
    handles = {Op.LOCK: await lock(led)}
    path = {
        HoldState.HELD: [],
        HoldState.RELEASED: [Op.RELEASE],
        HoldState.REFUNDED: [Op.REFUND],
        HoldState.DISPUTED: [Op.DISPUTE],
        HoldState.RESOLVED: [Op.DISPUTE, Op.RESOLVE],
    }[state]
    for op in path:
        handles[op] = await (
            led.resolve("job", Split(500, 500))
            if op is Op.RESOLVE
            else getattr(led, op.value)("job")
        )
    return led, handles


@pytest.mark.parametrize(
    ("state", "op"),
    list(itertools.product(HoldState, [Op.RELEASE, Op.REFUND, Op.DISPUTE, Op.RESOLVE])),
)
async def test_every_transition_follows_the_state_machine(state: HoldState, op: Op) -> None:
    led, handles = await _hold_in(state)
    before = await led.postings()
    call = (
        led.resolve("job", Split(500, 500)) if op is Op.RESOLVE else getattr(led, op.value)("job")
    )
    if op in handles:  # a repeat: the original handle, nothing recorded
        assert await call == handles[op]
        assert await led.postings() == before
        return
    try:
        target = next_state(state, op)
    except IllegalTransition:
        with pytest.raises(IllegalTransition) as err:
            await call
        assert (err.value.job_id, err.value.state, err.value.op) == ("job", state, op)
        assert await led.postings() == before
        return
    h = await call
    assert (h.job_id, h.op, h.state) == ("job", op, OK)
    assert (await led.get_hold("job")).state is target  # type: ignore[union-attr]


@pytest.mark.parametrize("op", [Op.RELEASE, Op.REFUND, Op.DISPUTE, Op.RESOLVE])
async def test_a_job_never_locked_is_an_unknown_hold(op: Op) -> None:
    led = fake_ledger()
    with pytest.raises(UnknownHold, match="no hold for job ghost"):
        await (
            led.resolve("ghost", Split(0, 0))
            if op is Op.RESOLVE
            else getattr(led, op.value)("ghost")
        )
    assert await led.get_hold("ghost") is None
    assert await led.postings() == []


async def test_refusals_come_in_a_fixed_order() -> None:
    led = await funded(1_000)
    h = await lock(led, amount=1_000)
    # arguments before the repeat rule
    with pytest.raises(InvalidParty):
        await lock(led, buyer=WORLD_PARTY)
    # the repeat rule before funds: the buyer is now broke, the repeat still answers
    assert await lock(led, amount=1_000) == h
    # unknown hold before the split
    with pytest.raises(UnknownHold):
        await led.resolve("ghost", Split(1, 1))
    # the transition before the split
    with pytest.raises(IllegalTransition):
        await led.resolve("job", Split(1, 1))
    await led.dispute("job")
    await led.resolve("job", Split(1_000, 0))
    # the repeat rule before the split
    with pytest.raises(HoldConflict):
        await led.resolve("job", Split(1, 1))


async def test_currencies_never_mix() -> None:
    led = await funded(1_000)
    with pytest.raises(InsufficientFunds):
        await led.lock("job", "buyer", "seller", Money(1, "USDC"), expires_at=EXP)
    await led.deposit("buyer", Money(5_000_000, "USDC"), ref="usdc")
    await led.lock("job", "buyer", "seller", Money(2_000_000, "USDC"), expires_at=EXP)
    await led.release("job")
    assert await bal(led, "buyer") == (1_000, 0)
    assert await bal(led, "buyer", "USDC") == (3_000_000, 0)
    assert await bal(led, "seller", "USDC") == (1_900_000, 0)
    assert await bal(led, "seller") == (0, 0)
    assert [(b.party_id, b.currency) for b in await led.balances()] == [
        ("buyer", "USD"),
        ("buyer", "USDC"),
        ("kontra", "USDC"),
        ("kontra-world", "USD"),
        ("kontra-world", "USDC"),
        ("seller", "USDC"),
    ]


# --------------------------------------------------------------------------- reads


async def test_reads_are_snapshots() -> None:
    led = await funded()
    await lock(led)
    hold = await led.get_hold("job")
    assert hold is not None
    hold.handles.clear()  # type: ignore[attr-defined]
    postings = await led.postings()
    postings.clear()
    rows = await led.balances()
    rows.clear()
    await led.release("job")
    assert hold.state is HoldState.HELD
    again = await led.get_hold("job")
    assert again is not None
    assert set(again.handles) == {Op.LOCK, Op.RELEASE}
    assert len(await led.postings()) == 7
    assert len(await led.balances()) == 4


async def test_balance_of_a_stranger_is_zero() -> None:
    assert await fake_ledger().balance("nobody", "USDC") == Balance("nobody", "USDC", 0, 0)


async def test_balances_are_the_sum_of_postings() -> None:
    led = await funded(50_000)
    await lock(led, "a", 1_000)
    await lock(led, "b", 2_000, seller="buyer")
    await lock(led, "c", 3_000)
    await led.release("a")
    await led.refund("b")
    await led.dispute("c")
    await led.resolve("c", Split(1_000, 2_000))
    await led.withdraw("seller", usd(500), ref="w")
    sums: dict[tuple[str, str, Account], int] = {}
    for p in await led.postings():
        key = (p.party_id, p.currency, p.account)
        sums[key] = sums.get(key, 0) + p.amount_minor
    for b in await led.balances():
        assert sums.get((b.party_id, b.currency, AV), 0) == b.available_minor
        assert sums.get((b.party_id, b.currency, HELD), 0) == b.held_minor
    await assert_invariants(led, fee_bps=500)


async def test_clock_stamps_postings_and_holds() -> None:
    ticks = iter(T0 + timedelta(seconds=s) for s in itertools.count())
    led = fake_ledger(clock=lambda: next(ticks))
    await led.deposit("buyer", usd(1_000), ref="d")
    await lock(led)
    await led.release("job")
    assert [p.at for p in await led.postings()] == [T0] * 2 + [T0 + timedelta(seconds=1)] * 2 + [
        T0 + timedelta(seconds=2)
    ] * 3
    assert (await led.get_hold("job")).created_at == T0 + timedelta(seconds=1)  # type: ignore[union-attr]


# --------------------------------------------------------------------------- concurrency


async def test_concurrent_locks_never_overdraw() -> None:
    led = await funded(1_000)
    results = await asyncio.gather(
        *(lock(led, f"j{i}", 100) for i in range(50)), return_exceptions=True
    )
    assert sum(isinstance(r, Handle) for r in results) == 10
    assert sum(isinstance(r, InsufficientFunds) for r in results) == 40
    assert await bal(led, "buyer") == (0, 1_000)


async def test_concurrent_retries_record_one_event() -> None:
    led = await funded()
    results = await asyncio.gather(*(lock(led) for _ in range(20)))
    assert len(set(results)) == 1
    assert len({p.event_id for p in await led.postings()}) == 2  # the funding deposit and one lock
    assert await bal(led, "buyer") == (9_000, 1_000)


async def test_concurrent_release_and_refund_one_wins() -> None:
    led = await funded()
    await lock(led)
    results = await asyncio.gather(
        led.release("job"), led.refund("job"), led.dispute("job"), return_exceptions=True
    )
    assert sum(isinstance(r, Handle) for r in results) == 1
    assert sum(isinstance(r, IllegalTransition) for r in results) == 2
    assert all(isinstance(r, Handle | LedgerError) for r in results)
    await assert_invariants(led, fee_bps=500)


async def test_handles_and_event_ids_are_unique_and_increasing() -> None:
    led = await funded()
    refs = [(await lock(led, f"j{i}", 10)).ref for i in range(5)]
    assert refs == ["2", "3", "4", "5", "6"]
    ids = [p.event_id for p in await led.postings()]
    assert ids == sorted(ids)
