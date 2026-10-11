"""The §9.3 suite against the in-memory ledger (acceptance criterion 6), and the suite's
own behaviour: reproducibility, coverage of every path, and the from-scratch audit."""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo

import pytest
from broken_ledgers import RacyLock

from kontra.ledger import Account, HoldState, Ledger, Money, Op, Posting, Split
from kontra.testing import fake_ledger, ledger_contract
from kontra.testing.ledger import InMemoryLedger, _Account
from kontra.testing.ledger_contract import (
    LedgerContractViolation,
    assert_invariants,
    check_interleavings,
    check_rules,
    run_ledger_contract,
)

BOTH = ("USD", "USDC")
EXP = datetime(2030, 1, 1, tzinfo=UTC)


async def test_fake_ledger_passes_10000_interleavings() -> None:
    """Acceptance criterion 6: 10,000 random interleavings, no invariant violated."""
    made = 0

    async def make() -> Ledger:
        nonlocal made
        made += 1
        return fake_ledger()

    started = time.perf_counter()
    await run_ledger_contract(make, interleavings=10_000, seed=0, currencies=BOTH)
    print(f"10,000 interleavings in {time.perf_counter() - started:.1f}s")
    assert made == 1  # adapters may be slow to create


async def test_a_usd_only_ledger_refuses_usdc() -> None:
    async def make() -> Ledger:
        return fake_ledger(currencies={"USD"})

    await run_ledger_contract(make, interleavings=1_000, seed=1)


@pytest.mark.parametrize("fee_bps", [0, 1, 333, 10_000])
async def test_any_fee_rate(fee_bps: int) -> None:
    async def make() -> Ledger:
        return fake_ledger(fee_bps)

    await run_ledger_contract(
        make, interleavings=300, seed=fee_bps, fee_bps=fee_bps, currencies=BOTH
    )


async def test_a_ledger_with_history() -> None:
    """The run works on a ledger that already holds data (a reused database)."""
    ledger = fake_ledger()

    async def make() -> Ledger:
        return ledger

    await run_ledger_contract(make, interleavings=300, seed=2, currencies=BOTH)
    await run_ledger_contract(make, interleavings=300, seed=2, currencies=BOTH)


async def test_audit_every_n_batches() -> None:
    async def make() -> Ledger:
        return fake_ledger()

    await run_ledger_contract(make, interleavings=1_000, seed=4, currencies=BOTH, audit_every=97)


async def test_interleavings_exercise_every_path() -> None:
    stats = await check_interleavings(fake_ledger(currencies={"USD"}), interleavings=2_000, seed=5)
    expected = [
        "lock:ok", "lock:repeat", "lock:HoldConflict", "lock:InsufficientFunds",
        "lock:InvalidParty", "lock:InvalidAmount", "lock:CurrencyMismatch",
        "release:ok", "release:repeat", "release:IllegalTransition", "release:UnknownHold",
        "refund:ok", "refund:repeat", "refund:IllegalTransition", "refund:UnknownHold",
        "dispute:ok", "dispute:repeat", "dispute:IllegalTransition", "dispute:UnknownHold",
        "resolve:ok", "resolve:repeat", "resolve:HoldConflict", "resolve:InvalidSplit",
        "resolve:IllegalTransition", "resolve:UnknownHold",
        "deposit:ok", "deposit:repeat", "deposit:HoldConflict", "deposit:InvalidParty",
        "deposit:InvalidAmount", "deposit:CurrencyMismatch",
        "withdraw:ok", "withdraw:repeat", "withdraw:HoldConflict", "withdraw:InsufficientFunds",
        "withdraw:InvalidParty", "withdraw:InvalidAmount",
    ]  # fmt: skip
    missing = [k for k in expected if stats[k] == 0]
    assert not missing, f"never exercised: {missing}; stats: {dict(stats)}"
    assert stats["lock:ok"] > 2_000
    assert stats["release:ok"] + stats["refund:ok"] + stats["resolve:ok"] > 2_000


def _untagged(text: str) -> str:
    return re.sub(r"\bt[0-9a-f]{7}\b", "<tag>", text)


async def test_a_seed_replays_exactly() -> None:
    a, b, c = fake_ledger(), fake_ledger(), fake_ledger()
    sa = await check_interleavings(a, interleavings=300, seed=3, currencies=BOTH)
    sb = await check_interleavings(b, interleavings=300, seed=3, currencies=BOTH)
    sc = await check_interleavings(c, interleavings=300, seed=4, currencies=BOTH)
    assert sa == sb
    assert sa != sc

    def shape(postings: list[Posting]) -> list[str]:
        return [
            _untagged(
                f"{p.event_id} {p.job_id} {p.kind} {p.party_id} {p.account} {p.amount_minor} {p.currency} {p.ref}"
            )
            for p in postings
        ]

    assert shape(await a.postings()) == shape(await b.postings())


async def test_a_violation_prints_the_seed_and_the_op_log() -> None:
    async def make() -> Ledger:
        return RacyLock()

    with pytest.raises(LedgerContractViolation) as first:
        await run_ledger_contract(make, interleavings=1_000, seed=0, currencies=BOTH)
    message = str(first.value)
    replay = re.search(
        r"replay: await run_ledger_contract\(make_ledger, seed=0, interleavings=(\d+)\)", message
    )
    assert replay is not None, message
    assert "interleaving" in message.splitlines()[0]
    assert "negative USD balance" in message
    assert "op log" in message
    assert re.search(r"lock\(\S+ -> \S+, \d+ USD", message)
    # The printed command reproduces the same failure, call for call.
    with pytest.raises(LedgerContractViolation) as again:
        await run_ledger_contract(make, interleavings=int(replay.group(1)), seed=0, currencies=BOTH)
    assert _untagged(str(again.value)) == _untagged(message)


class _DriftingClock:
    """Stands in for ``datetime`` in the contract module: every ``now()`` is an hour later
    than the last, as if each run and its replay were far apart."""

    def __init__(self) -> None:
        self.calls = 0

    def now(self, tz: tzinfo | None = None) -> datetime:
        self.calls += 1
        return datetime.now(tz) + timedelta(hours=self.calls, microseconds=7 * self.calls)


async def test_a_replay_prints_the_same_log_whenever_it_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The op log used to print each lock's deadline, taken from ``now()``: a replay that
    crossed a second boundary printed a different log (about 1 run in 20 of the test
    above). Nothing in an interleaving may depend on the wall clock."""
    clock = _DriftingClock()
    monkeypatch.setattr(ledger_contract, "datetime", clock)

    async def make() -> Ledger:
        return RacyLock()

    logs: list[str] = []
    for _ in range(2):
        with pytest.raises(LedgerContractViolation) as err:
            await run_ledger_contract(make, interleavings=1_000, seed=0, currencies=BOTH)
        logs.append(_untagged(str(err.value)))
    assert clock.calls >= 2  # the patch is live: check_rules still reads the clock
    assert logs[0] == logs[1]


async def test_make_ledger_must_return_a_ledger() -> None:
    async def make() -> Ledger:
        return object()  # type: ignore[return-value]

    with pytest.raises(LedgerContractViolation, match="not a Ledger"):
        await run_ledger_contract(make, interleavings=1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"audit_every": 0},
        {"currencies": ()},
        {"currencies": ("EUR",)},
        # A repeated currency used to pass as "another" one and fail the fake ledger.
        {"currencies": ("USD", "USD")},
    ],
)
async def test_bad_arguments(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):  # noqa: PT011 -- the message is the argument's
        await check_interleavings(fake_ledger(), interleavings=1, **kwargs)  # type: ignore[arg-type]
    if "currencies" in kwargs:
        with pytest.raises(ValueError):  # noqa: PT011
            await check_rules(fake_ledger(), currencies=kwargs["currencies"])  # type: ignore[arg-type]


async def test_rules_alone_pass_on_the_fake() -> None:
    await check_rules(fake_ledger(), currencies=BOTH)
    await check_rules(fake_ledger(currencies={"USDC"}), currencies=("USDC",))


# --------------------------------------------------------------------------- assert_invariants


async def _busy() -> InMemoryLedger:
    led = fake_ledger()
    await led.deposit("buyer", Money(10_000, "USD"), ref="d")
    await led.lock("a", "buyer", "seller", Money(1_000, "USD"), expires_at=EXP)
    await led.lock("b", "buyer", "seller", Money(2_000, "USD"), expires_at=EXP)
    await led.lock("c", "buyer", "seller", Money(3_000, "USD"), expires_at=EXP)
    await led.release("a")
    await led.dispute("b")
    await led.resolve("b", Split(500, 1_500))
    return led


async def test_assert_invariants_passes_on_a_sound_ledger() -> None:
    await assert_invariants(fake_ledger())
    await assert_invariants(await _busy(), fee_bps=500)


def _tamper_balance(led: InMemoryLedger) -> None:
    led._accounts[("seller", "USD")].available += 1


def _unbalanced_event(led: InMemoryLedger) -> None:
    last = led._postings[-1]
    led._postings.append(replace(last, event_id=last.event_id + 1, amount_minor=5))
    led._accounts[(last.party_id, last.currency)].available += 5


def _stranger(led: InMemoryLedger) -> None:
    last = led._postings[-1]
    eid = last.event_id + 1
    led._postings += [
        replace(
            last,
            event_id=eid,
            job_id="c",
            kind="release",
            party_id="buyer",
            account=Account.HELD,
            amount_minor=-3_000,
        ),
        replace(
            last,
            event_id=eid,
            job_id="c",
            kind="release",
            party_id="mallory",
            account=Account.AVAILABLE,
            amount_minor=3_000,
        ),
    ]
    led._accounts[("buyer", "USD")].held -= 3_000
    led._accounts[("mallory", "USD")] = _Account(3_000, 0)
    led._holds["c"].state = HoldState.RELEASED


def _state_without_event(led: InMemoryLedger) -> None:
    led._holds["c"].state = HoldState.REFUNDED


def _held_without_hold(led: InMemoryLedger) -> None:
    led._holds["c"].handles.pop(Op.LOCK)


@pytest.mark.parametrize(
    ("tamper", "complaint"),
    [
        (
            _tamper_balance,
            r"sum\(postings\) != balances: seller/USD/available: postings 2375, balance 2376",
        ),
        (_unbalanced_event, r"sums to \+5 USD, not 0"),
        (
            _stranger,
            r"touches \['mallory'\], neither its buyer buyer, its seller seller, nor the platform",
        ),
        (_state_without_event, r"hold c is refunded"),
        (_held_without_hold, r"hold c has handles for"),
    ],
    ids=lambda x: getattr(x, "__name__", ""),
)
async def test_assert_invariants_catches(
    tamper: Callable[[InMemoryLedger], None], complaint: str
) -> None:
    led = await _busy()
    tamper(led)
    with pytest.raises(LedgerContractViolation, match=complaint):
        await assert_invariants(led, fee_bps=500)


async def test_assert_invariants_checks_fees_when_told_the_rate() -> None:
    led = fake_ledger(fee_bps=0)
    await led.deposit("buyer", Money(10_000, "USD"), ref="d")
    await led.lock("a", "buyer", "seller", Money(1_000, "USD"), expires_at=EXP)
    await led.release("a")
    await assert_invariants(led)  # no rate given: any non-negative fee up to the amount
    with pytest.raises(
        LedgerContractViolation, match=r"fee of 0 on 1000; at 500 bps it is fee_for\(1000\) = 50"
    ):
        await assert_invariants(led, fee_bps=500)
