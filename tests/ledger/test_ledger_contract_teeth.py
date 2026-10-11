"""The suite has teeth: every deliberately broken ledger in ``broken_ledgers`` fails it,
both the whole suite and the random interleavings on their own."""

from __future__ import annotations

import pytest
from broken_ledgers import BROKEN, RacyLock, RacyTransitions

from kontra.ledger import Ledger
from kontra.testing.ledger import InMemoryLedger
from kontra.testing.ledger_contract import (
    LedgerContractViolation,
    check_interleavings,
    check_rules,
    run_ledger_contract,
)

BOTH = ("USD", "USDC")


@pytest.mark.parametrize("broken", BROKEN, ids=lambda c: c.__name__)
async def test_the_suite_rejects(broken: type[InMemoryLedger]) -> None:
    async def make() -> Ledger:
        return broken()

    with pytest.raises(LedgerContractViolation):
        await run_ledger_contract(make, interleavings=1_000, currencies=BOTH)


@pytest.mark.parametrize("broken", BROKEN, ids=lambda c: c.__name__)
async def test_random_interleavings_alone_reject(broken: type[InMemoryLedger]) -> None:
    with pytest.raises(LedgerContractViolation, match=r"violated in interleaving \d+ \(seed=0\)"):
        await check_interleavings(broken(), interleavings=3_000, currencies=BOTH)


@pytest.mark.parametrize(
    ("racy", "complaint"),
    [(RacyLock, "negative USD balance"), (RacyTransitions, "IllegalTransition")],
)
async def test_races_only_show_under_concurrency(
    racy: type[InMemoryLedger], complaint: str
) -> None:
    """A check-then-act race passes every sequential rule; only the interleavings catch it."""
    await check_rules(racy(), currencies=BOTH)
    with pytest.raises(LedgerContractViolation, match=complaint):
        await check_interleavings(racy(), interleavings=3_000, currencies=BOTH)
