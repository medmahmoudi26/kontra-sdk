"""Fakes for every seam (PRD §19.3), so a seller or buyer runs a full job lifecycle in a
unit test with no containers.

* :func:`fake_ledger` -- seam ④, an in-memory :class:`~kontra.ledger.Ledger`.
* :mod:`kontra.testing.ledger_contract` -- the §9.3 suite every ledger adapter passes.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime

from kontra.testing.ledger import InMemoryLedger

__all__ = ["fake_ledger"]


def fake_ledger(
    fee_bps: int = 500,
    *,
    currencies: Iterable[str] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> InMemoryLedger:
    """A fresh in-memory ledger with a ``fee_bps`` platform fee (default 5%, §9.5). It
    obeys the same contract as the Postgres adapter, so switching between them changes no
    test outcome (acceptance criterion 9)."""
    return InMemoryLedger(fee_bps, currencies=currencies, clock=clock)
