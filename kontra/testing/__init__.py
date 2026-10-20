"""Fakes for every seam (PRD §19.3), so a seller or buyer runs a full job lifecycle in a
unit test with no containers.

* :func:`fake_ledger` -- seam ④, an in-memory :class:`~kontra.ledger.Ledger`.
* :mod:`kontra.testing.ledger_contract` -- the §9.3 suite every ledger adapter passes.
* :func:`fake_gate` / :class:`FakeGate` -- seam ②, every gate route in memory, escrow by the
  platform's own rules (:mod:`kontra.escrow`), every job dispatched to its version's engine.
* :func:`fake_engine` / :class:`FakeEngine` -- seam ⑥, an in-memory
  :class:`~kontra.engine.Engine`: with it a FakeGate runs a whole job lifecycle with no
  Temporal at all. :mod:`kontra.testing.engine_contract` -- the suite every engine adapter
  passes (decisions I32).
* :func:`start_dev_server` / :class:`DevServer` -- a Temporal dev server with standalone
  Nexus operations (the seam ① stand-in).
* :class:`Marketplace` -- the three wired together, with logged-in :class:`TestParty` s.
* :class:`SoftAuthenticator` -- a software WebAuthn authenticator: real passkey answers for
  either gate (decisions I24); :mod:`kontra.testing.passkey_contract` -- the passkey rules
  both gates pass.
* :mod:`kontra.testing.jobkey_contract` -- the rules of devices and job-key wraps both gates
  pass (decisions I25).

The gate, the marketplace and the authenticator need the ``testing`` extra (FastAPI,
uvicorn, webauthn); they are imported on first use, so ``from kontra.testing import
fake_ledger`` needs none of it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any

from kontra.testing.ledger import InMemoryLedger

if TYPE_CHECKING:
    from collections.abc import Mapping

    from kontra._control import TemporalControl
    from kontra.engine import Engine, EngineName
    from kontra.testing.devserver import DevServer, start_dev_server
    from kontra.testing.engine import FakeEngine
    from kontra.testing.fake_gate import FakeGate
    from kontra.testing.marketplace import Marketplace, TestParty
    from kontra.testing.passkeys import AuthenticatorRefused, SoftAuthenticator

__all__ = [
    "AuthenticatorRefused",
    "DevServer",
    "FakeEngine",
    "FakeGate",
    "Marketplace",
    "SoftAuthenticator",
    "TestParty",
    "fake_engine",
    "fake_gate",
    "fake_ledger",
    "start_dev_server",
]

_LAZY = {
    "DevServer": "kontra.testing.devserver",
    "start_dev_server": "kontra.testing.devserver",
    "FakeEngine": "kontra.testing.engine",
    "FakeGate": "kontra.testing.fake_gate",
    "Marketplace": "kontra.testing.marketplace",
    "TestParty": "kontra.testing.marketplace",
    "AuthenticatorRefused": "kontra.testing.passkeys",
    "SoftAuthenticator": "kontra.testing.passkeys",
}


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module), name)


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


def fake_gate(
    control: TemporalControl | None = None,
    *,
    engines: Mapping[str, Engine] | None = None,
    **options: Any,
) -> FakeGate:
    """A :class:`~kontra.testing.fake_gate.FakeGate` on ``control`` (None: no Temporal), or
    on ``engines`` (by name: ``{"temporal": fake_engine()}`` runs jobs with no Temporal).
    ``options`` are its keyword arguments (``resolvers``, ``sweep_interval``, ``grace``...)."""
    from kontra.testing.fake_gate import FakeGate

    return FakeGate(control, engines=engines, **options)


def fake_engine(
    *, name: EngineName = "temporal", clock: Callable[[], datetime] | None = None
) -> FakeEngine:
    """A fresh :class:`~kontra.testing.FakeEngine` standing in for the engine ``name``. It
    passes the same :mod:`~kontra.testing.engine_contract` as the Temporal adapter, so a test
    on it means what it would mean on Temporal (seam ⑥)."""
    from kontra.testing.engine import FakeEngine

    return FakeEngine(name=name, clock=clock)
