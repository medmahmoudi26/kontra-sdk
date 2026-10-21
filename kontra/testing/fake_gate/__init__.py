"""The dev gate under its historical name, for tests, seller suites and docs.

The implementation moved to :mod:`kontra.devgate.gate` as
:class:`~kontra.devgate.gate.DevGate` (decisions I34, OD2): a shipped command
(``kontra dev``) depends on :mod:`kontra.devgate`, never on this testing package. This package
re-exports it as :class:`FakeGate`, with the names tests already import, so nothing here or in
a seller's suite changes. ``kontra.testing.FakeGate`` and ``kontra.devgate.DevGate`` are one
class.
"""

from __future__ import annotations

import sys
import types
from typing import TYPE_CHECKING, Any

from kontra.devgate.gate import (
    AGE_MAGIC,
    AUD_API,
    AUD_HOLD,
    AUD_TEMPORAL,
    HOLD_HEADER,
    SUBJECT_DEVICE_ADDED,
    SUBJECT_DEVICE_REMOVED,
    SUBJECT_PASSKEY_REMOVED,
    SUBJECT_SIGN_IN,
    DevGate,
    GateHTTPError,
    MagicOutcome,
    Principal,
    SentMail,
    _KeyRing,
)

if TYPE_CHECKING:
    from kontra._control import TemporalControl

#: The dev gate under its historical name. One class with :class:`kontra.devgate.gate.DevGate`.
FakeGate = DevGate

__all__ = [
    "AGE_MAGIC",
    "AUD_API",
    "AUD_HOLD",
    "AUD_TEMPORAL",
    "HOLD_HEADER",
    "SUBJECT_DEVICE_ADDED",
    "SUBJECT_DEVICE_REMOVED",
    "SUBJECT_PASSKEY_REMOVED",
    "SUBJECT_SIGN_IN",
    "DevGate",
    "FakeGate",
    "GateHTTPError",
    "MagicOutcome",
    "Principal",
    "SentMail",
    "_KeyRing",
]


class _CallableModule(types.ModuleType):
    """``kontra.testing.fake_gate`` is both this package and the factory
    ``kontra.testing.fake_gate(...)``: importing a submodule rebinds the package attribute
    of the same name to the module, so the module itself answers calls like the factory."""

    def __call__(self, control: TemporalControl | None = None, **options: Any) -> DevGate:
        return DevGate(control, **options)


sys.modules[__name__].__class__ = _CallableModule
