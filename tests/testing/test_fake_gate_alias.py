"""The FakeGate move (decisions I34, OD2): the implementation is
:class:`kontra.devgate.gate.DevGate`, and every historical name resolves to that one class,
so no seller suite, doc import or test breaks.
"""

from __future__ import annotations

import importlib


def test_fake_gate_is_the_dev_gate() -> None:
    import kontra.devgate
    import kontra.devgate.gate
    import kontra.testing
    import kontra.testing.fake_gate

    dev_gate = kontra.devgate.gate.DevGate
    # Every path that ever named the class is the same object.
    assert kontra.devgate.DevGate is dev_gate
    assert kontra.testing.FakeGate is dev_gate
    assert kontra.testing.fake_gate.FakeGate is dev_gate
    assert kontra.testing.fake_gate.DevGate is dev_gate


def test_the_old_module_still_re_exports_the_names_tests_import() -> None:
    module = importlib.import_module("kontra.testing.fake_gate")
    for name in (
        "AGE_MAGIC",
        "AUD_API",
        "AUD_TEMPORAL",
        "AUD_HOLD",
        "HOLD_HEADER",
        "SUBJECT_DEVICE_ADDED",
        "SUBJECT_DEVICE_REMOVED",
        "SUBJECT_PASSKEY_REMOVED",
        "SUBJECT_SIGN_IN",
        "GateHTTPError",
        "MagicOutcome",
        "Principal",
        "SentMail",
        "_KeyRing",
    ):
        assert hasattr(module, name), name


def test_the_old_module_is_still_callable_like_the_factory() -> None:
    import kontra.testing.fake_gate as module

    assert isinstance(module(None), module.FakeGate)  # type: ignore[operator]


def test_platform_defaults_do_not_shift_the_class_defaults() -> None:
    from datetime import timedelta

    from kontra.devgate.gate import DevGate, platform_defaults

    # The host passes these; the class keeps its test-friendly defaults (OD15).
    defaults = platform_defaults()
    assert defaults["sweep_interval"] == 2.0
    assert defaults["grace"] == timedelta(seconds=30)
    assert defaults["signup_grant"] == "100.00"

    gate = DevGate(None)
    assert gate.sweep_interval == 0.2
    assert gate.grace == timedelta(seconds=1)
