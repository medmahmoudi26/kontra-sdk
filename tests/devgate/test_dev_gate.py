"""The dev gate as ``kontra dev`` ships it: its messages never send a dev user to a login verb,
and the host never imports the testing package (decisions I34, OD2)."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime, timedelta

import pytest

from kontra.devgate import gate as g
from kontra.devgate.gate import DevGate, GateHTTPError, Principal


def _api_key_session() -> Principal:
    return Principal(
        party="acme", session_id="s", auth_time=datetime.now(UTC), kind="api", api_key=True
    )


@pytest.mark.parametrize(("dev", "verb"), [(True, "`kontra dev up`"), (False, "`kontra login`")])
def test_a_sign_in_hint_names_the_command_of_its_gate(dev: bool, verb: str) -> None:
    """An API key's session on the dev gate (``require_human``), and the confirmation
    messages, say ``kontra dev up`` there; the platform's verb only on a non-dev gate."""
    gate = DevGate(None, dev=dev)
    assert gate.sign_in == verb
    with pytest.raises(GateHTTPError) as refused:
        gate.require_human(_api_key_session())
    assert verb in refused.value.message
    confirm = g._confirmation_required_message(timedelta(hours=1), gate.sign_in)
    assert f"a new {verb} also counts" in confirm
    if dev:
        assert "kontra login" not in refused.value.message + confirm


#: Imports the host's modules with ``kontra.testing`` unimportable.
_WITHOUT_TESTING = """
import sys
class _NoTesting:
    def find_spec(self, name, path=None, target=None):
        if name == "kontra.testing" or name.startswith("kontra.testing."):
            raise ImportError(f"{name} is the testing package")
        return None
sys.meta_path.insert(0, _NoTesting())
import kontra.devgate.__main__, kontra.devgate.gate, kontra.devgate.ledger, kontra.devgate.stack
import kontra.devgate.state, kontra.devgate.store, kontra.devgate.temporal_cli
print("ok")
"""


def test_the_host_never_imports_the_testing_package() -> None:
    """OD2's direction: ``kontra.testing`` depends on ``kontra.devgate`` (``FakeGate``, the
    in-memory ledger, the dynamic config), never the other way."""
    done = subprocess.run(
        [sys.executable, "-c", _WITHOUT_TESTING],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    assert done.stdout.strip() == "ok"


def test_the_testing_ledger_is_the_dev_gates() -> None:
    """``kontra.testing.ledger`` re-exports the moved ledger: one class, one set of rows."""
    from kontra.devgate import ledger as moved
    from kontra.testing import ledger as historical

    assert historical.InMemoryLedger is moved.InMemoryLedger
    assert historical._HoldRow is moved._HoldRow
