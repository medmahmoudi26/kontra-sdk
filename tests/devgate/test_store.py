"""``kontra.devgate.store``: tagged JSON, a class allowlist, never pickle."""

from __future__ import annotations

import dataclasses
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from kontra.devgate.gate import DevGate
from kontra.devgate.store import FORMAT, dec, enc, restore, snapshot
from kontra.gate_client import models as m
from kontra.ledger import Money


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        3,
        1.5,
        "hi",
        datetime(2026, 1, 1, tzinfo=UTC),
        timedelta(seconds=30),
        Decimal("100.00"),
        b"\x00\x01\x02",
        {"a", "b"},
        (1, "two"),
        [1, "two"],
        {"k": 1, "n": None},
        Money.from_decimal("100.00", "USD"),
        m.JobState.SUBMITTED,
    ],
)
def test_enc_dec_round_trip(value: object) -> None:
    assert dec(enc(value)) == value


def test_a_forged_class_path_is_refused_before_any_import() -> None:
    with pytest.raises(ValueError, match=r"may not name os\.system"):
        dec({"$dc": "os.system", "f": {}})


def test_an_unknown_tag_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown tag"):
        dec({"$nope": 1})


def test_encoding_refuses_a_class_outside_the_allowlist() -> None:
    @dataclasses.dataclass
    class NotAllowed:
        x: int = 1

    with pytest.raises(ValueError, match="may not name"):
        enc(NotAllowed())


def test_an_empty_gate_round_trips() -> None:
    gate = DevGate(None)
    doc = snapshot(gate)
    assert doc["format"] == FORMAT
    raw = json.dumps(doc, default=str)
    other = DevGate(None)
    restore(other, json.loads(raw))
    assert other.keys.kid == gate.keys.kid
    assert other._url_secret == gate._url_secret
    assert other.job_key_proof_key == gate.job_key_proof_key


def test_an_unknown_format_is_refused() -> None:
    with pytest.raises(ValueError, match="`kontra dev down --reset`"):
        restore(DevGate(None), {"format": "evil/9"})
