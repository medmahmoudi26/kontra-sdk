"""Seam ② models of passkeys (decisions I24): opaque ids checked whole, the browser's JSON
carried as an opaque object of bounded size, a passkey's view consistent, nothing unknown."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from kontra.gate_client import models as m

NOW = "2026-10-16T12:00:00Z"
CEREMONY = "wac_" + "0123456789abcdef" * 2
PASSKEY = "pky_" + "0123456789abcdef" * 2


def _view(**fields: Any) -> dict[str, Any]:
    return {
        "id": PASSKEY,
        "label": "MacBook Touch ID",
        "created_at": NOW,
        "backed_up": False,
        "transports": ["internal"],
        **fields,
    }


@pytest.mark.parametrize(
    ("good", "check"),
    [(CEREMONY, m.is_ceremony_id), (PASSKEY, m.is_passkey_id)],
)
def test_ids_are_checked_whole(good: str, check: Any) -> None:
    assert check(good)
    for bad in (
        good + "\n",
        good + "0",
        good[:-1],
        good.upper(),
        " " + good,
        good.replace("0", "g", 1),
        good[:4],
        "",
    ):
        assert not check(bad), bad


@pytest.mark.parametrize("bad", [CEREMONY + "\n", CEREMONY[:-1], "cfm_" + "0" * 32, "x"])
def test_a_ceremony_id_off_the_pattern_is_refused(bad: str) -> None:
    with pytest.raises(ValidationError):
        m.PasskeyConfirm.model_validate({"ceremony_id": bad, "credential": {}})


@pytest.mark.parametrize("bad", [PASSKEY + "\n", "pky_" + "0" * 31, "key_" + "0" * 32])
def test_a_passkey_id_off_the_pattern_is_refused(bad: str) -> None:
    with pytest.raises(ValidationError):
        m.PasskeyView.model_validate(_view(id=bad))


def test_the_browsers_json_is_bounded_in_size() -> None:
    fits = {"id": "x", "response": {"clientDataJSON": "a" * (m.WEBAUTHN_JSON_MAX - 100)}}
    m.PasskeyConfirm.model_validate({"ceremony_id": CEREMONY, "credential": fits})
    m.PasskeyAdd.model_validate({"ceremony_id": CEREMONY, "label": "k", "credential": fits})
    big = {"id": "x", "response": {"clientDataJSON": "a" * m.WEBAUTHN_JSON_MAX}}
    for model, body in (
        (m.PasskeyConfirm, {"ceremony_id": CEREMONY, "credential": big}),
        (m.PasskeyAdd, {"ceremony_id": CEREMONY, "label": "k", "credential": big}),
        (
            m.PasskeyCeremony,
            {"ceremony_id": CEREMONY, "rp_id": "localhost", "options": big, "expires_in": 360},
        ),
    ):
        with pytest.raises(ValidationError, match="bytes of JSON"):
            model.model_validate(body)


def test_the_browsers_json_must_be_a_json_object() -> None:
    for credential in ([], "x", None, {"n": float("nan")}, {"k": {1, 2}}):
        with pytest.raises(ValidationError):
            m.PasskeyConfirm.model_validate({"ceremony_id": CEREMONY, "credential": credential})
    many = {f"k{i}": i for i in range(65)}
    with pytest.raises(ValidationError):
        m.PasskeyConfirm.model_validate({"ceremony_id": CEREMONY, "credential": many})


def test_the_answer_is_left_out_of_repr() -> None:
    body = m.PasskeyConfirm(ceremony_id=CEREMONY, credential={"id": "secret-looking"})
    assert "secret-looking" not in repr(body)
    assert body.model_dump(mode="json")["credential"] == {"id": "secret-looking"}


def test_a_passkey_view_is_revoked_wholly_or_not_at_all() -> None:
    live = m.PasskeyView.model_validate(_view())
    assert (live.revoked_at, live.revoked_by, live.added_by, live.last_used_at) == (
        None,
        None,
        None,
        None,
    )
    for by in ("revoked", "counter", "operator"):
        assert m.PasskeyView.model_validate(_view(revoked_at=NOW, revoked_by=by)).revoked_by == by
    for half in ({"revoked_at": NOW}, {"revoked_by": "revoked"}):
        with pytest.raises(ValidationError, match="revoked_at and revoked_by"):
            m.PasskeyView.model_validate(_view(**half))
    with pytest.raises(ValidationError):
        m.PasskeyView.model_validate(_view(revoked_at=NOW, revoked_by="stolen"))


def test_a_passkey_view_names_its_sign_in_by_twelve_hex() -> None:
    assert m.PasskeyView.model_validate(_view(added_by="0123456789ab")).added_by == "0123456789ab"
    for bad in ("0123456789a", "0123456789abc", "0123456789AB", "0123456789ab\n"):
        with pytest.raises(ValidationError):
            m.PasskeyView.model_validate(_view(added_by=bad))


def test_transports_are_the_webauthn_ones() -> None:
    every = ["usb", "nfc", "ble", "smart-card", "hybrid", "internal"]
    assert m.PasskeyView.model_validate(_view(transports=every)).transports == every
    with pytest.raises(ValidationError):
        m.PasskeyView.model_validate(_view(transports=["cable"]))  # the gate stores it as hybrid


@pytest.mark.parametrize(
    ("model", "body"),
    [
        (m.PasskeyView, _view()),
        (m.PasskeyList, {"rp_id": None, "passkeys": []}),
        (m.PasskeyConfirmed, {"confirmed_until": NOW, "passkey_id": PASSKEY}),
        (
            m.PasskeyCeremony,
            {"ceremony_id": CEREMONY, "rp_id": "localhost", "options": {}, "expires_in": 360},
        ),
        (m.PasskeyAdd, {"ceremony_id": CEREMONY, "label": "k", "credential": {}}),
        (m.PasskeyConfirm, {"ceremony_id": CEREMONY, "credential": {}}),
    ],
)
def test_unknown_fields_are_refused(model: Any, body: dict[str, Any]) -> None:
    model.model_validate(body)
    with pytest.raises(ValidationError):
        model.model_validate({**body, "credential_id": "AAAA"})


def test_a_label_is_an_api_key_label() -> None:
    assert m.PASSKEY_LABEL_PATTERN == m.API_KEY_LABEL_PATTERN
    for bad in ("", "x" * 65, "tab\there", "é"):
        with pytest.raises(ValidationError):
            m.PasskeyAdd.model_validate({"ceremony_id": CEREMONY, "label": bad, "credential": {}})


def test_the_constants() -> None:
    assert (m.RECENT_CONFIRMATION_SECONDS, m.MAX_PASSKEYS) == (300, 10)
    assert m.WEBAUTHN_JSON_MAX == 32 * 1024
