"""``kontra._passkeys`` (decisions I24), the WebAuthn parameters and verification both gates
use, against answers from :class:`kontra.testing.SoftAuthenticator`: the options' shapes, a
registration and an assertion that verify for every algorithm taken, one targeted defect for
each refusal reason, the use-count rule, and the settings helpers."""

from __future__ import annotations

import base64
import json
import logging
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any

import pytest
from webauthn.helpers import encode_cbor, parse_cbor

from kontra import _passkeys as pk
from kontra.testing import AuthenticatorRefused, SoftAuthenticator
from kontra.testing.passkeys import KEY_ALGORITHMS

ORIGIN = "https://app.kontra.run"
RP = "app.kontra.run"
CHALLENGE = b"c" * 32
HANDLE = b"h" * 32


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _creation(exclude: list[pk.Known] | None = None, challenge: bytes = CHALLENGE) -> Any:
    return pk.registration_options(
        rp_id=RP,
        rp_name="Kontra",
        user_handle=HANDLE,
        user_name="alice",
        challenge=challenge,
        exclude=exclude or [],
    )


def _request(known: list[pk.Known], challenge: bytes = CHALLENGE) -> Any:
    return pk.assertion_options(rp_id=RP, challenge=challenge, allow=known)


def _register(auth: SoftAuthenticator, **kw: Any) -> pk.NewPasskey:
    return pk.verify_registration(
        kw.pop("answer", None) or auth.create(_creation()),
        challenge=kw.get("challenge", CHALLENGE),
        rp_id=kw.get("rp_id", RP),
        origins=kw.get("origins", [ORIGIN]),
    )


def _known(new: pk.NewPasskey) -> pk.Known:
    return pk.Known(
        new.credential_id, new.public_key, new.sign_count, new.backup_eligible, new.transports
    )


def _assert(answer: dict[str, Any], known: pk.Known, **kw: Any) -> pk.Assertion:
    return pk.verify_assertion(
        answer,
        challenge=kw.get("challenge", CHALLENGE),
        rp_id=kw.get("rp_id", RP),
        origins=kw.get("origins", [ORIGIN]),
        known=known,
        user_handle=kw.get("user_handle", HANDLE),
    )


def _client_data(answer: dict[str, Any], **changes: Any) -> dict[str, Any]:
    """``answer`` with its clientDataJSON's fields changed (its signature no longer holds)."""
    data = json.loads(_unb64(answer["response"]["clientDataJSON"]))
    data.update(changes)
    out = json.loads(json.dumps(answer))
    out["response"]["clientDataJSON"] = _b64(json.dumps(data).encode())
    return out


# --------------------------------------------------------------------------- options


def test_registration_options_are_the_ceremonys_parameters() -> None:
    old = pk.Known(b"o" * 16, b"", 0, False, ("usb", "nfc"))
    bare = pk.Known(b"b" * 16, b"", 0, False, ())
    options = _creation([old, bare])
    assert options["rp"] == {"id": RP, "name": "Kontra"}
    assert options["user"] == {"id": _b64(HANDLE), "name": "alice", "displayName": "alice"}
    assert options["challenge"] == _b64(CHALLENGE)
    assert [p["alg"] for p in options["pubKeyCredParams"]] == [-8, -7, -257]
    assert options["timeout"] == pk.CLIENT_TIMEOUT_MS == 300_000
    assert options["attestation"] == "none"
    selection = options["authenticatorSelection"]
    assert selection["residentKey"] == "preferred"
    assert selection["userVerification"] == "required"
    assert "authenticatorAttachment" not in selection  # built in, a phone, a key: all welcome
    assert options["excludeCredentials"] == [
        {"id": _b64(b"o" * 16), "type": "public-key", "transports": ["usb", "nfc"]},
        {"id": _b64(b"b" * 16), "type": "public-key"},
    ]
    assert "hints" not in options
    json.dumps(options)  # JSON as it goes on the wire


def test_assertion_options_ask_for_user_verification() -> None:
    known = pk.Known(b"k" * 16, b"", 0, False, ("internal",))
    options = _request([known])
    assert options == {
        "challenge": _b64(CHALLENGE),
        "timeout": 300_000,
        "rpId": RP,
        "allowCredentials": [
            {"id": _b64(b"k" * 16), "type": "public-key", "transports": ["internal"]}
        ],
        "userVerification": "required",
    }


def test_a_handle_and_a_challenge_are_32_bytes() -> None:
    with pytest.raises(ValueError, match="32"):
        pk.registration_options(
            rp_id=RP, rp_name="K", user_handle=b"x", user_name="a", challenge=CHALLENGE, exclude=[]
        )
    with pytest.raises(ValueError, match="32"):
        pk.assertion_options(rp_id=RP, challenge=b"short", allow=[])


# --------------------------------------------------------------------------- what verifies


@pytest.mark.parametrize("algorithm", [-8, -7, -257])
def test_every_algorithm_taken_registers_and_asserts(algorithm: int) -> None:
    auth = SoftAuthenticator(origin=ORIGIN, algorithm=algorithm)
    new = _register(auth)
    assert (new.algorithm, new.sign_count) == (algorithm, 0)
    assert new.credential_id == auth.credentials[0].credential_id
    assert new.aaguid == "00000000-0000-0000-0000-000000000000"
    assert (new.backup_eligible, new.backed_up, new.transports) == (False, False, ("internal",))
    known = _known(new)
    one = _assert(auth.get(_request([known])), known)
    assert (one.credential_id, one.sign_count, one.backed_up) == (new.credential_id, 1, False)
    assert _assert(auth.get(_request([known])), known).sign_count == 2


def test_what_a_registration_records() -> None:
    auth = SoftAuthenticator(
        origin=ORIGIN,
        backup_eligible=True,
        backed_up=True,
        aaguid=bytes(range(16)),
        transports=("cable", "usb", "bogus", "usb", "internal"),
    )
    new = _register(auth)
    assert (new.backup_eligible, new.backed_up) == (True, True)
    assert new.aaguid == "00010203-0405-0607-0809-0a0b0c0d0e0f"
    assert new.transports == ("hybrid", "usb", "internal")  # hints: never a refusal


def test_any_listed_origin_is_taken() -> None:
    auth = SoftAuthenticator(origin="https://beta.app.kontra.run")
    _register(auth, origins=[ORIGIN, "https://beta.app.kontra.run"])


def test_a_synced_passkey_answers_zero_and_keeps_its_flags() -> None:
    auth = SoftAuthenticator(origin=ORIGIN, counter=False, backup_eligible=True, backed_up=True)
    known = _known(_register(auth))
    for _ in range(2):
        got = _assert(auth.get(_request([known])), known)
        assert (got.sign_count, got.backed_up) == (0, True)
    auth.backed_up = False  # the backup state may change; eligibility may not
    assert _assert(auth.get(_request([known])), known).backed_up is False


def test_a_missing_user_handle_is_taken() -> None:
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    answer = auth.get(_request([known]))
    del answer["response"]["userHandle"]
    _assert(answer, known)


# --------------------------------------------------------------------------- what is refused


def _rejected(reason: pk.Rejection, call: Any) -> None:
    with pytest.raises(pk.PasskeyRejected) as e:
        call()
    assert e.value.reason == reason


def _with_att_stmt(answer: dict[str, Any], statement: dict[str, Any]) -> dict[str, Any]:
    att = parse_cbor(_unb64(answer["response"]["attestationObject"]))
    att["attStmt"] = statement
    out = json.loads(json.dumps(answer))
    out["response"]["attestationObject"] = _b64(encode_cbor(att))
    return out


def test_each_registration_defect_has_its_reason() -> None:
    good = SoftAuthenticator(origin=ORIGIN)
    answer = good.create(_creation())
    _rejected("malformed", lambda: _register(good, answer={"id": "x"}))
    _rejected("malformed", lambda: _register(good, answer={**answer, "id": _b64(b"z" * 16)}))
    _rejected("malformed", lambda: _register(good, answer={**answer, "response": "x"}))
    _rejected("type", lambda: _register(good, answer=_client_data(answer, type="webauthn.get")))
    _rejected("challenge", lambda: _register(good, challenge=b"d" * 32))
    _rejected("origin", lambda: _register(SoftAuthenticator(origin="https://kontra.example")))
    _rejected("rp", lambda: _register(SoftAuthenticator(origin=ORIGIN, rp_id="staging.kontra.run")))
    _rejected("presence", lambda: _register(SoftAuthenticator(origin=ORIGIN, up=False)))
    _rejected("verification", lambda: _register(SoftAuthenticator(origin=ORIGIN, uv=False)))
    _rejected("algorithm", lambda: _register(SoftAuthenticator(origin=ORIGIN, algorithm=-36)))
    _rejected("backup", lambda: _register(SoftAuthenticator(origin=ORIGIN, backed_up=True)))
    _rejected("signature", lambda: _register(good, answer=_with_att_stmt(answer, {"sig": b"x"})))


def test_each_assertion_defect_has_its_reason() -> None:
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    options = _request([known])

    def answer(**flags: Any) -> dict[str, Any]:
        for name, value in flags.items():
            setattr(auth, name, value)
        try:
            return auth.get(options)
        finally:
            auth.up = auth.uv = True
            auth.origin, auth.rp_id = ORIGIN, None
            auth.backup_eligible = auth.backed_up = False

    good = answer()
    _rejected("malformed", lambda: _assert({**good, "rawId": "!!"}, known))
    _rejected("malformed", lambda: _assert({**good, "rawId": _b64(b"x")}, known))  # too short
    _rejected("malformed", lambda: _assert({**good, "id": _b64(b"z" * 16)}, known))
    _rejected("malformed", lambda: _assert({**good, "response": {}}, known))
    other = pk.Known(b"o" * 16, known.public_key, 0, False, ())
    _rejected("unknown", lambda: _assert(good, other))
    _rejected("type", lambda: _assert(_client_data(good, type="webauthn.create"), known))
    _rejected("challenge", lambda: _assert(good, known, challenge=b"d" * 32))
    _rejected("origin", lambda: _assert(answer(origin="https://kontra.example"), known))
    _rejected("rp", lambda: _assert(answer(rp_id="staging.kontra.run"), known))
    _rejected("presence", lambda: _assert(answer(up=False), known))
    _rejected("verification", lambda: _assert(answer(uv=False), known))
    forged = answer()
    signature = bytearray(_unb64(forged["response"]["signature"]))
    signature[-1] ^= 1
    forged["response"]["signature"] = _b64(bytes(signature))
    _rejected("signature", lambda: _assert(forged, known))
    forgery = pk.Known(known.credential_id, _another_key(), 0, False, ())
    _rejected("signature", lambda: _assert(answer(), forgery))
    _rejected("backup", lambda: _assert(answer(backup_eligible=True), known))
    _rejected("backup", lambda: _assert(answer(backed_up=True), known))  # BS without BE
    _rejected("user", lambda: _assert(good, known, user_handle=b"u" * 32))
    _assert(answer(), known)  # and the authenticator itself is fine


def test_an_answer_from_the_console_inside_another_page_is_refused() -> None:
    """The console's own origin, but in a frame of another site's page (``crossOrigin``,
    ``topOrigin``): the console is never framed (``frame-ancestors 'none'``), so a gate
    expects no such answer (WebAuthn L3, verifying topOrigin) and refuses it as ``origin``."""
    framed = SoftAuthenticator(origin=ORIGIN, top_origin="https://evil.example")
    _rejected("origin", lambda: _register(framed))
    answer = SoftAuthenticator(origin=ORIGIN).create(_creation())
    _rejected("origin", lambda: _register(framed, answer=_client_data(answer, crossOrigin=True)))
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    auth.top_origin = "https://evil.example"
    _rejected("origin", lambda: _assert(auth.get(_request([known])), known))
    auth.top_origin = None
    _assert(auth.get(_request([known])), known)


def _another_key() -> bytes:
    """Another passkey's public key: put under the first one's credential id, a forgery."""
    return _register(SoftAuthenticator(origin=ORIGIN)).public_key


def test_a_refusal_logs_the_librarys_words_never_the_answer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    forged = auth.get(_request([known]))
    forged["response"]["signature"] = _b64(b"\x30\x06\x02\x01\x01\x02\x01\x01")
    with caplog.at_level(logging.INFO, logger="kontra.passkeys"):
        _rejected("signature", lambda: _assert(forged, known))
    assert "passkey answer refused (signature)" in caplog.text
    for secret in (
        forged["response"]["signature"],
        forged["response"]["clientDataJSON"],
        forged["rawId"],
        _b64(CHALLENGE),
    ):
        assert secret not in caplog.text


# --------------------------------------------------------------------------- rules and helpers


@pytest.mark.parametrize(
    ("stored", "new", "moved"),
    [(0, 0, True), (0, 1, True), (5, 6, True), (5, 5, False), (5, 4, False), (5, 0, False)],
)
def test_the_use_count_must_move_forward(stored: int, new: int, moved: bool) -> None:
    assert pk.counter_moved(stored, new) is moved
    # Behind is a copy only for a passkey on one device; a synced one's copies are meant.
    for synced, behind in ((False, "copy"), (True, "behind")):
        verdict = pk.judge_counter(stored, new, backup_eligible=synced)
        assert verdict == ("moved" if moved else behind)


def test_transport_hints_are_normalized() -> None:
    assert pk.normalize_transports(["cable", "usb", "x", "usb", 3, None]) == ("hybrid", "usb")
    assert pk.normalize_transports("usb") == ()
    every = ["usb", "nfc", "ble", "smart-card", "hybrid", "internal"]
    assert pk.normalize_transports(every * 10) == tuple(every)


def test_the_credential_id_of_an_answer() -> None:
    cid = bytes(range(32))
    assert pk.answer_credential_id({"rawId": _b64(cid)}) == cid
    for bad in (
        _b64(cid) + "=",  # padded
        _b64(cid)[:-1] + "!",
        _b64(b"x" * 15),  # shorter than any gate takes
        _b64(b"x" * 1024),
        "AB",  # not canonical: decodes to a byte that encodes as "AA"
        None,
        3,
    ):
        assert pk.answer_credential_id({"rawId": bad}) is None, bad
    assert pk.answer_credential_id({}) is None


@pytest.mark.parametrize(
    ("text", "ok"),
    [
        ("app.kontra.run", True),
        ("staging-app.kontra.run", True),
        ("localhost", True),
        ("kontra", False),  # one label, not localhost
        ("App.kontra.run", False),
        ("127.0.0.1", False),
        ("[::1]", False),
        ("app.kontra.run:443", False),
        ("app.kontra.run.", False),
        ("-app.kontra.run", False),
        ("", False),
        ("a" * 64 + ".run", False),
    ],
)
def test_what_can_be_an_rp_id(text: str, ok: bool) -> None:
    assert pk.is_rp_id(text) is ok


def test_the_rp_id_is_derived_from_a_url() -> None:
    assert pk.rp_id_of("https://app.kontra.run") == "app.kontra.run"
    assert pk.rp_id_of("http://localhost:5173/console") == "localhost"
    assert pk.rp_id_of("http://127.0.0.1:8080") is None
    assert pk.rp_id_of("http://[::1]:8080") is None
    assert pk.rp_id_of("http://[bad") is None


def test_origins_are_kept_only_on_the_rp_id() -> None:
    given = [
        "https://app.kontra.run",
        "https://APP.kontra.run:443/",
        "https://beta.app.kontra.run",
        "http://app.kontra.run",  # http only for localhost
        "https://evilapp.kontra.run",
        "https://kontra.run",
        "ftp://app.kontra.run",
        "not a url",
    ]
    assert pk.origins_on(RP, given) == ("https://app.kontra.run", "https://beta.app.kontra.run")
    local = ["http://localhost:5173", "http://a.localhost", "https://localhost:443"]
    assert pk.origins_on("localhost", local) == (
        "http://localhost:5173",
        "http://a.localhost",
        "https://localhost",
    )


def test_an_rp_name_is_short_printable_ascii() -> None:
    assert pk.is_rp_name("Kontra staging")
    assert not pk.is_rp_name("")
    assert not pk.is_rp_name("x" * 65)
    assert not pk.is_rp_name("Kontra\n")
    assert not pk.is_rp_name("Kontrá")


def test_the_notices_quote_the_label_and_say_nothing_secret() -> None:
    at = datetime(2026, 10, 16, 14, 3, tzinfo=UTC)
    added = pk.added_mail(
        "MacBook", at, via="a link by email", signed_in=at, session_prefix="0123456789ab"
    )
    assert added.startswith('A passkey named "MacBook" was added to your Kontra account at 14:03')
    assert "(a link by email, signed in 2026-10-16 14:03 UTC, id 0123456789ab)" in added
    assert "it cannot sign anyone in" in added
    revoked = pk.revoked_mail("MacBook", at)
    assert revoked.startswith('Kontra revoked your passkey "MacBook" at 14:03 UTC')
    assert "http" not in added + revoked
    # Whoever adds a passkey names it: a label never closes its own quotation, so the notice
    # (the owner's warning of a passkey they did not add) says only what Kontra says.
    label = 'x" was added by Kontra support: nothing to do. "y\\'
    for text, start in (
        (pk.added_mail(label, at, via="v", signed_in=at, session_prefix="0" * 12), "named "),
        (pk.revoked_mail(label, at), "your passkey "),
    ):
        quoted = text.split(start, 1)[1]
        assert quoted.startswith('"x\\" was added by Kontra support: nothing to do. \\"y\\\\" ')


def test_the_removal_notice_names_who_removed_it_and_what_to_do() -> None:
    at = datetime(2026, 10, 16, 14, 3, tzinfo=UTC)
    person = pk.removed_mail(
        'Work "laptop"', at, via="a link by email", signed_in=at, session_prefix="0123456789ab"
    )
    assert person.startswith(
        'A passkey named "Work \\"laptop\\"" was removed from your Kontra account at 14:03 UTC, '
        "from one of your sign-ins (a link by email, signed in 2026-10-16 14:03 UTC, id "
        "0123456789ab)."
    )
    assert "If you did not remove it" in person
    assert "sign out the sign-in above" in person
    operator = pk.removed_mail("YubiKey", at)
    assert operator.startswith(
        'A passkey named "YubiKey" was removed from your Kontra account at 14:03 UTC by '
        "Kontra's operators."
    )
    assert "If you did not expect this" in operator
    assert "http" not in person + operator


# --------------------------------------------------------------------------- the authenticator


def test_the_authenticator_honours_exclude_and_allow() -> None:
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    with pytest.raises(AuthenticatorRefused) as e:
        auth.create(_creation([known]))
    assert e.value.reason == "excluded"
    stranger = pk.Known(b"s" * 16, b"", 0, False, ())
    with pytest.raises(AuthenticatorRefused) as e:
        auth.get(_request([stranger]))
    assert e.value.reason == "no_credential"
    discoverable = _request([])  # no list: the credential it holds for the RP ID
    assert _assert(auth.get(discoverable), known).sign_count == 1
    forced = auth.get(_request([stranger]), credential_id=known.credential_id)
    assert _unb64(forced["rawId"]) == known.credential_id


def test_a_clone_holds_the_same_keys_and_counts_on_its_own() -> None:
    auth = SoftAuthenticator(origin=ORIGIN)
    known = _known(_register(auth))
    _assert(auth.get(_request([known])), known)
    copy = auth.clone()
    assert _assert(copy.get(_request([known])), known).sign_count == 2
    assert _assert(auth.get(_request([known])), known).sign_count == 2
    assert [c.sign_count for c in auth.credentials] == [2]


def test_the_authenticator_refuses_what_it_cannot_be() -> None:
    with pytest.raises(ValueError, match="16 bytes"):
        SoftAuthenticator(origin=ORIGIN, aaguid=b"x")
    with pytest.raises(ValueError, match="makes keys for"):
        SoftAuthenticator(origin=ORIGIN, algorithm=-35)
    assert set(KEY_ALGORITHMS) - set(pk.ALGORITHMS) == {-36}


def test_the_library_is_only_loaded_by_what_verifies() -> None:
    """``import kontra`` (and the client, the CLI, the fake gate module) never imports
    ``webauthn``: only the testing extra and the gate install it."""
    code = (
        "import sys, kontra, kontra.auth, kontra.cli, kontra.gate_client, kontra._passkeys,"
        " kontra.testing.fake_gate, kontra.testing.passkeys;"
        " sys.exit('webauthn' in sys.modules)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_without_the_library_it_says_what_to_install(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "webauthn", None)
    with pytest.raises(RuntimeError, match=r"kontra-sdk\[testing\]"):
        _creation()
