"""Passkeys: the WebAuthn parameters and the verification both gates use (decisions I24).

A passkey confirms a person's sign-in in one step; it never signs anyone in. The platform gate
and :class:`~kontra.testing.fake_gate.FakeGate` call these functions with the same arguments,
as both call :mod:`kontra._control` for escrow: the platform imports this module, never copies
it, so the ceremony's parameters (RP, algorithms, user verification, timeouts) and what makes an
answer acceptable are written once.

The verifier is ``webauthn`` (duo-labs py_webauthn, BSD-3-Clause). It is imported inside the
functions that need it: ``import kontra`` never loads it, and only the gate (its own pin) and
``kontra-sdk[testing]`` install it.

The gate's settings, which both gates derive the same way: ``KONTRA_PASSKEYS`` (``0`` turns
every ceremony off; stored passkeys stay), ``KONTRA_PASSKEY_RP_ID`` (default: the console's
host, :func:`rp_id_of`; an explicit one must pass :func:`is_rp_id`), ``KONTRA_PASSKEY_RP_NAME``
(default ``Kontra``, what authenticators show); the origins an answer may come from are the
console's (``KONTRA_CONSOLE_ORIGINS``) on that RP ID (:func:`origins_on`).

Nothing here logs or returns an answer, a challenge or a key: a refusal is a
:class:`PasskeyRejected` whose ``reason`` is one word of a fixed set (a metric label), and the
library's own message goes to an INFO log line, escaped.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import re
from collections.abc import Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, NoReturn, cast, get_args
from urllib.parse import urlsplit

from pydantic import JsonValue

__all__ = [
    "ALGORITHMS",
    "CEREMONY_TTL",
    "CLIENT_TIMEOUT_MS",
    "DEFAULT_RP_NAME",
    "Assertion",
    "CounterVerdict",
    "Known",
    "NewPasskey",
    "PasskeyRejected",
    "Rejection",
    "Transport",
    "answer_credential_id",
    "assertion_options",
    "counter_moved",
    "is_rp_id",
    "is_rp_name",
    "judge_counter",
    "normalize_transports",
    "origins_on",
    "registration_options",
    "rp_id_of",
    "verify_assertion",
    "verify_registration",
]

log = logging.getLogger("kontra.passkeys")

#: How an authenticator reaches the browser, as a gate keeps it (``cable`` becomes ``hybrid``).
Transport = Literal["usb", "nfc", "ble", "smart-card", "hybrid", "internal"]
TRANSPORTS: tuple[Transport, ...] = get_args(Transport)
#: What a ceremony is for: adding a passkey, or confirming a session with one.
Purpose = Literal["register", "confirm"]
#: Why an answer was refused: a fixed set, so it can label a metric. ``malformed``: not a
#: WebAuthn answer at all; ``type``: a ``create`` answer where a ``get`` was due, or the
#: reverse; ``challenge``: another ceremony's; ``origin``: a page that is not the console,
#: or the console in a frame of another site's page; ``rp``: signed for another RP ID;
#: ``presence`` / ``verification``: the UP / UV flag is clear; ``algorithm``: a key type this
#: gate does not take; ``signature``: the signature or the attestation does not verify;
#: ``unknown``: not the passkey looked up; ``backup``: the backup flags are impossible or the
#: eligibility changed; ``user``: another account's; ``counter``: a synced passkey's use
#: count did not move forward (:func:`judge_counter`: refused, not revoked).
Rejection = Literal[
    "malformed",
    "type",
    "challenge",
    "origin",
    "rp",
    "presence",
    "verification",
    "algorithm",
    "signature",
    "unknown",
    "backup",
    "user",
    "counter",
]

#: A ceremony's life at the gate: the browser's own timeout plus a minute for the network.
CEREMONY_TTL = timedelta(minutes=6)
#: The browser's timeout. WebAuthn L3 recommends 300 to 600 s when user verification is
#: required (a PIN takes a while).
CLIENT_TIMEOUT_MS = 300_000
#: COSE algorithms taken, in the order offered: EdDSA, ES256, RS256.
ALGORITHMS: tuple[int, ...] = (-8, -7, -257)
CHALLENGE_BYTES = 32
USER_HANDLE_BYTES = 32
#: WebAuthn's credential id bounds (L3: at most 1023 bytes); 16 is the least the gate takes.
CREDENTIAL_ID_MIN = 16
CREDENTIAL_ID_MAX = 1023
DEFAULT_RP_NAME = "Kontra"
_RP_NAME = re.compile(r"[\x20-\x7e]{1,64}", re.ASCII)
_HOSTNAME = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?",
    re.ASCII,
)
_B64URL = re.compile(r"[A-Za-z0-9_-]*", re.ASCII)
_MISSING_LIBRARY = (
    "passkeys need the 'webauthn' package: kontra-sdk[testing], or the gate's own pin"
)

# --------------------------------------------------------------------------- refusals and records


class PasskeyRejected(Exception):
    """An answer the gate does not accept. ``reason`` is one word of :data:`Rejection`;
    clients are told only that it was not accepted (400 ``passkey_rejected``)."""

    def __init__(self, reason: Rejection) -> None:
        super().__init__(reason)
        self.reason: Rejection = reason


@dataclass(frozen=True, slots=True)
class Known:
    """What a gate knows of a passkey: enough to exclude it, allow it, and verify its
    assertions."""

    credential_id: bytes
    public_key: bytes
    sign_count: int
    backup_eligible: bool
    transports: tuple[Transport, ...] = ()


@dataclass(frozen=True, slots=True)
class NewPasskey:
    """A verified registration: what a gate stores. Nothing about the attestation is kept
    but the AAGUID (all zeros when the authenticator hides it)."""

    credential_id: bytes
    public_key: bytes
    algorithm: int
    sign_count: int
    aaguid: str
    backup_eligible: bool
    backed_up: bool
    transports: tuple[Transport, ...]


@dataclass(frozen=True, slots=True)
class Assertion:
    """A verified assertion. Its counter is judged by the caller (:func:`counter_moved`),
    under the passkey's row lock."""

    credential_id: bytes
    sign_count: int
    backed_up: bool


# --------------------------------------------------------------------------- settings


def is_rp_id(text: str) -> bool:
    """``text`` can be a WebAuthn RP ID here: a lower-case DNS name -- no port, no IP literal
    (a browser refuses those) -- that is ``localhost`` or has a dot. An explicit
    ``KONTRA_PASSKEY_RP_ID`` that is not one keeps the gate from starting."""
    # An IPv6 literal has colons, which the pattern refuses; an IPv4 one (or any name whose
    # last label is a number, which no DNS name has) is refused by its last label.
    if _HOSTNAME.fullmatch(text) is None or text.rsplit(".", 1)[-1].isdigit():
        return False
    return text == "localhost" or "." in text


def is_rp_name(text: str) -> bool:
    """``KONTRA_PASSKEY_RP_NAME``: 1 to 64 printable ASCII characters."""
    return _RP_NAME.fullmatch(text) is not None


def rp_id_of(url: str) -> str | None:
    """The RP ID a gate derives from the console's URL (or its own, when it serves the
    console): its host, or None when that host cannot be one (an IP address, as in a
    ``http://127.0.0.1:8080`` setup) -- passkeys are then off, never a refusal to start."""
    try:
        host = urlsplit(url).hostname
    except ValueError:
        return None
    return host if host is not None and is_rp_id(host) else None


def _origin(url: str) -> tuple[str, str, str] | None:
    """``(scheme, host, origin)`` of an http(s) URL, the origin as a browser writes it in
    clientDataJSON (lower case, default port dropped); None for anything else."""
    try:
        parts = urlsplit(url.strip())
        port = parts.port
    except ValueError:
        return None
    scheme, host = parts.scheme.lower(), parts.hostname
    if scheme not in ("http", "https") or not host:
        return None
    default = {"http": 80, "https": 443}[scheme]
    shown = f"[{host}]" if ":" in host else host
    return scheme, host, f"{scheme}://{shown}" + (f":{port}" if port not in (None, default) else "")


def origins_on(rp_id: str, origins: Iterable[str]) -> tuple[str, ...]:
    """The origins an answer may come from: those of ``origins`` whose host is ``rp_id`` or
    under it, over https (http only for ``localhost`` and ``*.localhost``). Order kept,
    duplicates dropped; a gate logs the ones it leaves out."""
    kept: list[str] = []
    for url in origins:
        parsed = _origin(url)
        if parsed is None:
            continue
        scheme, host, origin = parsed
        local = host == "localhost" or host.endswith(".localhost")
        on = host == rp_id or host.endswith("." + rp_id)
        if on and (scheme == "https" or local) and origin not in kept:
            kept.append(origin)
    return tuple(kept)


# --------------------------------------------------------------------------- the library


def _require() -> None:
    try:
        import webauthn  # noqa: F401  # pyright: ignore[reportUnusedImport]
    except ImportError as e:  # pragma: no cover - the testing extra and the gate install it
        raise RuntimeError(_MISSING_LIBRARY) from e


def _reject(reason: Rejection, why: object = "") -> NoReturn:
    if why:
        log.info("passkey answer refused (%s): %r", reason, str(why)[:300])
    raise PasskeyRejected(reason)


def _expect(ok: bool, reason: Rejection) -> None:
    if not ok:
        _reject(reason)


@contextmanager
def _step(reason: Rejection) -> Generator[None]:
    """Whatever an answer makes this step raise is a refusal for ``reason``: a malformed
    answer can make a parser raise almost anything (CBOR, base64, ``KeyError``, ...)."""
    try:
        yield
    except PasskeyRejected:
        raise
    except Exception as e:  # a refusal, never a 500
        _reject(reason, e)


def _framed(client_data_json: bytes, cross_origin: bool | None) -> bool:
    """Whether the client data says the ceremony ran in a frame inside another origin's page
    (``crossOrigin``, ``topOrigin``). The console is never framed (``frame-ancestors 'none'``),
    so a gate expects no such answer and refuses it (WebAuthn L3: an RP verifies
    ``topOrigin`` against the pages it expects to be framed in, and expects none)."""
    data = cast("dict[str, object]", json.loads(client_data_json))
    return bool(cross_origin) or "topOrigin" in data


def _rp_hash(rp_id: str) -> bytes:
    return hashlib.sha256(rp_id.encode("ascii")).digest()


def _b64url(value: object, *, max_bytes: int) -> bytes | None:
    """Strict unpadded base64url (canonical: it encodes back to itself), or None."""
    if (
        not isinstance(value, str)
        or len(value) > -(-4 * max_bytes // 3)
        or _B64URL.fullmatch(value) is None
        or len(value) % 4 == 1
    ):
        return None
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError):  # pragma: no cover - the pattern rules it out
        return None
    if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != value:
        return None
    return raw


def _descriptors(known: Sequence[Known]) -> list[Any]:
    from webauthn.helpers.structs import AuthenticatorTransport, PublicKeyCredentialDescriptor

    return [
        PublicKeyCredentialDescriptor(
            id=k.credential_id,
            transports=[AuthenticatorTransport(t) for t in k.transports] or None,
        )
        for k in known
    ]


def _json(options: Any) -> dict[str, JsonValue]:
    from webauthn.helpers import options_to_json_dict

    return cast("dict[str, JsonValue]", options_to_json_dict(options))


def registration_options(
    *,
    rp_id: str,
    rp_name: str,
    user_handle: bytes,
    user_name: str,
    challenge: bytes,
    exclude: Sequence[Known],
) -> dict[str, JsonValue]:
    """``PublicKeyCredentialCreationOptionsJSON`` for adding a passkey: the party's own
    random user handle and its handle as the name (never an address), attestation ``none``,
    user verification required, a discoverable credential preferred, any attachment
    (built in, a phone, a security key), and the party's live passkeys excluded so an
    authenticator never holds two of one account's."""
    _require()
    from webauthn import generate_registration_options
    from webauthn.helpers.cose import COSEAlgorithmIdentifier
    from webauthn.helpers.structs import (
        AttestationConveyancePreference,
        AuthenticatorSelectionCriteria,
        ResidentKeyRequirement,
        UserVerificationRequirement,
    )

    if len(user_handle) != USER_HANDLE_BYTES or len(challenge) != CHALLENGE_BYTES:
        raise ValueError("a user handle and a challenge are 32 random bytes")
    options = generate_registration_options(
        rp_id=rp_id,
        rp_name=rp_name,
        user_name=user_name,
        user_id=user_handle,
        user_display_name=user_name,
        challenge=challenge,
        timeout=CLIENT_TIMEOUT_MS,
        attestation=AttestationConveyancePreference.NONE,
        authenticator_selection=AuthenticatorSelectionCriteria(
            resident_key=ResidentKeyRequirement.PREFERRED,
            user_verification=UserVerificationRequirement.REQUIRED,
        ),
        exclude_credentials=_descriptors(exclude),
        supported_pub_key_algs=[COSEAlgorithmIdentifier(a) for a in ALGORITHMS],
    )
    return _json(options)


def assertion_options(
    *, rp_id: str, challenge: bytes, allow: Sequence[Known]
) -> dict[str, JsonValue]:
    """``PublicKeyCredentialRequestOptionsJSON`` for confirming with one of ``allow`` (the
    party's live passkeys), user verification required."""
    _require()
    from webauthn import generate_authentication_options
    from webauthn.helpers.structs import UserVerificationRequirement

    if len(challenge) != CHALLENGE_BYTES:
        raise ValueError("a challenge is 32 random bytes")
    options = generate_authentication_options(
        rp_id=rp_id,
        challenge=challenge,
        timeout=CLIENT_TIMEOUT_MS,
        allow_credentials=_descriptors(allow),
        user_verification=UserVerificationRequirement.REQUIRED,
    )
    return _json(options)


def answer_credential_id(answer: Mapping[str, object]) -> bytes | None:
    """The credential id an answer names (its ``rawId``), to look the passkey up; None when
    it names none a gate could hold."""
    raw = _b64url(answer.get("rawId"), max_bytes=CREDENTIAL_ID_MAX)
    if raw is None or not CREDENTIAL_ID_MIN <= len(raw) <= CREDENTIAL_ID_MAX:
        return None
    return raw


def normalize_transports(raw: object) -> tuple[Transport, ...]:
    """The browser's transport hints as a gate keeps them: ``cable`` is ``hybrid``, unknown
    values are dropped, duplicates too, at most six. Hints never fail a registration."""
    if not isinstance(raw, list):
        return ()
    kept: list[Transport] = []
    for item in cast("list[object]", raw)[:32]:
        name = "hybrid" if item == "cable" else item
        if name in TRANSPORTS and name not in kept:
            kept.append(name)
    return tuple(kept[: len(TRANSPORTS)])


def _response(answer: Mapping[str, object]) -> Mapping[str, object]:
    response = answer.get("response")
    return cast("Mapping[str, object]", response) if isinstance(response, dict) else {}


def verify_registration(
    answer: Mapping[str, object], *, challenge: bytes, rp_id: str, origins: Sequence[str]
) -> NewPasskey:
    """Verify the browser's ``RegistrationResponseJSON`` for the ceremony whose
    ``challenge`` this is: the client data's type, challenge and origin (one of
    ``origins``, in no other site's frame), the RP ID hash, user presence and verification,
    the key's algorithm, the backup flags, then the attestation (any format the library
    verifies). Raises :class:`PasskeyRejected`."""
    _require()
    from webauthn import verify_registration_response
    from webauthn.helpers import (
        bytes_to_base64url,
        decode_credential_public_key,
        parse_attestation_object,
        parse_backup_flags,
        parse_client_data_json,
        parse_registration_credential_json,
    )
    from webauthn.helpers.cose import COSEAlgorithmIdentifier
    from webauthn.helpers.structs import ClientDataType

    with _step("malformed"):
        credential = parse_registration_credential_json(dict(answer))
        client = parse_client_data_json(credential.response.client_data_json)
        framed = _framed(credential.response.client_data_json, client.cross_origin)
        attestation = parse_attestation_object(credential.response.attestation_object)
        data = attestation.auth_data.attested_credential_data
        if data is None or not data.credential_public_key:
            raise ValueError("no attested credential")
        if bytes_to_base64url(credential.raw_id) != credential.id:
            raise ValueError("id and rawId differ")
        if data.credential_id != credential.raw_id:
            raise ValueError("the attested credential id is not the answer's")
        if not CREDENTIAL_ID_MIN <= len(data.credential_id) <= CREDENTIAL_ID_MAX:
            raise ValueError("credential id length")
        key = decode_credential_public_key(data.credential_public_key)
    flags = attestation.auth_data.flags
    _expect(client.type == ClientDataType.WEBAUTHN_CREATE, "type")
    _expect(hmac.compare_digest(client.challenge, challenge), "challenge")
    _expect(client.origin in origins and not framed, "origin")
    _expect(hmac.compare_digest(attestation.auth_data.rp_id_hash, _rp_hash(rp_id)), "rp")
    _expect(flags.up, "presence")
    _expect(flags.uv, "verification")
    _expect(int(key.alg) in ALGORITHMS, "algorithm")
    with _step("backup"):
        parse_backup_flags(flags)
    with _step("signature"):
        verified = verify_registration_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=rp_id,
            expected_origin=list(origins),
            require_user_presence=True,
            require_user_verification=True,
            supported_pub_key_algs=[COSEAlgorithmIdentifier(a) for a in ALGORITHMS],
        )
    return NewPasskey(
        credential_id=verified.credential_id,
        public_key=verified.credential_public_key,
        algorithm=int(key.alg),
        sign_count=verified.sign_count,
        aaguid=verified.aaguid,
        backup_eligible=flags.be,
        backed_up=verified.credential_backed_up,
        transports=normalize_transports(_response(answer).get("transports")),
    )


def verify_assertion(
    answer: Mapping[str, object],
    *,
    challenge: bytes,
    rp_id: str,
    origins: Sequence[str],
    known: Known,
    user_handle: bytes,
) -> Assertion:
    """Verify the browser's ``AuthenticationResponseJSON`` for the ceremony whose
    ``challenge`` this is, as an answer of ``known`` (the passkey its ``rawId`` names, which
    the caller looked up and locked): the client data's type, challenge and origin (in no
    other site's frame), the RP ID hash, user presence and verification, the signature, then
    that the backup eligibility is the one registered and the user handle, when present, is
    the party's. The counter is not judged here (:func:`counter_moved`): passing the library
    0 switches its own counter refusal off, so a gate can tell a clone from a forgery once
    the signature verified. Raises :class:`PasskeyRejected`."""
    _require()
    from webauthn import verify_authentication_response
    from webauthn.helpers import (
        bytes_to_base64url,
        parse_authentication_credential_json,
        parse_authenticator_data,
        parse_client_data_json,
    )
    from webauthn.helpers.exceptions import InvalidBackupFlags
    from webauthn.helpers.structs import ClientDataType

    raw = answer_credential_id(answer)
    _expect(raw is not None, "malformed")
    _expect(raw == known.credential_id, "unknown")
    with _step("malformed"):
        credential = parse_authentication_credential_json(dict(answer))
        client = parse_client_data_json(credential.response.client_data_json)
        framed = _framed(credential.response.client_data_json, client.cross_origin)
        data = parse_authenticator_data(credential.response.authenticator_data)
        if bytes_to_base64url(credential.raw_id) != credential.id:
            raise ValueError("id and rawId differ")
    _expect(client.type == ClientDataType.WEBAUTHN_GET, "type")
    _expect(hmac.compare_digest(client.challenge, challenge), "challenge")
    _expect(client.origin in origins and not framed, "origin")
    _expect(hmac.compare_digest(data.rp_id_hash, _rp_hash(rp_id)), "rp")
    _expect(data.flags.up, "presence")
    _expect(data.flags.uv, "verification")
    try:
        verified = verify_authentication_response(
            credential=credential,
            expected_challenge=challenge,
            expected_rp_id=rp_id,
            expected_origin=list(origins),
            credential_public_key=known.public_key,
            credential_current_sign_count=0,
            require_user_verification=True,
        )
    except InvalidBackupFlags as e:  # checked by the library once the signature verified
        _reject("backup", e)
    except Exception as e:  # a refusal, never a 500
        _reject("signature", e)
    _expect(data.flags.be == known.backup_eligible, "backup")
    handle = credential.response.user_handle
    _expect(handle is None or hmac.compare_digest(handle, user_handle), "user")
    return Assertion(
        credential_id=known.credential_id,
        sign_count=verified.new_sign_count,
        backed_up=verified.credential_backed_up,
    )


def counter_moved(stored: int, new: int) -> bool:
    """Whether an assertion's use count is one the real authenticator could give: forward
    of the stored one, or 0 on both sides (an authenticator that keeps none, as synced
    passkeys do). False -- including a count that fell back to 0 -- is what a copy of the
    authenticator answers: a gate revokes the passkey (decisions I24)."""
    return (stored == 0 and new == 0) or new > stored


#: What a verified assertion's use count means (:func:`judge_counter`).
CounterVerdict = Literal["moved", "copy", "behind"]


def judge_counter(stored: int, new: int, *, backup_eligible: bool) -> CounterVerdict:
    """``moved``: the count went forward (or is 0 on both sides): the answer confirms.
    ``copy``: it did not, and the passkey lives on one device (not backup eligible): a copy
    of that device answered -- a gate revokes the passkey and asks every sign-in of the party
    to confirm again. ``behind``: it did not, but the passkey is synced (backup eligible):
    its copies on a password manager's other devices are meant, so this is no clone signal
    -- a gate refuses that answer (``counter``) and revokes nothing (decisions I24)."""
    if counter_moved(stored, new):
        return "moved"
    return "behind" if backup_eligible else "copy"


# --------------------------------------------------------------------------- what both gates say

#: 403 ``confirmation_required`` of a ``recent`` row.
RECENT_REQUIRED = (
    "adding a passkey needs you to confirm it's you now (in the last 5 minutes), even within "
    "the hour: a passkey can confirm every change after it"
)
UNAVAILABLE = "this gate takes no passkeys: confirm it's you by email"
NO_PASSKEYS = "this account has no passkey: confirm it's you by email"
EXISTS = "that passkey is registered already; nothing was added"
GONE = "this passkey request is gone; ask again"
REJECTED = "the passkey's answer was not accepted; nothing changed"
CLONED = (
    "this passkey answered with an old use count, as a copy of it would: Kontra revoked it. "
    "Confirm by email, and check your passkeys"
)
OPTIONS_LIMITED = "this sign-in asked for passkey requests too often; try again later"
FAILURES_LIMITED = "too many passkey answers of this sign-in were refused; try again later"

SUBJECT_ADDED = "A passkey was added to your Kontra account"
SUBJECT_REVOKED = "A passkey of yours was revoked"
SUBJECT_REMOVED = "A passkey was removed from your Kontra account"


def _hm(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%H:%M")


def _quoted(label: str) -> str:
    """A label in double quotes, its own ``\\`` and ``"`` escaped: whoever adds a passkey
    names it, and a name must never close its quotation and speak as Kontra in the notice
    that warns the owner."""
    return '"' + label.replace("\\", "\\\\").replace('"', '\\"') + '"'


def added_mail(
    label: str, at: datetime, *, via: str, signed_in: datetime, session_prefix: str
) -> str:
    """The "passkey added" notice: which passkey, when, and from which sign-in (how it
    signed in, when, the first 12 hex of its id), so a person who did not add it knows what
    to revoke and which sign-in to end. No link, nothing secret. Times are UTC."""
    return (
        f"A passkey named {_quoted(label)} was added to your Kontra account at {_hm(at)} UTC, "
        f"from one of your sign-ins ({via}, signed in "
        f"{signed_in.astimezone(UTC):%Y-%m-%d %H:%M} UTC, id {session_prefix}). It can confirm "
        "it's you before changes; it cannot sign anyone in.\n\n"
        "If you did not add it: in the Kontra console, open Passkeys and revoke it, then open "
        "API keys > Where you are signed in and sign out the sign-in above.\n"
    )


def removed_mail(
    label: str,
    at: datetime,
    *,
    via: str | None = None,
    signed_in: datetime | None = None,
    session_prefix: str | None = None,
) -> str:
    """The "passkey removed" notice of a revocation: by one of the party's sign-ins (``via``,
    ``signed_in`` and ``session_prefix`` name it, as in :func:`added_mail`), or by Kontra's
    operators (none given). Which passkey, when, and what to do if it was not them. No link,
    nothing secret. Times are UTC."""
    if session_prefix is None or via is None or signed_in is None:
        return (
            f"A passkey named {_quoted(label)} was removed from your Kontra account at "
            f"{_hm(at)} UTC by Kontra's operators. It no longer confirms it's you, and your "
            "sign-ins will ask you to confirm it's you again before their next change.\n\n"
            "If you did not expect this: check Passkeys and your sign-ins in the Kontra console. "
            "You can add a passkey again once you have confirmed it's you.\n"
        )
    return (
        f"A passkey named {_quoted(label)} was removed from your Kontra account at {_hm(at)} "
        f"UTC, from one of your sign-ins ({via}, signed in "
        f"{signed_in.astimezone(UTC):%Y-%m-%d %H:%M} UTC, id {session_prefix}). It no longer "
        "confirms it's you; your other sign-ins will ask you to confirm it's you again before "
        "their next change.\n\n"
        "If you did not remove it: someone holds that sign-in. In the Kontra console, open API "
        "keys > Where you are signed in and sign out the sign-in above, then check Passkeys.\n"
    )


def revoked_mail(label: str, at: datetime) -> str:
    """The notice of a passkey revoked on a use count that went backwards."""
    return (
        f"Kontra revoked your passkey {_quoted(label)} at {_hm(at)} UTC: it answered with an old "
        "use count, which is what a copy of it does. Your sign-ins will ask you to confirm it's "
        "you again before their next change.\n\n"
        "If you have not copied it yourself (a backup restored on another device), someone may "
        "hold a copy: check Passkeys and your sign-ins in the Kontra console.\n"
    )
