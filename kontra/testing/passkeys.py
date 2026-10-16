"""A software WebAuthn authenticator for tests (decisions I24).

:class:`SoftAuthenticator` makes the answers a browser hands the console --
``PublicKeyCredential.toJSON()`` of ``navigator.credentials.create()`` and ``.get()`` -- from
real keys (``cryptography``) and real CBOR, so a gate verifies them exactly as it verifies a
person's device. The fake gate's tests and the platform's own use it (imported, never copied).
It needs the ``testing`` extra (``webauthn`` brings the CBOR encoder).

It enforces what a browser enforces before an authenticator answers -- ``excludeCredentials``
(:class:`AuthenticatorRefused` ``excluded``, the browser's ``InvalidStateError``) and
``allowCredentials`` (``no_credential``) -- and nothing else, so a test can make it answer
what a misbehaving or hostile client could send: from another origin (:attr:`origin`), from
the console framed by another page (:attr:`top_origin`), signed for another RP ID
(:attr:`rp_id`), without user presence or verification (:attr:`up`, :attr:`uv`), with backup
flags that changed, a use count that went back (:attr:`counter`), a key type the options did
not offer (``algorithm``), or from a copy of its keys (:meth:`clone`). For tests only: its keys
live in memory and it signs whatever it is asked.
"""

from __future__ import annotations

import base64
import hashlib
import json
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, padding, rsa

__all__ = ["KEY_ALGORITHMS", "AuthenticatorRefused", "SoftAuthenticator", "SoftCredential"]

#: Why the authenticator would not answer, as the browser reports it: ``excluded``
#: (``InvalidStateError``: it holds one of ``excludeCredentials``), ``no_credential``
#: (``NotAllowedError``: it holds none of ``allowCredentials``).
Refusal = Literal["excluded", "no_credential"]
#: The COSE algorithms it makes keys for: EdDSA, ES256 and RS256, which a gate takes, and
#: ES512, which none does (to test the refusal).
KEY_ALGORITHMS: tuple[int, ...] = (-8, -7, -257, -36)

_UP, _UV, _BE, _BS, _AT = 0x01, 0x04, 0x08, 0x10, 0x40
_PrivateKey = ec.EllipticCurvePrivateKey | ed25519.Ed25519PrivateKey | rsa.RSAPrivateKey


class AuthenticatorRefused(Exception):
    """The authenticator did not answer (``reason``: :data:`Refusal`)."""

    def __init__(self, reason: Refusal) -> None:
        super().__init__(reason)
        self.reason: Refusal = reason


@dataclass(frozen=True, slots=True)
class SoftCredential:
    """One credential the authenticator holds (a passkey, seen from the device)."""

    credential_id: bytes
    rp_id: str
    user_handle: bytes
    algorithm: int
    #: The use count it keeps (0 until its first assertion; its answers say 0 while the
    #: authenticator's ``counter`` is off).
    sign_count: int


@dataclass(slots=True)
class _Credential:
    credential_id: bytes
    rp_id: str
    user_handle: bytes
    algorithm: int
    key: _PrivateKey
    sign_count: int = 0


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: object) -> bytes:
    if not isinstance(text, str):
        raise ValueError("not a base64url string")
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _ids(descriptors: object) -> list[bytes]:
    """The credential ids of ``excludeCredentials`` / ``allowCredentials``."""
    if not isinstance(descriptors, list):
        return []
    items = cast("list[object]", descriptors)
    return [_unb64(cast("Mapping[str, object]", d).get("id")) for d in items if isinstance(d, dict)]


def _new_key(algorithm: int) -> _PrivateKey:
    if algorithm == -7:
        return ec.generate_private_key(ec.SECP256R1())
    if algorithm == -36:
        return ec.generate_private_key(ec.SECP521R1())
    if algorithm == -8:
        return ed25519.Ed25519PrivateKey.generate()
    if algorithm == -257:
        return rsa.generate_private_key(public_exponent=65537, key_size=2048)
    raise ValueError(f"a SoftAuthenticator makes keys for {KEY_ALGORITHMS}, not {algorithm}")


def _cose(key: _PrivateKey, algorithm: int) -> dict[int, object]:
    """The credential public key as COSE_Key (RFC 9053), as an authenticator attests it."""
    if isinstance(key, ec.EllipticCurvePrivateKey):
        numbers = key.public_key().public_numbers()
        size = (key.curve.key_size + 7) // 8
        curve = 1 if isinstance(key.curve, ec.SECP256R1) else 3
        return {
            1: 2,
            3: algorithm,
            -1: curve,
            -2: numbers.x.to_bytes(size, "big"),
            -3: numbers.y.to_bytes(size, "big"),
        }
    if isinstance(key, ed25519.Ed25519PrivateKey):
        raw = key.public_key().public_bytes(
            serialization.Encoding.Raw, serialization.PublicFormat.Raw
        )
        return {1: 1, 3: algorithm, -1: 6, -2: raw}
    public = key.public_key().public_numbers()
    return {
        1: 3,
        3: algorithm,
        -1: public.n.to_bytes((public.n.bit_length() + 7) // 8, "big"),
        -2: public.e.to_bytes((public.e.bit_length() + 7) // 8, "big"),
    }


def _sign(key: _PrivateKey, data: bytes) -> bytes:
    if isinstance(key, ec.EllipticCurvePrivateKey):
        digest = hashes.SHA256() if isinstance(key.curve, ec.SECP256R1) else hashes.SHA512()
        return key.sign(data, ec.ECDSA(digest))
    if isinstance(key, ed25519.Ed25519PrivateKey):
        return key.sign(data)
    return key.sign(data, padding.PKCS1v15(), hashes.SHA256())


def _client_data(kind: str, challenge: object, origin: str, top_origin: str | None) -> bytes:
    if not isinstance(challenge, str):
        raise ValueError("the options carry no challenge")
    body: dict[str, object] = {"type": kind, "challenge": challenge, "origin": origin}
    # What a browser writes for a ceremony in a frame whose ancestors are of another origin.
    body["crossOrigin"] = top_origin is not None
    if top_origin is not None:
        body["topOrigin"] = top_origin
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


class SoftAuthenticator:
    """A software WebAuthn authenticator: real keys (ES256 by default), real CBOR, the
    answers ``PublicKeyCredential.toJSON()`` gives. For tests only.

    ``origin``: the page the answers say they come from (the console's origin; a phishing
    page's for a test). ``top_origin``: None, the page is a top-level one; set, the answers
    say the ceremony ran in a frame inside a page of that origin (``crossOrigin``,
    ``topOrigin``), as a browser writes when another site embeds the console. ``rp_id``: None
    signs for the options' RP ID, as a browser does; set, it signs for that one (another
    environment's, say). ``uv`` / ``up``: the user verification and presence flags it sets (a
    browser refuses a ``required`` request without UV; setting it False is a client that does
    not). ``counter``: True counts up by one per assertion (from 0 at registration); False
    always answers 0, as synced passkeys do. ``backup_eligible`` / ``backed_up``: the BE and
    BS flags. ``aaguid``: 16 bytes (zeros: hidden, as ``attestation: "none"`` makes most
    authenticators answer). ``transports``: what ``getTransports()`` reports. ``algorithm``:
    the COSE algorithm of the keys it makes (:data:`KEY_ALGORITHMS`), whatever the options
    offer. Every one of these but ``aaguid``, ``transports`` and ``algorithm`` may be changed
    between calls."""

    def __init__(
        self,
        *,
        origin: str,
        rp_id: str | None = None,
        uv: bool = True,
        counter: bool = True,
        backup_eligible: bool = False,
        backed_up: bool = False,
        aaguid: bytes = bytes(16),
        transports: Sequence[str] = ("internal",),
        algorithm: int = -7,
        up: bool = True,
        top_origin: str | None = None,
    ) -> None:
        if len(aaguid) != 16:
            raise ValueError("an AAGUID is 16 bytes")
        if algorithm not in KEY_ALGORITHMS:
            raise ValueError(f"a SoftAuthenticator makes keys for {KEY_ALGORITHMS}")
        self.origin = origin
        self.top_origin = top_origin
        self.rp_id = rp_id
        self.uv = uv
        self.up = up
        self.counter = counter
        self.backup_eligible = backup_eligible
        self.backed_up = backed_up
        self.aaguid = bytes(aaguid)
        self.transports: tuple[str, ...] = tuple(transports)
        self.algorithm = algorithm
        self._credentials: list[_Credential] = []

    @property
    def credentials(self) -> tuple[SoftCredential, ...]:
        """The credentials it holds, oldest first."""
        return tuple(
            SoftCredential(c.credential_id, c.rp_id, c.user_handle, c.algorithm, c.sign_count)
            for c in self._credentials
        )

    def clone(self) -> SoftAuthenticator:
        """A copy of the device: the same keys and use counts, counting on its own from now
        (what a cloned security key, or a restored backup of a counting one, is)."""
        copy = SoftAuthenticator(
            origin=self.origin,
            rp_id=self.rp_id,
            uv=self.uv,
            counter=self.counter,
            backup_eligible=self.backup_eligible,
            backed_up=self.backed_up,
            aaguid=self.aaguid,
            transports=self.transports,
            algorithm=self.algorithm,
            up=self.up,
            top_origin=self.top_origin,
        )
        copy._credentials = [replace(c) for c in self._credentials]
        return copy

    def _flags(self) -> int:
        return (
            (_UP if self.up else 0)
            | (_UV if self.uv else 0)
            | (_BE if self.backup_eligible else 0)
            | (_BS if self.backed_up else 0)
        )

    def _rp_hash(self, options_rp_id: object) -> tuple[str, bytes]:
        """The RP ID the answer is for -- the options' (else the origin's host, as a browser
        defaults it) -- and the one it signs for."""
        rp_id = options_rp_id if isinstance(options_rp_id, str) else urlsplit(self.origin).hostname
        if not rp_id:
            raise ValueError("no RP ID in the options and none in the origin")
        return rp_id, hashlib.sha256((self.rp_id or rp_id).encode("ascii")).digest()

    def _attachment(self) -> str:
        return "platform" if "internal" in self.transports else "cross-platform"

    def create(
        self, options: Mapping[str, Any], *, credential_id: bytes | None = None
    ) -> dict[str, Any]:
        """``navigator.credentials.create()`` for ``options``
        (``PublicKeyCredentialCreationOptionsJSON``): a new credential, attestation ``none``,
        as ``RegistrationResponseJSON``. ``credential_id``: use that id rather than 16 random
        bytes (a test of an id registered already)."""
        excluded = set(_ids(options.get("excludeCredentials")))
        if any(c.credential_id in excluded for c in self._credentials):
            raise AuthenticatorRefused("excluded")
        rp: object = options.get("rp")
        user: object = options.get("user")
        if not isinstance(rp, dict) or not isinstance(user, dict):
            raise ValueError("creation options carry rp and user")
        rp_id, rp_hash = self._rp_hash(cast("Mapping[str, object]", rp).get("id"))
        handle = _unb64(cast("Mapping[str, object]", user).get("id"))
        client_data = _client_data(
            "webauthn.create", options.get("challenge"), self.origin, self.top_origin
        )
        cid = secrets.token_bytes(16) if credential_id is None else credential_id
        key = _new_key(self.algorithm)
        cose: bytes = _encode_cbor(_cose(key, self.algorithm))
        auth_data = (
            rp_hash
            + bytes([self._flags() | _AT])
            + (0).to_bytes(4, "big")
            + self.aaguid
            + len(cid).to_bytes(2, "big")
            + cid
            + cose
        )
        attestation: bytes = _encode_cbor({"fmt": "none", "attStmt": {}, "authData": auth_data})
        self._credentials.append(_Credential(cid, rp_id, handle, self.algorithm, key))
        spki = key.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return {
            "id": _b64(cid),
            "rawId": _b64(cid),
            "type": "public-key",
            "response": {
                "clientDataJSON": _b64(client_data),
                "attestationObject": _b64(attestation),
                "authenticatorData": _b64(auth_data),
                "transports": list(self.transports),
                "publicKeyAlgorithm": self.algorithm,
                "publicKey": _b64(spki),
            },
            "authenticatorAttachment": self._attachment(),
            "clientExtensionResults": {},
        }

    def get(
        self, options: Mapping[str, Any], *, credential_id: bytes | None = None
    ) -> dict[str, Any]:
        """``navigator.credentials.get()`` for ``options``
        (``PublicKeyCredentialRequestOptionsJSON``): an assertion of the first credential it
        holds that ``allowCredentials`` lists (with none listed, the first it holds for the RP
        ID), as ``AuthenticationResponseJSON`` with the ``userHandle``. ``credential_id``:
        answer with that credential whatever the options list (a client that ignores them)."""
        rp_id, rp_hash = self._rp_hash(options.get("rpId"))
        allowed = _ids(options.get("allowCredentials"))
        if credential_id is not None:
            found = [c for c in self._credentials if c.credential_id == credential_id]
        elif allowed:
            found = [c for c in self._credentials if c.credential_id in allowed]
        else:
            found = [c for c in self._credentials if c.rp_id == rp_id]
        if not found:
            raise AuthenticatorRefused("no_credential")
        credential = found[0]
        if self.counter:
            credential.sign_count += 1
        count = credential.sign_count if self.counter else 0
        client_data = _client_data(
            "webauthn.get", options.get("challenge"), self.origin, self.top_origin
        )
        auth_data = rp_hash + bytes([self._flags()]) + count.to_bytes(4, "big")
        signature = _sign(credential.key, auth_data + hashlib.sha256(client_data).digest())
        return {
            "id": _b64(credential.credential_id),
            "rawId": _b64(credential.credential_id),
            "type": "public-key",
            "response": {
                "clientDataJSON": _b64(client_data),
                "authenticatorData": _b64(auth_data),
                "signature": _b64(signature),
                "userHandle": _b64(credential.user_handle),
            },
            "authenticatorAttachment": self._attachment(),
            "clientExtensionResults": {},
        }


def _encode_cbor(value: object) -> bytes:
    from webauthn.helpers import encode_cbor

    return encode_cbor(value)
