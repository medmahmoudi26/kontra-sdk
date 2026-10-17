"""Share to my account: the job-key wraps and proofs both gates use (decisions I25).

A job's key (decisions I7) is made where the job is hired. Each of the account's *devices* --
a browser profile's key, a ``kontra login`` machine's ``~/.kontra/key`` -- gets a copy of it,
encrypted to that device alone: a **wrap**, one age file with exactly one X25519 stanza, whose
plaintext is the CLI's key-file text (``# job: <id>``, ``# public key: <buyer_key>``, the
``AGE-SECRET-KEY-1...`` line), so ``age -d -i ~/.kontra/key wrap > ~/.kontra/jobs/<id>.key``
works by hand. The gate stores wraps and never reads them.

Only a holder of the job key may write a wrap: the gate encrypts a **challenge** to the job's
``buyer_key`` -- an HMAC over the job, the asking session and a ten-minute bucket, under a key
derived from the gate's signing secret -- and a share must carry it decrypted (the **proof**).
Nothing is stored for it. A stolen API key can then neither plant junk for a new device nor
squat its slot (wraps are first-write-wins).

The platform gate imports this module, as it imports :mod:`kontra._passkeys` and
:mod:`kontra._control`, and :class:`~kontra.testing.fake_gate.FakeGate` uses it too: the proof,
the wrap's shape and the notices are written once. The clients (``kontra.hire``, the console's
``devicekey.js``) make and open wraps the same way.

Nothing here logs a wrap, a proof, a challenge or a key.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
from datetime import UTC, datetime

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from kontra.codec import age
from kontra.gate_client.models import (
    CHALLENGE_MAX,
    DEVICE_ADDS_PER_HOUR,
    DEVICES_MAX,
    SHARE_ITEMS_MAX,
    WRAP_MAX,
)

__all__ = [
    "BUCKET_SECONDS",
    "CHALLENGE_MAX",
    "DEVICES_MAX",
    "PROOF_PATTERN",
    "SHARE_ITEMS_MAX",
    "SUBJECT_DEVICE_ADDED",
    "SUBJECT_DEVICE_REMOVED",
    "WRAP_MAX",
    "InvalidWrap",
    "JobKeyMismatch",
    "check_proof",
    "check_wrap_shape",
    "device_added_mail",
    "device_removed_mail",
    "expected_proof",
    "is_proof",
    "key_file_text",
    "make_challenge",
    "open_challenge",
    "proof_key",
    "unwrap_job_key",
    "wrap_job_key",
]

#: A challenge's bucket: a proof is good for the bucket it was made in and the next one, so
#: for ten to twenty minutes.
BUCKET_SECONDS = 600
#: A proof: base64url (unpadded) of an HMAC-SHA256.
PROOF_PATTERN = r"^[A-Za-z0-9_-]{43}$"

_CONTEXT = b"kontra:job-key:v1"
_PROOF = re.compile(r"[A-Za-z0-9_-]{43}", re.ASCII)
_B64 = re.compile(r"[A-Za-z0-9+/]*={0,2}", re.ASCII)
#: Exactly what one X25519 recipient's header is, nothing else: the version line, one stanza
#: (its 43-character share, its 43-character body), the MAC line. 168 bytes.
_HEADER = re.compile(
    rb"age-encryption\.org/v1\n"
    rb"-> X25519 ([A-Za-z0-9+/]{43})\n"
    rb"([A-Za-z0-9+/]{43})\n"
    rb"--- ([A-Za-z0-9+/]{43})\n",
    re.ASCII,
)
_HEADER_SIZE = 168
#: After the header: the 16-byte nonce and at least one chunk's 16-byte tag.
_PAYLOAD_MIN = 16 + 16


class InvalidWrap(ValueError):
    """Not a wrap: bad base64, too large, not exactly one X25519 stanza, not for this key,
    or a plaintext that is not one job key."""


class JobKeyMismatch(ValueError):
    """A wrap that opens, but to a key other than the job's ``buyer_key``: a wrap moved from
    another job. It is ignored, never used."""


# --------------------------------------------------------------------------- the proof


def proof_key(secret: bytes) -> bytes:
    """The HMAC key of every challenge, from the gate's signing secret (the platform: the
    first of ``KONTRA_SIGNING_KEYS``). Rotating that secret voids the open challenges, and a
    client asks again."""
    if not secret:
        raise ValueError("the proof key needs a secret")
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=_CONTEXT + b":proof").derive(
        secret
    )


def _bucket(now: datetime) -> int:
    return int(now.astimezone(UTC).timestamp()) // BUCKET_SECONDS


def expected_proof(key: bytes, job_id: str, session_id: str, bucket: int) -> str:
    """The proof for ``job_id``, asked by ``session_id``, in ``bucket``."""
    message = b"\x00".join(
        (_CONTEXT, job_id.encode("ascii"), session_id.encode("ascii"), str(bucket).encode())
    )
    digest = hmac.new(key, message, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def is_proof(text: object) -> bool:
    return isinstance(text, str) and _PROOF.fullmatch(text) is not None


def make_challenge(key: bytes, job_id: str, session_id: str, buyer_key: str, now: datetime) -> str:
    """``JobKey.challenge``: this session's proof for the job, now, encrypted to the job's
    ``buyer_key`` (base64 of the age file). Only a holder of the job's key opens it."""
    proof = expected_proof(key, job_id, session_id, _bucket(now))
    sealed = age.encrypt(proof.encode("ascii"), [buyer_key])
    return base64.b64encode(sealed).decode("ascii")


def check_proof(key: bytes, job_id: str, session_id: str, proof: str, now: datetime) -> bool:
    """Whether ``proof`` is this session's for ``job_id``, made in this bucket or the one
    before (compared in constant time)."""
    if not is_proof(proof):
        return False
    bucket = _bucket(now)
    good = False
    for b in (bucket, bucket - 1):
        good |= hmac.compare_digest(expected_proof(key, job_id, session_id, b), proof)
    return good


def open_challenge(challenge: str, identity: str) -> str:
    """The proof inside ``challenge``, opened with the job's secret key. :class:`InvalidWrap`
    when it is not a challenge for that key."""
    try:
        sealed = base64.b64decode(challenge, validate=True)
    except (binascii.Error, ValueError):
        raise InvalidWrap("the challenge is not base64") from None
    if len(sealed) > CHALLENGE_MAX:
        raise InvalidWrap("the challenge is too large")
    try:
        proof = age.decrypt(sealed, identity).decode("ascii")
    except (age.AgeError, UnicodeDecodeError):
        raise InvalidWrap("the challenge does not open with this job's key") from None
    if not is_proof(proof):
        raise InvalidWrap("the challenge holds no proof")
    return proof


# --------------------------------------------------------------------------- the wrap


def key_file_text(job_id: str, secret: str) -> str:
    """The plaintext of a wrap: the CLI's job key file (without its ``created`` line)."""
    return f"# job: {job_id}\n# public key: {age.public_key_of(secret)}\n{secret}\n"


def check_wrap_shape(wrap: str) -> bytes:
    """The wrap's bytes, if they can be one: base64, at most :data:`WRAP_MAX` bytes, an age v1
    header with exactly one X25519 stanza (canonical base64 throughout) and a payload. What a
    gate checks; it cannot check the recipient or the content and does not try."""
    if len(wrap) > 4 * -(-WRAP_MAX // 3) or not _B64.fullmatch(wrap):
        raise InvalidWrap("a wrap is base64")
    try:
        data = base64.b64decode(wrap, validate=True)
    except (binascii.Error, ValueError):
        raise InvalidWrap("a wrap is base64") from None
    if len(data) > WRAP_MAX:
        raise InvalidWrap(f"a wrap is at most {WRAP_MAX} bytes")
    found = _HEADER.fullmatch(data[:_HEADER_SIZE])
    if found is None or len(data) < _HEADER_SIZE + _PAYLOAD_MIN:
        raise InvalidWrap("a wrap is an age file with exactly one X25519 stanza")
    for field in found.groups():
        raw = base64.b64decode(field + b"=")
        if base64.b64encode(raw).rstrip(b"=") != field:
            raise InvalidWrap("a wrap's header is canonical base64")
    return data


def wrap_job_key(job_id: str, secret: str, recipient: str) -> str:
    """A wrap of the job key ``secret`` for the device ``recipient`` (base64)."""
    sealed = age.encrypt(key_file_text(job_id, secret).encode("ascii"), [recipient])
    return base64.b64encode(sealed).decode("ascii")


def unwrap_job_key(wrap: str, identity: str, buyer_key: str) -> str:
    """The job's secret key from a wrap to this device, checked against the job's
    ``buyer_key`` (from the gate's answer for *this* job): :class:`JobKeyMismatch` for a wrap
    of another job's key, :class:`InvalidWrap` for anything that is not one job key for this
    device."""
    data = check_wrap_shape(wrap)
    try:
        text = age.decrypt(data, identity).decode("utf-8")
    except (age.AgeError, UnicodeDecodeError):
        raise InvalidWrap("the wrap does not open with this device's key") from None
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if len(lines) != 1 or not age.is_identity(lines[0]):
        raise InvalidWrap("a wrap holds exactly one AGE-SECRET-KEY-1... line")
    if age.public_key_of(lines[0]) != buyer_key:
        raise JobKeyMismatch("the wrap holds another job's key")
    return lines[0]


# --------------------------------------------------------------------------- what both gates say

SUBJECT_DEVICE_ADDED = "A device can now read your Kontra jobs"
SUBJECT_DEVICE_REMOVED = "A device was removed from your Kontra account"
#: 403 ``confirmation_required`` of ``POST /v1/me/devices``.
RECENT_REQUIRED = (
    "adding a device needs you to confirm it's you now (in the last 5 minutes), even within "
    "the hour: a device gets the keys of your past jobs too"
)
DEVICE_REVOKED = (
    "that key was removed as a device of this account and cannot be added again; use a new key"
)
TOO_MANY_DEVICES = (
    f"an account holds at most {DEVICES_MAX} devices, and none of yours is a browser that could "
    "make room; remove one first"
)
#: 429 ``rate_limited`` of ``POST /v1/me/devices``.
TOO_MANY_ADDED = (
    f"this account added {DEVICE_ADDS_PER_HOUR} devices in the last hour; try again later"
)
DEVICE_NOT_FOUND = "this account has no such device"
#: ``kind`` in words, for notices and listings.
KIND_WORDS = {"browser": "a browser", "cli": "the command line"}


def _hm(when: datetime) -> str:
    return when.astimezone(UTC).strftime("%H:%M")


def _quoted(label: str) -> str:
    """A label in double quotes, its own ``\\`` and ``"`` escaped: whoever adds a device
    names it, and a name must never close its quotation and speak as Kontra."""
    return '"' + label.replace("\\", "\\\\").replace('"', '\\"') + '"'


def device_added_mail(
    label: str,
    kind: str,
    at: datetime,
    *,
    via: str,
    signed_in: datetime,
    session_prefix: str,
    retired: str | None = None,
) -> str:
    """The "device added" notice: which device, when, and from which sign-in (how it signed
    in, when, the first 12 hex of its id). No link, nothing secret. Times are UTC.
    ``retired``: the label of the browser device that made room for it (the account held
    :data:`DEVICES_MAX`), said too."""
    room = (
        f"To make room, the browser {_quoted(retired)}, the device used least recently, is no "
        "longer one of your devices; it becomes one again by itself the next time it signs "
        "in.\n\n"
        if retired is not None
        else ""
    )
    return (
        f"A device named {_quoted(label)} ({KIND_WORDS.get(kind, kind)}) was added to your "
        f"Kontra account at {_hm(at)} UTC, from one of your sign-ins ({via}, signed in "
        f"{signed_in.astimezone(UTC):%Y-%m-%d %H:%M} UTC, id {session_prefix}). Your other "
        "devices now copy it the keys of the jobs you hire, past ones included, so it can read "
        "their results.\n\n"
        f"{room}"
        "If you did not add it: in the Kontra console, open Devices and remove it, then open "
        "API keys > Where you are signed in and sign out the sign-in above. What it already "
        "read cannot be taken back.\n"
    )


def device_removed_mail(
    label: str, kind: str, at: datetime, *, via: str, signed_in: datetime, session_prefix: str
) -> str:
    """The "device removed" notice, naming the sign-in that removed it."""
    return (
        f"A device named {_quoted(label)} ({KIND_WORDS.get(kind, kind)}) was removed from your "
        f"Kontra account at {_hm(at)} UTC, from one of your sign-ins ({via}, signed in "
        f"{signed_in.astimezone(UTC):%Y-%m-%d %H:%M} UTC, id {session_prefix}). It gets no "
        "keys of new jobs, and Kontra deleted the copies it kept for it; keys it had already "
        "opened cannot be taken back.\n\n"
        "If you did not remove it: someone holds that sign-in. In the Kontra console, open API "
        "keys > Where you are signed in and sign out the sign-in above.\n"
    )
