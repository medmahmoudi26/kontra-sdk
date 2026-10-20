"""``kontra dev up --persist``: the dev gate's state as tagged JSON, never pickle (decisions I34).

A snapshot is one JSON document whose values carry a type tag (``$dt``, ``$dec``, ``$b64``,
``$set``, ``$dc`` ...). It is decoded **only** into an explicit allowlist of record classes
(:data:`ALLOWED`): a state file that could name any callable would be code execution at the next
``up``. Only the durable state is kept (:data:`KEPT`); quotes, device flows, links, ceremonies,
rate counters, the outbox, locks and relay tasks are transient and start empty (:data:`TRANSIENT`).

The drift guard is ``tests/devgate/test_store.py``: a full lifecycle through ``Marketplace``,
snapshot, restore into a fresh gate, every view compared, and a class the census finds but
:data:`ALLOWED` lacks fails the test -- so a new ``DevGate`` field cannot be silently dropped.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import enum
import importlib
import json
import os
import socket
from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

__all__ = [
    "ALLOWED",
    "FORMAT",
    "KEPT",
    "LEDGER_KEPT",
    "TRANSIENT",
    "dec",
    "enc",
    "read_doc",
    "restore",
    "snapshot",
    "write_doc",
    "write_snapshot",
]

FORMAT = "kontra-devgate/1"

#: What a snapshot may hold: these classes and nothing else. Keyed on where the class lives now
#: (``kontra.devgate.gate`` after the FakeGate move, decisions I34). It is the census of every
#: record class reachable from :data:`KEPT` and :data:`LEDGER_KEPT` (``tests/devgate/test_store.py``
#: walks the field types and fails on a class missing here, or listed here and never reached).
ALLOWED: dict[str, tuple[str, ...]] = {
    "kontra.devgate.gate": (
        "_Party", "_PartyKey", "_Session", "_Refresh", "_DeviceKey", "_KeyWrapRow",
        "_Offering", "_Job", "_ApiKey", "_Artifact", "_PasskeyUser", "_Passkey", "_RelayState",
    ),
    "kontra.escrow": ("JobRecord", "ObservedStatus", "EffectKind"),
    "kontra.engine": ("Liveness",),
    "kontra.gate_client.models": ("Escrow", "JobState", "StreamEvent"),
    "kontra.ledger": ("Money", "Account", "Handle", "HandleState", "HoldState", "Op", "Posting", "Split"),
    "kontra.testing.ledger": ("_Account", "_HoldRow", "_Movement"),
    "kontra.contract.validate": ("ContractInfo",),
    "kontra.contract.names": ("OfferingId", "Version"),
    "kontra.contract.facts": ("XKontra",),
}  # fmt: skip

#: Gate attributes kept; everything else is transient and starts empty.
KEPT = (
    "parties", "sessions", "_refresh", "devices", "job_key_wraps", "api_keys", "_api_key_ids",
    "identities", "allowed_emails", "offerings", "_by_endpoint", "jobs", "artifacts", "blobs",
    "_passkey_users", "_passkey_rows",
)  # fmt: skip
LEDGER_KEPT = ("_holds", "_moves", "_accounts", "_postings", "_next_event")
#: The gate's other stores, deliberately not kept (short-lived, or rebuilt by the constructor):
#: quotes (10-minute TTL), device flows, magic links, confirmations, ceremonies, handoffs, rate
#: counters, the outbox, per-party locks, the engines. The drift guard fails on a store of state
#: (a dict, list, set or deque attribute) that is in neither this nor :data:`KEPT`.
TRANSIENT = (
    "quotes", "_quote_rate", "_devices", "_magic", "_confirmations", "_handoffs", "handoffs",
    "_ceremonies", "_magic_rate", "outbox", "_party_locks", "engines",
)  # fmt: skip

#: Transient object types dropped field-by-field (a running task, a lock, a wake event).
_SKIP_TYPES: tuple[type, ...] = (
    asyncio.Event,
    asyncio.Lock,
    asyncio.Task,
    asyncio.Handle,
    asyncio.TimerHandle,
)
#: Fields dropped by name: live state of a running process, which a restored gate starts afresh
#: (§5.4). A job's stored stream (``events``, ``progress_final``, ``last_progress``) and its
#: relay's persisted offset and budgets are kept, so cursors still line up with the offset a
#: resumed relay polls from; the relay's task, its pause timer and its listener count are not
#: (no listener survives a restart: the relay starts again at the next one, from the offset).
_DROP_FIELDS = {
    "kontra.devgate.gate._Job": frozenset({"changed"}),
    "kontra.devgate.gate._RelayState": frozenset({"task", "pause", "listeners"}),
}


def _cls(path: str) -> type:
    """The class ``path`` names, if :data:`ALLOWED` permits it; else refuse (never import it)."""
    module, _, name = path.rpartition(".")
    if name not in ALLOWED.get(module, ()):
        raise ValueError(f"a dev state file may not name {path}")
    try:
        return getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as e:
        raise ValueError(f"a dev state file names {path}, which does not exist here: {e}") from None


def enc(value: Any) -> Any:
    if (
        value is None
        or isinstance(value, bool)
        or (isinstance(value, (int, float, str)) and not isinstance(value, enum.Enum))
    ):
        return value
    if isinstance(value, enum.Enum):
        return {"$enum": f"{type(value).__module__}.{type(value).__qualname__}", "v": value.value}
    if isinstance(value, datetime):
        return {"$dt": value.isoformat()}
    if isinstance(value, timedelta):
        return {"$td": value.total_seconds()}
    if isinstance(value, Decimal):
        return {"$dec": str(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"$b64": base64.b64encode(bytes(value)).decode()}
    if isinstance(value, (set, frozenset)):
        return {"$set": sorted((enc(x) for x in value), key=json.dumps)}
    if isinstance(value, deque):
        return {"$deque": [enc(x) for x in value]}
    if isinstance(value, tuple):
        return {"$tuple": [enc(x) for x in value]}
    if isinstance(value, list):
        return [enc(x) for x in value]
    if isinstance(value, dict):
        if all(isinstance(k, str) for k in value):
            return {"$map": {k: enc(x) for k, x in value.items()}}
        return {"$pairs": [[enc(k), enc(x)] for k, x in value.items()]}
    path = f"{type(value).__module__}.{type(value).__qualname__}"
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        _cls(path)  # refuse to WRITE what could not be read back
        drop = _DROP_FIELDS.get(path, frozenset())
        fields = {
            f.name: enc(getattr(value, f.name))
            for f in dataclasses.fields(value)
            if f.init and f.name not in drop and not isinstance(getattr(value, f.name), _SKIP_TYPES)
        }
        return {"$dc": path, "f": fields}
    if hasattr(type(value), "model_fields"):  # a pydantic model
        _cls(path)
        return {"$pd": path, "v": cast(Any, value).model_dump(mode="json")}
    raise TypeError(f"cannot store {path}")


def dec(value: Any) -> Any:
    if isinstance(value, list):
        return [dec(x) for x in value]
    if not isinstance(value, dict):
        return value
    tags = [k for k in value if isinstance(k, str) and k.startswith("$")]
    tag = tags[0] if tags else None
    if tag == "$enum":
        return _cls(value["$enum"])(value["v"])
    if tag == "$dt":
        return datetime.fromisoformat(value["$dt"])
    if tag == "$td":
        return timedelta(seconds=value["$td"])
    if tag == "$dec":
        return Decimal(value["$dec"])
    if tag == "$b64":
        return base64.b64decode(value["$b64"], validate=True)
    if tag == "$set":
        return {dec(x) for x in value["$set"]}
    if tag == "$deque":
        return deque(dec(x) for x in value["$deque"])
    if tag == "$tuple":
        return tuple(dec(x) for x in value["$tuple"])
    if tag == "$map":
        return {k: dec(x) for k, x in value["$map"].items()}
    if tag == "$pairs":
        return {dec(k): dec(x) for k, x in value["$pairs"]}
    if tag == "$dc":
        return _cls(value["$dc"])(**{k: dec(x) for k, x in value["f"].items()})
    if tag == "$pd":
        return _cls(value["$pd"]).model_validate(value["v"])
    if tag is not None:
        raise ValueError(f"unknown tag in a dev state file: {tag}")
    return value


def snapshot(gate: Any) -> dict[str, Any]:
    """``gate``'s durable state as a :data:`FORMAT` document (call on the gate's loop)."""
    from cryptography.hazmat.primitives import serialization

    key_pem = gate.keys._key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return {
        "format": FORMAT,
        "sdk": _sdk_version(),
        "signing_key": {"pem": key_pem, "kid": gate.keys.kid},
        "url_secret": enc(gate._url_secret),
        "job_key_proof_key": enc(gate.job_key_proof_key),
        "gate": {name: enc(getattr(gate, name)) for name in KEPT},
        "ledger": {name: enc(getattr(gate.ledger, name)) for name in LEDGER_KEPT},
    }


def restore(gate: Any, doc: dict[str, Any]) -> None:
    """Load a :data:`FORMAT` document into ``gate`` (a port change keeps the state: old tokens
    are refused on issuer and clients refresh transparently, §5.4). Refuses an unknown format."""
    import jwt.algorithms
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec

    if doc.get("format") != FORMAT:
        raise ValueError(
            f"the dev state in this home was written in {doc.get('format')!r}, not {FORMAT}; "
            "`kontra dev down --reset` discards it"
        )
    # Decode everything before touching the gate: a refused class (or any other error) leaves
    # it as it was, never half restored.
    unknown = (set(doc["gate"]) - set(KEPT)) | (set(doc["ledger"]) - set(LEDGER_KEPT))
    if unknown:
        raise ValueError(f"a dev state file may not set {sorted(unknown)}")
    kept = {name: dec(value) for name, value in doc["gate"].items()}
    ledger = {name: dec(value) for name, value in doc["ledger"].items()}
    url_secret = dec(doc["url_secret"])
    proof_key = dec(doc["job_key_proof_key"])
    key = serialization.load_pem_private_key(doc["signing_key"]["pem"].encode(), password=None)
    if not isinstance(key, ec.EllipticCurvePrivateKey):
        raise ValueError("a dev state file's signing key is not an ES256 key")
    ring = gate.keys
    ring._key = key
    ring.kid = doc["signing_key"]["kid"]
    jwk = json.loads(jwt.algorithms.ECAlgorithm.to_jwk(cast(Any, key.public_key())))
    jwk.update(kid=ring.kid, use="sig", alg="ES256")
    ring._jwk = jwk
    gate._url_secret = url_secret
    gate.job_key_proof_key = proof_key
    for name, value in kept.items():
        setattr(gate, name, value)
    for name, value in ledger.items():
        setattr(gate.ledger, name, value)


def write_snapshot(gate: Any, path: Path) -> None:
    # No ``default=``: :func:`enc` yields JSON values only, and anything else must fail loudly
    # rather than be written as a string that no longer decodes to what it was.
    write_doc(path, json.dumps(snapshot(gate)))


def read_doc(path: Path) -> dict[str, Any] | None:
    """The snapshot at ``path``, or None when there is none. Read through
    :func:`kontra.profile._read_private`: a file another user owns or may write is refused, as
    ``stack.json`` is (it holds the gate's signing key and every session)."""
    from kontra.profile import ProfileError, _read_private

    try:
        raw = _read_private(path)
    except FileNotFoundError:
        return None
    except ProfileError:
        raise ValueError(
            f"the dev state {path} is not this user's private file (another user owns it, or "
            "every user may write it); refusing to restore it. `kontra dev down --reset` "
            "discards it"
        ) from None
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise ValueError(f"{path} is not a dev state document")
    return cast(dict[str, Any], doc)


def write_doc(path: Path, raw: str) -> None:
    """Write ``raw`` to ``path`` as a 0600 file, atomically (temp, fsync, rename)."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{socket.gethostname()[:8]}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("kontra-sdk")
    except Exception:
        return "unknown"
