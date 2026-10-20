"""``kontra dev up --persist``: the dev gate's state as tagged JSON, never pickle (decisions I34).

A snapshot is one JSON document whose values carry a type tag (``$dt``, ``$dec``, ``$b64``,
``$set``, ``$dc`` ...). It is decoded **only** into an explicit allowlist of record classes
(:data:`ALLOWED`): a state file that could name any callable would be code execution at the next
``up``. Only the durable state is kept (:data:`KEPT`); quotes, device flows, links, ceremonies,
rate counters, the outbox, locks and relay tasks are transient and start empty (:data:`TRANSIENT`).
Under ``--persist`` every ``bytes`` value of :data:`BLOB_MIN` or more (claim-check blobs, artifact
parts) is a file ``data/blobs/<sha256>`` written once (:class:`BlobFiles`), named in ``gate.json``
by its digest (``$blob``), so a snapshot neither re-encodes nor re-writes a big blob (§5.4).

The drift guard is ``tests/devgate/test_store.py``: a full lifecycle through ``Marketplace``,
snapshot, restore into a fresh gate, every view compared, and a class the census finds but
:data:`ALLOWED` lacks fails the test -- so a new ``DevGate`` field cannot be silently dropped.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import enum
import hashlib
import importlib
import json
import os
import re
import shutil
import tempfile
from collections import deque
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

__all__ = [
    "ALLOWED",
    "BLOB_MIN",
    "FORMAT",
    "KEPT",
    "LEDGER_KEPT",
    "TRANSIENT",
    "BlobFiles",
    "Collected",
    "dec",
    "digest",
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
    "kontra.devgate.ledger": ("_Account", "_HoldRow", "_Movement"),
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

#: A ``bytes`` value at least this long is kept under ``--persist`` as its own file (written
#: once, :class:`BlobFiles`); a shorter one stays inline as ``$b64``.
BLOB_MIN = 4096
_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)

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


def enc(value: Any, blobs: Collected | None = None) -> Any:
    """``value`` as tagged JSON. With ``blobs``, a ``bytes`` of :data:`BLOB_MIN` or more is
    ``{"$blob": <sha256>}`` and ``blobs`` holds its bytes for :meth:`BlobFiles.commit`."""
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
        if blobs is not None and type(value) is bytes and len(value) >= BLOB_MIN:
            return {"$blob": blobs.put(value)}
        return {"$b64": base64.b64encode(bytes(value)).decode()}
    if isinstance(value, (set, frozenset)):
        return {"$set": sorted((enc(x, blobs) for x in value), key=json.dumps)}
    if isinstance(value, deque):
        return {"$deque": [enc(x, blobs) for x in value]}
    if isinstance(value, tuple):
        return {"$tuple": [enc(x, blobs) for x in value]}
    if isinstance(value, list):
        return [enc(x, blobs) for x in value]
    if isinstance(value, dict):
        if all(isinstance(k, str) for k in value):
            return {"$map": {k: enc(x, blobs) for k, x in value.items()}}
        return {"$pairs": [[enc(k, blobs), enc(x, blobs)] for k, x in value.items()]}
    path = f"{type(value).__module__}.{type(value).__qualname__}"
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        _cls(path)  # refuse to WRITE what could not be read back
        drop = _DROP_FIELDS.get(path, frozenset())
        fields = {
            f.name: enc(getattr(value, f.name), blobs)
            for f in dataclasses.fields(value)
            if f.init and f.name not in drop and not isinstance(getattr(value, f.name), _SKIP_TYPES)
        }
        return {"$dc": path, "f": fields}
    if hasattr(type(value), "model_fields"):  # a pydantic model
        _cls(path)
        return {"$pd": path, "v": cast(Any, value).model_dump(mode="json")}
    raise TypeError(f"cannot store {path}")


def dec(value: Any, load: Callable[[str], bytes] | None = None) -> Any:
    """Tagged JSON back to its value; ``load`` reads a ``$blob`` (:meth:`BlobFiles.load`)."""
    if isinstance(value, list):
        return [dec(x, load) for x in value]
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
    if tag == "$blob":
        if load is None:
            raise ValueError("a dev state document names a blob file, but no blob directory")
        return load(str(value["$blob"]))
    if tag == "$set":
        return {dec(x, load) for x in value["$set"]}
    if tag == "$deque":
        return deque(dec(x, load) for x in value["$deque"])
    if tag == "$tuple":
        return tuple(dec(x, load) for x in value["$tuple"])
    if tag == "$map":
        return {k: dec(x, load) for k, x in value["$map"].items()}
    if tag == "$pairs":
        return {dec(k, load): dec(x, load) for k, x in value["$pairs"]}
    if tag == "$dc":
        return _cls(value["$dc"])(**{k: dec(x, load) for k, x in value["f"].items()})
    if tag == "$pd":
        return _cls(value["$pd"]).model_validate(value["v"])
    if tag is not None:
        raise ValueError(f"unknown tag in a dev state file: {tag}")
    return value


def snapshot(gate: Any, blobs: Collected | None = None) -> dict[str, Any]:
    """``gate``'s durable state as a :data:`FORMAT` document (call on the gate's loop); with
    ``blobs``, big byte strings are named by digest rather than inlined (:func:`enc`)."""
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
        "gate": {name: enc(getattr(gate, name), blobs) for name in KEPT},
        "ledger": {name: enc(getattr(gate.ledger, name), blobs) for name in LEDGER_KEPT},
    }


def restore(gate: Any, doc: dict[str, Any], blobs: BlobFiles | None = None) -> None:
    """Load a :data:`FORMAT` document into ``gate`` (a port change keeps the state: old tokens
    are refused on issuer and clients refresh transparently, §5.4), reading its ``$blob`` s
    from ``blobs``. Refuses an unknown format, and a blob file that is missing, not ours, or
    not the bytes its name says."""
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
    read: dict[str, bytes] = {}

    def load(sha: str) -> bytes:
        if blobs is None:
            raise ValueError("a dev state document names a blob file, but no blob directory")
        if sha not in read:
            read[sha] = blobs.load(sha)
        return read[sha]

    kept = {name: dec(value, load) for name, value in doc["gate"].items()}
    ledger = {name: dec(value, load) for name, value in doc["ledger"].items()}
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


def write_snapshot(gate: Any, path: Path, files: BlobFiles | None = None) -> str:
    """Snapshot ``gate`` (on its loop) to ``path``, its big byte strings to ``files`` (default
    ``path``'s ``blobs/`` beside it); the :func:`digest` of what was written."""
    files = files if files is not None else BlobFiles(path.parent / "blobs")
    collected = files.collect()
    # No ``default=``: :func:`enc` yields JSON values only, and anything else must fail loudly
    # rather than be written as a string that no longer decodes to what it was.
    raw = json.dumps(snapshot(gate, collected))
    files.commit(path, raw, collected.refs)
    files.remember(collected)
    return digest(raw)


def digest(raw: str) -> str:
    """What the persist loop compares a pass by (it keeps this, never the document)."""
    return hashlib.sha256(raw.encode()).hexdigest()


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
    _write_private(path, raw.encode())


def _write_private(path: Path, data: bytes) -> None:
    """``data`` as ``path``, 0600, atomically, through ``mkstemp`` (``O_EXCL``, a random name)
    in a directory of ours (:func:`~kontra.devgate.state.private_dir`): never a predictable
    temp name that a planted link would turn into a write of somewhere else."""
    from kontra.devgate.state import private_dir

    private_dir(path.parent)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}-")
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as f:  # mkstemp's file is 0600
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# --------------------------------------------------------------------------- blob files


class Collected:
    """The byte strings one snapshot names by digest, filled by :func:`enc` on the gate's loop
    (:meth:`BlobFiles.collect`). A ``bytes`` object's digest is reused while the gate holds
    that same object (bytes are immutable), so a 50 MiB blob is hashed once, not every pass."""

    def __init__(self, known: dict[int, tuple[bytes, str]]) -> None:
        self._known = known
        self.seen: dict[int, tuple[bytes, str]] = {}
        self.refs: dict[str, bytes] = {}

    def put(self, data: bytes) -> str:
        hit = self.seen.get(id(data)) or self._known.get(id(data))
        sha = hit[1] if hit is not None and hit[0] is data else hashlib.sha256(data).hexdigest()
        self.seen[id(data)] = (data, sha)
        self.refs[sha] = data
        return sha


class BlobFiles:
    """``data/blobs/<sha256>`` (0700, its files 0600): every byte string a ``--persist``
    snapshot names by digest, each written once."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self._known: dict[int, tuple[bytes, str]] = {}

    def collect(self) -> Collected:
        return Collected(self._known)

    def remember(self, collected: Collected) -> None:
        """Keep ``collected``'s digests for the next pass (and only those: what the gate let
        go of is not held here)."""
        self._known = collected.seen

    def commit(self, path: Path, raw: str, refs: Mapping[str, bytes]) -> None:
        """Write each of ``refs`` not on disk yet, then ``raw`` as ``path`` (so ``gate.json``
        never names a missing file), then remove the blob files nothing names any more. Only
        immutable bytes are touched: safe off the event loop."""
        from kontra.devgate.state import private_dir

        private_dir(self.root)
        for sha, data in refs.items():
            blob = self.root / sha
            if not blob.is_file():
                _write_private(blob, data)
        write_doc(path, raw)
        for entry in self.root.iterdir():
            if _SHA256.fullmatch(entry.name) and entry.name not in refs:
                entry.unlink(missing_ok=True)

    def load(self, sha: str) -> bytes:
        """The blob file ``sha``, refused unless it is this user's private file holding
        exactly the bytes that hash to its name."""
        from kontra.profile import ProfileError, _read_private

        reset = "`kontra dev down --reset` discards it"
        if not _SHA256.fullmatch(sha):
            raise ValueError(f"a dev state file names a blob {sha!r}; {reset}")
        try:
            data = _read_private(self.root / sha)
        except FileNotFoundError:
            raise ValueError(
                f"the dev state names blob {sha}, which {self.root} lacks; {reset}"
            ) from None
        except ProfileError:
            raise ValueError(
                f"the blob {self.root / sha} is not this user's private file; {reset}"
            ) from None
        if hashlib.sha256(data).hexdigest() != sha:
            raise ValueError(f"the blob {self.root / sha} is not the bytes it names; {reset}")
        return data

    def discard(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("kontra-sdk")
    except Exception:
        return "unknown"
