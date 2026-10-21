"""``kontra.devgate.store``: tagged JSON, a class allowlist, never pickle.

The drift guard (decisions I34, §5.4): a real lifecycle through :class:`Marketplace` -- a
hire whose job streams (decisions I31: the stored events, the relay's offset and budgets), one
accepted, one with artifacts in and out that is disputed -- is snapshotted, restored into a
fresh gate and compared view by view; the census of every record class reachable from what is
kept must equal :data:`ALLOWED`, and every store of state on the gate must be either kept or
named transient, so a new ``DevGate`` field cannot be silently dropped."""

from __future__ import annotations

import asyncio
import dataclasses
import enum
import json
import sys
import typing
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from kontra.devgate import gate as g
from kontra.devgate import store
from kontra.devgate.gate import DevGate
from kontra.devgate.store import ALLOWED, FORMAT, dec, enc, restore, snapshot
from kontra.gate_client import models as m
from kontra.ledger import Money, Posting
from kontra.testing import ledger as tl

# --------------------------------------------------------------------------- the codec


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


@pytest.mark.parametrize(
    "forged",
    [
        # A module the allowlist knows, a name it does not: the gate class itself.
        {"$dc": "kontra.devgate.gate.DevGate", "f": {}},
        # A pydantic model and an enum outside the allowlist.
        {"$pd": "kontra.gate_client.models.JobView", "v": {}},
        {"$enum": "signal.Signals", "v": 9},
        # A module with an import side effect, which must never be imported to find out.
        {"$dc": "this.Zen", "f": {}},
        # Nested deep inside what is kept.
        {"$map": {"j": {"$tuple": [{"$dc": "subprocess.Popen", "f": {"args": "id"}}]}}},
    ],
)
def test_a_forged_class_is_refused(forged: dict[str, Any]) -> None:
    before = set(sys.modules)
    with pytest.raises(ValueError, match="may not name"):
        dec(forged)
    assert "this" not in set(sys.modules) - before


def test_a_forged_class_in_a_snapshot_leaves_the_gate_as_it_was() -> None:
    gate = DevGate(None)
    doc = json.loads(json.dumps(snapshot(gate)))
    doc["gate"]["parties"] = {"$map": {"x": {"$dc": "os.system", "f": {}}}}
    other = DevGate(None)
    own = other.keys.kid
    with pytest.raises(ValueError, match="may not name"):
        restore(other, doc)
    assert other.parties == {}
    assert other.keys.kid == own  # not even the signing key was taken


def test_a_snapshot_may_set_only_what_is_kept() -> None:
    doc = json.loads(json.dumps(snapshot(DevGate(None))))
    doc["gate"]["base_url"] = "http://evil.example"
    with pytest.raises(ValueError, match="may not set"):
        restore(DevGate(None), doc)


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
    raw = json.dumps(doc)
    other = DevGate(None)
    restore(other, json.loads(raw))
    assert other.keys.kid == gate.keys.kid
    assert other._url_secret == gate._url_secret
    assert other.job_key_proof_key == gate.job_key_proof_key


def test_an_unknown_format_is_refused() -> None:
    with pytest.raises(ValueError, match="`kontra dev down --reset`"):
        restore(DevGate(None), {"format": "evil/9"})


def test_a_state_file_others_may_write_is_refused(tmp_path: Path) -> None:
    """``gate.json`` holds the signing key and every session: read only when it is this
    user's and no one else may write it (as ``stack.json``), never with a login hint."""
    path = tmp_path / "gate.json"
    assert store.read_doc(path) is None
    store.write_snapshot(DevGate(None), path)
    assert (path.stat().st_mode & 0o777) == 0o600
    assert store.read_doc(path) is not None
    path.chmod(0o666)
    with pytest.raises(ValueError, match="refusing to restore it") as e:
        store.read_doc(path)
    assert "kontra login" not in str(e.value)


# --------------------------------------------------------------------------- blob files


def test_big_byte_strings_are_files_written_once(tmp_path: Path) -> None:
    """§5.4: a claim-check blob (or an artifact part) of :data:`store.BLOB_MIN` or more is
    ``blobs/<sha256>``, 0600, named by digest in ``gate.json`` (no base64 of it there); it
    round-trips; a blob the gate let go of is removed at the next snapshot."""
    import hashlib

    gate = DevGate(None)
    big, small = b"b" * (1 << 20), b"s" * 10
    big_sha = hashlib.sha256(big).hexdigest()
    gate.blobs[big_sha] = big
    gate.blobs[hashlib.sha256(small).hexdigest()] = small
    path = tmp_path / "data" / "gate.json"
    files = store.BlobFiles(path.parent / "blobs")
    store.write_snapshot(gate, path, files)
    raw = path.read_text()
    assert len(raw) < 64 * 1024
    assert f'{{"$blob": "{big_sha}"}}' in raw
    blob = path.parent / "blobs" / big_sha
    assert blob.read_bytes() == big
    assert (blob.stat().st_mode & 0o777) == 0o600
    other = DevGate(None)
    doc = store.read_doc(path)
    assert doc is not None
    restore(other, doc, files)
    assert other.blobs == gate.blobs
    inode = blob.stat().st_ino
    store.write_snapshot(gate, path, files)
    assert blob.stat().st_ino == inode  # written once
    del gate.blobs[big_sha]
    store.write_snapshot(gate, path, files)
    assert not blob.exists()


def test_a_blob_file_is_hashed_once_while_the_gate_holds_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A snapshot every 10 s must not re-hash a 50 MiB blob each time."""
    import hashlib

    hashed: list[int] = []
    real = hashlib.sha256

    def counting(data: bytes = b"") -> Any:
        if len(data) >= store.BLOB_MIN:
            hashed.append(len(data))
        return real(data)

    gate = DevGate(None)
    gate.blobs["x" * 64] = b"q" * (1 << 20)
    files = store.BlobFiles(Path("/nonexistent"))
    monkeypatch.setattr(store.hashlib, "sha256", counting)
    for _ in range(3):
        collected = files.collect()
        store.snapshot(gate, collected)
        files.remember(collected)
    assert hashed == [1 << 20]


@pytest.mark.parametrize("damage", ["missing", "tampered", "others may write"])
def test_a_damaged_blob_file_is_refused(tmp_path: Path, damage: str) -> None:
    import hashlib

    gate = DevGate(None)
    big = b"d" * (64 * 1024)
    sha = hashlib.sha256(big).hexdigest()
    gate.blobs[sha] = big
    path = tmp_path / "gate.json"
    store.write_snapshot(gate, path)
    blob = tmp_path / "blobs" / sha
    if damage == "missing":
        blob.unlink()
    elif damage == "tampered":
        blob.write_bytes(b"e" * len(big))
    else:
        blob.chmod(0o666)
    doc = store.read_doc(path)
    assert doc is not None
    other = DevGate(None)
    with pytest.raises(ValueError, match="`kontra dev down --reset` discards it"):
        restore(other, doc, store.BlobFiles(tmp_path / "blobs"))
    assert other.blobs == {}  # refused before the gate was touched


# --------------------------------------------------------------------------- the census

#: What each kept store holds: the roots of the census. A test below checks every runtime
#: value of a real lifecycle against these.
ROOTS: dict[str, type] = {
    "parties": g._Party,
    "sessions": g._Session,
    "_refresh": g._Refresh,
    "devices": g._DeviceKey,
    "job_key_wraps": g._KeyWrapRow,
    "api_keys": g._ApiKey,
    "offerings": g._Offering,
    "jobs": g._Job,
    "artifacts": g._Artifact,
    "_passkey_users": g._PasskeyUser,
    "_passkey_rows": g._Passkey,
}
LEDGER_ROOTS: dict[str, type] = {
    "_holds": tl._HoldRow,
    "_moves": tl._Movement,
    "_accounts": tl._Account,
    "_postings": Posting,
}
#: Kept stores of plain values (strings, bytes, sets of strings, a counter): no record class.
PLAIN = {"_api_key_ids", "identities", "allowed_emails", "_by_endpoint", "blobs", "blob_scopes"}
LEDGER_PLAIN = {"_next_event"}


def _census() -> set[str]:
    """Every dataclass, pydantic model and enum reachable from the roots through field types
    (a pydantic model is a leaf: it is stored as its JSON), minus the dropped fields."""
    seen: set[str] = set()

    def walk(tp: Any) -> None:
        origin = typing.get_origin(tp)
        if origin is not None:
            for arg in typing.get_args(tp):
                if arg is not Ellipsis:
                    walk(arg)
            return
        supertype = getattr(tp, "__supertype__", None)  # a NewType
        if supertype is not None:
            walk(supertype)
            return
        if not isinstance(tp, type):
            return
        path = f"{tp.__module__}.{tp.__qualname__}"
        is_record = (
            dataclasses.is_dataclass(tp) or issubclass(tp, enum.Enum) or hasattr(tp, "model_fields")
        )
        if not is_record or path in seen:
            return
        seen.add(path)
        if dataclasses.is_dataclass(tp):
            hints = typing.get_type_hints(tp)
            drop = store._DROP_FIELDS.get(path, frozenset())
            for f in dataclasses.fields(tp):
                if f.init and f.name not in drop:
                    walk(hints[f.name])

    for root in (*ROOTS.values(), *LEDGER_ROOTS.values()):
        walk(root)
    return seen


def test_the_allowlist_is_the_census() -> None:
    """Every class a kept record can hold is allowed, and nothing else is: a new field of a
    new record type fails here (add it to ALLOWED), and so does an entry nothing can reach."""
    allowed = {f"{module}.{name}" for module, names in ALLOWED.items() for name in names}
    census = _census()
    assert census - allowed == set(), "kept but not allowed: snapshots would refuse to encode"
    assert allowed - census == set(), "allowed but never kept: remove it from ALLOWED"
    assert set(ROOTS) | PLAIN == set(store.KEPT)
    assert set(LEDGER_ROOTS) | LEDGER_PLAIN == set(store.LEDGER_KEPT)


def _stores(obj: object) -> set[str]:
    """The attributes of ``obj`` that hold state: a dict, list, set or deque."""
    return {k for k, v in vars(obj).items() if isinstance(v, (dict, list, set, deque))}


def test_every_store_of_state_is_kept_or_named_transient() -> None:
    """A new ``DevGate`` (or in-memory ledger) store is a decision: keep it or say it is
    transient. Neither fails here, so ``--persist`` never drops one silently."""
    gate = DevGate(None)
    assert _stores(gate) - set(store.KEPT) - set(store.TRANSIENT) == set()
    assert set(store.KEPT) & set(store.TRANSIENT) == set()
    assert _stores(gate.ledger) - set(store.LEDGER_KEPT) == set()


# --------------------------------------------------------------------------- a real lifecycle


async def _snapshot_on(gate: DevGate) -> dict[str, Any]:
    async def snap() -> dict[str, Any]:
        return store.snapshot(gate)

    return await gate.run(snap)


@pytest.mark.temporal
async def test_a_lifecycle_survives_a_snapshot_and_a_restore(
    dev_server: Any, tmp_path: Path
) -> None:
    from kontra.serve import Worker
    from kontra.testing import Marketplace
    from tests.hire.flows import beat, feed, make_file, quiet, read_doc, upper
    from tests.hire.models import FeedEvent
    from tests.hire.sellers import feed_worker, text_offering
    from tests.hire.support import handle

    _, feed_bound = feed_worker(tmp_path / "feed")
    text_bound = text_offering(tmp_path / "text")
    worker = Worker(
        offerings=[feed_bound, text_bound],
        activities=[feed, upper, beat, quiet, make_file, read_doc],
    )
    async with Marketplace(dev_server=dev_server, relay_interval=0.2) as mp:
        acme = await mp.party("acme")
        async with mp.serve(worker, acme):
            buyer = await mp.party(handle("buyer"), balance="10.00")
            k = buyer.kontra(client=mp.client(buyer.namespace))
            try:
                streamed = await (await k.quote(feed_bound.key, count=5, note="x")).hire(
                    max_price=1
                )
                events = [e async for e in streamed.stream("events", type=FeedEvent)]
                await streamed.result()
                await streamed.accept()
                doc_in = tmp_path / "doc.txt"
                doc_in.write_text("a document for the seller")
                sent = await k.upload(text_bound.key, doc_in)
                disputed = await k.hire(
                    text_bound.key, text="files", mode="artifact", beats=2, doc=sent
                )
                await disputed.result()
                await disputed.reject("not what I asked for")
            finally:
                await k.aclose()
            gate = mp.gate
            await asyncio.sleep(0.5)  # the relay settles: no listener, nothing in flight
            doc = await _snapshot_on(gate)

            async def balances() -> list[Any]:
                return await gate.ledger.balances()

            want_balances = await gate.run(balances)
    assert events == [FeedEvent(n=n, note="x") for n in range(5)]

    # Every kept value of the real gate is of the store's root type (the census's roots).
    for name, root in ROOTS.items():
        for value in getattr(gate, name).values():
            assert isinstance(value, root), (name, type(value))
    for name, root in LEDGER_ROOTS.items():
        held = getattr(gate.ledger, name)
        for value in held.values() if isinstance(held, dict) else held:
            assert isinstance(value, root), (name, type(value))

    raw = json.dumps(doc)  # JSON only: no `default=`
    other = DevGate(None)
    restore(other, json.loads(raw))

    # Byte for byte: what the restored gate would snapshot is what the original did.
    assert json.dumps(store.snapshot(other)) == raw
    # The views a client reads.
    assert other.keys.jwks() == gate.keys.jwks()
    assert set(other.parties) == set(gate.parties) >= {"acme", buyer.handle}
    assert set(other.sessions) == set(gate.sessions)
    assert set(other._refresh) == set(gate._refresh)
    assert {k: v.view() for k, v in other.devices.items()} == {
        k: v.view() for k, v in gate.devices.items()
    }
    assert other.job_key_wraps == gate.job_key_wraps
    for key, offering in gate.offerings.items():
        recipients = gate.parties[offering.id.party].recipients()
        assert other.offerings[key].entry(recipients) == offering.entry(recipients)
    assert other._by_endpoint == gate._by_endpoint
    assert await other.ledger.balances() == want_balances
    assert other.artifacts.keys() == gate.artifacts.keys()
    assert other.blobs == gate.blobs
    assert any(a.completed for a in other.artifacts.values())

    def views(of: DevGate) -> list[m.JobView]:
        return [of.jobs[j].view(buyer_key=True) for j in sorted(of.jobs)]

    assert views(other) == views(gate)
    assert other.jobs[streamed.job_id].record.escrow is m.Escrow.RELEASED
    assert other.jobs[disputed.job_id].record.escrow is m.Escrow.DISPUTED
    assert other.jobs[disputed.job_id].delivery == gate.jobs[disputed.job_id].delivery
    streamed_job = other.jobs[streamed.job_id]
    original = gate.jobs[streamed.job_id]
    # #52's fields on a real hire: the stored stream and the relay's persisted offset and
    # budgets are kept, so a resumed relay's cursors line up with what is stored ...
    assert streamed_job.events == original.events
    assert [e.cursor for e in streamed_job.events] == list(range(1, len(original.events) + 1))
    assert any(e.kind is m.StreamEventKind.ITEM for e in streamed_job.events)
    assert streamed_job.progress_final == original.progress_final
    assert streamed_job.last_progress == original.last_progress
    assert streamed_job.relay.offset == original.relay.offset > 0
    assert streamed_job.relay.topic_bytes == original.relay.topic_bytes
    assert streamed_job.relay.job_bytes == original.relay.job_bytes
    assert streamed_job.relay.done == original.relay.done
    assert streamed_job.relay.session == original.relay.session
    # ... while a running process's state starts afresh: no task, no pause, no listener.
    assert streamed_job.relay.task is None
    assert streamed_job.relay.pause is None
    assert streamed_job.relay.listeners == 0
    assert not streamed_job.changed.is_set()
    assert other.jobs[disputed.job_id].holds == gate.jobs[disputed.job_id].holds
