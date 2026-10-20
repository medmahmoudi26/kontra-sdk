"""The stream envelope: a workflow stream's poll result, sealed item by item, the items stored
in the claim-check bucket and the answer a list of references (decisions I31, I39).

``temporalio.contrib.workflow_streams`` answers a poll (``__temporal_workflow_stream_poll``)
with one ``PollResult`` -- every item's ``Payload`` base64-encoded inside one JSON value -- and
the codec chain runs once, on that whole value. Encrypted whole, the gate's relay could read
neither topics nor offsets; encrypted whole to the buyer, a relay that asked for every topic
would hand the buyer the seller's private ones. So Kontra's workflow interceptor marks a poll
result on a job root (:data:`STREAM_MARK` = :data:`POLL_MARK`, with the topics the buyer may
read under :data:`BUYER_TOPICS_KEY`), cut to at most :data:`STREAM_ITEMS_MAX` items, and
:class:`~kontra.codec.KontraCodec` turns the marked payload into this envelope
(:func:`seal_poll`)::

    Payload.metadata = {"encoding": "binary/kontra-stream", "kontra-v": "1"}
    Payload.data     = UTF-8 JSON, at most ENVELOPE_MAX bytes:
    {"v": 2,
     "items": [{"topic": "turn_events" | null, "offset": 42, "size": 1311, "sha256": "<hex>"},
               {"topic": ... | null, "offset": 43, "size": 880, "sha256": null}],
     "next_offset": 57, "more_ready": false}

* Each item is sealed on its own: the age ciphertext of the item's own ``Payload`` with two
  more metadata keys, :data:`SEALED_TOPIC_KEY` and :data:`SEALED_OFFSET_KEY`, so the topic
  and the offset are authenticated by the ciphertext and a reader checks both against what the
  relay says (:func:`open_item`): a gate, a bucket or a token holder cannot relabel, reorder or
  swap items.
* A topic the buyer may read (the offering's ``streams:`` and ``kontra.progress``) is sealed
  to the seller's recipients and the job's buyer key, and named in the clear; any other topic
  to the seller's recipients only, its name ``null`` (it lives inside the ciphertext only).
* **Every item is a reference** (decisions I39): its ciphertext goes to the claim-check bucket
  under its sha256, scoped to the job (:class:`StreamStore`), and the envelope names its
  ``size`` and ``sha256``. An item not stored -- no store, a store that failed, or the
  completion's upload budget spent -- is a stub, ``sha256: null``: the relay records a gap, and
  a later read stores it. So every answer has a fixed upper size: at most
  :data:`STREAM_ITEMS_MAX` entries of at most :data:`REF_ENTRY_MAX` bytes.
* **Sealed deterministically per worker.** With a seal key (:func:`kontra.codec.age.seal_key`
  of the worker's identity) an item's file derives from a seed over the job, the recipients and
  its sealed plaintext, so the same worker re-sealing the same item writes the same bytes, and
  a re-read stores nothing new however long ago the item was first answered. The seed needs the
  worker's secret: no one else can confirm a guess at an item.
* ``next_offset``, ``more_ready``, the offsets and sizes are in the clear by design: they are
  the log's length and positions, which the platform reads from the history's size anyway.

:func:`open_poll` restores the exact ``PollResult`` JSON for the seller's codec, fetching each
reference, so a stock ``WorkflowStreamClient`` keeps working. :func:`parse_envelope` is the
relay's reader: no key, strict; :func:`verified_item` is what the relay keeps of a fetched
blob (its size and digest checked, an age file), so plaintext never reaches a gate's storage.
The offset query's answer is marked too (:data:`OFFSET_MARK`) and passes the codec in
plaintext: a log length; so does a poll's answer on replay (:data:`REPLAYED_MARK`), a constant.

Imports only the standard library, protobuf, ``temporalio.api``/``temporalio.exceptions``,
:mod:`kontra.codec.age` and :mod:`kontra.contract`: the gate's relay (:mod:`kontra._relay`)
reads envelopes with it, and Kontra's workflow interceptor takes its constants and
:class:`PollFailure` inside the sandbox.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import math
import re
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast, runtime_checkable

from google.protobuf.message import DecodeError
from temporalio.api.common.v1 import Payload
from temporalio.exceptions import ApplicationError

from kontra.codec import age
from kontra.contract.names import PROGRESS_TOPIC
from kontra.contract.offering import STREAMS_MAX, check_topic

__all__ = [
    "AGE_MAGIC",
    "BUYER_TOPICS_KEY",
    "ENVELOPE_MAX",
    "ENVELOPE_VERSION",
    "OFFSET_MARK",
    "POLL_MARK",
    "REF_ENTRY_MAX",
    "REPLAYED_MARK",
    "SEALED_OFFSET_KEY",
    "SEALED_TOPIC_KEY",
    "STREAM_BUSY",
    "STREAM_ENCODING",
    "STREAM_ERROR",
    "STREAM_FULL",
    "STREAM_ITEMS_MAX",
    "STREAM_MARK",
    "STREAM_POLLS_MAX",
    "STREAM_REFS_MAX",
    "STREAM_UPLOAD_BUDGET",
    "BlobRefs",
    "Envelope",
    "EnvelopeError",
    "EnvelopeItem",
    "ItemUnreadable",
    "PollFailure",
    "StreamStore",
    "buyer_topics",
    "check_upload_budget",
    "envelope_bound",
    "offset_payload",
    "open_item",
    "open_poll",
    "parse_envelope",
    "parse_offset",
    "replayed_payload",
    "seal_poll",
    "verified_item",
]

log = logging.getLogger("kontra.codec.stream")

#: The encoding of an envelope in history.
STREAM_ENCODING = b"binary/kontra-stream"
#: The metadata key the workflow interceptor marks a stream answer with, and its values.
STREAM_MARK = "kontra-stream"
POLL_MARK = b"poll/v1"
OFFSET_MARK = b"offset/v1"
#: A poll's answer on replay (:func:`replayed_payload`): already in the history, never sent.
REPLAYED_MARK = b"replayed/v1"
#: On a marked poll result: the JSON list of topics the buyer may read.
BUYER_TOPICS_KEY = "kontra-stream-buyer"
#: Inside a sealed item: its topic and its offset (ASCII decimal).
SEALED_TOPIC_KEY = "kontra-topic"
SEALED_OFFSET_KEY = "kontra-offset"
#: The envelope's own version (its JSON ``v``): references only (decisions I39).
ENVELOPE_VERSION = 2
#: The most items one answer lists. Kontra's workflow interceptor cuts an answer here (the
#: contrib's own truncation: ``next_offset`` at the first left out, ``more_ready``).
STREAM_ITEMS_MAX = 64
#: The longest item entry, its comma included: a 64-character topic (``check_topic`` admits no
#: character JSON escapes), a 19-digit offset and size, 64 hex digits
#: (``tests/codec/test_stream_envelope.py`` builds it).
REF_ENTRY_MAX = 209
#: ``{"v":2,"items":[`` ... ``],"next_offset":<19 digits>,"more_ready":false}`` with room.
_ENVELOPE_FRAME = 128
#: The largest envelope: :data:`STREAM_ITEMS_MAX` of the largest entries in the frame
#: (13,504 bytes). It is never claim-checked or stored externally: the relay must read it.
ENVELOPE_MAX = _ENVELOPE_FRAME + STREAM_ITEMS_MAX * REF_ENTRY_MAX
#: The most polls a job root accepts in one run: an eighth of Kontra's pinned
#: ``history.maxTotalUpdates`` (2,000), so the other 1,750 stay the seller's own updates'
#: (decisions I39; a test pins it). Past it a poll is refused (:data:`STREAM_FULL`). It is
#: also what a worker replays of a run's polls after a cache miss, each one again, within one
#: workflow task's timeout: 250 replay in under 2.5 s on a two-core worker at a load of
#: nine, a quarter of the 10 s default.
STREAM_POLLS_MAX = 250
#: The stored items a codec remembers (:class:`BlobRefs`): more than four runs' polls can list
#: (``STREAM_POLLS_MAX * STREAM_ITEMS_MAX``), about 16 MB. A cache: what it forgets is sealed
#: to the same bytes again (per-worker sealing) and found stored.
STREAM_REFS_MAX = 65_536
#: Seconds of uploads one workflow-task completion may spend on its answers' items, all of
#: them together (temporalio encodes a completion's payloads one after another); past it, an
#: answer's new items are stubs, so a slow bucket never holds a workflow task past its timeout.
#: The production default of the codec's ``stream_upload_budget`` (and
#: :class:`~kontra.plugin.KontraPlugin`'s): a fifth of Temporal's 10 s workflow task timeout.
STREAM_UPLOAD_BUDGET = 2.0
#: The largest stream upload budget a codec takes: under Temporal's 10 s workflow task timeout,
#: with room for the rest of the task, so no budget can hold a task past it.
STREAM_UPLOAD_BUDGET_MAX = 8.0
#: The failure types of a poll a job root refuses (or fails) itself: past its budget (stop),
#: busy -- too many polls in flight, or an offset past the stream's end (wait, retry) --, any
#: other failure of the stream's own handler (:class:`PollFailure`).
STREAM_FULL = "KontraStreamFull"
STREAM_BUSY = "KontraStreamBusy"
STREAM_ERROR = "KontraStreamError"
#: Every age v1 file starts with this line.
AGE_MAGIC = b"age-encryption.org/v1\n"

_VERSION_KEY = "kontra-v"
_VERSION = b"1"
_JSON_PLAIN = b"json/plain"
#: The topics a buyer may read: the offering's streams and kontra.progress.
_LISTED_MAX = STREAMS_MAX + 1
#: A log offset or length: what a JSON integer Temporal and Python agree on can hold.
_INT = re.compile(rb"0|[1-9][0-9]{0,18}", re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}", re.ASCII)
_ITEM_KEYS = frozenset({"topic", "offset", "size", "sha256"})
_TOP_KEYS = frozenset({"v", "items", "next_offset", "more_ready"})
#: What ``json.loads`` raises on input that is not JSON, or JSON nested too deep to parse.
_JSON_ERRORS = (ValueError, RecursionError)
#: Blobs fetched at once for one answer (the seller's own decode).
_FETCH_CONCURRENCY = 16

#: The failure types of a poll that keep their name (any other is :data:`STREAM_ERROR`), each
#: with its one message: a poll's failure is part of its outcome in the job's history.
_POLL_FAILURES = {
    STREAM_FULL: "the job's stream accepts no more polls in this run",
    STREAM_BUSY: "the job's stream is busy, or the offset is past its end: poll again later",
    STREAM_ERROR: "the job's stream could not answer this poll",
    "TruncatedOffset": "the offset was truncated from the job's stream",
    "StreamDraining": "the job's stream is draining for a continue-as-new",
}

#: ``fetch_blob(sha256, size) -> ciphertext``, verified against both.
FetchBlob = Callable[[str, int], Awaitable[bytes]]


class PollFailure(ApplicationError):
    """A poll of a job's stream refused or failed by Kontra's workflow interceptor: one of a
    fixed set of types, its one message, and -- by :class:`~kontra.codec.KontraFailureConverter`
    -- no stack trace, so an accepted poll's failed outcome is a small constant (decisions
    I39). Any type outside the set is :data:`STREAM_ERROR`."""

    def __init__(self, type: str) -> None:
        kind = type if type in _POLL_FAILURES else STREAM_ERROR
        super().__init__(_POLL_FAILURES[kind], type=kind, non_retryable=True)


def check_upload_budget(seconds: float) -> float:
    """``seconds`` as a stream upload budget (:data:`STREAM_UPLOAD_BUDGET`): a finite number
    of seconds above 0 and at most :data:`STREAM_UPLOAD_BUDGET_MAX`, or :class:`ValueError`.
    A budget of 0 would make every item a stub, and a larger one would let a slow bucket hold
    a workflow task past its timeout."""
    if not (math.isfinite(seconds) and 0 < seconds <= STREAM_UPLOAD_BUDGET_MAX):
        raise ValueError(
            "stream_upload_budget must be a number of seconds above 0 and at most "
            f"{STREAM_UPLOAD_BUDGET_MAX:g}"
        )
    return float(seconds)


@runtime_checkable
class StreamStore(Protocol):
    """Where a seller's codec stores a job's stream items: the claim-check bucket, scoped to
    the job (``POST /v1/jobs/{job_id}/stream/blobs``, :class:`~kontra.codec.GateBlobStore`)."""

    async def put_stream(
        self, job_id: str, blobs: Sequence[tuple[str, bytes]], done: Callable[[str], None]
    ) -> None:
        """Store each ``(sha256, ciphertext)`` of ``job_id``'s stream, calling ``done(sha256)``
        for each one stored, or found stored already, as soon as it is. May be cancelled at
        any point (the codec's upload budget): what ``done`` named is stored."""
        ...


class BlobRefs:
    """The stream items a codec stored already, by job, sealed plaintext and recipients, at
    most ``size`` of them (the least recently used go first): a poll that re-reads an item --
    the seller's own subscriber, a relay polling from 0 again -- reuses its reference without
    asking the gate. A cache in front of per-worker sealing (the module docstring): what it
    forgets seals to the same bytes again. Keyed per job, so two jobs' identical items never
    share a blob the platform could link."""

    def __init__(self, size: int = STREAM_REFS_MAX) -> None:
        if size < 1:
            raise ValueError("size is at least 1")
        self._size = size
        self._refs: OrderedDict[bytes, tuple[str, int]] = OrderedDict()

    @staticmethod
    def key(scope: str, plain: bytes, recipients: Sequence[str]) -> bytes:
        h = hashlib.sha256()
        for part in (scope.encode(), *sorted(r.encode() for r in recipients)):
            h.update(len(part).to_bytes(4, "big"))
            h.update(part)
        h.update(plain)
        return h.digest()

    def get(self, key: bytes) -> tuple[str, int] | None:
        ref = self._refs.get(key)
        if ref is not None:
            self._refs.move_to_end(key)
        return ref

    def put(self, key: bytes, ref: tuple[str, int]) -> None:
        self._refs[key] = ref
        self._refs.move_to_end(key)
        while len(self._refs) > self._size:
            self._refs.popitem(last=False)

    def __len__(self) -> int:
        return len(self._refs)


class EnvelopeError(ValueError):
    """A stream envelope, a marked payload or a sealed item that is not what this module
    writes. Messages are ours: never payload bytes."""


class ItemUnreadable(EnvelopeError):
    """A sealed item that will not open with the identities given (not sealed to us), or
    that opened to something other than a ``Payload``."""


@dataclass(frozen=True, slots=True)
class EnvelopeItem:
    """One item as the relay reads it: ``topic`` is None for a topic the buyer may not read;
    ``sha256`` names its blob, or is None for a stub (not stored); ``size`` is the
    ciphertext's size either way."""

    topic: str | None
    offset: int
    sha256: str | None
    size: int


@dataclass(frozen=True, slots=True)
class Envelope:
    items: tuple[EnvelopeItem, ...]
    next_offset: int
    more_ready: bool


# --------------------------------------------------------------------------- marks


def buyer_topics(listed: Sequence[str]) -> bytes:
    """The :data:`BUYER_TOPICS_KEY` value for an offering's ``streams``: sorted JSON, with
    ``kontra.progress``."""
    return json.dumps(sorted({*listed, PROGRESS_TOPIC}), separators=(",", ":")).encode()


def offset_payload(n: int) -> Payload:
    """The offset query's answer, marked: plaintext ``json/plain`` (a log length)."""
    if isinstance(n, bool) or not isinstance(n, int) or n < 0:  # pyright: ignore[reportUnnecessaryIsInstance]
        raise EnvelopeError("an offset is an integer >= 0")
    return Payload(
        metadata={"encoding": _JSON_PLAIN, STREAM_MARK: OFFSET_MARK}, data=str(n).encode()
    )


def replayed_payload() -> Payload:
    """What Kontra's workflow interceptor answers a poll with on replay: its answer is in the
    history already and the replayed response is never sent (Temporal matches it by its
    protocol instance), so nothing of the stream is encoded, sealed or stored again. Plaintext
    ``null``: were it ever sent, a poller reads no item and the relay stops at ``protocol``."""
    return Payload(metadata={"encoding": _JSON_PLAIN, STREAM_MARK: REPLAYED_MARK}, data=b"null")


def parse_offset(payload: Payload) -> int:
    """The offset in a marked offset answer; anything else is :class:`EnvelopeError`."""
    if (
        payload.metadata.get(STREAM_MARK) != OFFSET_MARK
        or payload.metadata.get("encoding") != _JSON_PLAIN
        or _INT.fullmatch(payload.data) is None
    ):
        raise EnvelopeError("not a Kontra stream offset")
    return int(payload.data)


def envelope_bound(items: int) -> int:
    """At least the size of the envelope :func:`seal_poll` writes for ``items`` items, its
    frame included: every entry is at most :data:`REF_ENTRY_MAX`. Kontra's workflow
    interceptor counts the answers of one workflow task with it (pure arithmetic, in the
    sandbox, the same on every worker)."""
    if items < 0:
        raise ValueError("items is >= 0")
    return _ENVELOPE_FRAME + items * REF_ENTRY_MAX


def _listed(marked: Payload) -> frozenset[str]:
    raw = marked.metadata.get(BUYER_TOPICS_KEY)
    if raw is None or len(raw) > 2048:
        raise EnvelopeError("a marked poll result names no buyer topics")
    try:
        value: object = json.loads(raw)
    except _JSON_ERRORS:
        raise EnvelopeError("a marked poll result's buyer topics are not JSON") from None
    if not isinstance(value, list):
        raise EnvelopeError("a marked poll result's buyer topics are not a list")
    topics = cast(list[object], value)
    if len(topics) > _LISTED_MAX:
        raise EnvelopeError(f"a marked poll result lists more than {_LISTED_MAX} buyer topics")
    try:
        listed = frozenset(check_topic(t) for t in topics)
    except ValueError as e:
        raise EnvelopeError(f"a marked poll result's buyer topics: {e}") from None
    if PROGRESS_TOPIC not in listed:
        raise EnvelopeError(f"a marked poll result's buyer topics lack {PROGRESS_TOPIC}")
    return listed


def _int(value: object, what: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 10**19:
        raise EnvelopeError(f"{what} is not an integer >= 0")
    return value


def _poll_result(marked: Payload) -> tuple[list[tuple[str, int, Payload]], int, bool]:
    """The items, ``next_offset`` and ``more_ready`` of a marked ``PollResult``."""
    if marked.metadata.get("encoding") != _JSON_PLAIN:
        raise EnvelopeError("a marked poll result is not json/plain")
    try:
        value: object = json.loads(marked.data)
    except _JSON_ERRORS:
        raise EnvelopeError("a marked poll result is not JSON") from None
    if not isinstance(value, dict):
        raise EnvelopeError("a marked poll result is not an object")
    result = cast(dict[str, Any], value)
    raw_items, more = result.get("items"), result.get("more_ready")
    next_offset = _int(result.get("next_offset"), "next_offset")
    if not isinstance(raw_items, list) or not isinstance(more, bool):
        raise EnvelopeError("a marked poll result is not a PollResult")
    items: list[tuple[str, int, Payload]] = []
    for raw in cast(list[object], raw_items):
        if not isinstance(raw, dict):
            raise EnvelopeError("a poll item is not an object")
        item = cast(dict[str, Any], raw)
        topic, data = item.get("topic"), item.get("data")
        offset = _int(item.get("offset"), "an item's offset")
        if not isinstance(topic, str) or not isinstance(data, str):
            raise EnvelopeError("a poll item is not {topic, data, offset}")
        inner = Payload()
        try:
            inner.ParseFromString(base64.b64decode(data, validate=True))
        except (binascii.Error, ValueError, DecodeError):
            raise EnvelopeError("a poll item's data is not a Payload") from None
        items.append((topic, offset, inner))
    return items, next_offset, more


# --------------------------------------------------------------------------- the codec side


def _sealed_plaintext(inner: Payload, topic: str, offset: int) -> bytes:
    sealed = Payload()
    sealed.CopyFrom(inner)
    sealed.metadata[SEALED_TOPIC_KEY] = topic.encode()
    sealed.metadata[SEALED_OFFSET_KEY] = str(offset).encode("ascii")
    return sealed.SerializeToString()


async def seal_poll(
    marked: Payload,
    *,
    buyer_recipients: Sequence[str],
    seller_recipients: Sequence[str],
    store: StreamStore | None,
    scope: str,
    refs: BlobRefs | None = None,
    seal_key: bytes | None = None,
    deadline: float | None = None,
) -> Payload:
    """A marked poll result as the envelope (see the module docstring). A listed topic is
    sealed to ``buyer_recipients`` (the seller's set and the job's buyer key), every other to
    ``seller_recipients``; each item to ``store`` under ``scope`` (the job), sealed from a seed
    under ``seal_key`` when given. ``refs``: the items stored already. ``deadline``: the
    event-loop time past which nothing more is uploaded (the completion's upload budget); an
    item not stored by then, or that ``store`` failed to store, is a stub. A store's failure
    never fails the answer. Raises :class:`EnvelopeError` for a marked payload that is not
    one, :class:`age.AgeError` for a recipient that is not one."""
    listed = _listed(marked)
    items, next_offset, more_ready = _poll_result(marked)
    if len(items) > STREAM_ITEMS_MAX:
        # Kontra's interceptor cuts here first; the contrib's truncation protocol either way.
        next_offset, more_ready = items[STREAM_ITEMS_MAX][1], True
        items = items[:STREAM_ITEMS_MAX]
    entries: list[tuple[str | None, int, int, str]] = []
    known: set[str] = set()
    fresh: dict[str, bytes] = {}
    to_store: list[tuple[str, bytes]] = []
    for topic, offset, inner in items:
        visible = topic in listed
        recipients = buyer_recipients if visible else seller_recipients
        plain = _sealed_plaintext(inner, topic, offset)
        key = BlobRefs.key(scope, plain, recipients)
        ref = refs.get(key) if refs is not None else None
        if ref is not None:
            digest, size = ref
            known.add(digest)
        else:
            seed = age.seed_for(seal_key, key) if seal_key is not None else None
            ct = age.encrypt(plain, recipients, seed=seed)
            digest, size = hashlib.sha256(ct).hexdigest(), len(ct)
            if digest not in fresh:
                fresh[digest] = key
                to_store.append((digest, ct))
        entries.append((topic if visible else None, offset, size, digest))
    stored = await _store(store, scope, to_store, deadline) if to_store else set[str]()
    if refs is not None:
        for digest, ct in to_store:
            if digest in stored:
                refs.put(fresh[digest], (digest, len(ct)))
    out = [
        {
            "topic": shown,
            "offset": offset,
            "size": size,
            "sha256": digest if digest in known or digest in stored else None,
        }
        for shown, offset, size, digest in entries
    ]
    envelope = {
        "v": ENVELOPE_VERSION,
        "items": out,
        "next_offset": next_offset,
        "more_ready": more_ready,
    }
    return Payload(
        metadata={"encoding": STREAM_ENCODING, _VERSION_KEY: _VERSION},
        data=json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
    )


async def _store(
    store: StreamStore | None, scope: str, blobs: list[tuple[str, bytes]], deadline: float | None
) -> set[str]:
    """What of ``blobs`` is stored by ``deadline``: ``store`` is cancelled there, and any
    failure of it only leaves what it reported stored (the rest are stubs)."""
    stored: set[str] = set()
    if store is None:
        return stored
    remaining = None if deadline is None else deadline - asyncio.get_running_loop().time()
    if remaining is not None and remaining <= 0:
        return stored  # the completion's upload budget is spent: stubs, at no cost
    try:
        async with asyncio.timeout(remaining):
            await store.put_stream(scope, blobs, stored.add)
    except TimeoutError:
        log.warning(
            "job %s: %d of %d new stream items not stored within the upload budget",
            scope,
            len(blobs) - len(stored),
            len(blobs),
        )
    except Exception as e:  # a bucket outage costs items, never the workflow task
        log.warning(
            "job %s: %d of %d new stream items not stored: %s",
            scope,
            len(blobs) - len(stored),
            len(blobs),
            type(e).__name__,
        )
    return stored


async def open_poll(
    envelope: Payload, identities: Sequence[str], *, fetch_blob: FetchBlob | None
) -> Payload:
    """The envelope as the ``PollResult`` JSON a stock subscriber expects, opened with this
    party's ``identities`` (every item is sealed to the seller's set). Each reference is
    fetched (``fetch_blob`` verifies it; a few at once); a stub, or every item without
    ``fetch_blob``, is dropped (an item nobody can relay). An item that will not open, or names
    another topic or offset than its place, raises :class:`EnvelopeError`."""
    parsed = parse_envelope(envelope)
    wanted = [i for i in parsed.items if i.sha256 is not None and fetch_blob is not None]
    fetched: dict[int, bytes] = {}
    if wanted:
        gate = asyncio.Semaphore(_FETCH_CONCURRENCY)

        async def fetch(item: EnvelopeItem) -> None:
            assert item.sha256 is not None
            assert fetch_blob is not None
            async with gate:
                fetched[item.offset] = await fetch_blob(item.sha256, item.size)

        done = await asyncio.gather(*(fetch(i) for i in wanted), return_exceptions=True)
        for failed in done:
            if isinstance(failed, BaseException):
                raise failed  # the codec's own error (a bucket that did not answer, ...)
    items: list[dict[str, Any]] = []
    for item in parsed.items:
        ct = fetched.get(item.offset)
        if ct is None:
            continue
        inner, topic = _open(ct, identities, item.offset)
        if item.topic is not None and item.topic != topic:
            raise EnvelopeError(f"the item at offset {item.offset} was relabelled")
        items.append(
            {
                "topic": topic,
                "data": base64.b64encode(inner.SerializeToString()).decode("ascii"),
                "offset": item.offset,
            }
        )
    result = {"items": items, "next_offset": parsed.next_offset, "more_ready": parsed.more_ready}
    return Payload(
        metadata={"encoding": _JSON_PLAIN}, data=json.dumps(result, separators=(",", ":")).encode()
    )


def _open(ct: bytes, identities: Sequence[str], offset: int) -> tuple[Payload, str]:
    """Decrypt one sealed item; its own (sealed) topic, its offset checked, both stripped."""
    try:
        plain = age.decrypt(ct, list(identities))
    except age.AgeError as e:
        raise ItemUnreadable(f"the item at offset {offset} does not decrypt: {e}") from None
    inner = Payload()
    try:
        inner.ParseFromString(plain)
    except DecodeError:
        raise ItemUnreadable(f"the item at offset {offset} is not a Payload") from None
    topic_raw = inner.metadata.get(SEALED_TOPIC_KEY)
    if inner.metadata.get(SEALED_OFFSET_KEY) != str(offset).encode("ascii") or topic_raw is None:
        raise EnvelopeError(f"the item at offset {offset} was relabelled")
    try:
        topic = topic_raw.decode("utf-8")
    except UnicodeDecodeError:
        raise EnvelopeError(f"the item at offset {offset} has no topic") from None
    del inner.metadata[SEALED_TOPIC_KEY]
    del inner.metadata[SEALED_OFFSET_KEY]
    return inner, topic


def open_item(ct: bytes, identities: Sequence[str], *, topic: str, offset: int) -> Payload:
    """The buyer's side: one relayed item, its ``topic`` and ``offset`` (as the relay said
    them) checked against the sealed ones, which are then stripped. :class:`ItemUnreadable`
    when it is not sealed to ``identities``; :class:`EnvelopeError` when it was relabelled."""
    inner, sealed = _open(ct, identities, offset)
    if sealed != topic:
        raise EnvelopeError(f"the item at offset {offset} was relabelled")
    return inner


# --------------------------------------------------------------------------- the relay side


def verified_item(data: bytes | None, item: EnvelopeItem) -> bytes | None:
    """``data``, a blob fetched for ``item``, if it is that item's: exactly ``item.size``
    bytes, hashing to ``item.sha256``, and an age file (nothing decrypted, no key); else None
    -- a gap, never plaintext in a gate's storage."""
    if (
        data is None
        or item.sha256 is None
        or len(data) != item.size
        or hashlib.sha256(data).hexdigest() != item.sha256
        or not data.startswith(AGE_MAGIC)
        or not age.is_age_file(data)
    ):
        return None
    return data


def _item(raw: object, next_offset: int, previous: int) -> EnvelopeItem:
    if not isinstance(raw, dict) or frozenset(cast(dict[str, Any], raw)) != _ITEM_KEYS:
        raise EnvelopeError("an envelope item is not {topic, offset, size, sha256}")
    item = cast(dict[str, Any], raw)
    topic = item["topic"]
    if topic is not None:
        try:
            check_topic(topic)
        except ValueError as e:
            raise EnvelopeError(f"an envelope item's topic: {e}") from None
    offset = _int(item["offset"], "an item's offset")
    if not previous < offset < next_offset:
        raise EnvelopeError("envelope offsets are not increasing below next_offset")
    size = _int(item["size"], "an item's size")
    digest = item["sha256"]
    if digest is not None and (not isinstance(digest, str) or _SHA256.fullmatch(digest) is None):
        raise EnvelopeError("an item's sha256 is not 64 lowercase hex digits")
    return EnvelopeItem(topic, offset, digest, size)


def parse_envelope(payload: Payload) -> Envelope:
    """The relay's reader (no key): :class:`Envelope`, or :class:`EnvelopeError` for
    anything that is not exactly what :func:`seal_poll` writes -- a wrong encoding or
    version (a v1 envelope with inline ciphertext included), more than :data:`ENVELOPE_MAX`
    bytes or :data:`STREAM_ITEMS_MAX` items, unknown or missing members, offsets that do not
    increase below ``next_offset``, a digest that is not one."""
    if (
        payload.metadata.get("encoding") != STREAM_ENCODING
        or payload.metadata.get(_VERSION_KEY) != _VERSION
    ):
        raise EnvelopeError("not a Kontra stream envelope")
    if len(payload.data) > ENVELOPE_MAX:
        raise EnvelopeError(f"a stream envelope is larger than {ENVELOPE_MAX} bytes")
    try:
        value: object = json.loads(payload.data)
    except _JSON_ERRORS:
        # RecursionError: JSON nested deeper than the interpreter's stack (``[`` * 10,000
        # fits ENVELOPE_MAX) -- a hostile worker's answer, refused like any other.
        raise EnvelopeError("a stream envelope is not JSON") from None
    if not isinstance(value, dict) or frozenset(cast(dict[str, Any], value)) != _TOP_KEYS:
        raise EnvelopeError("a stream envelope is not {v, items, next_offset, more_ready}")
    env = cast(dict[str, Any], value)
    version: object = env["v"]
    if isinstance(version, bool) or version != ENVELOPE_VERSION:
        raise EnvelopeError("unsupported stream envelope version")
    raw_items, more_ready = env["items"], env["more_ready"]
    next_offset = _int(env["next_offset"], "next_offset")
    if not isinstance(raw_items, list) or not isinstance(more_ready, bool):
        raise EnvelopeError("a stream envelope is not {v, items, next_offset, more_ready}")
    listed = cast(list[object], raw_items)
    if len(listed) > STREAM_ITEMS_MAX:
        raise EnvelopeError(f"a stream envelope lists more than {STREAM_ITEMS_MAX} items")
    items: list[EnvelopeItem] = []
    previous = -1
    for raw in listed:
        item = _item(raw, next_offset, previous)
        previous = item.offset
        items.append(item)
    return Envelope(tuple(items), next_offset, more_ready)
