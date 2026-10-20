"""The stream envelope: a workflow stream's poll result, sealed item by item (decisions I31).

``temporalio.contrib.workflow_streams`` answers a poll (``__temporal_workflow_stream_poll``)
with one ``PollResult`` -- every item's ``Payload`` base64-encoded inside one JSON value -- and
the codec chain runs once, on that whole value. Encrypted whole, the gate's relay could read
neither topics nor offsets; encrypted whole to the buyer, a relay that asked for every topic
would hand the buyer the seller's private ones. So Kontra's workflow interceptor marks a poll
result on a job root (:data:`STREAM_MARK` = :data:`POLL_MARK`, with the topics the buyer may
read under :data:`BUYER_TOPICS_KEY`) and :class:`~kontra.codec.KontraCodec` turns the marked
payload into this envelope (:func:`seal_poll`)::

    Payload.metadata = {"encoding": "binary/kontra-stream", "kontra-v": "1"}
    Payload.data     = UTF-8 JSON, at most ENVELOPE_MAX bytes:
    {"v": 1,
     "items": [{"topic": "turn_events" | null, "offset": 42, "ct": "<base64 age>"},
               {"topic": ... | null, "offset": 43, "size": N, "ref": {"sha256": ..., "size": N}},
               {"topic": ... | null, "offset": 44, "size": N, "ct": null}],
     "next_offset": 57, "more_ready": false}

* Each item is sealed on its own: the age ciphertext of the item's own ``Payload`` with two
  more metadata keys, :data:`SEALED_TOPIC_KEY` and :data:`SEALED_OFFSET_KEY`, so the topic
  and the offset are authenticated by the ciphertext and a buyer checks both against what the
  relay says (:func:`open_item`): a gate cannot relabel or reorder items.
* A topic the buyer may read (the offering's ``streams:`` and ``kontra.progress``) is sealed
  to the seller's recipients and the job's buyer key, and named in the clear; any other topic
  to the seller's recipients only, its name ``null`` (it lives inside the ciphertext only).
* An item whose ciphertext is above :data:`ITEM_INLINE_MAX` goes to the blob store (a
  ``ref``, fetched by the seller's own decode; the relay records a gap), or is a stub
  (``ct: null``) without one.
* The envelope stays within :data:`ENVELOPE_MAX`: sealing stops before the item that would
  cross it, with ``next_offset`` at that item and ``more_ready`` true -- the contrib's own
  truncation protocol, so a subscriber simply polls again.
* ``next_offset``, ``more_ready`` and the offsets are in the clear by design: they are the
  log's length and positions, which the platform reads from the history's size anyway.

:func:`open_poll` restores the exact ``PollResult`` JSON for the seller's codec, so a stock
``WorkflowStreamClient`` keeps working. :func:`parse_envelope` is the relay's reader: no key,
strict, and it refuses any ``ct`` that is not an age file, so plaintext never reaches a
gate's storage. The offset query's answer is marked too (:data:`OFFSET_MARK`) and passes the
codec in plaintext: a log length.

Imports only the standard library, protobuf, :mod:`kontra.codec.age` and
:mod:`kontra.contract`: the gate's relay (:mod:`kontra._relay`) reads envelopes with it.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

from google.protobuf.message import DecodeError
from temporalio.api.common.v1 import Payload

from kontra.codec import age
from kontra.contract.names import PROGRESS_TOPIC
from kontra.contract.offering import STREAMS_MAX, check_topic

__all__ = [
    "AGE_MAGIC",
    "BUYER_TOPICS_KEY",
    "ENVELOPE_MAX",
    "ITEM_INLINE_MAX",
    "OFFSET_MARK",
    "POLL_MARK",
    "SEALED_OFFSET_KEY",
    "SEALED_TOPIC_KEY",
    "STREAM_ENCODING",
    "STREAM_MARK",
    "Envelope",
    "EnvelopeError",
    "EnvelopeItem",
    "ItemUnreadable",
    "buyer_topics",
    "offset_payload",
    "open_item",
    "open_poll",
    "parse_envelope",
    "parse_offset",
    "seal_poll",
]

#: The encoding of an envelope in history.
STREAM_ENCODING = b"binary/kontra-stream"
#: The metadata key the workflow interceptor marks a stream answer with, and its values.
STREAM_MARK = "kontra-stream"
POLL_MARK = b"poll/v1"
OFFSET_MARK = b"offset/v1"
#: On a marked poll result: the JSON list of topics the buyer may read.
BUYER_TOPICS_KEY = "kontra-stream-buyer"
#: Inside a sealed item: its topic and its offset (ASCII decimal).
SEALED_TOPIC_KEY = "kontra-topic"
SEALED_OFFSET_KEY = "kontra-offset"
#: The largest envelope: under Temporal's ``limit.blobSize.warn`` (512 KiB) and far under
#: Kontra's pinned ``.error`` (2 MiB). It is never claim-checked or stored externally: the
#: relay must read it.
ENVELOPE_MAX = 512 * 1024
#: The largest item ciphertext carried inline (and relayed); above it, a ``ref`` or a stub.
ITEM_INLINE_MAX = 256 * 1024
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
#: ``{"v":1,"items":[`` ... ``],"next_offset":<19 digits>,"more_ready":false}`` with room.
_ENVELOPE_FRAME = 128
_ITEM_KEYS = frozenset({"topic", "offset", "ct", "size", "ref"})
_TOP_KEYS = frozenset({"v", "items", "next_offset", "more_ready"})

#: ``put_blob(ciphertext) -> (sha256, size)``: claim-check storage (the codec's blob store).
PutBlob = Callable[[bytes], Awaitable[tuple[str, int]]]
#: ``fetch_blob(sha256, size) -> ciphertext``, verified against both.
FetchBlob = Callable[[str, int], Awaitable[bytes]]


class EnvelopeError(ValueError):
    """A stream envelope, a marked payload or a sealed item that is not what this module
    writes. Messages are ours: never payload bytes."""


class ItemUnreadable(EnvelopeError):
    """A sealed item that will not open with the identities given (not sealed to us), or
    that opened to something other than a ``Payload``."""


@dataclass(frozen=True, slots=True)
class EnvelopeItem:
    """One item as the relay reads it: ``topic`` is None for a topic the buyer may not read;
    ``ciphertext`` is None for an item too large to relay (claim-checked, or a stub);
    ``size`` is the ciphertext's size either way."""

    topic: str | None
    offset: int
    ciphertext: bytes | None
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


def parse_offset(payload: Payload) -> int:
    """The offset in a marked offset answer; anything else is :class:`EnvelopeError`."""
    if (
        payload.metadata.get(STREAM_MARK) != OFFSET_MARK
        or payload.metadata.get("encoding") != _JSON_PLAIN
        or _INT.fullmatch(payload.data) is None
    ):
        raise EnvelopeError("not a Kontra stream offset")
    return int(payload.data)


def _listed(marked: Payload) -> frozenset[str]:
    raw = marked.metadata.get(BUYER_TOPICS_KEY)
    if raw is None or len(raw) > 2048:
        raise EnvelopeError("a marked poll result names no buyer topics")
    try:
        value: object = json.loads(raw)
    except ValueError:
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
    except ValueError:
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


def _b64_len(n: int) -> int:
    return 4 * -(-n // 3)


def _entry_len(entry: dict[str, Any]) -> int:
    return len(json.dumps(entry, separators=(",", ":"))) + 1  # and its comma


async def seal_poll(
    marked: Payload,
    *,
    buyer_recipients: Sequence[str],
    seller_recipients: Sequence[str],
    put_blob: PutBlob | None,
) -> Payload:
    """A marked poll result as the envelope (see the module docstring). A listed topic is
    sealed to ``buyer_recipients`` (the seller's set and the job's buyer key), every other
    to ``seller_recipients``. Raises :class:`EnvelopeError` for a marked payload that is not
    one, :class:`age.AgeError` for a recipient that is not one."""
    listed = _listed(marked)
    items, next_offset, more_ready = _poll_result(marked)
    size = _ENVELOPE_FRAME
    out: list[dict[str, Any]] = []
    for topic, offset, inner in items:
        visible = topic in listed
        recipients = buyer_recipients if visible else seller_recipients
        plain = _sealed_plaintext(inner, topic, offset)
        ct_size = age.ciphertext_size(len(plain), age.recipient_count(recipients))
        shown = topic if visible else None
        inline = ct_size <= ITEM_INLINE_MAX
        if inline:
            length = _entry_len({"topic": shown, "offset": offset, "ct": ""}) + _b64_len(ct_size)
        elif put_blob is not None:
            ref = {"sha256": "0" * 64, "size": ct_size}
            length = _entry_len({"topic": shown, "offset": offset, "size": ct_size, "ref": ref})
        else:
            length = _entry_len({"topic": shown, "offset": offset, "size": ct_size, "ct": None})
        if size + length > ENVELOPE_MAX and out:
            # The contrib's truncation protocol: the subscriber polls again from here.
            next_offset, more_ready = offset, True
            break
        size += length
        if inline:
            ct = base64.b64encode(age.encrypt(plain, recipients)).decode("ascii")
            out.append({"topic": shown, "offset": offset, "ct": ct})
        elif put_blob is not None:
            digest, stored = await put_blob(age.encrypt(plain, recipients))
            out.append(
                {
                    "topic": shown,
                    "offset": offset,
                    "size": stored,
                    "ref": {"sha256": digest, "size": stored},
                }
            )
        else:
            out.append({"topic": shown, "offset": offset, "size": ct_size, "ct": None})
    envelope = {"v": 1, "items": out, "next_offset": next_offset, "more_ready": more_ready}
    return Payload(
        metadata={"encoding": STREAM_ENCODING, _VERSION_KEY: _VERSION},
        data=json.dumps(envelope, separators=(",", ":")).encode("utf-8"),
    )


async def open_poll(
    envelope: Payload, identities: Sequence[str], *, fetch_blob: FetchBlob | None
) -> Payload:
    """The envelope as the ``PollResult`` JSON a stock subscriber expects, opened with this
    party's ``identities`` (every item is sealed to the seller's set). A ``ref`` is fetched
    (``fetch_blob`` verifies it), a stub is dropped (an item nobody can relay). An item that
    will not open, or names another topic or offset than its place, raises
    :class:`EnvelopeError`."""
    parsed = _parse(envelope)
    items: list[dict[str, Any]] = []
    for item, ref in parsed.pairs:
        ct = item.ciphertext
        if ct is None:
            if ref is None or fetch_blob is None:
                continue  # a stub, or a claim-check this codec cannot fetch: a gap
            ct = await fetch_blob(*ref)
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


@dataclass(frozen=True, slots=True)
class _Parsed:
    """An envelope with each item's claim-check reference (the seller's codec fetches it)."""

    pairs: list[tuple[EnvelopeItem, tuple[str, int] | None]]
    next_offset: int
    more_ready: bool


def _ref(value: object, size: int) -> tuple[str, int]:
    if not isinstance(value, dict) or set(cast(dict[str, Any], value)) != {"sha256", "size"}:
        raise EnvelopeError("an item's ref is not {sha256, size}")
    ref = cast(dict[str, Any], value)
    digest = ref["sha256"]
    if not isinstance(digest, str) or _SHA256.fullmatch(digest) is None:
        raise EnvelopeError("an item's ref names no sha256")
    if _int(ref["size"], "a ref's size") != size:
        raise EnvelopeError("an item's ref and size disagree")
    return digest, size


def _item(
    raw: object, next_offset: int, previous: int
) -> tuple[EnvelopeItem, tuple[str, int] | None]:
    if not isinstance(raw, dict):
        raise EnvelopeError("an envelope item is not an object")
    item = cast(dict[str, Any], raw)
    if not frozenset(item) <= _ITEM_KEYS or "topic" not in item or "offset" not in item:
        raise EnvelopeError("an envelope item has unknown or missing members")
    topic = item["topic"]
    if topic is not None:
        try:
            check_topic(topic)
        except ValueError as e:
            raise EnvelopeError(f"an envelope item's topic: {e}") from None
    offset = _int(item["offset"], "an item's offset")
    if not previous < offset < next_offset:
        raise EnvelopeError("envelope offsets are not increasing below next_offset")
    if "ref" in item:
        if "ct" in item:
            raise EnvelopeError("an envelope item has both ct and ref")
        size = _int(item.get("size"), "an item's size")
        return EnvelopeItem(topic, offset, None, size), _ref(item["ref"], size)
    if "ct" not in item:
        raise EnvelopeError("an envelope item has neither ct nor ref")
    ct = item["ct"]
    if ct is None:
        return EnvelopeItem(topic, offset, None, _int(item.get("size"), "an item's size")), None
    if "size" in item or not isinstance(ct, str):
        raise EnvelopeError("an inline item is {topic, offset, ct}")
    try:
        data = base64.b64decode(ct, validate=True)
    except (binascii.Error, ValueError):
        raise EnvelopeError("an item's ct is not base64") from None
    if not data.startswith(AGE_MAGIC):
        raise EnvelopeError("an item's ct is not an age file")
    if len(data) > ITEM_INLINE_MAX:
        raise EnvelopeError(f"an inline item is larger than {ITEM_INLINE_MAX} bytes")
    return EnvelopeItem(topic, offset, data, len(data)), None


def _parse(payload: Payload) -> _Parsed:
    if (
        payload.metadata.get("encoding") != STREAM_ENCODING
        or payload.metadata.get(_VERSION_KEY) != _VERSION
    ):
        raise EnvelopeError("not a Kontra stream envelope")
    if len(payload.data) > ENVELOPE_MAX:
        raise EnvelopeError(f"a stream envelope is larger than {ENVELOPE_MAX} bytes")
    try:
        value: object = json.loads(payload.data)
    except ValueError:
        raise EnvelopeError("a stream envelope is not JSON") from None
    if not isinstance(value, dict) or frozenset(cast(dict[str, Any], value)) != _TOP_KEYS:
        raise EnvelopeError("a stream envelope is not {v, items, next_offset, more_ready}")
    env = cast(dict[str, Any], value)
    version: object = env["v"]
    if isinstance(version, bool) or version != 1:
        raise EnvelopeError("unsupported stream envelope version")
    raw_items, more_ready = env["items"], env["more_ready"]
    next_offset = _int(env["next_offset"], "next_offset")
    if not isinstance(raw_items, list) or not isinstance(more_ready, bool):
        raise EnvelopeError("a stream envelope is not {v, items, next_offset, more_ready}")
    pairs: list[tuple[EnvelopeItem, tuple[str, int] | None]] = []
    previous = -1
    for raw in cast(list[object], raw_items):
        pair = _item(raw, next_offset, previous)
        previous = pair[0].offset
        pairs.append(pair)
    return _Parsed(pairs, next_offset, more_ready)


def parse_envelope(payload: Payload) -> Envelope:
    """The relay's reader (no key): :class:`Envelope`, or :class:`EnvelopeError` for
    anything that is not exactly what :func:`seal_poll` writes -- a wrong encoding or
    version, more than :data:`ENVELOPE_MAX` bytes, unknown members, offsets that do not
    increase below ``next_offset``, a ``ct`` that is not strict base64 of an age file (so a
    worker that answered in plaintext, or whole ciphertext, is refused here)."""
    parsed = _parse(payload)
    return Envelope(tuple(item for item, _ in parsed.pairs), parsed.next_offset, parsed.more_ready)
