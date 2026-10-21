"""Seam ⑤: the stream envelope (decisions I31, I39, ``kontra.codec.stream``) -- a marked poll
result sealed item by item, the listed topics to buyer and seller, every other topic to the
seller alone with its name hidden; every item a reference to its blob in the bucket, scoped to
the job; stubs, the upload budget and a failing bucket; per-worker sealing (a re-read stores
nothing new); the fixed upper sizes; the plaintext offset; the refusals (a mark outside a
seller's job, a malformed buyer list); relabelling and swapped references caught; and the
relay's strict reader."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import Sequence

import pytest
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.contrib.workflow_streams import PollResult
from temporalio.contrib.workflow_streams._types import (  # pyright: ignore[reportPrivateUsage]
    _WorkflowStreamWireItem,  # pyright: ignore[reportPrivateUsage]
)
from temporalio.converter import (
    ActivitySerializationContext,
    NexusSerializationContext,
    WorkflowSerializationContext,
)

from kontra.codec import CodecError, InvalidPayload, KontraCodec, KontraFailureConverter
from kontra.codec import stream as s
from kontra.codec.age import AgeError, decrypt, encrypt, generate_identity
from kontra.contract.names import PROGRESS_TOPIC, agent_workflow_id, job_workflow_id

from .test_codec import BUYER, JOB, SELLER, STRANGER, Blobs, Keys

MARKER = "KONTRA-STREAM-MARKER-41c2"
CONVERTER = PydanticPayloadConverter()
SECOND = generate_identity()  # another of the seller's deployments
OTHER_JOB = "b" * 32


def item(topic: str, offset: int, value: object) -> _WorkflowStreamWireItem:
    [p] = CONVERTER.to_payloads([value])
    return _WorkflowStreamWireItem(
        topic=topic, data=base64.b64encode(p.SerializeToString()).decode(), offset=offset
    )


def marked(
    items: Sequence[_WorkflowStreamWireItem],
    *,
    listed: Sequence[str] = ("events",),
    next_offset: int | None = None,
    more_ready: bool = False,
) -> Payload:
    """What Kontra's workflow interceptor hands the codec for a poll on a job root."""
    end = next_offset if next_offset is not None else (items[-1].offset + 1 if items else 0)
    [p] = CONVERTER.to_payloads(
        [PollResult(items=list(items), next_offset=end, more_ready=more_ready)]
    )
    p.metadata[s.STREAM_MARK] = s.POLL_MARK
    p.metadata[s.BUYER_TOPICS_KEY] = s.buyer_topics(listed)
    return p


def codec(job: str = JOB, identity: tuple[str, str] = SELLER, **kw: object) -> KontraCodec:
    kw.setdefault("buyer_keys", Keys())
    c = KontraCodec(identity[0], [SELLER[1], SECOND[1]], role="seller", **kw)  # type: ignore[arg-type]
    return c.with_context(
        WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(job))
    )


def sample() -> list[_WorkflowStreamWireItem]:
    return [
        item("events", 0, {"n": 0, "secret": f"{MARKER}-events-0"}),
        item("private", 1, {"secret": f"{MARKER}-private-1"}),
        item(PROGRESS_TOPIC, 2, {"percent": 50, "message": f"{MARKER}-progress-2"}),
        item("events", 3, {"n": 3}),
    ]


def blob(blobs: Blobs, i: s.EnvelopeItem) -> bytes:
    assert i.sha256 is not None
    return blobs.data[i.sha256]


async def test_round_trip_restores_the_exact_poll_result() -> None:
    blobs = Blobs()
    original = marked(sample(), next_offset=9)
    [envelope] = await codec(blobs=blobs).encode([original])
    assert envelope.metadata["encoding"] == s.STREAM_ENCODING
    [back] = await codec(blobs=blobs).decode([envelope])
    result = CONVERTER.from_payload(back, PollResult)
    assert result.next_offset == 9
    assert result.more_ready is False
    assert [(i.topic, i.offset) for i in result.items] == [
        ("events", 0),
        ("private", 1),
        (PROGRESS_TOPIC, 2),
        ("events", 3),
    ]
    for got, want in zip(result.items, sample(), strict=True):
        assert got.data == want.data  # the item's Payload, byte for byte (keys stripped)


async def test_every_item_is_a_reference_to_its_jobs_blob() -> None:
    """No ciphertext in the envelope (in history): each item names its blob's size and
    sha256, stored for the job; listed topics to buyer and seller, the rest to the seller."""
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    raw = envelope.data
    assert MARKER.encode() not in raw
    assert b"private" not in raw  # an unlisted topic's name is inside its ciphertext only
    assert b"age-encryption" not in raw
    assert b"YWdlLWVuY3J5cHRpb24" not in raw
    assert json.loads(raw)["v"] == s.ENVELOPE_VERSION == 2
    env = s.parse_envelope(envelope)
    assert [(i.topic, i.offset) for i in env.items] == [
        ("events", 0),
        (None, 1),
        (PROGRESS_TOPIC, 2),
        ("events", 3),
    ]
    assert len(blobs.data) == 4
    assert set(blobs.scopes.values()) == {JOB}
    for i in env.items:
        ct = blob(blobs, i)
        assert (len(ct), hashlib.sha256(ct).hexdigest()) == (i.size, i.sha256)
        assert s.verified_item(ct, i) == ct
        assert MARKER.encode() not in ct
        decrypt(ct, SELLER[0])  # the seller's whole set reads every item
        decrypt(ct, SECOND[0])
        if i.topic is None:
            with pytest.raises(AgeError):
                decrypt(ct, BUYER[0])
            with pytest.raises(s.ItemUnreadable):
                s.open_item(ct, [BUYER[0]], topic="private", offset=i.offset, job_id=JOB)
        else:
            value = CONVERTER.from_payload(
                s.open_item(ct, [BUYER[0]], topic=i.topic, offset=i.offset, job_id=JOB)
            )
            assert isinstance(value, dict)
        with pytest.raises(AgeError):
            decrypt(ct, STRANGER[0])


async def test_open_item_catches_a_relabelled_or_reordered_item() -> None:
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    first, _, progress, _ = s.parse_envelope(envelope).items
    with pytest.raises(s.EnvelopeError, match="relabelled"):
        s.open_item(blob(blobs, first), [BUYER[0]], topic=PROGRESS_TOPIC, offset=0, job_id=JOB)
    with pytest.raises(s.EnvelopeError, match="relabelled"):
        s.open_item(blob(blobs, first), [BUYER[0]], topic="events", offset=2, job_id=JOB)
    inner = s.open_item(
        blob(blobs, progress), [BUYER[0]], topic=PROGRESS_TOPIC, offset=2, job_id=JOB
    )
    assert s.SEALED_KEYS.isdisjoint(inner.metadata)


async def test_an_item_is_its_jobs_alone() -> None:
    """A gate that serves another job's item -- the same topic, the same offset, sealed by
    the same seller, opened with every job key a buyer holds -- as this job's: the job is
    sealed inside the item, so the buyer refuses it, and so does the seller's own decode of
    an envelope pointing at it."""
    blobs = Blobs()
    [mine] = await codec(blobs=blobs).encode([marked(sample())])
    [theirs] = await codec(OTHER_JOB, blobs=blobs).encode([marked(sample())])
    spliced = s.parse_envelope(theirs).items[0]  # events, offset 0, job OTHER_JOB's
    ct = blob(blobs, spliced)
    assert s.open_item(ct, [BUYER[0]], topic="events", offset=0, job_id=OTHER_JOB)
    with pytest.raises(s.EnvelopeError, match="not this job's"):
        s.open_item(ct, [BUYER[0]], topic="events", offset=0, job_id=JOB)
    env = json.loads(mine.data)
    env["items"][0]["sha256"], env["items"][0]["size"] = spliced.sha256, spliced.size
    forged = Payload(metadata=dict(mine.metadata), data=json.dumps(env).encode())
    with pytest.raises(InvalidPayload, match="not this job's"):
        await codec(blobs=blobs).decode([forged])


async def test_references_swapped_between_offsets_do_not_decode_for_the_seller() -> None:
    """A bucket, a gate or a token holder that swaps two items' blobs: each item's offset is
    sealed inside it, so the seller's decode refuses the answer."""
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    env = json.loads(envelope.data)
    a, b = env["items"][0], env["items"][3]
    (a["sha256"], a["size"]), (b["sha256"], b["size"]) = (b["sha256"], b["size"]), (a["sha256"], a["size"])  # fmt: skip
    forged = Payload(metadata=dict(envelope.metadata), data=json.dumps(env).encode())
    with pytest.raises(InvalidPayload, match="relabelled"):
        await codec(blobs=blobs).decode([forged])
    # A reference whose blob is not what it names: refused on fetch.
    env = json.loads(envelope.data)
    env["items"][0]["size"] += 1
    forged = Payload(metadata=dict(envelope.metadata), data=json.dumps(env).encode())
    with pytest.raises(InvalidPayload, match="does not match"):
        await codec(blobs=blobs).decode([forged])


async def test_an_answer_is_cut_at_the_item_cap_with_the_contribs_truncation_protocol() -> None:
    """Kontra's interceptor cuts an answer at ``STREAM_ITEMS_MAX``; the codec does too if
    handed more (the contrib's own truncation protocol)."""
    items = [item("events", i, {"n": i}) for i in range(s.STREAM_ITEMS_MAX + 6)]
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(items)])
    env = s.parse_envelope(envelope)
    assert len(env.items) == s.STREAM_ITEMS_MAX
    assert (env.next_offset, env.more_ready) == (s.STREAM_ITEMS_MAX, True)
    [back] = await codec(blobs=blobs).decode([envelope])
    assert CONVERTER.from_payload(back, PollResult).next_offset == s.STREAM_ITEMS_MAX


def test_the_fixed_sizes() -> None:
    """The largest entry is ``REF_ENTRY_MAX`` (a 64-character topic, the largest offset and
    size, a digest), the largest envelope ``ENVELOPE_MAX``: the frame and ``STREAM_ITEMS_MAX``
    of them. ``envelope_bound`` is what the interceptor counts."""
    longest = {"topic": "t" * 64, "offset": 10**19 - 1, "size": 10**19 - 1, "sha256": "f" * 64}
    assert len(json.dumps(longest, separators=(",", ":"))) + 1 == s.REF_ENTRY_MAX
    envelope = {
        "v": s.ENVELOPE_VERSION,
        "items": [longest] * s.STREAM_ITEMS_MAX,
        "next_offset": 10**19 - 1,
        "more_ready": False,
    }
    assert len(json.dumps(envelope, separators=(",", ":"))) <= s.ENVELOPE_MAX
    assert s.ENVELOPE_MAX == 128 + s.STREAM_ITEMS_MAX * s.REF_ENTRY_MAX == 13_504
    assert s.envelope_bound(s.STREAM_ITEMS_MAX) == s.ENVELOPE_MAX
    assert s.envelope_bound(0) == 128
    with pytest.raises(ValueError, match=">= 0"):
        s.envelope_bound(-1)
    assert s.STREAM_POLLS_MAX * s.STREAM_ITEMS_MAX <= s.STREAM_REFS_MAX


async def test_every_envelope_is_within_its_bound() -> None:
    """Items of every size, listed or not, any recipient count: the envelope is within
    ``envelope_bound`` of its item count, however large the items are."""
    keys = [SELLER, *(generate_identity() for _ in range(15))]
    c = KontraCodec(
        SELLER[0], [k[1] for k in keys], role="seller", buyer_keys=Keys(), blobs=Blobs()
    ).with_context(WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(JOB)))
    sizes = [10, 100_000, 300_000, 50]
    items = [
        item(("events", "private", PROGRESS_TOPIC)[i % 3], 10**12 + i * 7, {"pad": "z" * n})
        for i, n in enumerate(sizes * 3)
    ]
    [envelope] = await c.encode([marked(items, next_offset=10**18)])
    env = s.parse_envelope(envelope)
    assert len(env.items) == len(items)
    assert len(envelope.data) <= s.envelope_bound(len(items))
    held = marked([], next_offset=10**18, more_ready=True)  # a poll held back: a frame
    [envelope] = await c.encode([held])
    assert len(envelope.data) <= s.envelope_bound(0)


async def test_without_a_store_every_item_is_a_stub_the_seller_drops() -> None:
    [stub] = await codec().encode([marked(sample())])
    env = s.parse_envelope(stub)
    assert [i.sha256 for i in env.items] == [None] * 4
    assert all(i.size > 0 for i in env.items)
    [back] = await codec(blobs=Blobs()).decode([stub])
    assert CONVERTER.from_payload(back, PollResult).items == []


async def test_a_stream_item_is_never_opened_as_another_payload() -> None:
    """An unlisted item's digest and size are in every envelope a relay token holder reads.
    Named as an ordinary claim-check reference (in a job's input, say), or its ciphertext
    sent inline, the seller's codec would fetch it with the seller's own rights and decode it
    as that payload -- a seller-private item handed to whatever the offering does with its
    input. A payload that opens with a sealed item's keys is refused: only a stream envelope
    opens one."""
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    private = next(i for i in s.parse_envelope(envelope).items if i.topic is None)
    assert private.sha256 is not None
    reference = json.dumps({"sha256": private.sha256, "size": private.size}).encode()
    named = Payload(metadata={"encoding": b"binary/kontra-claim-check", "kontra-v": b"1"}, data=reference)  # fmt: skip
    inline = Payload(
        metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"}, data=blob(blobs, private)
    )
    for payload in (named, inline):
        with pytest.raises(InvalidPayload, match="sealed stream item"):
            await codec(blobs=blobs).decode([payload])


async def test_the_bucket_double_scopes_as_the_gate_does() -> None:
    """``MemoryBlobs``, the bucket of the hosting and wire tests, keeps the gate's rules: a
    stream blob is its job's alone -- another job's reader never reads it, and a call naming
    it for another job, or naming a claim-check's digest, is refused whole."""
    from memory_blobs import BlobScopeError, MemoryBlobs

    blobs = MemoryBlobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    first = s.parse_envelope(envelope).items[0]
    assert first.sha256 is not None
    ct = blobs.data[first.sha256]
    assert await blobs.reader(JOB)(first.sha256, first.size) == ct
    assert await blobs.reader(OTHER_JOB)(first.sha256, first.size) is None
    assert await blobs.reader(JOB)(first.sha256, first.size - 1) is None
    with pytest.raises(BlobScopeError):
        await blobs.put_stream(OTHER_JOB, [(first.sha256, ct)], lambda _: None)
    with pytest.raises(BlobScopeError):
        await blobs.put(first.sha256, ct)  # a stream blob is never a claim-check
    claim = encrypt(b"claim-checked", [SELLER[1]])
    await blobs.put(hashlib.sha256(claim).hexdigest(), claim)
    with pytest.raises(BlobScopeError):
        await blobs.put_stream(JOB, [(hashlib.sha256(claim).hexdigest(), claim)], lambda _: None)


async def test_a_codec_that_cannot_fetch_answers_the_envelope_as_it_is() -> None:
    """A seller codec with no blob store (``kontra dev``'s codec server, say) cannot fetch an
    envelope's items: it answers the envelope unchanged -- the references in view -- never a
    poll result with its items missing."""
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(sample())])
    assert all(i.sha256 is not None for i in s.parse_envelope(envelope).items)
    [back] = await codec().decode([envelope])
    assert back == envelope


async def test_a_failing_bucket_costs_items_never_the_answer() -> None:
    """A store that fails is caught: every new item a stub, the answer encoded. A stub is not
    remembered, so a later read stores it."""
    down = Blobs(fail=True)
    [envelope] = await codec(blobs=down).encode([marked(sample())])
    assert [i.sha256 for i in s.parse_envelope(envelope).items] == [None] * 4
    c = codec(blobs=Blobs())
    [envelope] = await c.encode([marked(sample())])
    assert all(i.sha256 is not None for i in s.parse_envelope(envelope).items)


async def test_one_upload_budget_per_completion() -> None:
    """temporalio encodes a completion's payloads one after another, in one codec copy: the
    copy's upload window is shared, so a slow bucket holds the completion at most the codec's
    ``stream_upload_budget``, whatever the number of answers. A new copy (the next
    completion) opens a new window, and stores what was a stub."""
    slow = Blobs(delay=0.2)  # per upload, one after another
    copy = codec(blobs=slow, stream_upload_budget=0.3)
    loop = asyncio.get_running_loop()
    began = loop.time()
    answers = [
        marked([item("events", 10 * w + i, {"w": w, "i": i}) for i in range(3)], next_offset=10 * w + 3)
        for w in range(10)
    ]  # fmt: skip
    envelopes = [(await copy.encode([a]))[0] for a in answers]
    took = loop.time() - began
    assert took < 0.3 + 0.3  # the budget, and one upload cut short: not 10 x 3 x 0.2 s
    stored = [i.sha256 is not None for e in envelopes for i in s.parse_envelope(e).items]
    assert 0 < sum(stored) < len(stored)  # some stored in time, the rest stubs
    assert stored == sorted(stored, reverse=True)  # the window closed once, for good
    assert len(slow.calls) <= 2  # past the window no answer even asks the bucket
    again = copy.with_context(
        WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(JOB))
    )
    [late] = await again.encode([answers[-1]])
    assert any(i.sha256 is not None for i in s.parse_envelope(late).items)


def test_a_worker_takes_its_upload_budget_from_the_environment(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``$KONTRA_STREAM_UPLOAD_BUDGET`` for a worker given none (a seller run as its own
    process): unset or empty is the 2 s default; a valid number of seconds is taken; anything
    else is logged and the default used, never a budget past the cap."""
    env = s.UPLOAD_BUDGET_ENV
    assert env == "KONTRA_STREAM_UPLOAD_BUDGET"
    assert s.upload_budget_from_env({}) == s.upload_budget_from_env({env: ""}) == 2.0
    assert s.upload_budget_from_env({env: "8"}) == s.upload_budget_from_env({env: "8.0"}) == 8.0
    assert s.upload_budget_from_env({env: "0.5"}) == 0.5
    for bad in ("0", "-1", "9", "inf", "nan", "two"):
        caplog.clear()
        assert s.upload_budget_from_env({env: bad}) == s.STREAM_UPLOAD_BUDGET
        assert env in caplog.text


def test_the_upload_budget_is_the_codecs_to_set() -> None:
    """2 s by default (``kontra serve``'s), any number of seconds above 0 and at most 8 given;
    0, a negative, one past 8 (under the 10 s task timeout), an infinite or a NaN budget
    refused when the codec is built."""
    assert codec().stream_upload_budget == s.STREAM_UPLOAD_BUDGET == 2.0
    assert codec(stream_upload_budget=8).stream_upload_budget == 8.0
    assert s.STREAM_UPLOAD_BUDGET_MAX == 8.0
    for bad in (0, -1.0, 8.01, 30, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="stream_upload_budget"):
            codec(stream_upload_budget=bad)


async def test_a_reread_stores_nothing_new() -> None:
    """A thousand polls from 0 (a relay token holder's re-reads) upload each item once: the
    cache answers them."""
    blobs = Blobs()
    c = codec(blobs=blobs)
    items = [item("events", i, {"n": i}) for i in range(s.STREAM_ITEMS_MAX)]
    first = None
    for _ in range(1000):
        [envelope] = await c.encode([marked(items)])
        first = first or envelope.data
        assert envelope.data == first
    assert blobs.uploads == len(blobs.data) == s.STREAM_ITEMS_MAX
    assert blobs.calls == [s.STREAM_ITEMS_MAX]  # the gate asked once


async def test_a_reread_past_the_cache_seals_the_same_bytes() -> None:
    """The cache is per process and shared by jobs: two jobs overflowing it evict each
    other's items, and each re-read seals them again -- to the same bytes (per-worker
    sealing), so the bucket stores nothing new and nothing is uploaded again."""
    blobs = Blobs()
    c = codec(blobs=blobs)
    c._stream_refs = s.BlobRefs(size=2)  # pyright: ignore[reportPrivateUsage]
    items = [item("events", i, {"n": i}) for i in range(8)]
    other = c.with_context(
        WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(OTHER_JOB))
    )
    [one] = await c.encode([marked(items)])
    [two] = await other.encode([marked(items)])
    stored, uploads = len(blobs.data), blobs.uploads
    assert stored == uploads == 16  # two jobs, their own blobs: nothing links them
    for _ in range(5):
        assert (await c.encode([marked(items)]))[0].data == one.data
        assert (await other.encode([marked(items)]))[0].data == two.data
    assert (len(blobs.data), blobs.uploads) == (stored, uploads)
    assert len(blobs.calls) == 12  # the gate was asked (exists), never uploaded to


_SEAL_IN_A_PROCESS = """
import asyncio, base64, json, os, sys
from temporalio.api.common.v1 import Payload
from temporalio.contrib.workflow_streams import PollResult
from temporalio.contrib.workflow_streams._types import _WorkflowStreamWireItem
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import WorkflowSerializationContext
from kontra.codec import KontraCodec
from kontra.codec import stream as s
from kontra.contract.names import job_workflow_id

class Keys:
    async def buyer_key(self, job_id):
        return os.environ["BUYER"]

class Store:
    async def put_stream(self, job_id, blobs, done):
        for digest, _ in blobs:
            done(digest)

conv = PydanticPayloadConverter()
[inner] = conv.to_payloads([{"n": 1}])
inner.metadata.update({"a": b"1", "b": b"2", "c": b"3"})
items = [_WorkflowStreamWireItem(topic="events", data=base64.b64encode(inner.SerializeToString()).decode(), offset=0)]
[marked] = conv.to_payloads([PollResult(items=items, next_offset=1)])
marked.metadata[s.STREAM_MARK] = s.POLL_MARK
marked.metadata[s.BUYER_TOPICS_KEY] = s.buyer_topics(["events"])
codec = KontraCodec(os.environ["SECRET"], [os.environ["PUBLIC"]], role="seller", buyer_keys=Keys(), blobs=Store())
codec = codec.with_context(WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(os.environ["JOB"])))
[envelope] = asyncio.run(codec.encode([marked]))
print(json.loads(envelope.data)["items"][0]["sha256"])
"""


def test_a_restarted_worker_seals_an_item_to_the_same_bytes() -> None:
    """Per-worker sealing holds across processes: a worker restarted, or another pod with the
    same mounted key, seals the same item to the same file (its sealed plaintext is
    serialized deterministically, whatever order a process keeps a Payload's metadata in), so
    a re-read after a restart stores nothing new."""
    import os
    import subprocess
    import sys

    env = {
        **os.environ,
        "SECRET": SELLER[0],
        "PUBLIC": SELLER[1],
        "BUYER": BUYER[1],
        "JOB": JOB,
        "PYTHONHASHSEED": "random",
    }
    digests = {
        subprocess.run(
            [sys.executable, "-c", _SEAL_IN_A_PROCESS],
            env=env,
            capture_output=True,
            text=True,
            check=True,
            timeout=120,
        ).stdout.strip()
        for _ in range(4)
    }
    assert len(digests) == 1, digests
    assert all(len(d) == 64 for d in digests)


async def test_sealing_is_per_worker_and_per_job() -> None:
    """The same item sealed by the same worker for the same job is the same file; another
    worker of the seller (another identity, the same recipients), another job, or a
    different item is another file: the seed needs the worker's secret."""
    items = [item("events", 0, {"n": 0})]

    async def ct(c: KontraCodec) -> bytes:
        blobs = Blobs()
        c._blobs = blobs  # pyright: ignore[reportPrivateUsage]
        [envelope] = await c.encode([marked(items)])
        [i] = s.parse_envelope(envelope).items
        return blob(blobs, i)

    same = await ct(codec())
    assert await ct(codec()) == same
    assert await ct(codec(identity=SECOND)) != same
    assert await ct(codec(job=OTHER_JOB)) != same
    # A seeded file is a standard age file: every recipient opens it.
    assert decrypt(same, SELLER[0]) == decrypt(same, SECOND[0]) == decrypt(same, BUYER[0])


def test_blob_refs_are_bounded() -> None:
    refs = s.BlobRefs(size=2)
    keys = [s.BlobRefs.key("job", bytes([i]), ["age1x"]) for i in range(3)]
    for i, k in enumerate(keys):
        refs.put(k, (f"{i:064x}", i))
    assert len(refs) == 2
    assert refs.get(keys[0]) is None
    assert refs.get(keys[2]) == (f"{2:064x}", 2)
    assert s.BlobRefs.key("a", b"x", ["k"]) != s.BlobRefs.key("b", b"x", ["k"])
    assert s.BlobRefs.key("a", b"x", ["k", "l"]) == s.BlobRefs.key("a", b"x", ["l", "k"])
    assert s.BlobRefs.key("a", b"x", ["k"]) != s.BlobRefs.key("a", b"x", ["k", "l"])
    with pytest.raises(ValueError, match="size"):
        s.BlobRefs(size=0)
    assert len(s.BlobRefs()._refs) == 0  # pyright: ignore[reportPrivateUsage]
    assert s.BlobRefs()._size == s.STREAM_REFS_MAX  # pyright: ignore[reportPrivateUsage]


async def test_the_offset_is_plaintext_and_validated() -> None:
    p = s.offset_payload(15)
    [out] = await codec().encode([p])
    assert out == p
    assert s.parse_offset(out) == 15
    assert CONVERTER.from_payload(out, int) == 15
    [back] = await codec().decode([out])
    assert CONVERTER.from_payload(back, int) == 15
    for bad in (b"-1", b"01", b"1.5", b"1" * 20, b"15\n", b'"15"'):
        forged = Payload(metadata=dict(p.metadata), data=bad)
        with pytest.raises(CodecError, match="offset"):
            await codec().encode([forged])
    with pytest.raises(s.EnvelopeError):
        s.parse_offset(Payload(metadata={"encoding": b"json/plain"}, data=b"15"))


async def test_a_replayed_answer_passes_as_the_constant() -> None:
    """On replay Kontra's interceptor answers a poll with a marked constant (its answer is in
    the history already, never sent again): the codec passes the constant whatever bytes the
    marked payload carries, and seals, stores and looks up nothing -- no buyer key, no
    bucket."""
    blobs = Blobs()
    replayed = s.replayed_payload()
    forged = Payload(metadata=dict(replayed.metadata), data=b'{"items": "anything"}')
    c = codec(blobs=blobs, buyer_keys=None)
    assert await c.encode([replayed, forged]) == [replayed, replayed]
    assert blobs.data == {}


@pytest.mark.parametrize(
    "context",
    [
        None,
        WorkflowSerializationContext(namespace="acme", workflow_id=agent_workflow_id(JOB)),
        WorkflowSerializationContext(namespace="acme", workflow_id="anything"),
        ActivitySerializationContext(
            namespace="acme",
            activity_id="1",
            activity_type="a",
            activity_task_queue="q",
            workflow_id=job_workflow_id(JOB),
            workflow_type="W",
            is_local=False,
        ),
        NexusSerializationContext(endpoint="k" + "5" * 32, service="acme.x", operation="execute"),
    ],
    ids=["none", "agent-child", "not-a-job", "activity", "nexus"],
)
async def test_a_mark_outside_a_sellers_job_workflow_is_refused(context: object) -> None:
    c = KontraCodec(SELLER[0], [SELLER[1]], role="seller", buyer_keys=Keys())
    if context is not None:
        c = c.with_context(context)  # type: ignore[arg-type]
    for p in (marked(sample()), s.offset_payload(3)):
        with pytest.raises(CodecError, match="outside a seller's job workflow"):
            await c.encode([p])


async def test_a_mark_in_the_buyer_role_is_refused() -> None:
    c = KontraCodec(BUYER[0], [BUYER[1]], role="buyer", seller_keys=Keys())
    c = c.with_context(
        WorkflowSerializationContext(namespace="b", workflow_id=job_workflow_id(JOB))
    )
    with pytest.raises(CodecError, match="outside a seller's job workflow"):
        await c.encode([marked(sample())])


@pytest.mark.parametrize(
    "listed",
    [None, b"not json", b'{"a": 1}', b'["events"]', b'["kontra.progress", 3]', b'["kontra.progress", "bad topic"]',
     json.dumps([PROGRESS_TOPIC, *[f"t{i}" for i in range(9)]]).encode()],
    ids=["absent", "not-json", "object", "no-progress", "not-a-string", "bad-name", "too-many"],
)  # fmt: skip
async def test_a_malformed_buyer_topic_list_is_refused(listed: bytes | None) -> None:
    p = marked(sample())
    if listed is None:
        del p.metadata[s.BUYER_TOPICS_KEY]
    else:
        p.metadata[s.BUYER_TOPICS_KEY] = listed
    with pytest.raises(CodecError, match="buyer topics"):
        await codec().encode([p])


async def test_a_marked_payload_that_is_not_a_poll_result_is_refused() -> None:
    p = marked(sample())
    for data in (b"[]", b'{"items": 3, "next_offset": 0, "more_ready": false}', b"{"):
        forged = Payload(metadata=dict(p.metadata), data=data)
        with pytest.raises(CodecError):
            await codec().encode([forged])
    forged = Payload(metadata={**p.metadata, s.STREAM_MARK: b"poll/v9"}, data=p.data)
    with pytest.raises(CodecError, match="unknown"):
        await codec().encode([forged])


async def test_an_unresolvable_buyer_key_fails_closed() -> None:
    with pytest.raises(CodecError, match="buyer"):
        await codec(buyer_keys=Keys(fail=True)).encode([marked(sample())])


async def test_unmarked_payloads_in_the_same_batch_take_the_usual_path() -> None:
    plain = CONVERTER.to_payloads([{"x": MARKER}])[0]
    out = await codec(blobs=Blobs()).encode([plain, marked(sample()), s.offset_payload(4)])
    assert [p.metadata["encoding"] for p in out] == [
        b"binary/kontra-age",
        s.STREAM_ENCODING,
        b"json/plain",
    ]
    assert MARKER.encode() not in out[0].data


def test_a_poll_failure_is_a_fixed_outcome_without_a_traceback() -> None:
    """Kontra's interceptor raises :class:`PollFailure` for a poll it refuses or that fails:
    a fixed message per type and, through Kontra's failure converter, no stack trace -- an
    accepted poll's failed outcome is a small constant (decisions I39)."""
    for kind in (s.STREAM_FULL, s.STREAM_BUSY, "TruncatedOffset", "StreamDraining", "Other"):
        with pytest.raises(s.PollFailure) as raised:
            raise s.PollFailure(kind)
        e = raised.value
        failure = Failure()
        converter = KontraFailureConverter().with_context(
            WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(JOB))
        )
        converter.to_failure(e, CONVERTER, failure)
        assert failure.stack_trace == ""
        assert failure.application_failure_info.type == (
            kind if kind != "Other" else s.STREAM_ERROR
        )
        assert e.__traceback__ is not None  # the exception keeps its own
        assert len(failure.SerializeToString()) < 512


# --------------------------------------------------------------------------- the relay's reader


def envelope(env: object, **meta: bytes) -> Payload:
    metadata = {"encoding": s.STREAM_ENCODING, "kontra-v": b"1", **meta}
    return Payload(metadata=metadata, data=json.dumps(env).encode())


CT = base64.b64encode(encrypt(b"x", [BUYER[1]])).decode()
D = "0" * 64


def ref(offset: int, **kw: object) -> dict[str, object]:
    return {"topic": "a", "offset": offset, "size": 9, "sha256": D, **kw}


@pytest.mark.parametrize(
    "env",
    [
        [],
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": CT}], "next_offset": 1, "more_ready": False},
        {"v": 3, "items": [], "next_offset": 0, "more_ready": False},
        {"v": True, "items": [], "next_offset": 0, "more_ready": False},
        {"v": 2, "items": [], "next_offset": -1, "more_ready": False},
        {"v": 2, "items": [], "next_offset": 0, "more_ready": 0},
        {"v": 2, "items": [], "next_offset": 0, "more_ready": False, "extra": 1},
        {"v": 2, "items": [ref(0)], "next_offset": 0, "more_ready": False},
        {"v": 2, "items": [ref(1), ref(1)], "next_offset": 5, "more_ready": False},
        {"v": 2, "items": [ref(0, topic="bad topic")], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, sha256="F" * 64)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, sha256="0" * 63)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, sha256=1)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, size=-1)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, size=True)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(0, ct=CT)], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [{"topic": "a", "offset": 0, "size": 9}], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [{"topic": "a", "offset": 0, "sha256": D}], "next_offset": 1, "more_ready": False},
        {"v": 2, "items": [ref(i, topic=None) for i in range(s.STREAM_ITEMS_MAX + 1)], "next_offset": 100, "more_ready": True},
    ],
    ids=lambda env: json.dumps(env)[:60],
)  # fmt: skip
def test_parse_envelope_is_strict(env: object) -> None:
    with pytest.raises(s.EnvelopeError):
        s.parse_envelope(envelope(env))


def test_parse_envelope_refuses_what_is_not_an_envelope() -> None:
    good = {"v": 2, "items": [ref(0), ref(2, sha256=None)], "next_offset": 3, "more_ready": False}
    items = s.parse_envelope(envelope(good)).items
    assert items == (s.EnvelopeItem("a", 0, D, 9), s.EnvelopeItem("a", 2, None, 9))
    for meta in ({"encoding": b"binary/kontra-age"}, {"kontra-v": b"2"}):
        with pytest.raises(s.EnvelopeError):
            s.parse_envelope(envelope(good, **meta))
    huge = Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=b" " * (s.ENVELOPE_MAX + 1))  # fmt: skip
    with pytest.raises(s.EnvelopeError, match="larger"):
        s.parse_envelope(huge)


def test_a_fetched_blob_is_kept_only_when_it_is_the_item() -> None:
    """What the relay keeps of a fetched blob: exactly the size and digest its reference
    names, and an age file -- never plaintext, never another item's bytes."""
    ct = base64.b64decode(CT)
    i = s.EnvelopeItem("a", 0, hashlib.sha256(ct).hexdigest(), len(ct))
    assert s.verified_item(ct, i) == ct
    plain = b"age-encryption.org/v1\nnot an age file at all, just text"
    for data, it in (
        (None, i),
        (ct[:-1], i),
        (ct + b"x", i),
        (ct, s.EnvelopeItem("a", 0, None, len(ct))),
        (ct, s.EnvelopeItem("a", 0, "0" * 64, len(ct))),
        (plain, s.EnvelopeItem("a", 0, hashlib.sha256(plain).hexdigest(), len(plain))),
        (b'{"x": 1}', s.EnvelopeItem("a", 0, hashlib.sha256(b'{"x": 1}').hexdigest(), 8)),
    ):
        assert s.verified_item(data, it) is None


async def test_the_buyers_codec_cannot_open_an_envelope_with_unlisted_items() -> None:
    blobs = Blobs()
    [env] = await codec(blobs=blobs).encode([marked(sample())])
    buyer = KontraCodec(BUYER[0], [BUYER[1]], role="buyer", seller_keys=Keys(), blobs=blobs)
    with pytest.raises(InvalidPayload):
        await buyer.decode([env])
    nexus = codec(blobs=blobs).with_context(
        NexusSerializationContext(endpoint="k" + "5" * 32, service="acme.x", operation="execute")
    )
    with pytest.raises(InvalidPayload, match="never a Nexus payload"):
        await nexus.decode([env])


# --------------------------------------------------------------------------- hostile JSON


DEEP = b"[" * 10_000  # under ENVELOPE_MAX, past what the JSON parser nests


def test_json_nested_past_the_stack_is_an_envelope_error() -> None:
    """A hostile worker's answer nested deeper than ``json`` can parse is refused like any
    other answer that is not an envelope (``RecursionError`` never escapes the reader)."""
    with pytest.raises(s.EnvelopeError, match="not JSON"):
        s.parse_envelope(
            Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=DEEP)
        )
    inside = b'{"v":2,"items":[' + b"[" * 5_000 + b"]" * 5_000 + b'],"next_offset":1,"more_ready":false}'  # fmt: skip
    with pytest.raises(s.EnvelopeError):
        s.parse_envelope(
            Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=inside)
        )


async def test_json_nested_past_the_stack_fails_the_codec_closed() -> None:
    nested = Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=DEEP)
    with pytest.raises(InvalidPayload, match="not JSON"):
        await codec().decode([nested])
    deep_result = marked(sample())
    deep_result.data = b"[" * 100_000
    with pytest.raises(CodecError, match="not JSON"):
        await codec().encode([deep_result])
    deep_listed = marked(sample())
    deep_listed.metadata[s.BUYER_TOPICS_KEY] = b"[" * 2000  # under the list's 2,048 bytes
    with pytest.raises(CodecError, match="not JSON"):
        await codec().encode([deep_listed])
