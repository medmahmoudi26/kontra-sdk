"""Seam ⑤: the stream envelope (decisions I31, ``kontra.codec.stream``) -- a marked poll result
sealed item by item, the listed topics to buyer and seller, every other topic to the seller
alone with its name hidden; trimming, claim-check and stubs; the plaintext offset; the
refusals (a mark outside a seller's job, a malformed buyer list); relabelling caught; and the
relay's strict reader."""

from __future__ import annotations

import base64
import json
from collections.abc import Sequence

import pytest
from temporalio.api.common.v1 import Payload
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

from kontra.codec import CodecError, InvalidPayload, KontraCodec
from kontra.codec import stream as s
from kontra.codec.age import AgeError, decrypt, encrypt, generate_identity
from kontra.contract.names import PROGRESS_TOPIC, agent_workflow_id, job_workflow_id

from .test_codec import BUYER, JOB, SELLER, STRANGER, Blobs, Keys

MARKER = "KONTRA-STREAM-MARKER-41c2"
CONVERTER = PydanticPayloadConverter()
SECOND = generate_identity()  # another of the seller's deployments


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


def codec(**kw: object) -> KontraCodec:
    kw.setdefault("buyer_keys", Keys())
    c = KontraCodec(SELLER[0], [SELLER[1], SECOND[1]], role="seller", **kw)  # type: ignore[arg-type]
    return c.with_context(
        WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(JOB))
    )


def sample() -> list[_WorkflowStreamWireItem]:
    return [
        item("events", 0, {"n": 0, "secret": f"{MARKER}-events-0"}),
        item("private", 1, {"secret": f"{MARKER}-private-1"}),
        item(PROGRESS_TOPIC, 2, {"percent": 50, "message": f"{MARKER}-progress-2"}),
        item("events", 3, {"n": 3}),
    ]


async def test_round_trip_restores_the_exact_poll_result() -> None:
    original = marked(sample(), next_offset=9)
    [envelope] = await codec().encode([original])
    assert envelope.metadata["encoding"] == s.STREAM_ENCODING
    [back] = await codec().decode([envelope])
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


async def test_listed_topics_to_buyer_and_seller_the_rest_to_the_seller_only() -> None:
    [envelope] = await codec().encode([marked(sample())])
    raw = envelope.data
    assert MARKER.encode() not in raw
    assert b"private" not in raw  # an unlisted topic's name is inside its ciphertext only
    env = s.parse_envelope(envelope)
    assert [(i.topic, i.offset) for i in env.items] == [
        ("events", 0),
        (None, 1),
        (PROGRESS_TOPIC, 2),
        ("events", 3),
    ]
    for i in env.items:
        assert i.ciphertext is not None
        assert i.ciphertext.startswith(s.AGE_MAGIC)
        decrypt(i.ciphertext, SELLER[0])  # the seller's whole set reads every item
        decrypt(i.ciphertext, SECOND[0])
        if i.topic is None:
            with pytest.raises(AgeError):
                decrypt(i.ciphertext, BUYER[0])
            with pytest.raises(s.ItemUnreadable):
                s.open_item(i.ciphertext, [BUYER[0]], topic="private", offset=i.offset)
        else:
            value = CONVERTER.from_payload(
                s.open_item(i.ciphertext, [BUYER[0]], topic=i.topic, offset=i.offset)
            )
            assert isinstance(value, dict)
        with pytest.raises(AgeError):
            decrypt(i.ciphertext, STRANGER[0])


async def test_open_item_catches_a_relabelled_or_reordered_item() -> None:
    [envelope] = await codec().encode([marked(sample())])
    first, _, progress, _ = s.parse_envelope(envelope).items
    assert first.ciphertext is not None
    assert progress.ciphertext is not None
    with pytest.raises(s.EnvelopeError, match="relabelled"):
        s.open_item(first.ciphertext, [BUYER[0]], topic=PROGRESS_TOPIC, offset=0)
    with pytest.raises(s.EnvelopeError, match="relabelled"):
        s.open_item(first.ciphertext, [BUYER[0]], topic="events", offset=2)
    inner = s.open_item(progress.ciphertext, [BUYER[0]], topic=PROGRESS_TOPIC, offset=2)
    assert s.SEALED_TOPIC_KEY not in inner.metadata
    assert s.SEALED_OFFSET_KEY not in inner.metadata


async def test_a_swapped_envelope_does_not_decode_for_the_seller() -> None:
    [envelope] = await codec().encode([marked(sample())])
    env = json.loads(envelope.data)
    env["items"][0]["ct"], env["items"][3]["ct"] = env["items"][3]["ct"], env["items"][0]["ct"]
    forged = Payload(metadata=dict(envelope.metadata), data=json.dumps(env).encode())
    with pytest.raises(InvalidPayload, match="relabelled"):
        await codec().decode([forged])


async def test_trimmed_at_the_envelope_limit_with_the_contribs_truncation_protocol() -> None:
    big = "x" * (100 * 1024)
    items = [item("events", i, {"blob": big}) for i in range(8)]
    [envelope] = await codec().encode([marked(items, next_offset=8)])
    assert len(envelope.data) <= s.ENVELOPE_MAX
    env = s.parse_envelope(envelope)
    assert 0 < len(env.items) < 8
    assert env.more_ready is True
    assert env.next_offset == env.items[-1].offset + 1
    [back] = await codec().decode([envelope])
    assert CONVERTER.from_payload(back, PollResult).next_offset == env.next_offset


async def test_an_item_above_the_inline_limit_is_claim_checked_or_a_stub() -> None:
    huge = {"blob": "y" * (s.ITEM_INLINE_MAX + 1024)}
    items = [item("events", 0, {"n": 0}), item("events", 1, huge), item("events", 2, {"n": 2})]
    blobs = Blobs()
    [envelope] = await codec(blobs=blobs).encode([marked(items)])
    env = s.parse_envelope(envelope)
    assert [i.ciphertext is None for i in env.items] == [False, True, False]
    assert env.items[1].size > s.ITEM_INLINE_MAX
    assert len(blobs.data) == 1
    [back] = await codec(blobs=blobs).decode([envelope])
    result = CONVERTER.from_payload(back, PollResult)
    assert [i.offset for i in result.items] == [0, 1, 2]  # the seller fetches it
    assert result.items[1].data == items[1].data
    # Without a blob store: a stub, which the seller's decode drops (a gap for everyone).
    [stub] = await codec().encode([marked(items)])
    env = s.parse_envelope(stub)
    assert env.items[1].ciphertext is None
    assert json.loads(stub.data)["items"][1]["ct"] is None
    [back] = await codec().decode([stub])
    assert [i.offset for i in CONVERTER.from_payload(back, PollResult).items] == [0, 2]


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
    out = await codec().encode([plain, marked(sample()), s.offset_payload(4)])
    assert [p.metadata["encoding"] for p in out] == [
        b"binary/kontra-age",
        s.STREAM_ENCODING,
        b"json/plain",
    ]
    assert MARKER.encode() not in out[0].data


# --------------------------------------------------------------------------- the relay's reader


def envelope(env: object, **meta: bytes) -> Payload:
    metadata = {"encoding": s.STREAM_ENCODING, "kontra-v": b"1", **meta}
    return Payload(metadata=metadata, data=json.dumps(env).encode())


CT = base64.b64encode(encrypt(b"x", [BUYER[1]])).decode()


@pytest.mark.parametrize(
    "env",
    [
        [],
        {"v": 2, "items": [], "next_offset": 0, "more_ready": False},
        {"v": True, "items": [], "next_offset": 0, "more_ready": False},
        {"v": 1, "items": [], "next_offset": -1, "more_ready": False},
        {"v": 1, "items": [], "next_offset": 0, "more_ready": 0},
        {"v": 1, "items": [], "next_offset": 0, "more_ready": False, "extra": 1},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": CT}], "next_offset": 0, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 1, "ct": CT}, {"topic": "a", "offset": 1, "ct": CT}], "next_offset": 5, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": base64.b64encode(b'{"plain": 1}').decode()}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": CT + "!"}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "bad topic", "offset": 0, "ct": CT}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": CT, "x": 1}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": None}], "next_offset": 1, "more_ready": False},
        {"v": 1, "items": [{"topic": "a", "offset": 0, "size": 9, "ref": {"sha256": "0" * 64, "size": 8}}], "next_offset": 1, "more_ready": False},
    ],
)  # fmt: skip
def test_parse_envelope_is_strict(env: object) -> None:
    with pytest.raises(s.EnvelopeError):
        s.parse_envelope(envelope(env))


def test_parse_envelope_refuses_what_is_not_an_envelope() -> None:
    good = {"v": 1, "items": [{"topic": "a", "offset": 0, "ct": CT}], "next_offset": 1, "more_ready": False}  # fmt: skip
    assert s.parse_envelope(envelope(good)).items[0].ciphertext == base64.b64decode(CT)
    for meta in ({"encoding": b"binary/kontra-age"}, {"kontra-v": b"2"}):
        with pytest.raises(s.EnvelopeError):
            s.parse_envelope(envelope(good, **meta))
    huge = Payload(metadata={"encoding": s.STREAM_ENCODING, "kontra-v": b"1"}, data=b" " * (s.ENVELOPE_MAX + 1))  # fmt: skip
    with pytest.raises(s.EnvelopeError, match="larger"):
        s.parse_envelope(huge)


async def test_the_buyers_codec_cannot_open_an_envelope_with_unlisted_items() -> None:
    [env] = await codec().encode([marked(sample())])
    buyer = KontraCodec(BUYER[0], [BUYER[1]], role="buyer", seller_keys=Keys())
    with pytest.raises(InvalidPayload):
        await buyer.decode([env])
    nexus = codec().with_context(
        NexusSerializationContext(endpoint="k" + "5" * 32, service="acme.x", operation="execute")
    )
    with pytest.raises(InvalidPayload, match="never a Nexus payload"):
        await nexus.decode([env])
