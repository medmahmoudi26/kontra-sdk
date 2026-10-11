"""KontraCodec: recipients by serialization context (decisions I7), plaintext passthrough
where the gate must read, fail-closed key resolution, claim-check, tamper detection, and a
DataConverter round trip through temporalio's own context plumbing."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256

import pytest
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    NexusSerializationContext,
    SerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ApplicationError

from kontra.codec import (
    CLAIM_CHECK_ENCODING,
    ENCODING,
    CodecError,
    KontraCodec,
    data_converter,
)
from kontra.codec.age import AgeError, decrypt, encrypt, generate_identity
from kontra.contract.names import job_workflow_id

JOB = "0123456789abcdef0123456789abcdef"
ENDPOINT = "k" + "5" * 32
MARKER = b"KONTRA-PLAINTEXT-MARKER-7f3a"

SELLER = generate_identity()
BUYER = generate_identity()
STRANGER = generate_identity()


@dataclass
class Keys:
    """BuyerKeys + SellerKeys fake that records what it was asked."""

    buyer: object = BUYER[1]
    seller: object = SELLER[1]
    fail: bool = False
    asked: list[tuple[str, str]] = field(default_factory=list[tuple[str, str]])

    async def buyer_key(self, job_id: str) -> str:
        self.asked.append(("buyer", job_id))
        if self.fail:
            raise LookupError("gate unreachable")
        return self.buyer  # type: ignore[return-value]

    async def seller_key(self, endpoint: str) -> str:
        self.asked.append(("seller", endpoint))
        if self.fail:
            raise LookupError("gate unreachable")
        return self.seller  # type: ignore[return-value]


class Blobs:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.gets = 0

    async def put(self, sha256: str, data: bytes) -> None:
        self.data[sha256] = data

    async def get(self, sha256: str) -> bytes:
        self.gets += 1
        return self.data[sha256]


def seller_codec(**kw: object) -> KontraCodec:
    kw.setdefault("buyer_keys", Keys())
    return KontraCodec(SELLER[0], SELLER[1], role="seller", **kw)  # type: ignore[arg-type]


#: For a seller-role codec that calls an offering: the endpoint's seller is someone else.
OTHER_SELLER = Keys(seller=STRANGER[1])


def buyer_codec(**kw: object) -> KontraCodec:
    kw.setdefault("seller_keys", Keys())
    return KontraCodec(BUYER[0], BUYER[1], role="buyer", **kw)  # type: ignore[arg-type]


def payload(data: bytes = b'"hello ' + MARKER + b'"') -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=data)


def readers(p: Payload) -> set[str]:
    """Which of the three parties can decrypt an encoded payload."""
    assert p.metadata["encoding"] == ENCODING
    assert p.metadata["kontra-v"] == b"1"
    out: set[str] = set()
    for name, (secret, _) in {"seller": SELLER, "buyer": BUYER, "stranger": STRANGER}.items():
        try:
            inner = Payload.FromString(decrypt(p.data, secret))
        except AgeError:
            continue
        assert inner == payload()
        out.add(name)
    return out


WF_JOB = WorkflowSerializationContext(namespace="acme", workflow_id=job_workflow_id(JOB))
ACT_JOB = ActivitySerializationContext(
    namespace="acme",
    activity_id="1",
    activity_type="fetch",
    activity_task_queue="acme/enrich@1.2.0",
    workflow_id=job_workflow_id(JOB),
    workflow_type="EnrichFlow",
    is_local=False,
)


def nexus(op: str, endpoint: str = ENDPOINT) -> NexusSerializationContext:
    return NexusSerializationContext(endpoint=endpoint, service="acme.enrich", operation=op)


@pytest.fixture
def handling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pretend this encode runs inside a Nexus operation handled on ``ENDPOINT``: what
    temporalio's Nexus worker does while it encodes the operation's reply (verified over the
    wire in ``test_codec_nexus.py``)."""
    info = temporalio.nexus.Info(endpoint=ENDPOINT, namespace="acme", task_queue="acme/enrich@1")
    monkeypatch.setattr(temporalio.nexus, "in_operation", lambda: True)
    monkeypatch.setattr(temporalio.nexus, "info", lambda: info)


# --------------------------------------------------------------------------- recipient rules


@pytest.mark.parametrize(
    ("role", "context", "expected"),
    [
        # seller: the root handler workflow goes to the job's buyer too
        ("seller", WF_JOB, {"seller", "buyer"}),
        # seller: everything else is the seller's alone
        ("seller", ACT_JOB, {"seller"}),
        ("seller", WorkflowSerializationContext("acme", "job-" + JOB + "-child"), {"seller"}),
        ("seller", WorkflowSerializationContext("acme", "job-" + JOB.upper()), {"seller"}),
        ("seller", WorkflowSerializationContext("acme", "job-" + JOB + "\n"), {"seller"}),
        ("seller", WorkflowSerializationContext("acme", JOB), {"seller"}),
        ("seller", WorkflowSerializationContext("acme", "enrich-1"), {"seller"}),
        ("seller", None, {"seller"}),
        # seller: a Nexus *call* its workflow makes (it hires another offering) is a
        # buyer's input: the endpoint's seller and ourselves, never plaintext
        ("seller", nexus("execute"), {"seller", "stranger"}),
        ("seller", nexus("quote"), {"seller", "stranger"}),
        ("seller", nexus("other"), {"seller", "stranger"}),
        # buyer: Nexus inputs go to the seller behind the endpoint and the buyer
        ("buyer", nexus("execute"), {"seller", "buyer"}),
        ("buyer", nexus("quote"), {"seller", "buyer"}),
        # buyer: everything else (its own workflows, activities) is the buyer's alone
        ("buyer", WF_JOB, {"buyer"}),
        ("buyer", ACT_JOB, {"buyer"}),
        ("buyer", None, {"buyer"}),
    ],
)
async def test_recipients_by_context(
    role: str, context: SerializationContext | None, expected: set[str]
) -> None:
    codec = seller_codec(seller_keys=OTHER_SELLER) if role == "seller" else buyer_codec()
    if context is not None:
        codec = codec.with_context(context)
    [enc] = await codec.encode([payload()])
    assert readers(enc) == expected
    assert MARKER not in enc.SerializeToString()
    # Whoever can read it gets the original back through the codec.
    reader = seller_codec() if "seller" in expected else buyer_codec()
    assert await reader.decode([enc]) == [payload()]


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize("op", ["quote", "execute"])
async def test_seller_nexus_replies_are_plaintext(op: str) -> None:
    """The quote's output is a price the gate must read (I4); ``execute`` only encodes
    handler refusals, produced before a buyer key can be trusted."""
    keys = Keys()
    codec = seller_codec(buyer_keys=keys, seller_keys=keys).with_context(nexus(op))
    original = [payload(b'{"price": "12.40"}'), payload()]
    assert await codec.encode(original) == original
    assert keys.asked == []


@pytest.mark.usefixtures("handling")
async def test_seller_reply_of_another_operation_is_seller_only() -> None:
    [enc] = (
        await seller_codec(seller_keys=OTHER_SELLER)
        .with_context(nexus("status"))
        .encode([payload()])
    )
    assert readers(enc) == {"seller"}


@pytest.mark.usefixtures("handling")
async def test_seller_call_from_inside_a_handler_is_encrypted() -> None:
    """A handler that calls a *different* endpoint is making a call, not replying."""
    other = "k" + "6" * 32
    keys = Keys(seller=STRANGER[1])
    [enc] = (
        await seller_codec(seller_keys=keys)
        .with_context(nexus("execute", other))
        .encode([payload()])
    )
    assert readers(enc) == {"seller", "stranger"}
    assert keys.asked == [("seller", other)]


@pytest.mark.parametrize("op", ["quote", "execute"])
async def test_seller_nexus_call_without_seller_lookup_fails_closed(op: str) -> None:
    """Outside a handler a Nexus context is an outgoing call: with no way to find the
    endpoint's seller the codec refuses rather than send the input in plaintext."""
    with pytest.raises(CodecError, match="no seller key lookup"):
        await seller_codec().with_context(nexus(op)).encode([payload()])


async def test_seller_decodes_buyer_nexus_input_in_any_context() -> None:
    [enc] = await buyer_codec().with_context(nexus("quote")).encode([payload()])
    for ctx in (nexus("quote"), nexus("execute"), WF_JOB, None):
        codec = seller_codec()
        if ctx is not None:
            codec = codec.with_context(ctx)
        assert await codec.decode([enc]) == [payload()]


async def test_lookups_get_exactly_the_context_values() -> None:
    keys = Keys()
    await seller_codec(buyer_keys=keys).with_context(WF_JOB).encode([payload()])
    await buyer_codec(seller_keys=keys).with_context(nexus("execute")).encode([payload()])
    assert keys.asked == [("buyer", JOB), ("seller", ENDPOINT)]


async def test_one_lookup_per_encode_call() -> None:
    keys = Keys()
    out = await seller_codec(buyer_keys=keys).with_context(WF_JOB).encode([payload()] * 3)
    assert len(out) == 3
    assert keys.asked == [("buyer", JOB)]
    assert await seller_codec(buyer_keys=keys).with_context(WF_JOB).encode([]) == []
    assert len(keys.asked) == 1


async def test_buyer_equal_to_seller_key_collapses() -> None:
    keys = Keys(buyer=SELLER[1])
    [enc] = await seller_codec(buyer_keys=keys).with_context(WF_JOB).encode([payload()])
    assert readers(enc) == {"seller"}


async def test_with_context_returns_a_configured_copy() -> None:
    base = seller_codec()
    ctx = base.with_context(WF_JOB)
    assert ctx is not base
    assert ctx.context == WF_JOB
    assert base.context is None
    [enc] = await base.encode([payload()])
    assert readers(enc) == {"seller"}


# --------------------------------------------------------------------------- fail closed


@pytest.mark.parametrize(
    ("codec", "context"),
    [
        (lambda: seller_codec(buyer_keys=None), WF_JOB),
        (lambda: seller_codec(buyer_keys=Keys(fail=True)), WF_JOB),
        (lambda: seller_codec(buyer_keys=Keys(buyer="age1nope")), WF_JOB),
        (lambda: seller_codec(buyer_keys=Keys(buyer=None)), WF_JOB),
        (lambda: seller_codec(buyer_keys=Keys(buyer=BUYER[1] + "\n")), WF_JOB),
        (lambda: seller_codec(buyer_keys=Keys(buyer=BUYER[0])), WF_JOB),  # a secret, not a key
        (lambda: buyer_codec(seller_keys=None), nexus("execute")),
        (lambda: buyer_codec(seller_keys=Keys(fail=True)), nexus("quote")),
        (lambda: buyer_codec(seller_keys=Keys(seller=b"age1...")), nexus("execute")),
    ],
)
async def test_unresolvable_recipient_fails_closed(
    codec: object, context: SerializationContext
) -> None:
    c: KontraCodec = codec()  # type: ignore[operator]
    with pytest.raises(CodecError):
        await c.with_context(context).encode([payload()])


def test_constructor_validation() -> None:
    with pytest.raises(ValueError, match="public key"):
        KontraCodec(SELLER[0], BUYER[1], role="seller")
    with pytest.raises(ValueError, match="role"):
        KontraCodec(SELLER[0], SELLER[1], role="admin")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="identity"):
        KontraCodec("AGE-SECRET-KEY-1nope", SELLER[1], role="seller")
    with pytest.raises(ValueError, match="threshold"):
        KontraCodec(SELLER[0], SELLER[1], role="seller", claim_check_threshold=-1)


# --------------------------------------------------------------------------- decode


async def test_foreign_encodings_pass_through_untouched() -> None:
    others = [
        payload(b'{"price": "1.00"}'),
        Payload(metadata={"encoding": b"binary/null"}),
        Payload(metadata={"encoding": b"binary/encrypted", "x": b"y"}, data=b"\x00\x01"),
        Payload(),
    ]
    assert await seller_codec().decode(others) == others
    assert await buyer_codec().decode(others) == others


async def test_decrypted_non_payload_is_a_codec_error() -> None:
    """Anyone can encrypt to our public key, so what decrypts is still untrusted."""
    junk = Payload(
        metadata={"encoding": ENCODING, "kontra-v": b"1"},
        data=encrypt(b"\x0a\xff", [SELLER[1]]),  # field 1, length 255, truncated
    )
    with pytest.raises(CodecError, match="not a Payload"):
        await seller_codec().decode([junk])


async def test_not_for_us_is_an_error_not_plaintext() -> None:
    [enc] = await seller_codec().encode([payload()])
    with pytest.raises(CodecError, match="no identity matched"):
        await buyer_codec().decode([enc])


@pytest.mark.parametrize("where", [0, 50, 150, 200, -1])
async def test_tampered_payload_is_refused(where: int) -> None:
    [enc] = await seller_codec().with_context(WF_JOB).encode([payload()])
    data = bytearray(enc.data)
    data[where] ^= 0x04
    enc.data = bytes(data)
    with pytest.raises(CodecError):
        await buyer_codec().decode([enc])


async def test_unknown_version_is_refused() -> None:
    [enc] = await seller_codec().encode([payload()])
    enc.metadata["kontra-v"] = b"2"
    with pytest.raises(CodecError, match="version"):
        await seller_codec().decode([enc])
    del enc.metadata["kontra-v"]
    with pytest.raises(CodecError, match="version"):
        await seller_codec().decode([enc])


async def test_metadata_is_encrypted_too() -> None:
    p = Payload(
        metadata={"encoding": b"json/protobuf", "messageType": b"acme.Secret" + MARKER},
        data=b"{}",
    )
    [enc] = await seller_codec().encode([p])
    assert MARKER not in enc.SerializeToString()
    assert set(enc.metadata) == {"encoding", "kontra-v"}
    assert await seller_codec().decode([enc]) == [p]


# --------------------------------------------------------------------------- claim-check


async def test_claim_check_above_threshold() -> None:
    blobs = Blobs()
    big = payload(b'"' + MARKER * 2000 + b'"')
    small = payload()
    seller = seller_codec(blobs=blobs, claim_check_threshold=4096).with_context(WF_JOB)
    enc_big, enc_small = await seller.encode([big, small])

    assert enc_small.metadata["encoding"] == ENCODING
    assert enc_big.metadata["encoding"] == CLAIM_CHECK_ENCODING
    assert enc_big.metadata["kontra-v"] == b"1"
    ref = json.loads(enc_big.data)
    assert set(ref) == {"sha256", "size"}
    stored = blobs.data[ref["sha256"]]
    assert sha256(stored).hexdigest() == ref["sha256"]
    assert len(stored) == ref["size"] > 4096
    # Neither history nor the bucket holds plaintext; the bucket holds age ciphertext.
    assert MARKER not in enc_big.SerializeToString()
    assert MARKER not in stored
    assert Payload.FromString(decrypt(stored, BUYER[0])) == big

    buyer = buyer_codec(blobs=blobs)
    assert await buyer.decode([enc_big, enc_small]) == [big, small]
    assert blobs.gets == 1


async def test_claim_check_boundary_and_disabled() -> None:
    from kontra.codec.age import ciphertext_size

    blobs = Blobs()
    p = payload()
    size = ciphertext_size(len(p.SerializeToString()), 1)
    [at] = await seller_codec(blobs=blobs, claim_check_threshold=size).encode([p])
    [above] = await seller_codec(blobs=blobs, claim_check_threshold=size - 1).encode([p])
    assert at.metadata["encoding"] == ENCODING
    assert above.metadata["encoding"] == CLAIM_CHECK_ENCODING
    [inline] = await seller_codec(blobs=None, claim_check_threshold=0).encode([p])
    assert inline.metadata["encoding"] == ENCODING


async def test_claim_check_fetch_is_verified() -> None:
    blobs = Blobs()
    [enc] = await seller_codec(blobs=blobs, claim_check_threshold=0).encode([payload()])
    digest = json.loads(enc.data)["sha256"]

    blobs.data[digest] = blobs.data[digest][:-1] + bytes([blobs.data[digest][-1] ^ 1])
    with pytest.raises(CodecError, match="does not match"):
        await seller_codec(blobs=blobs).decode([enc])

    blobs.data.clear()
    with pytest.raises(CodecError, match="download failed"):
        await seller_codec(blobs=blobs).decode([enc])

    with pytest.raises(CodecError, match="no blob store"):
        await seller_codec(blobs=None).decode([enc])


@pytest.mark.parametrize(
    "ref",
    [
        b"not json",
        b"[]",
        b'{"sha256": "' + b"a" * 64 + b'"}',
        b'{"sha256": "' + b"A" * 64 + b'", "size": 1}',
        b'{"sha256": "' + b"a" * 63 + b'", "size": 1}',
        b'{"sha256": "' + b"a" * 64 + b'", "size": -1}',
        b'{"sha256": "' + b"a" * 64 + b'", "size": true}',
        b'{"sha256": "' + b"a" * 64 + b'", "size": 1, "url": "http://x"}',
        b'{"sha256": "' + b"a" * 64 + b'\\n", "size": 1}',
        b" " * 300,
    ],
)
async def test_malformed_reference_is_refused(ref: bytes) -> None:
    p = Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"}, data=ref)
    blobs = Blobs()
    with pytest.raises(CodecError, match="malformed"):
        await seller_codec(blobs=blobs).decode([p])
    assert blobs.gets == 0


async def test_claim_check_upload_failure_is_an_error() -> None:
    class Broken(Blobs):
        async def put(self, sha256: str, data: bytes) -> None:
            raise OSError("bucket down")

    with pytest.raises(CodecError, match="upload failed"):
        await seller_codec(blobs=Broken(), claim_check_threshold=0).encode([payload()])


# --------------------------------------------------------------------------- DataConverter


async def test_data_converter_round_trip_with_contexts() -> None:
    blobs = Blobs()
    seller = data_converter(seller_codec(blobs=blobs, claim_check_threshold=10_000))
    buyer = data_converter(buyer_codec(blobs=blobs))
    value = {"records": ["x" * 20_000], "note": MARKER.decode()}

    # The seller's result of job-<id>, as temporalio encodes it (context applied by it).
    encoded = await seller.with_context(WF_JOB).encode([value, "small"])
    assert [p.metadata["encoding"] for p in encoded] == [CLAIM_CHECK_ENCODING, ENCODING]
    assert all(MARKER not in p.SerializeToString() for p in encoded)
    assert await buyer.decode(encoded, [dict, str]) == [value, "small"]

    # An activity payload of the same job is not readable by the buyer.
    act = await seller.with_context(ACT_JOB).encode(["internal"])
    with pytest.raises(CodecError):
        await buyer.decode(act)
    assert await seller.decode(act) == ["internal"]

    # The buyer's Nexus input is readable by the seller.
    nexus_in = await buyer.with_context(nexus("execute")).encode([{"urls": ["a"]}])
    assert await seller.with_context(nexus("execute")).decode(nexus_in) == [{"urls": ["a"]}]


async def test_failure_messages_and_traces_are_encrypted() -> None:
    seller = data_converter(seller_codec()).with_context(WF_JOB)
    buyer = data_converter(buyer_codec())
    failure = Failure()
    try:
        raise ApplicationError("bad input " + MARKER.decode(), {"why": MARKER.decode()})
    except ApplicationError as e:
        await seller.encode_failure(e, failure)
    raw = failure.SerializeToString()
    assert MARKER not in raw
    assert failure.message == "Encoded failure"
    err = await buyer.decode_failure(failure)
    assert isinstance(err, ApplicationError)
    assert MARKER.decode() in err.message
    assert err.details == ({"why": MARKER.decode()},)


async def test_plain_data_converter_cannot_read_kontra_payloads() -> None:
    encoded = await data_converter(seller_codec()).encode(["secret"])
    with pytest.raises(KeyError, match="Unknown payload encoding binary/kontra-age"):
        await DataConverter.default.decode(encoded, [str])
