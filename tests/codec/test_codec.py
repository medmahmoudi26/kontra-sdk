"""KontraCodec: recipients by serialization context (decisions I7), plaintext only where the
gate must read and only for the services a worker generated, plaintext Nexus results refused
on the calling side, fail-closed key resolution, claim-check (bounded, and deferred for an
unverified ``execute``), tamper detection, and a DataConverter round trip through
temporalio's own context plumbing."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256

import httpx
import pytest
import temporalio.nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.converter import (
    ActivitySerializationContext,
    DataConverter,
    DefaultFailureConverterWithEncodedAttributes,
    NexusSerializationContext,
    SerializationContext,
    WorkflowSerializationContext,
)
from temporalio.exceptions import ApplicationError

from kontra.codec import (
    CLAIM_CHECK_ENCODING,
    ENCODING,
    MAX_CLAIM_CHECK,
    PLAINTEXT_KEY,
    CodecError,
    GateBlobStore,
    InvalidPayload,
    KontraCodec,
    data_converter,
)
from kontra.codec.age import AgeError, decrypt, encrypt, generate_identity
from kontra.contract.names import agent_workflow_id, batch_workflow_id, job_workflow_id
from kontra.gate_client import models as m

JOB = "0123456789abcdef0123456789abcdef"
ENDPOINT = "k" + "5" * 32
#: The Nexus service a seller's worker generated for acme/enrich (OfferingId.service_name).
SERVICE = "acme.enrich"
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

    async def seller_recipients(self, endpoint: str) -> Sequence[str]:
        self.asked.append(("seller", endpoint))
        if self.fail:
            raise LookupError("gate unreachable")
        if isinstance(self.seller, (list, tuple)):
            return list(self.seller)  # pyright: ignore[reportUnknownArgumentType]
        return [self.seller]  # type: ignore[list-item]


class Blobs:
    """An in-memory claim-check bucket, and a job's stream blobs (``StreamStore``): which job
    each was stored for, how many uploads there were, and an optional failure or delay."""

    def __init__(self, *, delay: float = 0.0, fail: bool = False) -> None:
        self.data: dict[str, bytes] = {}
        self.gets = 0
        self.limits: list[int | None] = []
        self.scopes: dict[str, str] = {}
        #: Stream blobs uploaded (absent before), and every ``put_stream`` call's size.
        self.uploads = 0
        self.calls: list[int] = []
        self.delay = delay
        self.fail = fail

    async def put(self, sha256: str, data: bytes) -> None:
        self.data[sha256] = data

    async def put_stream(
        self, job_id: str, blobs: Sequence[tuple[str, bytes]], done: Callable[[str], None]
    ) -> None:
        self.calls.append(len(blobs))
        if self.fail:
            raise CodecError("the bucket is down")
        for digest, data in blobs:
            if digest in self.data:
                done(digest)  # stored already: nothing uploaded
                continue
            if self.delay:
                await asyncio.sleep(self.delay)
            assert sha256(data).hexdigest() == digest
            self.data[digest] = data
            self.scopes[digest] = job_id
            self.uploads += 1
            done(digest)

    async def get(self, sha256: str, *, limit: int | None = None) -> bytes:
        self.gets += 1
        self.limits.append(limit)
        data = self.data[sha256]
        if limit is not None and len(data) > limit:
            raise CodecError(f"larger than {limit} bytes")
        return data


def seller_codec(**kw: object) -> KontraCodec:
    kw.setdefault("buyer_keys", Keys())
    return KontraCodec(SELLER[0], [SELLER[1]], role="seller", **kw)  # type: ignore[arg-type]


#: For a seller-role codec that calls an offering: the endpoint's seller is someone else.
OTHER_SELLER = Keys(seller=STRANGER[1])


def buyer_codec(**kw: object) -> KontraCodec:
    kw.setdefault("seller_keys", Keys())
    return KontraCodec(BUYER[0], [BUYER[1]], role="buyer", **kw)  # type: ignore[arg-type]


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


def nexus(op: str, endpoint: str = ENDPOINT, service: str = SERVICE) -> NexusSerializationContext:
    return NexusSerializationContext(endpoint=endpoint, service=service, operation=op)


def serving(**kw: object) -> KontraCodec:
    """The codec of a worker that generated the ``acme.enrich`` service."""
    kw.setdefault("services", [SERVICE])
    return seller_codec(**kw)


def marked(p: Payload) -> Payload:
    out = Payload()
    out.CopyFrom(p)
    out.metadata[PLAINTEXT_KEY] = b"1"
    return out


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
        # seller: a bound handler's agent child, and a batch child, are never job-shaped:
        # their histories are the seller's alone (the job's result reaches the buyer from
        # the job-<id> wrapper)
        ("seller", WorkflowSerializationContext("acme", agent_workflow_id(JOB)), {"seller"}),
        (
            "seller",
            WorkflowSerializationContext("acme", batch_workflow_id(job_workflow_id(JOB), "a", "t")),
            {"seller"},
        ),
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
async def test_the_quote_reply_of_a_generated_service_is_plaintext() -> None:
    """The quote's output is a price the gate must read (I4): untouched, so the frontend's
    Nexus HTTP API hands the gate plain JSON."""
    keys = Keys()
    codec = serving(buyer_keys=keys, seller_keys=keys).with_context(nexus("quote"))
    original = [payload(b'{"price": "12.40"}'), payload()]
    assert await codec.encode(original) == original
    assert keys.asked == []


@pytest.mark.usefixtures("handling")
async def test_the_execute_reply_of_a_generated_service_is_marked_plaintext() -> None:
    """``execute`` only encodes handler refusals, produced before a buyer key can be
    trusted: plaintext, and marked so the caller's codec can tell them from a leak."""
    keys = Keys()
    codec = serving(buyer_keys=keys, seller_keys=keys).with_context(nexus("execute"))
    original = [payload(b'{"message": "hold token refused"}'), payload()]
    out = await codec.encode(original)
    assert out == [marked(p) for p in original]
    assert original[0].metadata.get(PLAINTEXT_KEY) is None  # the input was not mutated
    assert keys.asked == []


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize(
    ("services", "context"),
    [
        # FIXUPS #20: a codec that generated nothing sends no plaintext at all ...
        ((), nexus("quote")),
        ((), nexus("execute")),
        # ... and one that did matches the service by its exact name, never by the shape
        # of the operation: a service of the seller's own named like ours stays encrypted.
        ([SERVICE], nexus("quote", service="acme.enrich2")),
        ([SERVICE], nexus("execute", service="other.enrich")),
        ([SERVICE], nexus("execute", service="Acme.enrich")),
        ([SERVICE], nexus("execute", service=SERVICE + " ")),
        ([SERVICE], nexus("quote", service="")),
        (["acme.enrich2"], nexus("quote")),
        # an operation the generated service does not have
        ([SERVICE], nexus("status")),
        ([SERVICE], nexus("Quote")),
        ([SERVICE], nexus("execute ")),
    ],
)
async def test_other_replies_are_encrypted_to_the_seller(
    services: list[str], context: NexusSerializationContext
) -> None:
    keys = Keys()
    codec = seller_codec(services=services, buyer_keys=keys, seller_keys=keys)
    [enc] = await codec.with_context(context).encode([payload()])
    assert readers(enc) == {"seller"}
    assert PLAINTEXT_KEY not in enc.metadata
    assert keys.asked == []


async def test_a_generated_service_outside_its_handler_is_a_call() -> None:
    """Outside the operation (a workflow of this seller calling the same service name on
    another endpoint) the input is encrypted like a buyer's, generated service or not."""
    [enc] = await serving(seller_keys=OTHER_SELLER).with_context(nexus("quote")).encode([payload()])
    assert readers(enc) == {"seller", "stranger"}


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
    with pytest.raises(ValueError, match="must contain the public key"):
        KontraCodec(SELLER[0], [BUYER[1]], role="seller")
    with pytest.raises(TypeError, match="sequence of age keys"):
        KontraCodec(SELLER[0], SELLER[1], role="seller")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="at least this machine"):
        KontraCodec(SELLER[0], [], role="seller")
    with pytest.raises(ValueError, match="role"):
        KontraCodec(SELLER[0], [SELLER[1]], role="admin")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="identity"):
        KontraCodec("AGE-SECRET-KEY-1nope", [SELLER[1]], role="seller")
    with pytest.raises(ValueError, match="threshold"):
        KontraCodec(SELLER[0], [SELLER[1]], role="seller", claim_check_threshold=-1)
    with pytest.raises(ValueError, match="max_claim_check"):
        KontraCodec(SELLER[0], [SELLER[1]], role="seller", max_claim_check=0)
    # One name is not a collection of names (it would read as its characters).
    with pytest.raises(TypeError, match="collection"):
        KontraCodec(SELLER[0], [SELLER[1]], role="seller", services=SERVICE)
    with pytest.raises(ValueError, match="non-empty"):
        KontraCodec(SELLER[0], [SELLER[1]], role="seller", services=[SERVICE, ""])
    with pytest.raises(ValueError, match="seller role"):
        KontraCodec(BUYER[0], [BUYER[1]], role="buyer", services=[SERVICE])
    codec = KontraCodec(SELLER[0], [SELLER[1]], role="seller", services=(SERVICE, SERVICE))
    assert codec.services == frozenset({SERVICE})
    assert KontraCodec(BUYER[0], [BUYER[1]], role="buyer").services == frozenset()


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


# --------------------------------------------------------------------------- plaintext results


@pytest.mark.parametrize("op", ["execute", "other", "Quote", "quote ", ""])
@pytest.mark.parametrize("role", ["buyer", "seller"])
async def test_the_caller_refuses_a_plaintext_nexus_result(role: str, op: str) -> None:
    """FIXUPS #1/#21: a result that reaches the caller unencrypted was written into the
    platform's history by its seller. The codec says so instead of handing it on as a
    delivery -- a buyer, or a seller's own workflow hiring another offering."""
    codec = buyer_codec() if role == "buyer" else seller_codec(seller_keys=OTHER_SELLER)
    leaked = payload(b'{"rows": ["' + MARKER + b'"]}')
    with pytest.raises(CodecError) as err:
        await codec.with_context(nexus(op)).decode([leaked])
    text = str(err.value)
    assert f"result of {op!r}" in text
    assert ENDPOINT in text
    assert SERVICE in text
    assert "plaintext" in text
    assert "must be encrypted" in text
    assert MARKER.decode() not in text  # the leaked value is not repeated in logs


@pytest.mark.parametrize("role", ["buyer", "seller"])
async def test_the_quote_answer_is_the_one_plaintext_result(role: str) -> None:
    codec = buyer_codec() if role == "buyer" else seller_codec(seller_keys=OTHER_SELLER)
    price = payload(b'{"status": "ok", "amount_minor": 1240}')
    assert await codec.with_context(nexus("quote")).decode([price]) == [price]


async def test_a_marked_refusal_reaches_the_caller_of_execute_only() -> None:
    refusal = payload(b'{"message": "hold token refused: it expired"}')
    out = await buyer_codec().with_context(nexus("execute")).decode([marked(refusal)])
    assert out == [refusal]  # the mark is the seller codec's, not part of the payload
    # The mark means "a refusal of execute": on any other operation it proves nothing.
    with pytest.raises(CodecError, match="arrived in plaintext"):
        await buyer_codec().with_context(nexus("status")).decode([marked(refusal)])
    wrong = marked(refusal)
    wrong.metadata[PLAINTEXT_KEY] = b"yes"
    with pytest.raises(CodecError, match="arrived in plaintext"):
        await buyer_codec().with_context(nexus("execute")).decode([wrong])


async def test_encrypted_results_and_other_contexts_are_unaffected() -> None:
    [result] = await seller_codec().with_context(WF_JOB).encode([payload()])
    assert await buyer_codec().with_context(nexus("execute")).decode([result]) == [payload()]
    # No Nexus context (the buyer's own workflows, a stored delivery): passthrough as before.
    assert await buyer_codec().decode([payload()]) == [payload()]
    assert await buyer_codec().with_context(WF_JOB).decode([payload()]) == [payload()]


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize("op", ["quote", "execute", "status"])
async def test_the_handler_still_accepts_a_plaintext_input(op: str) -> None:
    """Inputs are the other direction: the seller's handler reads what it is given."""
    for codec in (serving(), seller_codec()):
        assert await codec.with_context(nexus(op)).decode([payload()]) == [payload()]


async def test_a_refusal_through_the_data_converters() -> None:
    """What temporalio does on each end: the handler's failure is converted and encoded
    under the reply's context, the caller decodes it under the call's. The refusal's text
    stays readable; the same failure from a seller that marks nothing is refused."""
    import nexusrpc

    seller = data_converter(serving()).with_context(nexus("execute"))
    buyer = data_converter(buyer_codec()).with_context(nexus("execute"))
    refusal = nexusrpc.HandlerError(
        "hold token refused: it expired", type=nexusrpc.HandlerErrorType.BAD_REQUEST
    )

    async def encoded(converter: DataConverter) -> Failure:
        failure = Failure()
        info = temporalio.nexus.Info(endpoint=ENDPOINT, namespace="acme", task_queue="q")
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(temporalio.nexus, "in_operation", lambda: True)
            mp.setattr(temporalio.nexus, "info", lambda: info)
            await converter.encode_failure(refusal, failure)
        return failure

    failure = await encoded(seller)
    assert failure.encoded_attributes.metadata[PLAINTEXT_KEY] == b"1"
    err = await buyer.decode_failure(failure)
    assert "hold token refused: it expired" in str(err)

    # A seller whose codec marks nothing (no Kontra codec on its replies): refused.
    plain = Failure()
    unmarked = DataConverter(failure_converter_class=DefaultFailureConverterWithEncodedAttributes)
    await unmarked.encode_failure(refusal, plain)
    with pytest.raises(CodecError, match="arrived in plaintext"):
        await buyer.decode_failure(plain)


# --------------------------------------------------------------------------- unverified fetches


async def _claim_checked_input(blobs: Blobs, data: bytes = b'"' + MARKER * 100 + b'"') -> Payload:
    """A buyer's ``execute`` input big enough to be claim-checked."""
    codec = buyer_codec(blobs=blobs, claim_check_threshold=0).with_context(nexus("execute"))
    [ref] = await codec.encode([payload(data)])
    assert ref.metadata["encoding"] == CLAIM_CHECK_ENCODING
    return ref


@pytest.mark.usefixtures("handling")
async def test_an_execute_input_is_fetched_only_after_the_hold_check() -> None:
    """FIXUPS #20: temporalio decodes a Nexus input before the handler runs, so before the
    hold token is checked. For a generated ``execute`` the codec leaves the reference in
    place; the handler opens it once the token verified."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    codec = serving(blobs=blobs).with_context(nexus("execute"))
    [left] = await codec.decode([ref])
    assert left == ref
    assert blobs.gets == 0  # a forged execute stops here: nothing downloaded
    assert await codec.open_deferred(left) == payload(b'"' + MARKER * 100 + b'"')
    assert blobs.gets == 1
    assert blobs.limits == [json.loads(ref.data)["size"]]
    # Anything else is passed through by open_deferred (inline input, already opened).
    assert await codec.open_deferred(payload()) == payload()


@pytest.mark.usefixtures("handling")
async def test_a_malformed_deferred_reference_is_refused_at_once() -> None:
    blobs = Blobs()
    bad = Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"}, data=b"[]")
    with pytest.raises(CodecError, match="malformed"):
        await serving(blobs=blobs).with_context(nexus("execute")).decode([bad])
    bad.metadata["kontra-v"] = b"2"
    with pytest.raises(CodecError, match="version"):
        await serving(blobs=blobs).with_context(nexus("execute")).decode([bad])
    with pytest.raises(CodecError, match="version"):
        await serving(blobs=blobs).open_deferred(bad)
    assert blobs.gets == 0


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize(
    ("generated", "context"),
    [
        # quote has no token to wait for: fetched now, within the bound
        ([SERVICE], nexus("quote")),
        # not a generated service, or not this worker's handler: nothing to defer for
        ([], nexus("execute")),
        ([SERVICE], nexus("execute", service="other.enrich")),
        ([SERVICE], nexus("execute", endpoint="k" + "6" * 32)),
        ([SERVICE], WF_JOB),
    ],
)
async def test_other_claim_checks_are_fetched_on_decode(
    generated: list[str], context: SerializationContext
) -> None:
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    c = seller_codec(blobs=blobs, services=generated)
    [opened] = await c.with_context(context).decode([ref])
    assert opened == payload(b'"' + MARKER * 100 + b'"')
    assert blobs.gets == 1


async def test_a_claim_check_above_the_limit_is_never_downloaded() -> None:
    """A reference is a claim anyone can write: the size it names is checked before the
    fetch, and the store is told to stop at that size."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    size = json.loads(ref.data)["size"]
    with pytest.raises(CodecError, match="above this codec's limit"):
        await seller_codec(blobs=blobs, max_claim_check=size - 1).decode([ref])
    assert blobs.gets == 0
    huge = Payload(
        metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"},
        data=json.dumps({"sha256": "a" * 64, "size": MAX_CLAIM_CHECK + 1}).encode(),
    )
    with pytest.raises(CodecError, match="above this codec's limit"):
        await seller_codec(blobs=blobs).decode([huge])
    assert blobs.gets == 0
    assert await seller_codec(blobs=blobs, max_claim_check=size).decode([ref])
    assert blobs.limits == [size]


async def test_a_blob_larger_than_its_reference_is_not_read_whole() -> None:
    """The bucket object behind a small reference is huge (the sha256 cannot be checked
    before the bytes arrive): the store gives up at the referenced size."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    digest = json.loads(ref.data)["sha256"]
    blobs.data[digest] += b"\x00" * 10_000
    with pytest.raises(CodecError, match="larger than"):
        await seller_codec(blobs=blobs).decode([ref])


# --------------------------------------------------------------------------- GateBlobStore


class _Bucket:
    """The gate's blob route and a bucket that serves ``size`` bytes in 4 KiB chunks, with
    or without a Content-Length, counting what it actually sent."""

    def __init__(self, size: int, *, declare: int | None) -> None:
        self.size, self.declare, self.sent = size, declare, 0
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self._serve))

    async def _body(self) -> AsyncIterator[bytes]:
        while self.sent < self.size:
            chunk = b"\x00" * min(4096, self.size - self.sent)
            self.sent += len(chunk)
            yield chunk

    async def _serve(self, request: httpx.Request) -> httpx.Response:
        headers = {} if self.declare is None else {"Content-Length": str(self.declare)}
        return httpx.Response(200, headers=headers, content=self._body())

    async def create_blob(self, req: m.BlobCreate) -> m.BlobUrl:
        raise AssertionError("not used")

    async def get_blob(self, sha256: str) -> m.BlobUrl:
        return m.BlobUrl(
            sha256=sha256, url="http://bucket.invalid/b", exists=True, expires_at=datetime.now(UTC)
        )


@pytest.mark.parametrize("declare", [None, 10 * 1024 * 1024], ids=["chunked", "declared"])
async def test_gate_blob_store_stops_at_the_limit(declare: int | None) -> None:
    bucket = _Bucket(10 * 1024 * 1024, declare=declare)
    with pytest.raises(CodecError, match="larger than 8192 bytes"):
        await GateBlobStore(bucket).get("a" * 64, limit=8192)
    # A declared length above the limit is refused before the body; an undeclared one is
    # read only until it passes the limit.
    assert bucket.sent <= (0 if declare is not None else 8192 + 4096)
    await bucket.http.aclose()


async def test_gate_blob_store_reads_a_blob_within_the_limit() -> None:
    bucket = _Bucket(10_000, declare=None)
    assert await GateBlobStore(bucket).get("a" * 64, limit=10_000) == b"\x00" * 10_000
    bucket.sent = 0
    assert await GateBlobStore(bucket).get("a" * 64) == b"\x00" * 10_000  # no limit asked
    await bucket.http.aclose()


# --------------------------------------------------------------------------- review: bounds


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize("op", ["quote", "status"])
async def test_a_handled_input_fetches_at_most_one_claim_check(op: str) -> None:
    """A Nexus input is one payload, but temporalio opens a Temporal "system" envelope found
    in it and decodes every payload nested there, before any handler code runs. A ``quote``
    (no hold token to check first) naming the same blob a thousand times must not make the
    seller download it a thousand times: one claim-check per handled operation, and a call
    that names more is refused before anything is fetched."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    with pytest.raises(CodecError, match="at most one claim-check"):
        await serving(blobs=blobs).with_context(nexus(op)).decode([ref] * 1000)
    assert blobs.gets == 0
    # Nested fields are decoded one call each: the budget is the operation's, not the call's.
    codec = serving(blobs=blobs).with_context(nexus(op))
    assert await codec.decode([ref]) == [payload(b'"' + MARKER * 100 + b'"')]
    with pytest.raises(CodecError, match="at most one claim-check"):
        await codec.decode([ref])
    assert blobs.gets == 1
    # Each operation is a fresh context (temporalio makes one per Nexus task).
    assert await serving(blobs=blobs).with_context(nexus(op)).decode([ref])
    assert blobs.gets == 2


async def test_the_one_claim_check_rule_is_for_handled_inputs_only() -> None:
    """A workflow's history, a buyer's delivery: payloads that are not a Nexus input this
    worker handles keep fetching every claim-check they hold."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    for codec in (seller_codec(blobs=blobs).with_context(WF_JOB), buyer_codec(blobs=blobs)):
        assert len(await codec.decode([ref, ref, ref])) == 3
    assert blobs.gets == 6


async def test_an_input_the_seller_cannot_open_is_invalid_not_transient() -> None:
    """What the handler refuses at once (retrying the same input cannot help) is an
    :class:`InvalidPayload`; a bucket that did not answer stays a plain, retryable
    :class:`CodecError`."""
    blobs = Blobs()
    ref = await _claim_checked_input(blobs)
    size = json.loads(ref.data)["size"]
    digest = json.loads(ref.data)["sha256"]
    seller = seller_codec(blobs=blobs)

    for bad in (
        Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"1"}, data=b"[]"),
        Payload(metadata={"encoding": CLAIM_CHECK_ENCODING, "kontra-v": b"9"}, data=ref.data),
    ):
        with pytest.raises(InvalidPayload):
            await seller.open_deferred(bad)
    not_ours = Payload(
        metadata={"encoding": ENCODING, "kontra-v": b"1"}, data=encrypt(b"x", [STRANGER[1]])
    )
    with pytest.raises(InvalidPayload, match="cannot decrypt"):
        await seller.decode([not_ours])
    with pytest.raises(InvalidPayload, match="above this codec's limit"):
        await seller_codec(blobs=blobs, max_claim_check=size - 1).open_deferred(ref)
    blobs.data[digest] = blobs.data[digest][:-1] + bytes([blobs.data[digest][-1] ^ 1])
    with pytest.raises(InvalidPayload, match="does not match"):
        await seller.open_deferred(ref)

    blobs.data.clear()  # the bucket lost it, or did not answer: worth another try
    with pytest.raises(CodecError, match="download failed") as err:
        await seller.open_deferred(ref)
    assert not isinstance(err.value, InvalidPayload)


@pytest.mark.parametrize("limit", [None, 70_000])
async def test_gate_blob_store_reads_the_stored_bytes_never_decompressed(
    limit: int | None,
) -> None:
    """A claim-check is the sha256 of the bytes stored. A bucket object stored with a
    ``Content-Encoding`` (a header its uploader chose) must come back as those bytes: decoded,
    64 KiB of gzip become 64 MiB in one chunk, past any limit before it can be counted."""
    import gzip

    stored = gzip.compress(b"\x00" * (64 * 1024 * 1024), compresslevel=9)
    assert len(stored) < 70_000
    seen: list[httpx.Request] = []

    async def body() -> AsyncIterator[bytes]:
        yield stored  # streamed, as a bucket sends it

    async def serve(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, content=body())

    class Gate:
        http = httpx.AsyncClient(transport=httpx.MockTransport(serve))

        async def create_blob(self, req: m.BlobCreate) -> m.BlobUrl:
            raise AssertionError("not used")

        async def get_blob(self, sha256: str) -> m.BlobUrl:
            return m.BlobUrl(
                sha256=sha256,
                url="http://bucket.invalid/b",
                exists=True,
                expires_at=datetime.now(UTC),
            )

    gate = Gate()
    try:
        assert await GateBlobStore(gate).get(sha256(stored).hexdigest(), limit=limit) == stored
    finally:
        await gate.http.aclose()
    # ... and asks for them as stored, so no proxy on the way compresses them either.
    assert [r.headers["accept-encoding"] for r in seen] == ["identity"]


async def _attributes(failure: Failure) -> list[dict[str, str]]:
    """The message and stack trace of every level of an encoded failure, as its reader sees
    them (the seller's codec opens what was encrypted, plaintext passes)."""
    out: list[dict[str, str]] = []
    level = failure
    while True:
        [opened] = await seller_codec().decode([level.encoded_attributes])
        out.append(json.loads(opened.data))
        if not level.HasField("cause"):
            return out
        level = level.cause


@pytest.mark.usefixtures("handling")
@pytest.mark.parametrize("op", ["quote", "execute", "status"])
async def test_a_handled_reply_carries_no_python_stack_trace(op: str) -> None:
    """temporalio's own handler errors (an input the codec or the payload converter could not
    read) chain the Python exception that caused them, stack trace included, and the reply of
    a generated ``quote``/``execute`` is plaintext in the caller's history and the platform's:
    the trace would hand both the seller's file system layout. A reply keeps every message and
    a handler's own ``stack_trace``, never a traceback; the exceptions are left as they were."""
    import nexusrpc

    seller = data_converter(serving()).with_context(nexus(op))
    try:
        try:
            raise CodecError("malformed claim-check reference")
        except CodecError as cause:
            raise nexusrpc.HandlerError(
                "Payload codec failed to decode Nexus operation input",
                type=nexusrpc.HandlerErrorType.INTERNAL,
            ) from cause
    except nexusrpc.HandlerError as e:
        error = e
    failure = Failure()
    await seller.encode_failure(error, failure)
    assert await _attributes(failure) == [
        {"message": "Payload codec failed to decode Nexus operation input", "stack_trace": ""},
        {"message": "malformed claim-check reference", "stack_trace": ""},
    ]
    # Only the reply lost its traces: the exceptions (and the worker's logs) keep theirs.
    assert error.__traceback__ is not None
    assert error.__cause__ is not None
    assert error.__cause__.__traceback__ is not None

    # The handler's own stack_trace (kontra.serve's "no stack trace" note) is kept.
    own = nexusrpc.HandlerError(
        "refused", type=nexusrpc.HandlerErrorType.BAD_REQUEST, stack_trace="(none here)"
    )
    failure = Failure()
    await seller.encode_failure(own, failure)
    assert await _attributes(failure) == [{"message": "refused", "stack_trace": "(none here)"}]


async def test_traces_stay_where_they_are_encrypted_to_the_seller() -> None:
    """Outside a handled reply (a job's failure, an activity's) the trace is the seller's to
    read and stays."""
    seller = data_converter(seller_codec()).with_context(WF_JOB)
    failure = Failure()
    try:
        raise ApplicationError("boom")
    except ApplicationError as e:
        await seller.encode_failure(e, failure)
    [attributes] = await _attributes(failure)
    assert 'File "' in attributes["stack_trace"]


# --------------------------------------------------------------------------- seam ⑥: sealed


async def test_what_the_codec_writes_for_a_job_is_a_sealed_delivery() -> None:
    """``check_sealed`` (decisions I32) accepts exactly what this codec stores as a job's
    result, inline and claim-checked, serialized as Temporal stores it, and refuses the
    plaintext a seller that bypassed the codec would store."""
    from kontra.codec import check_sealed
    from kontra.engine import NotSealed, SealedPayload

    blobs = Blobs()
    big = payload(b'"' + MARKER * 2000 + b'"')
    seller = seller_codec(blobs=blobs, claim_check_threshold=4096).with_context(WF_JOB)
    inline, checked = await seller.encode([payload(), big])
    assert (inline.metadata["encoding"], checked.metadata["encoding"]) == (
        ENCODING,
        CLAIM_CHECK_ENCODING,
    )
    for p in (inline, checked):
        stored = p.SerializeToString(deterministic=True)
        check_sealed(stored)
        assert MARKER not in SealedPayload(stored).data
    with pytest.raises(NotSealed):
        check_sealed(payload().SerializeToString(deterministic=True))
    # The envelope's shape with plaintext behind an age file's first line: no age file.
    stored = inline.SerializeToString(deterministic=True)
    first_line = inline.data[: inline.data.index(b"\n") + 1]
    forged = Payload()
    forged.CopyFrom(inline)
    forged.data = first_line + b'{"text": "' + MARKER + b'"}'
    with pytest.raises(NotSealed):
        check_sealed(forged.SerializeToString(deterministic=True))
    check_sealed(stored)
