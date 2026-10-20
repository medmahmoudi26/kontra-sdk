"""End-to-end encryption of Temporal payloads (seam ⑤, PRD §10.7, decisions I7).

``KontraCodec`` is a :class:`temporalio.converter.PayloadCodec` that age-encrypts every
payload it encodes and decrypts every Kontra payload it decodes. Whom a payload is
encrypted to depends on where it is going, which temporalio 1.34 tells a codec through the
*serialization context* (``with_context``):

========  ============================================  ==================================
role      context                                       recipients
========  ============================================  ==================================
seller    workflow ``job-<id>`` (the root handler       ourselves + the job's buyer
          workflow: its input, result, failures)
seller    the reply of ``quote`` / ``execute`` of a     none: plaintext. The quote output is
          service this worker *generated* (``services``, a price the gate must read (I4);
          exact names), while it handles that           ``execute`` only encodes handler
          operation                                     refusals, sent before a buyer key
                                                        can be trusted (marked as such)
seller    any other Nexus reply (another operation,     ourselves
          a service of the seller's own)
seller    Nexus *call* (a seller's workflow hiring      the offering's seller + ourselves
          another offering)
seller    anything else (activities, children,          ourselves
          no context)
buyer     Nexus (any operation)                         the offering's seller + ourselves
buyer     anything else                                 ourselves
========  ============================================  ==================================

A :class:`~temporalio.converter.NexusSerializationContext` looks the same on both ends of a
Nexus call: temporalio uses it for the *caller's* input and result (a workflow's
``ScheduleNexusOperation``, a standalone ``start_operation``) and for the *handler's* input
and reply. So the seller rules ask temporalio whether this runs inside the Nexus operation
being handled on that endpoint (``temporalio.nexus.in_operation()`` and
``info().endpoint``); a seller-role process that hires another offering gets the caller rules
for that call instead of leaking its input.

Decoding is strict where plaintext means a leak. On the *calling* side of Nexus (a buyer, or
a seller's workflow hiring), a result that arrives unencrypted is refused with
:class:`CodecError`: only ``quote`` answers in plaintext, and the only other plaintext a
seller's codec sends is the refusal of an ``execute``, which it marks (``kontra-plaintext``)
because temporalio decodes a failure's attributes through the same :meth:`decode` as a
result. Anything else in plaintext was written into the platform's history by a seller that
did not encrypt it. On the *handling* side, plaintext inputs pass through.

It fails closed: if a recipient it needs cannot be resolved (no key lookup, the lookup
raised, a malformed key) ``encode`` raises; it never falls back to plaintext. The seller's
key comes from the caller's ``SellerKeys`` (the hire response, else the catalog's
``GET /v1/endpoints/{endpoint}``); an endpoint name is opaque and never parsed.

An encoded payload is ``{encoding: binary/kontra-age, kontra-v: 1}`` with the age ciphertext
of the original ``Payload`` proto as data, so metadata (the original encoding, a message
type) is encrypted too. Above ``claim_check_threshold`` bytes of ciphertext the ciphertext
goes to a :class:`BlobStore`, content-addressed by its sha256, and history holds a
``binary/kontra-claim-check`` reference ``{"sha256", "size"}`` that is verified on fetch.

A reference is a claim anyone can write, so a fetch is bounded before any byte arrives: the
size it names must be at most ``max_claim_check``, and the download stops at that size (the
stored bytes, never decompressed on the way). The input of a generated ``execute`` is not
fetched at all while its hold token is unverified: :meth:`KontraCodec.decode` leaves the
reference in place there, and the handler calls :meth:`KontraCodec.open_deferred` once the
token checks out, so a forged ``execute`` costs the seller no download. ``quote`` has no token
to wait for; its input is fetched within the bound, and only once: a Nexus input is one
payload, so a handled operation fetches at most one claim-check, however many references
temporalio finds nested in it (it opens a Temporal "system" envelope and decodes every
payload inside). A payload that can never be opened (malformed, too large, altered, not
encrypted to us) raises :class:`InvalidPayload`, which the handler refuses at once rather
than download it again on every retry.

The reply of a handled operation carries no Python stack trace (:func:`data_converter`):
temporalio chains the exception behind its own handler errors, and the reply of a generated
service is plaintext in the caller's history and the platform's.

A job's workflow stream crosses the boundary sealed item by item (decisions I31,
:mod:`kontra.codec.stream`). Kontra's workflow interceptor marks the answer of a stream poll
on a job root (``kontra-stream: poll/v1``); :meth:`KontraCodec.encode` turns it into the
stream envelope, each item sealed on its own (a topic the buyer may read to the seller's set
and the job's buyer key, any other to the seller's set alone, its name hidden), the offsets
in the clear; :meth:`KontraCodec.decode` turns the envelope back into the exact poll result.
A marked offset answer (``offset/v1``) passes in plaintext: a log length. A marked payload
anywhere but a seller's job workflow is refused, never encrypted some other way.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from hashlib import sha256
from typing import Any, Literal, Protocol, Self

import httpx
from google.protobuf.message import DecodeError
from temporalio import nexus as temporal_nexus
from temporalio.api.common.v1 import Payload
from temporalio.api.failure.v1 import Failure
from temporalio.converter import (
    DataConverter,
    DefaultFailureConverterWithEncodedAttributes,
    NexusSerializationContext,
    PayloadCodec,
    PayloadConverter,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)

from kontra.codec import age
from kontra.codec import stream as _stream
from kontra.codec.stream import (
    OFFSET_MARK,
    POLL_MARK,
    STREAM_ENCODING,
    STREAM_MARK,
    EnvelopeError,
)
from kontra.contract.names import job_id_from_workflow_id
from kontra.engine import AGE_MAGIC, NotSealed
from kontra.gate_client import models as m

__all__ = [
    "CLAIM_CHECK_ENCODING",
    "ENCODING",
    "EXECUTE_OPERATION",
    "MAX_CLAIM_CHECK",
    "PLAINTEXT_KEY",
    "QUOTE_OPERATION",
    "STREAM_ENCODING",
    "STREAM_MARK",
    "BlobGate",
    "BlobStore",
    "BuyerKeys",
    "CodecError",
    "GateBlobStore",
    "InvalidPayload",
    "KontraCodec",
    "KontraFailureConverter",
    "SellerKeys",
    "check_sealed",
    "data_converter",
]

ENCODING = b"binary/kontra-age"
CLAIM_CHECK_ENCODING = b"binary/kontra-claim-check"
_VERSION_KEY = "kontra-v"
_VERSION = b"1"
#: Metadata a seller's codec puts on the payloads of an ``execute`` refusal, which it sends
#: in plaintext on purpose: a caller's codec accepts exactly these and ``quote`` answers
#: unencrypted, and refuses every other plaintext Nexus result as a leak.
PLAINTEXT_KEY = "kontra-plaintext"
_PLAINTEXT_MARK = b"1"

#: The two Nexus operations of every offering (D2; ``kontra.contract.validate.OPERATIONS``).
QUOTE_OPERATION = "quote"
EXECUTE_OPERATION = "execute"
_PLAINTEXT_SELLER_OPERATIONS = frozenset({QUOTE_OPERATION, EXECUTE_OPERATION})

#: The largest claim-checked payload fetched: the gate's limit for one claim-check blob.
MAX_CLAIM_CHECK = 64 * 1024 * 1024
#: Claim-checks fetched for the input of one handled Nexus operation: the input is one payload.
_HANDLED_CLAIM_CHECKS = 1

_SHA256_RE = re.compile(r"[0-9a-f]{64}", re.ASCII)
#: A claim-check reference is tiny; anything bigger is not one of ours.
_MAX_REFERENCE = 256


class CodecError(Exception):
    """A payload could not be encrypted (unresolvable recipient) or decrypted (not ours,
    tampered, claim-check missing, altered or too large, a Nexus result that arrived in
    plaintext). Never swallowed into plaintext."""


class InvalidPayload(CodecError):
    """The payload itself can never be opened: a malformed or unsupported reference, a
    claim-check above the limit or not matching its reference, ciphertext not for us. Unlike
    a bucket that did not answer, retrying the same payload cannot help, so a handler refuses
    it instead of fetching it again on every retry. Messages are ours: no payload bytes."""


class BuyerKeys(Protocol):
    """Seller side: the buyer's ``age1...`` key for a job (from the hold token, gate on miss)."""

    async def buyer_key(self, job_id: str) -> str: ...


class SellerKeys(Protocol):
    """Buyer side: the ``age1...`` recipients of the party behind a Nexus endpoint.

    A seller holds a SET of them -- one per deployment -- so the input is encrypted to all
    of them and whichever worker picks the task up can decrypt it (decisions I7).
    Implementations look the set up (hire response cache, then ``GateClient.by_endpoint``);
    never parse the endpoint name."""

    async def seller_recipients(self, endpoint: str) -> Sequence[str]: ...


class BlobStore(Protocol):
    """Claim-check storage, content-addressed by the sha256 of the (ciphertext) bytes."""

    async def put(self, sha256: str, data: bytes) -> None: ...

    async def get(self, sha256: str, *, limit: int | None = None) -> bytes:
        """The stored bytes; with ``limit``, raise rather than read more than that many (a
        reference names the size, and a bucket object larger than it must not be
        downloaded whole to find out): :class:`InvalidPayload` if the object is larger,
        :class:`CodecError` if it could not be read."""
        ...


class BlobGate(Protocol):
    """The part of :class:`kontra.gate_client.GateClient` a :class:`GateBlobStore` uses."""

    @property
    def http(self) -> httpx.AsyncClient: ...

    async def create_blob(self, req: m.BlobCreate) -> m.BlobUrl: ...

    async def get_blob(self, sha256: str) -> m.BlobUrl: ...


class GateBlobStore:
    """:class:`BlobStore` on the gate's presigned bucket URLs (``POST /v1/blobs``,
    ``GET /v1/blobs/{sha256}``). The bucket only ever sees ciphertext; the gate's client
    credential never goes to the bucket (``gate.http``)."""

    def __init__(self, gate: BlobGate) -> None:
        self._gate = gate

    async def put(self, sha256: str, data: bytes) -> None:
        url = await self._gate.create_blob(m.BlobCreate(sha256=sha256, size=len(data)))
        if url.exists:
            return  # content-addressed: the same bytes are already there
        resp = await self._gate.http.put(
            url.url, content=data, headers={"Content-Length": str(len(data))}
        )
        if not resp.is_success:
            raise CodecError(f"claim-check upload failed: HTTP {resp.status_code}")

    async def get(self, sha256: str, *, limit: int | None = None) -> bytes:
        url = await self._gate.get_blob(sha256)
        # The stored bytes are what the sha256 names: asked for as stored, and read raw. A
        # Content-Encoding (one the uploader set on the object, say) is never decoded: one
        # 64 KiB chunk of gzip inflates to 64 MiB before it could be counted.
        async with self._gate.http.stream(
            "GET", url.url, headers={"Accept-Encoding": "identity"}
        ) as resp:
            if not resp.is_success:
                raise CodecError(f"claim-check download failed: HTTP {resp.status_code}")
            declared = resp.headers.get("content-length", "")
            if (
                limit is not None
                and declared.isascii()
                and declared.isdigit()
                and int(declared) > limit
            ):
                raise InvalidPayload(f"the claim-checked payload is larger than {limit} bytes")
            data = bytearray()
            # Counted as it arrives: neither a missing nor a lying Content-Length gets more
            # than ``limit`` bytes (plus one network chunk) in.
            async for chunk in _raw_body(resp):
                data += chunk
                if limit is not None and len(data) > limit:
                    raise InvalidPayload(f"the claim-checked payload is larger than {limit} bytes")
            return bytes(data)


async def _raw_body(resp: httpx.Response) -> AsyncIterator[bytes]:
    """The body as it was sent. A transport that read it already (``httpx.MockTransport``
    does, as an in-process test bucket) has no raw stream left: what it read is the body."""
    if resp.is_stream_consumed:
        yield resp.content
        return
    async for chunk in resp.aiter_raw():
        yield chunk


class KontraCodec(PayloadCodec, WithSerializationContext):
    """See the module docstring for the recipient rules.

    ``identity`` is THIS machine's age secret key -- the one thing that decrypts.
    ``recipients`` is the party's whole set, which is what "to ourselves" means once a
    seller runs a fleet: a result written by one worker must be readable by the seller's
    laptop and by its other deployments, not only by the pod that wrote it. This
    machine's own public key must be in the set, or the codec could not read back what it
    just wrote.

    ``services`` (seller): the exact names of the Nexus services this worker generated from
    its offerings (:attr:`~kontra.contract.names.OfferingId.service_name`). Only their
    ``quote`` and ``execute`` replies are sent in plaintext; a codec without them encrypts
    every reply. Their ``execute`` input's claim-check is left for the handler to open
    (:meth:`open_deferred`), which :mod:`kontra.serve.handler` does after the hold check.
    ``max_claim_check``: the largest claim-checked payload this codec fetches.
    """

    def __init__(
        self,
        identity: str,
        recipients: Sequence[str],
        *,
        role: Literal["seller", "buyer"],
        buyer_keys: BuyerKeys | None = None,
        seller_keys: SellerKeys | None = None,
        blobs: BlobStore | None = None,
        claim_check_threshold: int = 256 * 1024,
        services: Iterable[str] = (),
        max_claim_check: int = MAX_CLAIM_CHECK,
        extra_identities: Callable[[], Sequence[str]] | None = None,
    ) -> None:
        if role not in ("seller", "buyer"):
            raise ValueError("role must be 'seller' or 'buyer'")
        try:
            ours = age.public_key_of(identity)
        except age.AgeError as e:
            raise ValueError(f"identity: {e}") from None
        if isinstance(recipients, str):
            raise TypeError("recipients is a sequence of age keys, not one key")
        ordered = _distinct(list(recipients))
        if not ordered:
            raise ValueError("recipients must hold at least this machine's own key")
        for r in ordered:
            if not age.is_recipient(r):
                raise ValueError("every recipient must be an age X25519 recipient (age1...)")
        if ours not in ordered:
            raise ValueError("recipients must contain the public key of identity")
        if claim_check_threshold < 0:
            raise ValueError("claim_check_threshold must be >= 0")
        if max_claim_check < 1:
            raise ValueError("max_claim_check must be >= 1")
        if isinstance(services, str):
            raise TypeError("services is a collection of service names, not one name")
        generated = frozenset(services)
        if any(not isinstance(s, str) or not s for s in generated):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise ValueError("services must be non-empty service names")
        if generated and role != "seller":
            raise ValueError("services is for the seller role: a buyer replies to no Nexus call")
        self._identity = identity
        self._recipients = ordered
        #: More secret keys to try when decrypting, read at each call because the set
        #: grows: a buyer makes one per job at hire and shreds it at accept (decisions
        #: I7). Never used to encrypt -- only :attr:`_recipients` says who may read.
        self._extra_identities = extra_identities
        self._role: Literal["seller", "buyer"] = role
        self._buyer_keys = buyer_keys
        self._seller_keys = seller_keys
        self._blobs = blobs
        self._threshold = claim_check_threshold
        self._services = generated
        self._max_claim_check = max_claim_check
        self._context: SerializationContext | None = None
        #: Claim-checked stream items stored already, shared by every context's copy.
        self._stream_refs = _stream.BlobRefs()
        #: Claim-checks fetched under this context: the input of one handled Nexus operation
        #: (temporalio makes a context, so a copy of this codec, per Nexus task).
        self._fetched = 0

    @property
    def role(self) -> Literal["seller", "buyer"]:
        return self._role

    @property
    def services(self) -> frozenset[str]:
        """The generated Nexus services whose ``quote``/``execute`` replies are plaintext."""
        return self._services

    @property
    def claim_check_threshold(self) -> int | None:
        """The most bytes of ciphertext an encoded payload carries inline: above it, the
        ciphertext goes to the blob store. None: no blob store, so every payload is inline."""
        return self._threshold if self._blobs is not None else None

    @property
    def context(self) -> SerializationContext | None:
        return self._context

    def with_context(self, context: SerializationContext) -> Self:
        clone = copy.copy(self)
        clone._context = context
        clone._fetched = 0
        return clone

    # ------------------------------------------------------------------ recipients

    async def recipients(self) -> list[str] | None:
        """Who payloads encoded under the current context are encrypted to, or None for
        plaintext. Raises :class:`CodecError` rather than drop a recipient."""
        ctx = self._context
        if self._role == "seller":
            if isinstance(ctx, NexusSerializationContext):
                if not _handling(ctx):
                    return await self._nexus_call_recipients(ctx)
                if self._generated(ctx) and ctx.operation in _PLAINTEXT_SELLER_OPERATIONS:
                    return None
                return list(self._recipients)
            if isinstance(ctx, WorkflowSerializationContext):
                job_id = job_id_from_workflow_id(ctx.workflow_id)
                if job_id is not None:
                    if self._buyer_keys is None:
                        raise CodecError(
                            f"cannot encrypt job {job_id}'s payload: no buyer key lookup"
                        )
                    buyer = await _resolve(self._buyer_keys.buyer_key, job_id, "buyer")
                    return _distinct([*self._recipients, buyer])
            return list(self._recipients)
        if isinstance(ctx, NexusSerializationContext):
            return await self._nexus_call_recipients(ctx)
        return list(self._recipients)

    async def _nexus_call_recipients(self, ctx: NexusSerializationContext) -> list[str]:
        """A Nexus call's input: the seller behind the endpoint (never derived from its
        name) and ourselves."""
        if self._seller_keys is None:
            raise CodecError(
                f"cannot encrypt a Nexus input for endpoint {ctx.endpoint!r}: no seller key lookup"
            )
        seller = await _resolve_many(self._seller_keys.seller_recipients, ctx.endpoint, "seller")
        return _distinct([*seller, *self._recipients])

    def _generated(self, ctx: NexusSerializationContext) -> bool:
        """``ctx`` names one of the services this worker generated, by its exact name."""
        return ctx.service in self._services

    def _defers_claim_checks(self) -> bool:
        """Decoding the input of a generated ``execute`` this worker is handling: its hold
        token is not verified yet, so a claim-check is left for :meth:`open_deferred`."""
        ctx = self._context
        return (
            self._role == "seller"
            and isinstance(ctx, NexusSerializationContext)
            and ctx.operation == EXECUTE_OPERATION
            and self._generated(ctx)
            and _handling(ctx)
        )

    def _handled_input(self) -> bool:
        """Decoding the input of a Nexus operation this worker handles (any service)."""
        ctx = self._context
        return (
            self._role == "seller" and isinstance(ctx, NexusSerializationContext) and _handling(ctx)
        )

    def _calling(self) -> NexusSerializationContext | None:
        """The Nexus context when this process is the *caller* (a buyer, a seller's workflow
        hiring): what it decodes there is the operation's result or failure."""
        ctx = self._context
        if not isinstance(ctx, NexusSerializationContext):
            return None
        if self._role == "seller" and _handling(ctx):
            return None
        return ctx

    # ------------------------------------------------------------------ PayloadCodec

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if not payloads:
            return []
        if not any(STREAM_MARK in p.metadata for p in payloads):
            return await self._encode(payloads)
        out: list[Payload] = []
        for p in payloads:
            if STREAM_MARK in p.metadata:
                out.append(await self._encode_marked(p))
            else:
                out += await self._encode([p])
        return out

    async def _encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        recipients = await self.recipients()
        if recipients is None:
            ctx = self._context
            if isinstance(ctx, NexusSerializationContext) and ctx.operation == EXECUTE_OPERATION:
                return [_mark_plaintext(p) for p in payloads]
            return list(payloads)  # quote: the gate reads the price as plain JSON
        out: list[Payload] = []
        for p in payloads:
            try:
                ciphertext = age.encrypt(p.SerializeToString(), recipients)
            except age.AgeError as e:
                raise CodecError(f"encryption failed: {e}") from e
            out.append(await self._maybe_claim_check(ciphertext))
        return out

    async def _encode_marked(self, p: Payload) -> Payload:
        """A stream answer Kontra's workflow interceptor marked (decisions I31): a poll
        result becomes the stream envelope, an offset passes in plaintext. Only a seller's
        job workflow marks one; anywhere else it is refused, never encoded another way."""
        ctx = self._context
        job_id = (
            job_id_from_workflow_id(ctx.workflow_id)
            if isinstance(ctx, WorkflowSerializationContext)
            else None
        )
        if self._role != "seller" or job_id is None:
            raise CodecError(
                "a workflow-stream answer is marked outside a seller's job workflow; refusing "
                "it rather than choose its recipients"
            )
        mark = p.metadata[STREAM_MARK]
        if mark == OFFSET_MARK:
            try:
                _stream.parse_offset(p)
            except EnvelopeError as e:
                raise CodecError(f"a marked stream offset: {e}") from e
            return p  # plaintext by design: a log length
        if mark != POLL_MARK:
            raise CodecError("an unknown workflow-stream mark")
        if self._buyer_keys is None:
            raise CodecError(f"cannot seal job {job_id}'s stream: no buyer key lookup")
        buyer = await _resolve(self._buyer_keys.buyer_key, job_id, "buyer")
        try:
            return await _stream.seal_poll(
                p,
                buyer_recipients=_distinct([*self._recipients, buyer]),
                seller_recipients=list(self._recipients),
                put_blob=self._put_blob if self._blobs is not None else None,
                refs=self._stream_refs,
                scope=job_id,
            )
        except (EnvelopeError, age.AgeError) as e:
            raise CodecError(f"cannot seal job {job_id}'s stream: {e}") from e

    async def _put_blob(self, ciphertext: bytes) -> tuple[str, int]:
        """A sealed stream item above the inline limit, to the blob store."""
        assert self._blobs is not None
        digest = sha256(ciphertext).hexdigest()
        try:
            await self._blobs.put(digest, ciphertext)
        except CodecError:
            raise
        except Exception as e:
            raise CodecError(f"claim-check upload failed: {e}") from e
        return digest, len(ciphertext)

    async def _open_stream(self, p: Payload) -> Payload:
        """A stream envelope as the poll result it was sealed from."""
        if isinstance(self._context, NexusSerializationContext):
            raise InvalidPayload("a workflow-stream envelope is never a Nexus payload")
        fetch = self._fetch_blob if self._blobs is not None else None
        try:
            return await _stream.open_poll(p, self._identities(), fetch_blob=fetch)
        except EnvelopeError as e:
            raise InvalidPayload(f"cannot open a workflow-stream envelope: {e}") from e

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        calling = self._calling()
        defer = self._defers_claim_checks()
        if not defer and self._handled_input():
            # A Nexus input is one payload; temporalio decodes every payload nested in a
            # "system" envelope too. Counted before any fetch: none is made for a call that
            # names more than the operation may fetch.
            named = sum(p.metadata.get("encoding") == CLAIM_CHECK_ENCODING for p in payloads)
            if self._fetched + named > _HANDLED_CLAIM_CHECKS:
                raise InvalidPayload(
                    "a Nexus operation's input is one payload: at most one claim-check is "
                    f"fetched for it, and this one names {self._fetched + named}"
                )
        out: list[Payload] = []
        for p in payloads:
            encoding = p.metadata.get("encoding")
            if encoding == ENCODING:
                self._check_version(p)
                out.append(self._open(p.data))
            elif encoding == CLAIM_CHECK_ENCODING:
                self._check_version(p)
                if defer:
                    _parse_reference(p.data)  # malformed is refused now; fetched after the hold
                    out.append(p)
                else:
                    self._fetched += 1
                    out.append(self._open(await self._fetch(p.data)))
            elif encoding == STREAM_ENCODING:
                out.append(await self._open_stream(p))
            elif calling is not None:
                out.append(_plaintext_result(calling, p))
            else:
                out.append(p)  # not ours (e.g. a buyer's plaintext input): untouched
        return out

    async def open_deferred(self, payload: Payload) -> Payload:
        """The input of a generated ``execute`` as the codec would have decoded it, once the
        handler has verified the hold token: a claim-check reference that :meth:`decode`
        left in place is fetched (within ``max_claim_check``), verified and decrypted now.
        Any other payload is returned as it is."""
        if payload.metadata.get("encoding") != CLAIM_CHECK_ENCODING:
            return payload
        self._check_version(payload)
        return self._open(await self._fetch(payload.data))

    # ------------------------------------------------------------------ internals

    @staticmethod
    def _check_version(p: Payload) -> None:
        if p.metadata.get(_VERSION_KEY) != _VERSION:
            raise InvalidPayload("unsupported kontra payload version")

    def _identities(self) -> list[str]:
        """Every secret key this codec may decrypt with: this machine's, then the job
        keys. age tries each stanza against each identity and stops at the first that
        opens the file key."""
        found = [self._identity]
        if self._extra_identities is not None:
            for extra in self._extra_identities():
                if extra not in found:
                    found.append(extra)
        return found

    def _open(self, ciphertext: bytes) -> Payload:
        try:
            plain = age.decrypt(ciphertext, self._identities())
        except age.AgeError as e:
            raise InvalidPayload(f"cannot decrypt payload: {e}") from e
        inner = Payload()
        try:
            # Anyone can encrypt to a public key: what decrypts is still untrusted input.
            inner.ParseFromString(plain)
        except DecodeError as e:
            raise InvalidPayload(f"decrypted payload is not a Payload: {e}") from e
        return inner

    async def _maybe_claim_check(self, ciphertext: bytes) -> Payload:
        meta = {_VERSION_KEY: _VERSION}
        if self._blobs is None or len(ciphertext) <= self._threshold:
            return Payload(metadata={"encoding": ENCODING, **meta}, data=ciphertext)
        digest = sha256(ciphertext).hexdigest()
        try:
            await self._blobs.put(digest, ciphertext)
        except CodecError:
            raise
        except Exception as e:
            raise CodecError(f"claim-check upload failed: {e}") from e
        ref = json.dumps({"sha256": digest, "size": len(ciphertext)}, separators=(",", ":"))
        return Payload(
            metadata={"encoding": CLAIM_CHECK_ENCODING, **meta}, data=ref.encode("ascii")
        )

    async def _fetch(self, reference: bytes) -> bytes:
        if self._blobs is None:
            raise CodecError("payload is claim-checked but no blob store is configured")
        digest, size = _parse_reference(reference)
        return await self._fetch_blob(digest, size)

    async def _fetch_blob(self, digest: str, size: int) -> bytes:
        if self._blobs is None:
            raise CodecError("payload is claim-checked but no blob store is configured")
        if size > self._max_claim_check:
            # Refused before asking for it: a reference is a claim anyone can write.
            raise InvalidPayload(
                f"the claim-checked payload is {size} bytes, above this codec's limit of "
                f"{self._max_claim_check}"
            )
        try:
            data = await self._blobs.get(digest, limit=size)
        except CodecError:
            raise
        except Exception as e:
            raise CodecError(f"claim-check download failed: {e}") from e
        if len(data) != size or sha256(data).hexdigest() != digest:
            raise InvalidPayload("claim-checked payload does not match its reference")
        return data


def _handling(ctx: NexusSerializationContext) -> bool:
    """True when this runs inside a Nexus operation this worker is handling on
    ``ctx.endpoint``: an encode is its reply (output or failure), a decode its input. False
    for a call this process makes. temporalio decodes a handler's input and encodes its
    completion inside the operation's context, and a workflow's or a client's outgoing call
    outside of it."""
    return temporal_nexus.in_operation() and temporal_nexus.info().endpoint == ctx.endpoint


def _mark_plaintext(p: Payload) -> Payload:
    marked = Payload()
    marked.CopyFrom(p)
    marked.metadata[PLAINTEXT_KEY] = _PLAINTEXT_MARK
    return marked


def _plaintext_result(ctx: NexusSerializationContext, p: Payload) -> Payload:
    """A plaintext payload in the result (or failure) of a Nexus call this process made.
    ``quote``'s answer is plaintext by design, by the exact operation name (decisions I4);
    a seller's codec marks the refusals of ``execute``. Anything else is a result the seller
    did not encrypt: it is in the platform's history now, and it is not passed on as if it
    were a delivery."""
    if ctx.operation == QUOTE_OPERATION:
        return p
    if ctx.operation == EXECUTE_OPERATION and p.metadata.get(PLAINTEXT_KEY) == _PLAINTEXT_MARK:
        unmarked = Payload()
        unmarked.CopyFrom(p)
        del unmarked.metadata[PLAINTEXT_KEY]
        return unmarked
    raise CodecError(
        f"the result of {ctx.operation!r} on Nexus endpoint {ctx.endpoint!r} (service "
        f"{ctx.service!r}) arrived in plaintext; only 'quote' answers in plaintext and every "
        "other result must be encrypted to this party, so the seller sent it unencrypted "
        "and it is readable in Temporal's history"
    )


async def _resolve(lookup: Callable[[str], Awaitable[str]], arg: str, who: str) -> str:
    try:
        key = await lookup(arg)
    except Exception as e:
        raise CodecError(f"cannot resolve the {who}'s key: {e}") from e
    # is_recipient accepts any object: a lookup that returns junk fails closed here.
    if not age.is_recipient(key):
        raise CodecError(f"the {who}'s key is not an age X25519 recipient")
    return key


async def _resolve_many(
    lookup: Callable[[str], Awaitable[Sequence[str]]], arg: str, who: str
) -> list[str]:
    """Like :func:`_resolve`, for a party's whole recipient set. An empty set is an error,
    not "encrypt to nobody": a party with no recipient cannot be sent anything."""
    try:
        keys = await lookup(arg)
    except Exception as e:
        raise CodecError(f"cannot resolve the {who}'s keys: {e}") from e
    if isinstance(keys, str) or not isinstance(keys, Sequence):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise CodecError(f"the {who}'s keys are not a list of age X25519 recipients")
    ordered = _distinct(list(keys))
    if not ordered:
        raise CodecError(f"the {who} has no recipient to encrypt to")
    for key in ordered:
        if not age.is_recipient(key):
            raise CodecError(f"one of the {who}'s keys is not an age X25519 recipient")
    return ordered


def _distinct(keys: list[str]) -> list[str]:
    return list(dict.fromkeys(keys))


def _parse_reference(raw: bytes) -> tuple[str, int]:
    if len(raw) > _MAX_REFERENCE:
        raise InvalidPayload("malformed claim-check reference")
    try:
        ref = json.loads(raw)
    except ValueError:
        raise InvalidPayload("malformed claim-check reference") from None
    if not isinstance(ref, dict) or set(ref) != {"sha256", "size"}:  # pyright: ignore[reportUnknownArgumentType]
        raise InvalidPayload("malformed claim-check reference")
    digest, size = ref["sha256"], ref["size"]
    if (
        not isinstance(digest, str)
        or not _SHA256_RE.fullmatch(digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise InvalidPayload("malformed claim-check reference")
    return digest, size


def check_sealed(data: bytes) -> None:
    """Raise :class:`~kontra.engine.NotSealed` unless ``data`` is exactly one serialized
    Kontra payload envelope as this codec writes it: canonical (deterministic) protobuf, the
    metadata ``{encoding, kontra-v: 1}`` and nothing else, and either ``binary/kontra-age``
    with an age v1 file as data (its structure: :func:`kontra.codec.age.is_age_file`) or
    ``binary/kontra-claim-check`` with a well-formed reference.

    What seam ⑥ hands a gate as a delivery (:class:`kontra.engine.SealedPayload`): a result
    that fails this is plaintext, or bytes the codec did not write, and is never a delivery
    (decisions I32). Nothing is decrypted or fetched: only the envelope is read."""
    p = Payload()
    try:
        p.ParseFromString(data)
    except DecodeError:
        raise NotSealed("not a serialized payload") from None
    if p.SerializeToString(deterministic=True) != data:
        raise NotSealed("not a canonical payload")
    if set(p.metadata) != {"encoding", _VERSION_KEY} or p.metadata[_VERSION_KEY] != _VERSION:
        raise NotSealed("not a kontra v1 payload envelope")
    encoding = p.metadata["encoding"]
    if encoding == ENCODING:
        if not p.data.startswith(AGE_MAGIC) or not age.is_age_file(p.data):
            raise NotSealed("a binary/kontra-age payload without an age file")
        return
    if encoding == CLAIM_CHECK_ENCODING:
        try:
            _parse_reference(p.data)
        except InvalidPayload:
            raise NotSealed("a malformed claim-check reference") from None
        return
    raise NotSealed("its encoding is not Kontra ciphertext")


class KontraFailureConverter(
    DefaultFailureConverterWithEncodedAttributes, WithSerializationContext
):
    """Failure messages and stack traces moved into an encoded payload, so the codec
    encrypts them too (decisions I7) -- and no Python stack trace in the reply of a Nexus
    operation this worker handles. temporalio chains the exception behind its own handler
    errors (an input the codec or the payload converter could not read) with its traceback,
    and the reply of a generated ``quote``/``execute`` is plaintext in the caller's history
    and the platform's: the trace would hand both the seller's file system layout. Messages
    stay, and so does a handler error's own ``stack_trace`` (kontra.serve's note); only the
    reply loses the tracebacks, the exceptions keep theirs."""

    def __init__(self) -> None:
        super().__init__()
        self._context: SerializationContext | None = None

    def with_context(self, context: SerializationContext) -> Self:
        clone = copy.copy(self)
        clone._context = context
        return clone

    def to_failure(
        self, exception: BaseException, payload_converter: PayloadConverter, failure: Failure
    ) -> None:
        ctx = self._context
        if not (isinstance(ctx, NexusSerializationContext) and _handling(ctx)):
            super().to_failure(exception, payload_converter, failure)
            return
        held: list[tuple[BaseException, Any]] = []
        seen: set[int] = set()
        cur: BaseException | None = exception
        while cur is not None and id(cur) not in seen:  # the chain temporalio converts
            seen.add(id(cur))
            held.append((cur, cur.__traceback__))
            cur.__traceback__ = None
            cur = cur.__cause__ or (None if cur.__suppress_context__ else cur.__context__)
        try:
            super().to_failure(exception, payload_converter, failure)
        finally:
            for err, tb in held:
                err.__traceback__ = tb


def data_converter(codec: KontraCodec) -> DataConverter:
    """A ``DataConverter`` with ``codec`` whose :class:`KontraFailureConverter` moves failure
    messages and stack traces into an encoded payload, so they are encrypted too (I7)."""
    return DataConverter(payload_codec=codec, failure_converter_class=KontraFailureConverter)
