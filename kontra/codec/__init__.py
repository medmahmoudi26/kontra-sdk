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
seller    Nexus ``quote`` / ``execute`` *reply*, while  none: plaintext. The quote output is
          this worker handles that operation            a price the gate must read (I4);
                                                        ``execute`` only encodes handler
                                                        refusals, sent before a buyer key
                                                        can be trusted
seller    Nexus reply of any other operation            ourselves
seller    Nexus *call* (a seller's workflow hiring      the offering's seller + ourselves
          another offering)
seller    anything else (activities, children,          ourselves
          no context)
buyer     Nexus (any operation)                         the offering's seller + ourselves
buyer     anything else                                 ourselves
========  ============================================  ==================================

A :class:`~temporalio.converter.NexusSerializationContext` looks the same on both ends of a
Nexus call: temporalio uses it for the *caller's* input (a workflow's
``ScheduleNexusOperation``, a standalone ``start_operation``) and for the *handler's* reply.
Plaintext is only ever right for the reply, so the seller rule asks temporalio whether this
encode runs inside the Nexus operation being handled on that endpoint
(``temporalio.nexus.in_operation()`` and ``info().endpoint``); a seller-role process that
hires another offering gets the buyer rule for that call instead of leaking its input.

It fails closed: if a recipient it needs cannot be resolved (no key lookup, the lookup
raised, a malformed key) ``encode`` raises; it never falls back to plaintext. The seller's
key comes from the caller's ``SellerKeys`` (the hire response, else the catalog's
``GET /v1/endpoints/{endpoint}``); an endpoint name is opaque and never parsed.

An encoded payload is ``{encoding: binary/kontra-age, kontra-v: 1}`` with the age ciphertext
of the original ``Payload`` proto as data, so metadata (the original encoding, a message
type) is encrypted too. Above ``claim_check_threshold`` bytes of ciphertext the ciphertext
goes to a :class:`BlobStore`, content-addressed by its sha256, and history holds a
``binary/kontra-claim-check`` reference ``{"sha256", "size"}`` that is verified on fetch.
"""

from __future__ import annotations

import copy
import json
import re
from collections.abc import Awaitable, Callable, Sequence
from hashlib import sha256
from typing import Literal, Protocol, Self

import httpx
from google.protobuf.message import DecodeError
from temporalio import nexus as temporal_nexus
from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    DataConverter,
    DefaultFailureConverterWithEncodedAttributes,
    NexusSerializationContext,
    PayloadCodec,
    SerializationContext,
    WithSerializationContext,
    WorkflowSerializationContext,
)

from kontra.codec import age
from kontra.contract.names import job_id_from_workflow_id
from kontra.gate_client import models as m

__all__ = [
    "CLAIM_CHECK_ENCODING",
    "ENCODING",
    "EXECUTE_OPERATION",
    "QUOTE_OPERATION",
    "BlobGate",
    "BlobStore",
    "BuyerKeys",
    "CodecError",
    "GateBlobStore",
    "KontraCodec",
    "SellerKeys",
    "data_converter",
]

ENCODING = b"binary/kontra-age"
CLAIM_CHECK_ENCODING = b"binary/kontra-claim-check"
_VERSION_KEY = "kontra-v"
_VERSION = b"1"

#: The two Nexus operations of every offering (D2; ``kontra.contract.validate.OPERATIONS``).
QUOTE_OPERATION = "quote"
EXECUTE_OPERATION = "execute"
_PLAINTEXT_SELLER_OPERATIONS = frozenset({QUOTE_OPERATION, EXECUTE_OPERATION})

_SHA256_RE = re.compile(r"[0-9a-f]{64}", re.ASCII)
#: A claim-check reference is tiny; anything bigger is not one of ours.
_MAX_REFERENCE = 256


class CodecError(Exception):
    """A payload could not be encrypted (unresolvable recipient) or decrypted (not ours,
    tampered, claim-check missing or altered). Never swallowed into plaintext."""


class BuyerKeys(Protocol):
    """Seller side: the buyer's ``age1...`` key for a job (from the hold token, gate on miss)."""

    async def buyer_key(self, job_id: str) -> str: ...


class SellerKeys(Protocol):
    """Buyer side: the ``age1...`` key of the offering behind a Nexus endpoint. Implementations
    look it up (hire response cache, then ``GateClient.by_endpoint``); never parse the name."""

    async def seller_key(self, endpoint: str) -> str: ...


class BlobStore(Protocol):
    """Claim-check storage, content-addressed by the sha256 of the (ciphertext) bytes."""

    async def put(self, sha256: str, data: bytes) -> None: ...

    async def get(self, sha256: str) -> bytes: ...


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

    async def get(self, sha256: str) -> bytes:
        url = await self._gate.get_blob(sha256)
        resp = await self._gate.http.get(url.url)
        if not resp.is_success:
            raise CodecError(f"claim-check download failed: HTTP {resp.status_code}")
        return resp.content


class KontraCodec(PayloadCodec, WithSerializationContext):
    """See the module docstring for the recipient rules. ``identity``/``recipient`` are
    this party's age keypair; they must belong together."""

    def __init__(
        self,
        identity: str,
        recipient: str,
        *,
        role: Literal["seller", "buyer"],
        buyer_keys: BuyerKeys | None = None,
        seller_keys: SellerKeys | None = None,
        blobs: BlobStore | None = None,
        claim_check_threshold: int = 256 * 1024,
    ) -> None:
        if role not in ("seller", "buyer"):
            raise ValueError("role must be 'seller' or 'buyer'")
        try:
            ours = age.public_key_of(identity)
        except age.AgeError as e:
            raise ValueError(f"identity: {e}") from None
        if ours != recipient:
            raise ValueError("recipient is not the public key of identity")
        if claim_check_threshold < 0:
            raise ValueError("claim_check_threshold must be >= 0")
        self._identity = identity
        self._recipient = recipient
        self._role: Literal["seller", "buyer"] = role
        self._buyer_keys = buyer_keys
        self._seller_keys = seller_keys
        self._blobs = blobs
        self._threshold = claim_check_threshold
        self._context: SerializationContext | None = None

    @property
    def role(self) -> Literal["seller", "buyer"]:
        return self._role

    @property
    def context(self) -> SerializationContext | None:
        return self._context

    def with_context(self, context: SerializationContext) -> Self:
        clone = copy.copy(self)
        clone._context = context
        return clone

    # ------------------------------------------------------------------ recipients

    async def recipients(self) -> list[str] | None:
        """Who payloads encoded under the current context are encrypted to, or None for
        plaintext. Raises :class:`CodecError` rather than drop a recipient."""
        ctx = self._context
        if self._role == "seller":
            if isinstance(ctx, NexusSerializationContext):
                if not _replying(ctx):
                    return await self._nexus_call_recipients(ctx)
                if ctx.operation in _PLAINTEXT_SELLER_OPERATIONS:
                    return None
                return [self._recipient]
            if isinstance(ctx, WorkflowSerializationContext):
                job_id = job_id_from_workflow_id(ctx.workflow_id)
                if job_id is not None:
                    if self._buyer_keys is None:
                        raise CodecError(
                            f"cannot encrypt job {job_id}'s payload: no buyer key lookup"
                        )
                    buyer = await _resolve(self._buyer_keys.buyer_key, job_id, "buyer")
                    return _distinct([self._recipient, buyer])
            return [self._recipient]
        if isinstance(ctx, NexusSerializationContext):
            return await self._nexus_call_recipients(ctx)
        return [self._recipient]

    async def _nexus_call_recipients(self, ctx: NexusSerializationContext) -> list[str]:
        """A Nexus call's input: the seller behind the endpoint (never derived from its
        name) and ourselves."""
        if self._seller_keys is None:
            raise CodecError(
                f"cannot encrypt a Nexus input for endpoint {ctx.endpoint!r}: no seller key lookup"
            )
        seller = await _resolve(self._seller_keys.seller_key, ctx.endpoint, "seller")
        return _distinct([seller, self._recipient])

    # ------------------------------------------------------------------ PayloadCodec

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        if not payloads:
            return []
        recipients = await self.recipients()
        if recipients is None:
            return list(payloads)
        out: list[Payload] = []
        for p in payloads:
            try:
                ciphertext = age.encrypt(p.SerializeToString(), recipients)
            except age.AgeError as e:
                raise CodecError(f"encryption failed: {e}") from e
            out.append(await self._maybe_claim_check(ciphertext))
        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out: list[Payload] = []
        for p in payloads:
            encoding = p.metadata.get("encoding")
            if encoding == ENCODING:
                self._check_version(p)
                out.append(self._open(p.data))
            elif encoding == CLAIM_CHECK_ENCODING:
                self._check_version(p)
                out.append(self._open(await self._fetch(p.data)))
            else:
                out.append(p)  # not ours (e.g. a plaintext quote): untouched
        return out

    # ------------------------------------------------------------------ internals

    @staticmethod
    def _check_version(p: Payload) -> None:
        if p.metadata.get(_VERSION_KEY) != _VERSION:
            raise CodecError("unsupported kontra payload version")

    def _open(self, ciphertext: bytes) -> Payload:
        try:
            plain = age.decrypt(ciphertext, self._identity)
        except age.AgeError as e:
            raise CodecError(f"cannot decrypt payload: {e}") from e
        inner = Payload()
        try:
            # Anyone can encrypt to a public key: what decrypts is still untrusted input.
            inner.ParseFromString(plain)
        except DecodeError as e:
            raise CodecError(f"decrypted payload is not a Payload: {e}") from e
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
        try:
            data = await self._blobs.get(digest)
        except CodecError:
            raise
        except Exception as e:
            raise CodecError(f"claim-check download failed: {e}") from e
        if len(data) != size or sha256(data).hexdigest() != digest:
            raise CodecError("claim-checked payload does not match its reference")
        return data


def _replying(ctx: NexusSerializationContext) -> bool:
    """True when this encode is the reply (output or failure) of a Nexus operation this
    worker is handling on ``ctx.endpoint``; False for a call this process makes. temporalio
    encodes a handler's completion inside the operation's context, and a workflow's or a
    client's outgoing call outside of it."""
    return temporal_nexus.in_operation() and temporal_nexus.info().endpoint == ctx.endpoint


async def _resolve(lookup: Callable[[str], Awaitable[str]], arg: str, who: str) -> str:
    try:
        key = await lookup(arg)
    except Exception as e:
        raise CodecError(f"cannot resolve the {who}'s key: {e}") from e
    # is_recipient accepts any object: a lookup that returns junk fails closed here.
    if not age.is_recipient(key):
        raise CodecError(f"the {who}'s key is not an age X25519 recipient")
    return key


def _distinct(keys: list[str]) -> list[str]:
    return list(dict.fromkeys(keys))


def _parse_reference(raw: bytes) -> tuple[str, int]:
    if len(raw) > _MAX_REFERENCE:
        raise CodecError("malformed claim-check reference")
    try:
        ref = json.loads(raw)
    except ValueError:
        raise CodecError("malformed claim-check reference") from None
    if not isinstance(ref, dict) or set(ref) != {"sha256", "size"}:  # pyright: ignore[reportUnknownArgumentType]
        raise CodecError("malformed claim-check reference")
    digest, size = ref["sha256"], ref["size"]
    if (
        not isinstance(digest, str)
        or not _SHA256_RE.fullmatch(digest)
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
    ):
        raise CodecError("malformed claim-check reference")
    return digest, size


def data_converter(codec: KontraCodec) -> DataConverter:
    """A ``DataConverter`` with ``codec`` that also moves failure messages and stack traces
    into an encoded payload, so they are encrypted too (decisions I7)."""
    return DataConverter(
        payload_codec=codec,
        failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
    )
