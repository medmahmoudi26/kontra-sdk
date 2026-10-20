"""The engine contract's rig for :class:`~kontra.testing.FakeEngine` (seam ⑥), and the sealing
helpers every engine harness here shares: the seller's output sealed the way
:class:`~kontra.codec.KontraCodec` writes a delivery (``binary/kontra-age``, ``kontra-v`` 1,
an age file of the inner payload), and opened again with the job's key."""

from __future__ import annotations

import asyncio
import json
import uuid

from temporalio.api.common.v1 import Payload

from kontra.codec import age
from kontra.contract.names import OfferingId, new_endpoint_name
from kontra.engine import Engine, JobHandle, Sealed, SealedPayload
from kontra.testing.engine import FakeEngine, RunCanceled, RunContext
from kontra.testing.engine_contract import (
    LISTED_TOPIC,
    UNLISTED_TOPIC,
    Behaviour,
    QuoteBehaviour,
)

#: The seller's quote answer (a ``KontraQuote``): the one plaintext that crosses.
QUOTE_DOCUMENT = b'{"amount_minor":1234,"currency":"USD","max_duration_seconds":600,"status":"ok"}'


def party(prefix: str) -> str:
    """A party handle no other test uses (namespaces outlive a test on a shared server)."""
    return f"{prefix}{uuid.uuid4().hex[:10]}"


def seal_text(text: str, recipient: str) -> bytes:
    """``text`` as a delivery: what the seller's codec stores as the job's result."""
    inner = Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(text).encode())
    ciphertext = age.encrypt(inner.SerializeToString(), [recipient])
    return Payload(
        metadata={"encoding": b"binary/kontra-age", "kontra-v": b"1"}, data=ciphertext
    ).SerializeToString(deterministic=True)


def plain_text(text: str) -> bytes:
    """``text`` as a seller that bypassed the codec would store it."""
    return Payload(
        metadata={"encoding": b"json/plain"}, data=json.dumps(text).encode()
    ).SerializeToString(deterministic=True)


def open_text(result: SealedPayload, identity: str) -> str:
    envelope = Payload.FromString(result.data)
    inner = Payload.FromString(age.decrypt(envelope.data, [identity]))
    return str(json.loads(inner.data))


def item_text(offset: int, marker: str) -> str:
    return f"ITEM {marker} {offset}"


#: What a ``publish`` job puts on its stream before an ASK, and after it: (topic, offset).
FIRST_ITEMS = ((LISTED_TOPIC, 0), (UNLISTED_TOPIC, 1), (LISTED_TOPIC, 2))
LATER_ITEMS = ((LISTED_TOPIC, 3), (UNLISTED_TOPIC, 4), (LISTED_TOPIC, 5))


class FakeHarness:
    """:class:`~kontra.testing.engine_contract.EngineHarness` for a FakeEngine. ``engine``: a
    (possibly broken) FakeEngine to rig instead of a fresh one."""

    enforces_isolation = True
    buyer_presence = True
    streams = True

    def __init__(self, engine: FakeEngine | None = None) -> None:
        self.identity, self.recipient = age.generate_identity()
        self.seller, self.buyer, self.stranger = party("s"), party("b"), party("x")
        self.offering = OfferingId.parse(f"{self.seller}/contract@1.0.0")
        self.other_offering = OfferingId.parse(f"{self.seller}/contract-other@1.0.0")
        self.route = new_endpoint_name()
        self.fake = engine if engine is not None else FakeEngine()

    async def setup(self) -> FakeHarness:
        await self.fake.isolate(self.seller)
        self.fake.serve(self.offering, self._program)
        self.fake.serve_quote(self.offering, self._quote)
        await self.fake.expose(self.offering, self.route)
        return self

    @property
    def engine(self) -> Engine:
        return self.fake

    # ------------------------------------------------------------------ the seller's code

    async def _program(self, ctx: RunContext, raw: bytes) -> bytes:
        plan = json.loads(raw)
        behaviour, marker = plan["b"], plan["m"]
        if behaviour == "hang":
            await ctx.cancelled()
            raise RunCanceled
        if behaviour == "ignore_ask":
            await ctx.cancelled()
        if behaviour == "can2":
            ctx.restart()
            ctx.restart()
        if behaviour == "plain":
            return plain_text(f"PLAIN {marker}")
        if behaviour == "publish":
            for topic, offset in FIRST_ITEMS:
                ctx.publish(
                    topic, age.encrypt(item_text(offset, marker).encode(), [self.recipient])
                )
            await ctx.cancelled()
            for topic, offset in LATER_ITEMS:
                ctx.publish(
                    topic, age.encrypt(item_text(offset, marker).encode(), [self.recipient])
                )
        ctx.step("seal")
        return seal_text(f"DONE {marker}", self.recipient)

    async def _quote(self, raw: bytes) -> bytes:
        kind = json.loads(raw)["q"]
        if kind == "fail":
            raise RuntimeError("the seller's quote refused")
        if kind == "slow":
            await asyncio.sleep(60)
        return QUOTE_DOCUMENT

    # ------------------------------------------------------------------ the probes

    def input(self, behaviour: Behaviour, marker: str) -> bytes:
        return json.dumps({"b": behaviour, "m": marker}).encode()

    def quote_input(self, behaviour: QuoteBehaviour) -> bytes:
        return json.dumps({"q": behaviour}).encode()

    def quote_document(self) -> bytes:
        return QUOTE_DOCUMENT

    def hold(self, job_id: str) -> str:
        return f"testhold.{job_id}.sig"

    def open(self, result: SealedPayload) -> str:
        return open_text(result, self.identity)

    def open_item(self, item: Sealed) -> str:
        return age.decrypt(item.data, [self.identity]).decode()

    async def seller_start(
        self, handle: JobHandle, job_id: str, marker: str, behaviour: Behaviour = "ok"
    ) -> None:
        result = None if behaviour == "hang" else seal_text(f"SELLER {marker}", self.recipient)
        self.fake.seller_start(handle, result)

    async def seller_revive(self, handle: JobHandle, job_id: str) -> None:
        self.fake.seller_revive(handle)

    async def buyer_withdraw(self, handle: JobHandle, job_id: str) -> None:
        self.fake.buyer_withdraws(handle)

    async def reach(self, credential: str, handle: JobHandle) -> bool:
        return self.fake.reaches(credential, handle)

    async def stops(self, credential: str, handle: JobHandle) -> bool:
        return self.fake.stops(credential, handle)

    async def controls(self, credential: str) -> bool:
        return False  # a FakeEngine credential is a party's, with no control power

    async def fresh_engine(self) -> Engine:
        return self.fake  # in memory, the engine's state is the instance

    async def unreachable_engine(self) -> Engine:
        dead = type(self.fake)()
        dead.unreachable = True
        return dead
