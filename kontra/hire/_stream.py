"""``job.stream(topic, type=T)``: one topic of a job's workflow stream, as the gate relays it
(decisions I31).

The seller publishes with temporalio's own API on the job's stream; the gate's relay reads
the topics the offering lists (``offering.yaml`` ``streams:``) and ``kontra.progress``, each
item sealed to the job's parties, and serves them as server-sent events
(``GET /v1/jobs/{id}/stream``). Here each item is opened with this client's keys (the job's
key among them), checked against the topic and offset the gate said it has -- they are
sealed inside the ciphertext, so a gate cannot relabel or reorder items -- and validated as
``type`` (a pydantic model, ``dict``, any type a ``TypeAdapter`` takes; the item's JSON is the
seller's).

Delivery: at-least-once from the gate, deduplicated here by the gate's ``cursor`` (a replay
after a reconnect) and by the item's ``offset`` in the job's log (a gate that re-read a poll).
An item not sealed for us, or relabelled, is logged and skipped. An item too large to relay,
or past the gate's budget, is a gap: logged, skipped. The relay stopping (the job's history
budget, a seller worker that does not speak the stream envelope) is
:class:`~kontra.hire.StreamInterrupted`. The end of the iteration is the gate saying the job
is closed and its relay finished. ``cursor`` is the last event consumed: pass it as ``after``
to resume elsewhere.
"""

from __future__ import annotations

import base64
import binascii
import logging
from collections.abc import AsyncIterator, Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import TypeAdapter, ValidationError
from temporalio.api.common.v1 import Payload

from kontra.codec import stream as _stream
from kontra.gate_client import models as m
from kontra.hire._errors import StreamInterrupted, StreamItemInvalid

if TYPE_CHECKING:
    from kontra.hire._client import Kontra

__all__ = ["JobStream", "open_event"]

log = logging.getLogger("kontra.hire")

T = TypeVar("T")

_JSON_PLAIN = b"json/plain"
_NULL = b"binary/null"


def open_event(event: m.StreamEvent, identities: Sequence[str], job_id: str) -> Payload | None:
    """An ``item`` event's ``Payload``, opened with ``identities`` and its sealed topic and
    offset checked against the event's; None (logged) when it was not sealed for us or was
    relabelled -- what a catalog that lists a topic the seller's worker does not, or a gate
    that misbehaves, looks like."""
    if event.ciphertext_b64 is None or event.topic is None or event.offset is None:
        return None
    try:
        ct = base64.b64decode(event.ciphertext_b64, validate=True)
    except (binascii.Error, ValueError):
        log.warning("job %s: stream item %d is not base64; skipped", job_id, event.cursor)
        return None
    try:
        return _stream.open_item(ct, identities, topic=event.topic, offset=event.offset)
    except _stream.ItemUnreadable as err:
        log.warning("job %s: stream item %d is not sealed for us: %s", job_id, event.cursor, err)
    except _stream.EnvelopeError as err:
        log.warning("job %s: stream item %d was relabelled: %s", job_id, event.cursor, err)
    return None


def decode_item(payload: Payload, adapter: TypeAdapter[Any]) -> Any:
    """The item's value: its JSON (``json/plain``) or ``None`` (``binary/null``) validated
    by ``adapter``. Anything else is :class:`ValueError`: a value Kontra cannot read without
    the seller's own converter."""
    encoding = payload.metadata.get("encoding")
    if encoding == _JSON_PLAIN:
        return adapter.validate_json(payload.data)
    if encoding == _NULL:
        return adapter.validate_python(None)
    raise ValueError(f"an item encoded as {encoding!r} (only JSON items are read)")


class JobStream(Generic[T]):
    """``async for value in job.stream(topic, type=T)``: see the module docstring."""

    def __init__(
        self, kontra: Kontra, job_id: str, topic: str, adapter: TypeAdapter[T], after: int
    ) -> None:
        if isinstance(after, bool) or after < 0:
            raise ValueError("after is a cursor >= 0")
        self._kontra = kontra
        self.job_id = job_id
        self.topic = topic
        self._adapter = adapter
        #: The last event consumed (the gate's cursor): pass it as ``after`` to resume.
        self.cursor = after
        self._offset = -1
        self._events: AsyncIterator[m.StreamEvent] | None = None
        self._closed = False

    def __aiter__(self) -> JobStream[T]:
        return self

    async def __anext__(self) -> T:
        if self._closed:
            raise StopAsyncIteration
        if self._events is None:
            await self._kontra._ensure_job_key(self.job_id, required=False)  # pyright: ignore[reportPrivateUsage]
            self._events = self._kontra.gate.stream(
                self.job_id, topics=[self.topic], history=False, after=self.cursor
            )
        while True:
            try:
                event = await anext(self._events)
            except StopAsyncIteration:
                await self.aclose()
                raise
            if event.cursor <= self.cursor:
                continue
            self.cursor = event.cursor
            if event.kind is m.StreamEventKind.HISTORY:
                continue
            if event.kind is m.StreamEventKind.GAP:
                if event.offset is None:
                    await self.aclose()
                    reason = event.reason.value if event.reason is not None else "relay_failed"
                    raise StreamInterrupted(
                        f"job {self.job_id}: the gate stopped relaying its stream ({reason})",
                        job_id=self.job_id,
                        reason=reason,
                    )
                if event.offset > self._offset:
                    self._offset = event.offset
                log.info(
                    "job %s: stream item at offset %d was not relayed (%s, %s bytes)",
                    self.job_id,
                    event.offset,
                    event.reason.value if event.reason is not None else "gap",
                    event.size,
                )
                continue
            if event.topic != self.topic or event.offset is None or event.offset <= self._offset:
                continue  # another topic, or an item seen already (a gate that re-read)
            payload = open_event(event, self._kontra.identities, self.job_id)
            if payload is None:
                continue
            self._offset = event.offset
            try:
                return decode_item(payload, self._adapter)
            except (ValidationError, ValueError) as err:
                raise StreamItemInvalid(
                    f"job {self.job_id}: the {self.topic} item at offset {event.offset} is not "
                    f"what it was read as: {_said(err)}",
                    job_id=self.job_id,
                    topic=self.topic,
                    offset=event.offset,
                ) from None

    async def aclose(self) -> None:
        """Close the gate's stream (a caller that stops early: now, not at garbage
        collection)."""
        self._closed = True
        events, self._events = self._events, None
        close = getattr(events, "aclose", None)
        if close is not None:
            await close()


def _said(err: Exception) -> str:
    """Where and what, never the value (the item is the buyer's to read, not to log)."""
    if isinstance(err, ValidationError):
        return "; ".join(
            f"{'.'.join(map(str, e['loc'])) or 'value'}: {e['msg']}" for e in err.errors()
        )
    return str(err)
