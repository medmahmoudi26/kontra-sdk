"""Heartbeat details to the buyer, through the job's own workflow stream (PRD §8.4,
decisions I31).

Seller code calls ``activity.heartbeat(details)`` as it would for Temporal anyway; Kontra's
activity outbound interceptor sees every call (before the SDK throttles them) and offers
``(job, activity, details)`` here. Nothing in seller code knows: delete ``worker.py`` and the
heartbeats simply stop being forwarded (decisions I29).

The forwarder publishes each one it forwards as a :class:`~kontra.types.ProgressRecord` on
the ``kontra.progress`` topic of the job's stream (``job-<id>``, the stream Kontra's workflow
interceptor hosts there), with temporalio's own ``WorkflowStreamClient`` on the worker's
client: the publish signal is encrypted by the codec like every job payload, the relay
carries the item to the buyer sealed to the buyer and the seller, and the gate stores
ciphertext only. There is no second progress channel.

A heartbeat now costs the seller's history (one publish signal is about four events), and
Temporal terminates a run past 20,480 events, refunded against the seller. So forwarding is
budgeted, per job: only the latest details, at most one publish per ``interval`` seconds
(``$KONTRA_PROGRESS_INTERVAL``, default :data:`DEFAULT_INTERVAL`, 5); the interval doubles
after every :data:`BACKOFF_EVERY` items forwarded, up to :data:`INTERVAL_MAX`; and at most
``budget`` (:data:`PROGRESS_BUDGET`, 1,000) items per job, after which nothing more is
forwarded (one log line; history events still show liveness). With an interval of 0 every
heartbeat is published, in order, batched into as few signals as the sends allow, still
within the budget.

When an activity returns, :meth:`ProgressForwarder.settle` publishes the job's queued
heartbeat -- the latest -- at once, whatever the interval, and waits *briefly* for it, so the
activity's last word is in the job's stream before the job can close (acceptance 13: the
latest heartbeat reaches the buyer). That is at most one item more per activity, within the
budget. It is bounded: a send that outlives the bound carries on in the background and stops
all waiting for a while, so a slow Temporal costs the seller's activities at most one short
pause per ``_QUIET_FOR`` seconds, not one per activity.

Forwarding is otherwise best effort and invisible to the activity:
:meth:`ProgressForwarder.offer` never raises and never blocks, and failures (Temporal
unreachable, the job closed, details that are not JSON) are logged and dropped.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pydantic_core
from pydantic import ValidationError
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.contrib.workflow_streams import WorkflowStreamClient

from kontra.contract.names import PROGRESS_TOPIC, job_workflow_id
from kontra.types import ProgressRecord

__all__ = [
    "BACKOFF_EVERY",
    "DEFAULT_INTERVAL",
    "INTERVAL_ENV",
    "INTERVAL_MAX",
    "PROGRESS_BUDGET",
    "ProgressForwarder",
    "progress_interval",
]

log = logging.getLogger("kontra.serve.progress")

INTERVAL_ENV = "KONTRA_PROGRESS_INTERVAL"
DEFAULT_INTERVAL = 5.0
#: Items forwarded per job before its interval doubles, and the longest interval.
BACKOFF_EVERY = 360
INTERVAL_MAX = 300.0
#: Items forwarded per job, at most: about 4,000 history events, a fifth of the limit.
PROGRESS_BUDGET = 1000
#: Details above this are not forwarded: an item stays far under the relay's 256 KiB.
_MAX_PLAINTEXT = 48 * 1024
_ACTIVITY_MAX = 255
#: With interval 0 every heartbeat is queued; a job whose sends are slower than its
#: heartbeats keeps this many and drops the oldest.
_QUEUE_MAX = 1000
_SEND_TIMEOUT = 10.0
#: A job that has been quiet this long is forgotten (its state is a few bytes, but a
#: worker runs for months).
_FORGET_AFTER = 300.0
#: How long a returning activity may wait for its due heartbeats to be published, and how
#: long nothing waits after that bound was hit (Temporal is slow or down).
SETTLE_TIMEOUT = 5.0
_QUIET_FOR = 30.0
_JSON_PLAIN = b"json/plain"


def progress_interval(env: Mapping[str, str] | None = None) -> float:
    """``$KONTRA_PROGRESS_INTERVAL`` in seconds (>= 0), else the default."""
    raw = (env if env is not None else os.environ).get(INTERVAL_ENV)
    if not raw:
        return DEFAULT_INTERVAL
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if not math.isfinite(value) or value < 0:
        log.warning(
            "%s=%r is not a number of seconds >= 0; using %s", INTERVAL_ENV, raw, DEFAULT_INTERVAL
        )
        return DEFAULT_INTERVAL
    return value


@dataclass
class _Event:
    at: datetime
    activity: str
    plaintext: bytes


@dataclass
class _Job:
    queue: deque[_Event] = field(default_factory=deque[_Event])
    last_sent: float = -math.inf
    last_offer: float = 0.0
    #: Items published for the job (its budget and its back-off count them).
    forwarded: int = 0
    #: The job's budget is spent and that was said once.
    spent: bool = False
    #: Its stream publisher: one per job, dropped after a failed send.
    publisher: WorkflowStreamClient | None = None
    #: Serialises sending per job, so events leave in the order they were offered.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _serialise(details: tuple[Any, ...]) -> bytes:
    value: Any = details[0] if len(details) == 1 else list(details)
    return pydantic_core.to_json(value)


def _running_loop() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


class ProgressForwarder:
    """``bind(client)`` with the worker's Temporal client, ``start()`` on the worker's loop,
    ``offer()`` from any thread, ``settle(job)`` when an activity returns, ``aclose()`` to
    publish what is left and stop. ``budget``: items per job, at most. ``clock`` is
    monotonic seconds, ``wall`` the record's timestamp; tests may replace either."""

    def __init__(
        self,
        *,
        interval: float | None = None,
        budget: int = PROGRESS_BUDGET,
        settle_timeout: float = SETTLE_TIMEOUT,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.interval = progress_interval() if interval is None else float(interval)
        if not math.isfinite(self.interval) or self.interval < 0:
            raise ValueError("interval must be a number of seconds >= 0")
        if budget < 0:
            raise ValueError("budget must be >= 0")
        self.budget = budget
        self.settle_timeout = settle_timeout
        self._clock = clock
        self._wall = wall
        self._client: Client | None = None
        #: Until when (``clock``) :meth:`settle` does not wait: a settle hit its bound.
        self._quiet_until = -math.inf
        #: Sends a :meth:`settle` stopped waiting for; they finish in the background.
        self._settling: set[asyncio.Task[None]] = set()
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        #: Items published / dropped (not JSON, too big, queue full, budget spent, failed).
        self.sent = 0
        self.dropped = 0

    def bind(self, client: Client) -> None:
        """Publish with ``client`` (the worker's own: its codec encrypts each publish signal
        to the job's parties, and its token may signal its own jobs)."""
        self._client = client

    # ------------------------------------------------------------------ producer side

    def offer(self, job_id: str, activity: str, details: tuple[Any, ...]) -> None:
        """Called for every heartbeat of a job's activity. Never raises, never blocks."""
        try:
            self._offer(job_id, activity, details)
        except Exception:  # the activity must never see a forwarding problem
            log.exception("could not queue the progress of job %s", job_id)

    def _offer(self, job_id: str, activity: str, details: tuple[Any, ...]) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            plaintext = _serialise(details)
        except (pydantic_core.PydanticSerializationError, ValueError, TypeError) as exc:
            self.dropped += 1
            log.warning("heartbeat details of %s are not JSON (%s); not forwarded", activity, exc)
            return
        if len(plaintext) > _MAX_PLAINTEXT:
            self.dropped += 1
            log.warning(
                "heartbeat details of %s are over %d bytes; not forwarded", activity, _MAX_PLAINTEXT
            )
            return
        event = _Event(self._wall(), activity[:_ACTIVITY_MAX] or "activity", plaintext)
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                job = self._jobs[job_id] = _Job()
            if job.forwarded >= self.budget:
                self.dropped += 1
                self._spent(job_id, job)
                return
            if self._interval(job) > 0:
                job.queue.clear()  # latest wins
            elif len(job.queue) >= _QUEUE_MAX:
                job.queue.popleft()
                self.dropped += 1
            job.queue.append(event)
            job.last_offer = self._clock()
        if _running_loop() is loop:
            self._poke()
        else:
            loop.call_soon_threadsafe(self._poke)

    def _spent(self, job_id: str, job: _Job) -> None:
        if not job.spent:
            job.spent = True
            log.info(
                "job %s forwarded its %d progress items: no more are published (each costs "
                "the job's history); its history events still show it alive",
                job_id,
                self.budget,
            )

    def _interval(self, job: _Job) -> float:
        """The job's interval: the base, doubled every :data:`BACKOFF_EVERY` items."""
        if self.interval == 0:
            return 0.0
        doublings = min(job.forwarded // BACKOFF_EVERY, 32)
        return min(self.interval * 2**doublings, max(INTERVAL_MAX, self.interval))

    def _poke(self) -> None:
        if self._wake is not None:
            self._wake.set()

    # ------------------------------------------------------------------ the sender

    def start(self) -> None:
        """Start sending, on the running loop."""
        if self._task is not None:
            return
        self._loop = asyncio.get_running_loop()
        self._wake = asyncio.Event()
        self._task = self._loop.create_task(self._run(), name="kontra-progress")

    async def _run(self) -> None:
        wake = self._wake
        assert wake is not None
        while True:
            wake.clear()
            delay = await self._send_due()
            # asyncio.timeout, never wait_for, in this module: on Python 3.11 wait_for drops
            # a cancel that lands as its wait completes (aclose while a heartbeat pokes), and
            # this loop would then wait for a heartbeat forever, and aclose with it.
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(delay):
                    await wake.wait()

    async def _send_due(self) -> float | None:
        """Send every job that is due; the delay until the next one is (None: nothing queued)."""
        now = self._clock()
        due: list[tuple[str, _Job]] = []
        delay: float | None = None
        with self._lock:
            for job_id, job in list(self._jobs.items()):
                if not job.queue:
                    if not job.lock.locked() and now - job.last_offer > _FORGET_AFTER:
                        del self._jobs[job_id]
                    continue
                wait = job.last_sent + self._interval(job) - now
                if wait <= 0:
                    due.append((job_id, job))
                else:
                    delay = wait if delay is None else min(delay, wait)
        if due:
            await asyncio.gather(*(self._drain(job_id, job, paced=True) for job_id, job in due))
            # Heartbeats that arrived meanwhile poked the wake event; the loop looks again.
            if self.interval == 0:
                return 0.0 if any(job.queue for _, job in due) else delay
            soonest = min(self._interval(job) for _, job in due)
            delay = soonest if delay is None else min(delay, soonest)
        return delay

    def _due(self, job: _Job) -> bool:
        return self.interval == 0 or self._clock() >= job.last_sent + self._interval(job)

    async def _drain(self, job_id: str, job: _Job, *, paced: bool = False) -> None:
        """Publish what the job has queued, oldest first, as one batch. Under the job's
        lock, so the sender, :meth:`settle` and :meth:`flush` never reorder or double-send.
        ``paced`` sends only what is due, checked under the lock: with an interval that is
        at most one event, and a heartbeat that arrived while the previous batch was in
        flight waits its turn."""
        async with job.lock:
            while True:
                with self._lock:
                    if not job.queue or (paced and not self._due(job)):
                        return
                    room = self.budget - job.forwarded
                    if room <= 0:
                        self.dropped += len(job.queue)
                        job.queue.clear()
                        self._spent(job_id, job)
                        return
                    batch = [job.queue.popleft() for _ in range(min(len(job.queue), room))]
                job.last_sent = self._clock()
                await self._send(job_id, job, batch)

    async def _send(self, job_id: str, job: _Job, batch: list[_Event]) -> None:
        client = self._client
        if client is None:
            self.dropped += len(batch)
            log.info("progress of job %s not forwarded: the forwarder has no client", job_id)
            return
        try:
            items = [_payload(e) for e in batch]
        except ValidationError as exc:
            self.dropped += len(batch)
            log.info("progress of job %s not forwarded: %s", job_id, exc.errors()[0]["msg"])
            return
        try:
            if job.publisher is None:
                job.publisher = WorkflowStreamClient.create(client, job_workflow_id(job_id))
            topic = job.publisher.topic(PROGRESS_TOPIC)
            for item in items:
                topic.publish(item)
            async with asyncio.timeout(_SEND_TIMEOUT):
                await job.publisher.flush()
        except asyncio.CancelledError:
            job.publisher = None
            raise
        except Exception as exc:
            # A fresh publisher next time: this one would send the failed batch again.
            job.publisher = None
            self.dropped += len(batch)
            log.info("progress of job %s not forwarded: %s", job_id, exc)
        else:
            job.forwarded += len(batch)
            self.sent += len(batch)

    # ------------------------------------------------------------------ flushing

    async def settle(self, job_id: str) -> None:
        """An activity of ``job_id`` is returning: publish what the job has queued (its
        latest heartbeat) now, whatever the interval, and wait until it is in the job's
        stream, so it lands before the job can close. Bounded by :attr:`settle_timeout`; a
        send still running then is not cancelled, and no settle waits for the next
        ``_QUIET_FOR`` seconds. Never raises."""
        job = self._jobs.get(job_id)
        loop = self._loop
        if job is None or loop is None or self._task is None:
            return
        if self._clock() < self._quiet_until:
            return
        with self._lock:
            pending = bool(job.queue)
        if not pending and not job.lock.locked():
            return
        drain = loop.create_task(self._drain(job_id, job), name="kontra-progress-settle")
        self._settling.add(drain)
        drain.add_done_callback(self._settling.discard)
        try:
            async with asyncio.timeout(self.settle_timeout):
                await asyncio.shield(drain)
        except TimeoutError:
            self._quiet_until = self._clock() + _QUIET_FOR
            log.info(
                "progress of job %s is slow to reach its stream; activities stop waiting for "
                "it for %.0fs",
                job_id,
                _QUIET_FOR,
            )
        except Exception:
            log.exception("settling the progress of job %s failed", job_id)

    async def flush(self, job_id: str, timeout: float = 5.0) -> None:  # noqa: ASYNC109
        """Send what ``job_id`` has queued now, whatever the interval (the worker is
        stopping: its last heartbeat is the one the buyer wants). Bounded by ``timeout``;
        never raises. Not for an activity's path (:meth:`settle` is)."""
        job = self._jobs.get(job_id)
        if job is None or self._loop is None:
            return
        try:
            async with asyncio.timeout(timeout):
                await self._drain(job_id, job)
        except TimeoutError:
            log.info("progress of job %s still queued after %.1fs", job_id, timeout)
        except Exception:
            log.exception("flushing the progress of job %s failed", job_id)

    async def aclose(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109
        """Send what is queued (bounded by ``timeout`` overall), then stop."""
        task, self._task = self._task, None
        if task is None:
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(timeout):
                for job_id in list(self._jobs):
                    await self.flush(job_id, timeout)
        tasks = [task, *self._settling]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._settling.clear()
        self._loop = None
        self._wake = None


def _payload(event: _Event) -> Payload:
    """The item: a :class:`~kontra.types.ProgressRecord` as ``json/plain`` (published as a
    ready ``Payload``, so it does not depend on the client's payload converter)."""
    record = ProgressRecord(
        at=event.at, activity=event.activity, details=pydantic_core.from_json(event.plaintext)
    )
    return Payload(metadata={"encoding": _JSON_PLAIN}, data=record.model_dump_json().encode())
