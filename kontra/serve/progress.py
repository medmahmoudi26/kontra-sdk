"""Heartbeat details to the buyer, encrypted, through the gate (PRD §8.4).

Seller code calls ``activity.heartbeat(details)`` as it would for Temporal anyway; Kontra's
activity outbound interceptor sees every call (before the SDK throttles them) and offers
``(job, activity, details)`` here. Nothing in seller code knows: delete ``kontra`` and the
heartbeats simply stop being forwarded (D1).

Per job the forwarder keeps only the latest details and sends at most one event per
``interval`` seconds (``$KONTRA_PROGRESS_INTERVAL``, default 1) -- however slow the gate is
and however many activities the job runs, since the gate drops what exceeds its own per-job
rate. With an interval of 0 it sends every heartbeat, in order. Details are serialised to
JSON, encrypted with age to the job's buyer key (from the job directory, the gate on a
miss), and posted with ``POST /v1/jobs/{id}/progress``. The gate stores ciphertext only.

When an activity returns, :meth:`ProgressForwarder.settle` waits *briefly* for the job's
heartbeats that are already due, because the gate refuses progress for a job it has seen
close: without it, a job whose activity heartbeats 100 times and returns loses the tail
(acceptance 13). It never sends ahead of the interval, and it is bounded: a send that
outlives the bound carries on in the background and stops all waiting for a while, so a
slow or unreachable gate costs the seller's activities at most one short pause per
``_QUIET_FOR`` seconds, not one per activity.

Forwarding is otherwise best effort and invisible to the activity:
:meth:`ProgressForwarder.offer` never raises and never blocks, and failures (gate down, job
closed, details that are not JSON) are logged and dropped.
"""

from __future__ import annotations

import asyncio
import base64
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
from typing import Any, Protocol

import pydantic_core

from kontra.codec import age
from kontra.gate_client import models as m

__all__ = ["DEFAULT_INTERVAL", "INTERVAL_ENV", "ProgressForwarder", "progress_interval"]

log = logging.getLogger("kontra.serve.progress")

INTERVAL_ENV = "KONTRA_PROGRESS_INTERVAL"
DEFAULT_INTERVAL = 1.0
#: The gate refuses events above 64 KiB of ciphertext; dropping them here saves the trip.
_MAX_PLAINTEXT = 48 * 1024
_ACTIVITY_MAX = 255
#: With interval 0 every heartbeat is queued; a job whose gate is slower than its
#: heartbeats keeps this many and drops the oldest.
_QUEUE_MAX = 1000
_SEND_TIMEOUT = 10.0
#: A job that has been quiet this long is forgotten (its state is a few bytes, but a
#: worker runs for months).
_FORGET_AFTER = 300.0
#: How long a returning activity may wait for its due heartbeats to reach the gate, and
#: how long nothing waits after that bound was hit (the gate is slow or down).
SETTLE_TIMEOUT = 5.0
_QUIET_FOR = 30.0


class ProgressGate(Protocol):
    async def post_progress(self, job_id: str, event: m.ProgressPost) -> None: ...


class BuyerKeys(Protocol):
    async def buyer_key(self, job_id: str) -> str: ...


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
    """``start()`` on the worker's loop, ``offer()`` from any thread, ``settle(job)`` when an
    activity returns, ``aclose()`` to send what is left and stop. ``clock`` is monotonic
    seconds, ``wall`` the event timestamp; tests may replace either."""

    def __init__(
        self,
        gate: ProgressGate,
        keys: BuyerKeys,
        *,
        interval: float | None = None,
        settle_timeout: float = SETTLE_TIMEOUT,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._gate = gate
        self._keys = keys
        self.interval = progress_interval() if interval is None else float(interval)
        if not math.isfinite(self.interval) or self.interval < 0:
            raise ValueError("interval must be a number of seconds >= 0")
        self.settle_timeout = settle_timeout
        self._clock = clock
        self._wall = wall
        #: Until when (``clock``) :meth:`settle` does not wait: a settle hit its bound.
        self._quiet_until = -math.inf
        #: Sends a :meth:`settle` stopped waiting for; they finish in the background.
        self._settling: set[asyncio.Task[None]] = set()
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wake: asyncio.Event | None = None
        self._task: asyncio.Task[None] | None = None
        #: Events posted / dropped (not JSON, too big, queue full, gate refused).
        self.sent = 0
        self.dropped = 0

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
            if self.interval > 0:
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
                wait = job.last_sent + self.interval - now
                if wait <= 0:
                    due.append((job_id, job))
                else:
                    delay = wait if delay is None else min(delay, wait)
        if due:
            await asyncio.gather(*(self._drain(job_id, job, paced=True) for job_id, job in due))
            # Heartbeats that arrived meanwhile poked the wake event; the loop looks again.
            if self.interval == 0:
                return 0.0 if any(job.queue for _, job in due) else delay
            delay = self.interval if delay is None else min(delay, self.interval)
        return delay

    def _due(self, job: _Job) -> bool:
        return self.interval == 0 or self._clock() >= job.last_sent + self.interval

    async def _drain(self, job_id: str, job: _Job, *, paced: bool = False) -> None:
        """Send what the job has queued, oldest first. Under the job's lock, so the sender,
        :meth:`settle` and :meth:`flush` never reorder or double-send. ``paced`` sends only
        what is due, checked under the lock: with an interval that is at most one event, and
        a heartbeat that arrived while the previous event was in flight waits its turn."""
        async with job.lock:
            while True:
                with self._lock:
                    if not job.queue or (paced and not self._due(job)):
                        return
                    event = job.queue.popleft()
                job.last_sent = self._clock()
                await self._send(job_id, event)

    async def _send(self, job_id: str, event: _Event) -> None:
        try:
            key = await self._keys.buyer_key(job_id)
            ciphertext = age.encrypt(event.plaintext, [key])
            post = m.ProgressPost(
                at=event.at,
                activity=event.activity,
                ciphertext_b64=base64.b64encode(ciphertext).decode("ascii"),
            )
            async with asyncio.timeout(_SEND_TIMEOUT):
                await self._gate.post_progress(job_id, post)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.dropped += 1
            log.info("progress of job %s not forwarded: %s", job_id, exc)
        else:
            self.sent += 1

    # ------------------------------------------------------------------ flushing

    async def settle(self, job_id: str) -> None:
        """An activity of ``job_id`` is returning: wait until the job's *due* heartbeats
        reached the gate (an event not due yet keeps its turn), so they land before the job
        can close. Bounded by :attr:`settle_timeout`; a send still running then is not
        cancelled, and no settle waits for the next ``_QUIET_FOR`` seconds. Never raises."""
        job = self._jobs.get(job_id)
        loop = self._loop
        if job is None or loop is None or self._task is None:
            return
        if self._clock() < self._quiet_until:
            return
        with self._lock:
            pending = bool(job.queue) and self._due(job)
        if not pending and not job.lock.locked():
            return
        drain = loop.create_task(
            self._drain(job_id, job, paced=True), name="kontra-progress-settle"
        )
        self._settling.add(drain)
        drain.add_done_callback(self._settling.discard)
        try:
            async with asyncio.timeout(self.settle_timeout):
                await asyncio.shield(drain)
        except TimeoutError:
            self._quiet_until = self._clock() + _QUIET_FOR
            log.info(
                "progress of job %s is slow to reach the gate; activities stop waiting for "
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
