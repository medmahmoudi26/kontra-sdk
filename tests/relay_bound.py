"""The relay's bound (decisions I39), computed from Kontra's pinned files: what any relay token
holder's polls can add to one run of a job, whatever offsets it polls from.

    R_e <= 5 P + C_e + 3 W_s
    R_b <= min(P b_max, G_b + F b_max) + C_b + w_t W_s

P the run's poll budget, F the updates in flight, G_b the poll guard's bytes, b_max the most
one accepted poll writes (its workflow task's events, the request echoed twice, the answer),
C_e/C_b the drain at the run's end (a timer and two workflow tasks); W_s the seller's own
workflow tasks in the run and w_t one of their triples (a refused poll's speculative task kept
beside one of them). The job survives when its own history and the relay's share stay under
``limit.historyCount.error`` and ``limit.historySize.error``, on a worker that completes its
workflow tasks in time (a task that times out writes up to :data:`TIMEOUT_EVENTS` more events
and two triples: decisions I39). Shared by ``tests/serve/test_interceptors.py`` (the numbers)
and ``tests/temporal/test_relay_bound_wire.py`` (measured against them)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

import yaml

from kontra.codec import stream as s
from kontra.serve import interceptors

AUTHORIZER = Path(__file__).resolve().parents[1] / "temporal/authorizer"

#: A workflow task's triple (scheduled, started, completed) and the update events' headers:
#: every string in them is the seller's worker's (its task queue, identity, build id), each at
#: most ``limit.maxIDLength``.
TASK_BYTES_MAX = 4096
#: The drain at a run's end, once per run when anyone polled: a timer (started, fired or
#: cancelled) and its task for the wait, one for releasing the pollers -- two triples of at
#: most ``TASK_BYTES_MAX`` and the timer's events.
DRAIN_EVENTS, DRAIN_BYTES = 10, 2 * TASK_BYTES_MAX + 256
#: What one workflow task that timed out writes, beyond the triple that replaces it: a
#: speculative task's scheduled, started and timed-out events kept (``server@v1.32.1``
#: ``workflow_task_state_machine.go``), and its retry's triple.
TIMEOUT_EVENTS = 6
#: The events one accepted poll writes, at most: ``UpdateAccepted``, ``UpdateCompleted`` and
#: the workflow task that accepted it.
EVENTS_PER_POLL = 5
#: Payload framing around an envelope in an event.
FRAMING = 64


@cache
def pinned() -> dict[str, Any]:
    """``dynamicconfig/kontra.yaml``, each key's (unconstrained) value."""
    raw = yaml.safe_load((AUTHORIZER / "dynamicconfig/kontra.yaml").read_text())
    return {k: v[0]["value"] for k, v in raw.items()}


@cache
def authorizer_constant(name: str) -> int | str:
    """A constant of ``authorizer.go``: ``N``, ``N << M`` or a string literal."""
    text = (AUTHORIZER / "authorizer.go").read_text()
    m = re.search(rf"\b{name}\s*=\s*(\d+)(?:\s*<<\s*(\d+))?\b", text)
    if m is not None:
        return int(m[1]) << int(m[2] or 0)
    m = re.search(rf'\b{name}\s*=\s*"([^"]*)"', text)
    assert m is not None, name
    return m[1]


@dataclass(frozen=True)
class Bound:
    """The relay's share of one run: ``events`` and ``bytes`` with no seller task beside it,
    plus ``task_events``/``task_bytes`` for each of the seller's own workflow tasks."""

    events: int
    bytes: int
    task_events: int
    task_bytes: int
    per_poll_bytes: int

    def events_with(self, seller_tasks: int) -> int:
        return self.events + self.task_events * seller_tasks

    def bytes_with(self, seller_tasks: int, triple: int = TASK_BYTES_MAX) -> int:
        return self.bytes + min(triple, self.task_bytes) * seller_tasks


def bound(polls_max: int = s.STREAM_POLLS_MAX) -> Bound:
    """The formula with the pinned numbers (``polls_max`` for a test that scales P)."""
    p = pinned()
    request = int(authorizer_constant("RelayRequestMax"))
    id_max = int(p["limit.maxIDLength"])
    in_flight = int(p["history.maxInFlightUpdates"])
    answer = s.ENVELOPE_MAX + FRAMING
    accepted = request + 2 * (id_max + 8) + 60  # accepted_request, protocol and message ids
    completed = request + answer + 60  # meta (in the request's size), the outcome
    per_poll = TASK_BYTES_MAX + accepted + completed
    guard = interceptors.STREAM_HISTORY_BYTES
    return Bound(
        events=EVENTS_PER_POLL * polls_max + DRAIN_EVENTS,
        bytes=min(polls_max * per_poll, guard + in_flight * per_poll) + DRAIN_BYTES,
        task_events=3,
        task_bytes=TASK_BYTES_MAX,
        per_poll_bytes=per_poll,
    )
