"""Shared helpers for the fake-gate tests: a scriptable control plane (so most gate rules
are tested without Temporal), contract documents, and logged-in parties."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict
from temporalio.api.common.v1 import Payload

from kontra._control import (
    DEFAULT_RETENTION,
    EndpointConflict,
    EndpointInfo,
    HistorySummary,
    NexusSyncKind,
    NexusSyncResult,
    TemporalError,
)
from kontra.auth import Session, login
from kontra.contract import derive_document, parse_offering
from kontra.escrow import Observation, ObservedStatus
from kontra.gate_client import models as m

# --------------------------------------------------------------------------- the contract


class EnrichInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
    mode: str = "ok"


class EnrichOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str


def offering_doc(party: str, public_key: str, **facts: Any) -> dict[str, Any]:
    """A sealed contract document for ``<party>/enrich``; ``facts`` override offering.yaml."""
    spec: dict[str, Any] = {
        "name": f"{party}/enrich",
        "version": "1.0.0",
        "description": "Upper-cases text, slowly",
        "pricing_hint": "$1 per character",
        "tags": ["text"],
        "max_duration": "1h",
        "review_window": "1h",
        "max_output_bytes": "1MB",
        "artifact_retention": "1d",
    }
    spec.update(facts)
    return derive_document(parse_offering(spec), EnrichInput, EnrichOutput, public_key)


def handle(prefix: str = "p") -> str:
    """A party handle no other test uses (namespaces outlive a test on a shared server)."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def json_payload(value: object) -> Payload:
    return Payload(metadata={"encoding": b"json/plain"}, data=json.dumps(value).encode())


def wire(payload: Payload) -> m.WirePayload:
    return m.WirePayload(b64=base64.b64encode(payload.SerializeToString()).decode())


def unwire(w: m.WirePayload) -> Payload:
    p = Payload()
    p.ParseFromString(base64.b64decode(w.b64))
    return p


def quote_payload(**fields: Any) -> Payload:
    """A seller's plaintext ``KontraQuote`` answer."""
    return json_payload(fields)


def ok_quote(amount_minor: int = 1234, seconds: int = 3600) -> Payload:
    return quote_payload(
        status="ok", amount_minor=amount_minor, currency="USD", max_duration_seconds=seconds
    )


# --------------------------------------------------------------------------- parties


async def logged_in(
    base_url: str, home: Path, handle_: str, transport: httpx.AsyncBaseTransport | None = None
) -> Session:
    """``kontra login --dev`` as ``handle_`` into ``home``: profile, age key registered with
    the gate, and a session on it."""
    await asyncio.to_thread(home.mkdir, mode=0o700, parents=True, exist_ok=True)
    profile = await login(base_url, handle=handle_, dev=True, home=home, transport=transport)
    return Session(profile, home=home, transport=transport)


# --------------------------------------------------------------------------- a scripted control plane

Responder = Callable[
    [Payload, Mapping[str, str]],
    "Payload | NexusSyncResult | Awaitable[Payload | NexusSyncResult]",
]


@dataclass
class Started:
    namespace: str
    operation_id: str
    endpoint: str
    service: str
    operation: str
    payload: Payload
    headers: dict[str, str]
    schedule_to_close: timedelta


@dataclass
class ScriptedControl:
    """An in-memory :class:`kontra._control.TemporalControl`: records every call and
    answers from a script, so gate rules are tested without a Temporal server."""

    namespaces: dict[str, timedelta] = field(default_factory=dict[str, timedelta])
    endpoints: dict[str, EndpointInfo] = field(default_factory=dict[str, EndpointInfo])
    responder: Responder | None = None
    observations: dict[tuple[str, str], tuple[Observation, bytes | None]] = field(
        default_factory=dict[tuple[str, str], tuple[Observation, bytes | None]]
    )
    summaries: dict[tuple[str, str], HistorySummary] = field(
        default_factory=dict[tuple[str, str], HistorySummary]
    )
    sync_calls: list[tuple[str, str, str, str, Payload, dict[str, str], float]] = field(
        default_factory=list[tuple[str, str, str, str, Payload, dict[str, str], float]]
    )
    starts: list[Started] = field(default_factory=list[Started])
    cancels: list[tuple[str, str, str]] = field(default_factory=list[tuple[str, str, str]])
    terminates: list[tuple[str, str, str]] = field(default_factory=list[tuple[str, str, str]])
    fail_endpoints: Exception | None = None
    fail_observe: Exception | None = None
    fail_stops: int = 0

    def observe_as(
        self,
        namespace: str,
        workflow_id: str,
        status: ObservedStatus,
        result: bytes | None = None,
        **kw: Any,
    ) -> None:
        self.observations[(namespace, workflow_id)] = (Observation(status=status, **kw), result)

    async def ensure_namespace(
        self, namespace: str, retention: timedelta = DEFAULT_RETENTION
    ) -> None:
        self.namespaces.setdefault(namespace, retention)

    async def find_endpoint(self, name: str) -> EndpointInfo | None:
        return self.endpoints.get(name)

    async def create_endpoint(self, name: str, namespace: str, task_queue: str) -> EndpointInfo:
        if self.fail_endpoints is not None:
            raise self.fail_endpoints
        if namespace not in self.namespaces:
            raise TemporalError(f"namespace {namespace} is not registered")
        existing = self.endpoints.get(name)
        if existing is not None:
            if (existing.namespace, existing.task_queue) != (namespace, task_queue):
                raise EndpointConflict(name)
            return existing
        info = EndpointInfo(f"id-{len(self.endpoints)}", name, namespace, task_queue)
        self.endpoints[name] = info
        return info

    async def nexus_sync(
        self,
        namespace: str,
        task_queue: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        timeout: float,  # noqa: ASYNC109
    ) -> NexusSyncResult:
        self.sync_calls.append(
            (namespace, task_queue, service, operation, payload, dict(headers), timeout)
        )
        if self.responder is None:
            return NexusSyncResult(NexusSyncKind.UNAVAILABLE, message="no worker")
        out = self.responder(payload, headers)
        if inspect.isawaitable(out):
            out = await out
        if isinstance(out, NexusSyncResult):
            return out
        return NexusSyncResult(NexusSyncKind.OK, payload=out)

    async def observe(self, namespace: str, workflow_id: str) -> tuple[Observation, bytes | None]:
        if self.fail_observe is not None:
            raise self.fail_observe
        return self.observations.get(
            (namespace, workflow_id), (Observation(status=ObservedStatus.NOT_FOUND), None)
        )

    async def history_summary(self, namespace: str, workflow_id: str) -> HistorySummary | None:
        return self.summaries.get((namespace, workflow_id))

    async def cancel_workflow(self, namespace: str, workflow_id: str, reason: str) -> None:
        if self.fail_stops:
            self.fail_stops -= 1
            raise TemporalError("frontend down")
        self.cancels.append((namespace, workflow_id, reason))

    async def terminate_workflow(self, namespace: str, workflow_id: str, reason: str) -> None:
        if self.fail_stops:
            self.fail_stops -= 1
            raise TemporalError("frontend down")
        self.terminates.append((namespace, workflow_id, reason))

    async def start_operation(
        self,
        namespace: str,
        operation_id: str,
        endpoint: str,
        service: str,
        operation: str,
        payload: Payload,
        headers: Mapping[str, str],
        schedule_to_close: timedelta,
    ) -> None:
        if any(s.operation_id == operation_id and s.namespace == namespace for s in self.starts):
            return
        self.starts.append(
            Started(
                namespace,
                operation_id,
                endpoint,
                service,
                operation,
                payload,
                dict(headers),
                schedule_to_close,
            )
        )
