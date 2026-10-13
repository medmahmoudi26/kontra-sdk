"""Helpers for driving GateClient through httpx.MockTransport."""

from __future__ import annotations

import json
import types
from collections.abc import AsyncIterator, Callable, Iterable
from datetime import datetime
from enum import Enum
from typing import Any, Literal, Union, get_args, get_origin

import httpx
from pydantic import BaseModel

from kontra.gate_client import models as m

NOW = "2026-10-11T00:00:00Z"


def minimal(model: type[BaseModel]) -> dict[str, Any]:
    """The smallest JSON object ``model`` accepts: every required field, nothing else."""
    out: dict[str, Any] = {}
    for name, field in model.model_fields.items():
        if field.is_required():
            patterns = [getattr(c, "pattern", None) for c in field.metadata]
            patterned = next((PATTERNED[p] for p in patterns if p in PATTERNED), None)
            out[field.alias or name] = patterned or _value(field.annotation)
    return out


#: Values for constrained string fields (opaque gate-assigned ids, digests).
PATTERNED = {r"^[0-9a-f]{32}$": "0" * 32, r"^[0-9a-f]{64}$": "0" * 64}


def _value(tp: Any) -> Any:
    origin, args = get_origin(tp), get_args(tp)
    if origin in (Union, types.UnionType):
        non_none = [a for a in args if a is not type(None)]
        return None if len(non_none) < len(args) else _value(non_none[0])
    if origin is Literal:
        return args[0]
    if origin is list:
        return []
    if origin is dict or tp is Any:
        return {}
    if isinstance(tp, type):
        if issubclass(tp, Enum):
            return next(iter(tp)).value
        if issubclass(tp, BaseModel):
            return minimal(tp)
        if tp is bool:
            return False
        if tp is int:
            return 1
        if tp is float:
            return 1.0
        if tp is str:
            return "x"
        if tp is datetime:
            return NOW
    raise TypeError(f"no minimal value for {tp!r}")


def static(token: str | None) -> Callable[[], Any]:
    async def provider() -> str | None:
        return token

    return provider


class Recorder:
    """A MockTransport that records requests and answers with ``respond``."""

    def __init__(self, respond: Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: list[httpx.Request] = []
        self._respond = respond
        self.transport = httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)


def json_response(status: int, body: Any) -> httpx.Response:
    return httpx.Response(status, json=body)


def error(status: int, code: str, message: str = "nope", detail: Any = None) -> httpx.Response:
    return httpx.Response(status, json={"code": code, "message": message, "detail": detail})


def event(seq: int, *, extra: str = "") -> str:
    body = m.ProgressEvent.model_validate(
        {"seq": seq, "at": NOW, "source": "history", "state": "working"}
    ).model_dump_json()
    return f"id: {seq}\n{extra}data: {body}\n\n"


class Chunks(httpx.AsyncByteStream):
    """A response body delivered in the given chunks, optionally failing afterwards like a
    dropped connection."""

    def __init__(self, chunks: Iterable[bytes | str], fail: Exception | None = None) -> None:
        self._chunks = [c.encode() if isinstance(c, str) else c for c in chunks]
        self._fail = fail

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for c in self._chunks:
            yield c
        if self._fail is not None:
            raise self._fail


def sse(chunks: Iterable[bytes | str], fail: Exception | None = None) -> httpx.Response:
    return httpx.Response(
        200, headers={"content-type": "text/event-stream"}, stream=Chunks(chunks, fail)
    )


def query(request: httpx.Request) -> dict[str, str]:
    return dict(request.url.params)


def body(request: httpx.Request) -> Any:
    return json.loads(request.content) if request.content else None
