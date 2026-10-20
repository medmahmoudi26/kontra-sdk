"""A job's stream (decisions I31): the SSE decoder, and GateClient.stream() -- its query,
the events it reads, reconnection from the last cursor, cursor dedupe, and its bounds."""

from __future__ import annotations

import json

import httpx
import pytest

import kontra.gate_client as gc
from kontra.gate_client import GateClient, GateError, ServerSentEvent, SSEDecoder
from kontra.gate_client import models as m

from ._support import NOW, Recorder, error, event, query, sse, static

JOB = "0123456789abcdef0123456789abcdef"


def decode(*chunks: bytes | str) -> list[ServerSentEvent]:
    d = SSEDecoder()
    out: list[ServerSentEvent] = []
    for c in chunks:
        out += d.feed(c.encode() if isinstance(c, str) else c)
    return out


# --------------------------------------------------------------------------- decoder


def test_multi_line_data_is_joined_with_newlines() -> None:
    assert decode('data: {"a":\ndata:  1}\n\n') == [ServerSentEvent("message", '{"a":\n 1}')]


def test_comments_and_keepalives_are_ignored() -> None:
    assert decode(": keepalive\n\n:\n", "data: x\n: in the middle\ndata: y\n\n") == [
        ServerSentEvent("message", "x\ny")
    ]


@pytest.mark.parametrize("eol", ["\n", "\r\n", "\r"])
def test_every_line_ending(eol: str) -> None:
    text = f"event: progress{eol}id: 7{eol}data: a{eol}data: b{eol}{eol}"
    assert decode(text) == [ServerSentEvent("progress", "a\nb", "7")]


def test_crlf_split_across_chunks_is_one_line_break() -> None:
    assert decode("data: a\r", "\ndata: b\r", "\n\r", "\n") == [ServerSentEvent("message", "a\nb")]


def test_byte_by_byte_including_multibyte_utf8() -> None:
    raw = "data: héllo €\n\n".encode()
    assert decode(*[raw[i : i + 1] for i in range(len(raw))]) == [
        ServerSentEvent("message", "héllo €")
    ]


def test_unicode_line_separators_inside_data_do_not_split() -> None:
    value = json.dumps({"m": "a\u2028b\u2029c\x0cd\x85e"}, ensure_ascii=False)
    assert decode(f"data: {value}\n\n") == [ServerSentEvent("message", value)]


def test_field_parsing_rules() -> None:
    assert decode(
        "\ufeffdata\n\n",  # BOM stripped; a field with no colon has an empty value
        "data:no-space\n\n",
        "data:  two spaces\n\n",
        "event: e\n\n",  # no data: not dispatched, and the event type resets
        "data: z\n\n",
        "id: a\0b\ndata: n\n\n",  # an id containing NUL is ignored
        "retry: 10\nunknown: q\ndata: r\n\n",
    ) == [
        ServerSentEvent("message", ""),
        ServerSentEvent("message", "no-space"),
        ServerSentEvent("message", " two spaces"),
        ServerSentEvent("message", "z"),
        ServerSentEvent("message", "n"),
        ServerSentEvent("message", "r"),
    ]


def test_last_event_id_persists_and_unfinished_event_is_dropped() -> None:
    assert decode("id: 1\ndata: a\n\ndata: b\n\n", "data: never finished\n") == [
        ServerSentEvent("message", "a", "1"),
        ServerSentEvent("message", "b", "1"),
    ]


# --------------------------------------------------------------------------- the stream


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded.append(delay)

    monkeypatch.setattr(gc, "_sleep", fake_sleep)
    return recorded


def _client(rec: Recorder) -> GateClient:
    c = GateClient("http://gate.test", token=static("t"), transport=rec.transport)
    c.reconnect_delays = (0.1, 0.2, 0.3)
    return c


async def test_events_keepalives_and_multi_line_data(sleeps: list[float]) -> None:
    body = m.StreamEvent.model_validate(
        {
            "cursor": 2,
            "kind": "item",
            "at": NOW,
            "topic": "turn_events",
            "offset": 7,
            "ciphertext_b64": "QQ",
        }
    ).model_dump_json(indent=1)
    multi = "event: item\n" + "".join(f"data: {line}\n" for line in body.splitlines()) + "\n"
    gap = m.StreamEvent.model_validate(
        {"cursor": 3, "kind": "gap", "at": NOW, "topic": "t", "offset": 8, "size": 9, "reason": "too_large"}
    ).model_dump_json()  # fmt: skip
    rec = Recorder(
        lambda _: sse(
            [
                ": hello\n\n",
                event(1),
                ": keepalive\n\n",
                multi,
                f"id: 3\nevent: gap\ndata: {gap}\n\n",
                "event: end\ndata: {}\n\n",
            ]
        )
    )
    async with _client(rec) as c:
        got = [e async for e in c.stream(JOB)]
    assert [(e.cursor, e.kind, e.topic, e.offset) for e in got] == [
        (1, m.StreamEventKind.HISTORY, None, None),
        (2, m.StreamEventKind.ITEM, "turn_events", 7),
        (3, m.StreamEventKind.GAP, "t", 8),
    ]
    assert len(rec.requests) == 1
    assert rec.requests[0].url.path == f"/v1/jobs/{JOB}/stream"
    assert rec.requests[0].headers["accept"] == "text/event-stream"
    assert query(rec.requests[0]) == {"after": "0"}
    assert sleeps == []


async def test_topics_and_history_are_asked_for(sleeps: list[float]) -> None:
    rec = Recorder(lambda _: sse([event(1)]))
    async with _client(rec) as c:
        [e async for e in c.stream(JOB, topics=["turn_events", "kontra.progress"], history=True)]
    [r] = rec.requests
    assert r.url.params.get_list("topic") == ["turn_events", "kontra.progress"]
    assert r.url.params["history"] == "true"
    with pytest.raises(TypeError):
        c.stream(JOB, topics="turn_events")


async def test_reconnects_from_the_last_cursor_after_a_dropped_connection(
    sleeps: list[float],
) -> None:
    def respond(r: httpx.Request) -> httpx.Response:
        after = int(r.url.params["after"])
        if after == 0:
            return sse([event(1), event(2)], fail=httpx.ReadError("connection reset"))
        # the replay overlaps what was already delivered; it must not be yielded twice
        return sse([event(2), event(3)])

    rec = Recorder(respond)
    async with _client(rec) as c:
        got = [e.cursor async for e in c.stream(JOB)]
    assert got == [1, 2, 3]
    assert [query(r)["after"] for r in rec.requests] == ["0", "2"]
    assert sleeps == [0.1]


async def test_starts_after_the_given_cursor(sleeps: list[float]) -> None:
    rec = Recorder(lambda _: sse([event(5), event(6)]))
    async with _client(rec) as c:
        got = [e.cursor async for e in c.stream(JOB, after=5)]
    assert got == [6]
    assert query(rec.requests[0]) == {"after": "5"}


async def test_backoff_is_bounded(sleeps: list[float]) -> None:
    def respond(r: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=r)

    rec = Recorder(respond)
    async with _client(rec) as c:
        with pytest.raises(httpx.ConnectError):
            [e async for e in c.stream(JOB)]
    assert sleeps == [0.1, 0.2, 0.3]
    assert len(rec.requests) == 4


async def test_a_successful_connect_resets_the_backoff(sleeps: list[float]) -> None:
    script = iter(
        [
            "refuse",
            sse([event(1)], fail=httpx.RemoteProtocolError("incomplete chunked read")),
            "refuse",
            "refuse",
            sse([event(2)]),
        ]
    )

    def respond(r: httpx.Request) -> httpx.Response:
        step = next(script)
        if step == "refuse":
            raise httpx.ConnectError("refused", request=r)
        assert isinstance(step, httpx.Response)
        return step

    rec = Recorder(respond)
    async with _client(rec) as c:
        got = [e.cursor async for e in c.stream(JOB)]
    assert got == [1, 2]
    assert sleeps == [0.1, 0.1, 0.2, 0.3]


async def test_gateway_errors_are_retried_other_errors_are_not(sleeps: list[float]) -> None:
    script = iter(
        [error(503, "unavailable"), httpx.Response(502, text="bad gateway"), sse([event(1)])]
    )
    rec = Recorder(lambda _: next(script))
    async with _client(rec) as c:
        assert [e.cursor async for e in c.stream(JOB)] == [1]
    assert sleeps == [0.1, 0.2]

    rec = Recorder(lambda _: error(404, "not_found", "no such job"))
    async with _client(rec) as c:
        with pytest.raises(GateError) as ei:
            [e async for e in c.stream(JOB)]
    assert (ei.value.status, ei.value.code) == (404, "not_found")
    assert len(rec.requests) == 1


async def test_persistent_gateway_errors_raise_the_last_one(sleeps: list[float]) -> None:
    rec = Recorder(lambda _: error(503, "unavailable", "restarting"))
    async with _client(rec) as c:
        with pytest.raises(GateError) as ei:
            [e async for e in c.stream(JOB)]
    assert ei.value.code == "unavailable"
    assert len(rec.requests) == 4


async def test_401_on_the_stream_refreshes_the_token(sleeps: list[float]) -> None:
    tokens = iter(["old", "new"])

    async def provider() -> str | None:
        return next(tokens)

    def respond(r: httpx.Request) -> httpx.Response:
        if r.headers["authorization"] == "Bearer old":
            return error(401, "unauthenticated")
        return sse([event(1)])

    rec = Recorder(respond)
    c = GateClient("http://gate.test", token=provider, transport=rec.transport)
    async with c:
        assert [e.cursor async for e in c.stream(JOB)] == [1]
    assert len(rec.requests) == 2
    assert sleeps == []


async def test_a_malformed_event_is_invalid_response(sleeps: list[float]) -> None:
    rec = Recorder(lambda _: sse(['data: {"cursor": "x"}\n\n']))
    async with _client(rec) as c:
        with pytest.raises(GateError, match="invalid_response"):
            [e async for e in c.stream(JOB)]


async def test_breaking_out_early_closes_the_response(sleeps: list[float]) -> None:
    closed: list[bool] = []

    class Tracked(httpx.AsyncByteStream):
        async def __aiter__(self):  # type: ignore[override]
            yield event(1).encode()
            yield event(2).encode()

        async def aclose(self) -> None:
            closed.append(True)

    rec = Recorder(lambda _: httpx.Response(200, stream=Tracked()))
    async with _client(rec) as c:
        it = c.stream(JOB)
        async for e in it:
            assert e.cursor == 1
            break
        await it.aclose()  # type: ignore[attr-defined]
    assert closed == [True]


async def test_an_event_breaking_its_kinds_rules_is_invalid_response(sleeps: list[float]) -> None:
    bad = '{"cursor": 1, "kind": "item", "at": "2026-01-01T00:00:00Z", "topic": "t"}'
    rec = Recorder(lambda _: sse([f"event: item\ndata: {bad}\n\n"]))
    async with _client(rec) as c:
        with pytest.raises(GateError, match="not a StreamEvent"):
            [e async for e in c.stream(JOB)]


async def test_a_resumed_stream_carries_the_cursor_and_retries_the_topics(
    sleeps: list[float],
) -> None:
    def respond(r: httpx.Request) -> httpx.Response:
        if r.url.params["after"] == "0":
            return sse([event(1)], fail=httpx.ReadError("reset"))
        return sse([event(2)])

    rec = Recorder(respond)
    async with _client(rec) as c:
        assert [e.cursor async for e in c.stream(JOB, topics=["a"])] == [1, 2]
    assert [r.url.params.get_list("topic") for r in rec.requests] == [["a"], ["a"]]
    assert [r.url.params["after"] for r in rec.requests] == ["0", "1"]


# --------------------------------------------------------------------------- bounds


def test_decoder_bounds_an_unterminated_line() -> None:
    d = SSEDecoder(max_size=64)
    d.feed(b"data: " + b"x" * 50)
    with pytest.raises(ValueError, match="64"):
        d.feed(b"x" * 20)


def test_decoder_bounds_an_event_that_never_ends() -> None:
    d = SSEDecoder(max_size=64)
    for _ in range(5):  # 11 characters of data each (value + the joining newline)
        d.feed(b"data: 0123456789\n")
    with pytest.raises(ValueError, match="64"):
        d.feed(b"data: 0123456789\n")


def test_decoder_size_bound_is_per_event() -> None:
    d = SSEDecoder(max_size=64)
    for _ in range(100):
        assert d.feed(b"data: 0123456789\n\n") == [ServerSentEvent("message", "0123456789")]


async def test_an_oversized_event_is_invalid_response_not_unbounded_memory(
    sleeps: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gc, "_SSE_MAX", 1024)
    rec = Recorder(lambda _: sse([event(1), "data: " + "x" * 600, "x" * 600]))
    got: list[int] = []

    async def follow(c: GateClient) -> None:
        async for e in c.stream(JOB):
            got.append(e.cursor)

    async with _client(rec) as c:
        with pytest.raises(GateError) as ei:
            await follow(c)
    assert got == [1]
    assert (ei.value.status, ei.value.code) == (200, "invalid_response")
    assert len(rec.requests) == 1  # not retried: the server is misbehaving, not away
