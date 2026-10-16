"""GateClient behaviour beyond the route table: error decoding, the 401 retry, the
unauthenticated presigned-URL client."""

from __future__ import annotations

import httpx
import pytest
from pydantic import BaseModel

from kontra import __version__
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m

from ._support import Recorder, error, json_response, minimal, static

# --------------------------------------------------------------------------- errors


async def test_error_body_is_decoded() -> None:
    rec = Recorder(lambda _: error(409, "price_above_max", "quote is 12.40", {"max": 1000}))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        with pytest.raises(GateError) as ei:
            await c.hire("q1", buyer_key="age1job", max_price_minor=1000)
    e = ei.value
    assert (e.status, e.code, e.message, e.detail) == (
        409,
        "price_above_max",
        "quote is 12.40",
        {"max": 1000},
    )
    assert str(e) == "409 price_above_max: quote is 12.40"


@pytest.mark.parametrize(
    ("response", "message", "detail"),
    [
        (
            httpx.Response(502, text="<html><body>Bad Gateway</body></html>"),
            "<html><body>Bad Gateway</body></html>",
            None,
        ),
        (httpx.Response(404, json={"detail": "Not Found"}), "Not Found", {"detail": "Not Found"}),
        (httpx.Response(500), "Internal Server Error", None),
        (httpx.Response(503, json=["not", "an", "object"]), "Service Unavailable", None),
        (httpx.Response(418, text="x" * 2000), "x" * 500, None),
    ],
    ids=["html", "fastapi-default", "empty", "json-array", "long-text"],
)
async def test_non_gate_errors_still_become_gate_errors(
    response: httpx.Response, message: str, detail: object
) -> None:
    rec = Recorder(lambda _: response)
    async with GateClient("http://gate.test", transport=rec.transport) as c:
        with pytest.raises(GateError) as ei:
            await c.party("acme")
    assert (ei.value.status, ei.value.code) == (response.status_code, "http_error")
    assert ei.value.message == message
    assert ei.value.detail == detail


async def test_a_2xx_that_does_not_match_the_model_is_invalid_response() -> None:
    rec = Recorder(lambda _: json_response(200, {"party": "acme", "surprise": 1}))
    async with GateClient("http://gate.test", transport=rec.transport) as c:
        with pytest.raises(GateError) as ei:
            await c.party("acme")
    assert (ei.value.status, ei.value.code) == (200, "invalid_response")
    assert "PartyView" in ei.value.message


@pytest.mark.parametrize("payload", [b"[1, 2]", b"not json"])
async def test_object_routes_need_a_json_object(payload: bytes) -> None:
    rec = Recorder(lambda _: httpx.Response(200, content=payload))
    async with GateClient("http://gate.test", transport=rec.transport) as c:
        with pytest.raises(GateError, match="invalid_response"):
            await c.jwks()


async def test_transport_errors_propagate_unchanged() -> None:
    def respond(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    async with GateClient("http://gate.test", transport=httpx.MockTransport(respond)) as c:
        with pytest.raises(httpx.ConnectError):
            await c.health()


# --------------------------------------------------------------------------- 401 retry


class Tokens:
    """A provider that hands out tok1, tok2, ... and records invalidations."""

    def __init__(self, *, same: bool = False) -> None:
        self.issued = 0
        self.same = same
        self.invalidated: list[str] = []
        self._current: str | None = None

    async def __call__(self) -> str | None:
        if self._current is None or (not self.same and self._current in self.invalidated):
            self.issued += 1
            self._current = f"tok{self.issued}"
        return self._current

    def invalidate(self, token: str) -> None:
        self.invalidated.append(token)


def _auth_gate(valid: set[str]) -> Recorder:
    def respond(r: httpx.Request) -> httpx.Response:
        if r.headers.get("authorization", "").removeprefix("Bearer ") in valid:
            return json_response(200, minimal(m.Me))
        return error(401, "unauthenticated", "token rejected")

    return Recorder(respond)


async def test_401_invalidates_refreshes_and_retries_once() -> None:
    tokens = Tokens()
    rec = _auth_gate({"tok2"})
    async with GateClient("http://gate.test", token=tokens, transport=rec.transport) as c:
        me = await c.me()
    assert isinstance(me, m.Me)
    assert tokens.invalidated == ["tok1"]
    assert [r.headers["authorization"] for r in rec.requests] == ["Bearer tok1", "Bearer tok2"]


async def test_401_with_a_plain_callable_retries_when_it_returns_a_new_token() -> None:
    seq = iter(["a", "b"])

    async def provider() -> str | None:
        return next(seq)

    rec = _auth_gate({"b"})
    async with GateClient("http://gate.test", token=provider, transport=rec.transport) as c:
        await c.me()
    assert len(rec.requests) == 2


async def test_401_is_retried_only_once() -> None:
    tokens = Tokens()
    rec = _auth_gate(set())
    async with GateClient("http://gate.test", token=tokens, transport=rec.transport) as c:
        with pytest.raises(GateError) as ei:
            await c.me()
    assert ei.value.status == 401
    assert len(rec.requests) == 2
    assert tokens.invalidated == ["tok1"]


async def test_401_with_the_same_token_again_is_not_retried() -> None:
    tokens = Tokens(same=True)
    rec = _auth_gate(set())
    async with GateClient("http://gate.test", token=tokens, transport=rec.transport) as c:
        with pytest.raises(GateError):
            await c.me()
    assert len(rec.requests) == 1
    assert tokens.invalidated == ["tok1"]


async def test_401_on_an_anonymous_route_does_not_touch_the_provider() -> None:
    async def provider() -> str | None:
        raise AssertionError("anonymous routes never ask for a token")

    rec = Recorder(lambda _: error(401, "unauthenticated"))
    async with GateClient("http://gate.test", token=provider, transport=rec.transport) as c:
        with pytest.raises(GateError):
            await c.party("acme")
    assert len(rec.requests) == 1


async def test_provider_returning_none_sends_no_authorization() -> None:
    rec = Recorder(lambda _: error(401, "unauthenticated"))
    async with GateClient("http://gate.test", token=static(None), transport=rec.transport) as c:
        with pytest.raises(GateError):
            await c.me()
    assert "authorization" not in rec.requests[0].headers
    assert len(rec.requests) == 1


# --------------------------------------------------------------------------- http


async def test_http_client_shares_the_transport_but_not_the_credential() -> None:
    rec = Recorder(lambda _: httpx.Response(200, content=b"ciphertext"))
    async with GateClient("http://gate.test", token=static("secret"), transport=rec.transport) as c:
        assert c.http is c.http
        r = await c.http.get("https://bucket.test/obj?X-Amz-Signature=abc")
        assert r.content == b"ciphertext"
        http = c.http
    assert http.is_closed
    req = rec.requests[0]
    assert req.url.host == "bucket.test"
    assert "authorization" not in req.headers
    assert req.headers["user-agent"] == f"kontra-sdk/{__version__}"


async def test_user_agent() -> None:
    rec = Recorder(lambda _: json_response(200, {"ok": True}))
    async with GateClient(
        "http://gate.test", transport=rec.transport, user_agent="kontra-cli/9"
    ) as c:
        assert await c.health() == {"ok": True}
    assert rec.requests[0].headers["user-agent"] == "kontra-cli/9"


# --------------------------------------------------------------------------- confirm it's you

NEED = {"confirm_with": "email", "confirm_ttl_seconds": 3600}
JOB = "0123456789abcdef0123456789abcdef"


class Confirms:
    """A confirm hook that records what it was asked and answers ``answer``."""

    def __init__(self, answer: bool = True, raises: Exception | None = None) -> None:
        self.asked: list[tuple[str, m.ConfirmationRequired]] = []
        self._answer, self._raises = answer, raises

    async def __call__(self, route: str, need: m.ConfirmationRequired) -> bool:
        self.asked.append((route, need))
        if self._raises is not None:
            raise self._raises
        return self._answer


#: What each route's success answers, for :func:`_refuses_first`.
_ANSWERS: dict[tuple[str, str], type[BaseModel] | None] = {
    ("POST", "/v1/keys"): m.ApiKeyCreated,
    ("GET", "/v1/keys"): m.ApiKeyList,
    ("DELETE", "/v1/keys/key_0123456789abcdef"): None,
    ("GET", "/v1/me"): m.Me,
    ("POST", "/v1/catalog"): m.CatalogEntry,
    ("POST", f"/v1/admin/jobs/{JOB}/resolve"): m.JobView,
    ("POST", "/v1/passkeys/options"): m.PasskeyCeremony,
    ("GET", "/v1/passkeys"): m.PasskeyList,
    ("POST", "/v1/auth/confirm/passkey/options"): m.PasskeyCeremony,
}


def _refuses_first(
    code: str = "confirmation_required", detail: object = NEED, times: int = 1
) -> Recorder:
    """A gate that answers the first ``times`` requests 403 ``code`` and then succeeds."""
    seen: list[httpx.Request] = []

    def respond(r: httpx.Request) -> httpx.Response:
        seen.append(r)
        if len(seen) <= times:
            return error(403, code, "confirm it's you", detail)
        model = _ANSWERS[(r.method, r.url.path)]
        return httpx.Response(204) if model is None else json_response(200, minimal(model))

    return Recorder(respond)


async def test_a_change_is_sent_once_more_after_the_hook_confirms() -> None:
    rec = _refuses_first()
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        made = await c.create_api_key("ci")
    assert isinstance(made, m.ApiKeyCreated)
    assert [(route, need.confirm_with) for route, need in hook.asked] == [
        ("create_api_key", "email")
    ]
    assert [(r.method, r.url.path) for r in rec.requests] == [("POST", "/v1/keys")] * 2
    assert rec.requests[0].content == rec.requests[1].content == b'{"label":"ci"}'


async def test_a_resolver_change_is_confirmable_too() -> None:
    rec = _refuses_first()
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        await c.resolve(JOB, m.ResolveRequest(buyer_minor=1, seller_minor=2))
    assert [route for route, _ in hook.asked] == ["resolve"]
    assert len(rec.requests) == 2


async def test_adding_a_passkey_is_confirmable_once() -> None:
    """``passkey_options`` is ``recent`` (decisions I24): the hook is asked, once, and the
    request is sent once more; a second refusal is the gate's answer, never a loop."""
    rec = _refuses_first()
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        assert isinstance(await c.passkey_options(), m.PasskeyCeremony)
    assert [route for route, _ in hook.asked] == ["passkey_options"]
    assert [(r.method, r.url.path) for r in rec.requests] == [("POST", "/v1/passkeys/options")] * 2

    rec, hook = _refuses_first(times=2), Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        with pytest.raises(GateError) as ei:
            await c.passkey_options()
    assert ei.value.code == "confirmation_required"
    assert (len(hook.asked), len(rec.requests)) == (1, 2)


async def test_a_bodiless_change_is_sent_again_too() -> None:
    rec = _refuses_first()
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        await c.revoke_api_key("key_0123456789abcdef")
    assert [r.url.path for r in rec.requests] == ["/v1/keys/key_0123456789abcdef"] * 2


@pytest.mark.parametrize(
    ("call", "code", "detail"),
    [
        # a read (human) or a party route: never confirmed, whatever the gate says
        ("api_keys", "confirmation_required", NEED),
        ("me", "confirmation_required", NEED),
        ("passkeys", "confirmation_required", NEED),
        ("confirm_passkey_options", "confirmation_required", NEED),
        # another code on a change
        ("create_api_key", "fresh_login_required", None),
        ("create_api_key", "stale_session", {"max_age_seconds": 600}),
        ("create_api_key", "not_resolver", None),
        # a detail that does not say how to confirm
        ("create_api_key", "confirmation_required", None),
        ("create_api_key", "confirmation_required", {"confirm_with": "carrier pigeon"}),
    ],
)
async def test_the_hook_is_asked_only_for_a_changes_confirmation(
    call: str, code: str, detail: object
) -> None:
    rec = _refuses_first(code, detail)
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        with pytest.raises(GateError) as ei:
            await (c.create_api_key("ci") if call == "create_api_key" else getattr(c, call)())
    assert (ei.value.status, ei.value.code) == (403, code)
    assert hook.asked == []
    assert len(rec.requests) == 1


async def test_a_hook_that_does_not_confirm_leaves_the_gates_answer() -> None:
    rec = _refuses_first()
    hook = Confirms(answer=False)
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        with pytest.raises(GateError) as ei:
            await c.create_api_key("ci")
    assert ei.value.code == "confirmation_required"
    assert ei.value.detail == NEED
    assert len(hook.asked) == 1
    assert len(rec.requests) == 1


async def test_a_second_refusal_is_raised_not_looped() -> None:
    rec = _refuses_first(times=5)
    hook = Confirms()
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        with pytest.raises(GateError) as ei:
            await c.publish({"nexusrpc": "1.0.0"})
    assert ei.value.code == "confirmation_required"
    assert len(hook.asked) == 1
    assert len(rec.requests) == 2


async def test_what_the_hook_raises_is_what_the_call_raises() -> None:
    rec = _refuses_first()
    hook = Confirms(raises=RuntimeError("the person pressed Ctrl-C"))
    async with GateClient(
        "http://gate.test", token=static("t"), transport=rec.transport, confirm=hook
    ) as c:
        with pytest.raises(RuntimeError, match="Ctrl-C"):
            await c.create_api_key("ci")
    assert len(rec.requests) == 1


async def test_without_a_hook_the_gates_answer_stands() -> None:
    rec = _refuses_first()
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        with pytest.raises(GateError) as ei:
            await c.create_api_key("ci")
    assert ei.value.code == "confirmation_required"
    assert len(rec.requests) == 1
