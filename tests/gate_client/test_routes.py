"""Seam ②, client side: every row of ROUTES has a GateClient method that sends exactly
that HTTP method and path, carries the bearer token exactly when the route is
authenticated, sends a body the route's request model accepts, and decodes the route's
response model."""

from __future__ import annotations

import re
import tomllib
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from kontra.contract.names import NameError_
from kontra.gate_client import GateClient
from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME, ROUTES, Route

from ._support import Recorder, event, json_response, minimal, query, sse, static

JOB = "0123456789abcdef0123456789abcdef"
KEY_ID = "key_0123456789abcdef"
EP = "k" + "a" * 32
SHA = "0" * 64
WP = m.WirePayload(b64="AAAA")
AUTHENTICATED = {
    "party",
    "human",
    "fresh",
    "recent",
    "worker",
    "party|worker",
    "resolver",
    "resolver_human",
}
AGE = "age1" + "q" * 58
PROOF = "p" * 43
CONFIRM_ID = "cfm_" + "0" * 32
CEREMONY_ID = "wac_" + "0" * 32
PASSKEY_ID = "pky_" + "0" * 32


async def _drain(it: Any) -> list[Any]:
    return [x async for x in it]


Call = Callable[[GateClient], Awaitable[Any]]

#: route name -> (how to call it, the path parameters that call must produce)
CALLS: dict[str, tuple[Call, dict[str, str]]] = {
    "jwks": (lambda c: c.jwks(), {}),
    "info": (lambda c: c.info(), {}),
    "revocations": (lambda c: c.revocations(), {}),
    "device_start": (lambda c: c.device_start(), {}),
    "token": (
        lambda c: c.token(m.TokenRequest(grant_type="refresh_token", refresh_token="r")),
        {},
    ),
    "magic_link": (lambda c: c.magic_link("alice@example.com", "BCDF-GHJK"), {}),
    "dev_login": (lambda c: c.dev_login("alice"), {}),
    "revoke": (lambda c: c.revoke(), {}),
    "temporal_token": (lambda c: c.temporal_token(), {}),
    "me": (lambda c: c.me(), {}),
    "add_recipient": (lambda c: c.add_recipient("laptop", "age1abc"), {}),
    "revoke_recipient": (lambda c: c.revoke_recipient("age1abc"), {"public_key": "age1abc"}),
    "party": (lambda c: c.party("acme"), {"party": "acme"}),
    "create_api_key": (lambda c: c.create_api_key("ci"), {}),
    "api_keys": (lambda c: c.api_keys(), {}),
    "revoke_api_key": (lambda c: c.revoke_api_key(KEY_ID), {"key_id": KEY_ID}),
    "sessions": (lambda c: c.sessions(), {}),
    "end_session": (lambda c: c.end_session(JOB), {"session_id": JOB}),
    "confirm_start": (lambda c: c.confirm_start(), {}),
    "confirm_status": (lambda c: c.confirm_status(CONFIRM_ID), {"confirm_id": CONFIRM_ID}),
    "passkeys": (lambda c: c.passkeys(), {}),
    "passkey_options": (lambda c: c.passkey_options(), {}),
    "add_passkey": (lambda c: c.add_passkey(CEREMONY_ID, "laptop", {"id": "x"}), {}),
    "revoke_passkey": (lambda c: c.revoke_passkey(PASSKEY_ID), {"passkey_id": PASSKEY_ID}),
    "confirm_passkey_options": (lambda c: c.confirm_passkey_options(), {}),
    "confirm_passkey": (lambda c: c.confirm_passkey(CEREMONY_ID, {"id": "x"}), {}),
    "devices": (lambda c: c.devices(), {}),
    "add_device": (lambda c: c.add_device("laptop", AGE, "cli"), {}),
    "remove_device": (lambda c: c.remove_device(AGE), {"public_key": AGE}),
    "publish": (lambda c: c.publish({"nexusrpc": "1.0.0"}, streams=["turn_events"]), {}),
    "search": (lambda c: c.search("enrich"), {}),
    "get_offering": (
        lambda c: c.get_offering("acme/enrich@1.2.0"),
        {"party": "acme", "name": "enrich", "version": "1.2.0"},
    ),
    "card": (
        lambda c: c.card("acme/enrich"),
        {"party": "acme", "name": "enrich", "version": "latest"},
    ),
    "by_endpoint": (lambda c: c.by_endpoint(EP), {"endpoint": EP}),
    "workers": (
        lambda c: c.workers("acme/enrich@1.2.0"),
        {"party": "acme", "name": "enrich", "version": "1.2.0"},
    ),
    "quote": (lambda c: c.quote("acme/enrich", WP), {}),
    "hire": (lambda c: c.hire("q1", buyer_key="age1job", max_price_minor=2000), {}),
    "jobs": (lambda c: c.jobs(), {}),
    "job": (lambda c: c.job(JOB), {"job_id": JOB}),
    "seller_job": (lambda c: c.seller_job(JOB), {"job_id": JOB}),
    "accept": (lambda c: c.accept(JOB), {"job_id": JOB}),
    "reject": (lambda c: c.reject(JOB, "empty records"), {"job_id": JOB}),
    "cancel": (lambda c: c.cancel(JOB), {"job_id": JOB}),
    "start": (lambda c: c.start(JOB, WP), {"job_id": JOB}),
    "result": (lambda c: c.result(JOB), {"job_id": JOB}),
    "job_key": (lambda c: c.job_key(JOB), {"job_id": JOB}),
    "job_keys": (lambda c: c.job_keys(AGE), {}),
    "share_job_keys": (
        lambda c: c.share_job_keys(
            [m.JobKeyShare(job_id=JOB, proof=PROOF, wraps=[m.WrapIn(recipient=AGE, wrap="AAAA")])]
        ),
        {},
    ),
    "stream": (lambda c: _drain(c.stream(JOB, topics=["turn_events"])), {"job_id": JOB}),
    "create_artifact": (lambda c: c.create_artifact(m.ArtifactCreate(size=10)), {}),
    "complete_artifact": (
        lambda c: c.complete_artifact(
            "0123456789abcdef0123456789abcdef",
            m.ArtifactComplete(upload_id=None, etags=["e"], ciphertext_sha256=SHA),
        ),
        {"key": "0123456789abcdef0123456789abcdef"},
    ),
    "download_artifact": (
        lambda c: c.download_artifact("0123456789abcdef0123456789abcdef"),
        {"key": "0123456789abcdef0123456789abcdef"},
    ),
    "create_blob": (lambda c: c.create_blob(m.BlobCreate(sha256=SHA, size=1)), {}),
    "get_blob": (lambda c: c.get_blob(SHA), {"sha256": SHA}),
    "balance": (lambda c: c.balance(), {}),
    "topup": (lambda c: c.topup(1000), {}),
    "disputes": (lambda c: c.disputes(), {}),
    "resolve": (
        lambda c: c.resolve(JOB, m.ResolveRequest(buyer_minor=1, seller_minor=2)),
        {"job_id": JOB},
    ),
    "health": (lambda c: c.health(), {}),
}


def _answer(route: Route) -> Callable[[httpx.Request], httpx.Response]:
    def respond(_: httpx.Request) -> httpx.Response:
        if route.stream:
            return sse([": keepalive\n\n", event(1)])
        if route.response is None:
            return json_response(200, {})
        return json_response(200, minimal(route.response))

    return respond


def test_every_route_has_a_call_and_a_method() -> None:
    names = {r.name for r in ROUTES}
    assert set(CALLS) == names
    methods = {
        n
        for n, v in vars(GateClient).items()
        if not n.startswith("_") and callable(v) and n != "aclose"
    }
    assert methods == names, "GateClient's public methods and ROUTES must match one to one"


def test_minimal_responses_are_valid() -> None:
    for r in ROUTES:
        if r.response is not None:
            r.response.model_validate(minimal(r.response))


@pytest.mark.parametrize("route", ROUTES, ids=lambda r: r.name)
async def test_route_conformance(route: Route) -> None:
    call, params = CALLS[route.name]
    rec = Recorder(_answer(route))
    async with GateClient("http://gate.test", token=static("tok"), transport=rec.transport) as c:
        result = await call(c)

    assert len(rec.requests) == 1
    req = rec.requests[0]
    assert req.method == route.method
    assert req.url.raw_path.split(b"?")[0].decode() == route.path.format(**params)
    if route.auth in AUTHENTICATED:
        assert req.headers["authorization"] == "Bearer tok"
    else:
        assert "authorization" not in req.headers
    if route.request is not None:
        route.request.model_validate_json(req.content)
    else:
        assert req.content == b""
    if route.stream:
        assert req.headers["accept"] == "text/event-stream"
        assert [e.cursor for e in result] == [1]
    elif route.response is not None:
        assert isinstance(result, route.response)


async def test_none_response_routes_accept_204() -> None:
    rec = Recorder(lambda _: httpx.Response(204))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        assert await c.revoke() is None
        assert (
            await c.complete_artifact(
                "k", m.ArtifactComplete(upload_id="u", etags=[], ciphertext_sha256=SHA)
            )
            is None
        )
    assert [r.url.path for r in rec.requests] == ["/v1/auth/revoke", "/v1/artifacts/k/complete"]


# --------------------------------------------------------------------------- paths


@pytest.mark.parametrize(
    ("job_id", "raw"),
    [
        ("a/b", "a%2Fb"),
        ("a?b#c", "a%3Fb%23c"),
        ("..", "%2E%2E"),
        (".", "%2E"),
        ("x y%", "x%20y%25"),
        ("é", "%C3%A9"),
    ],
)
async def test_path_parameters_are_one_quoted_segment(job_id: str, raw: str) -> None:
    rec = Recorder(lambda _: json_response(200, minimal(m.JobView)))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.accept(job_id)
    assert rec.requests[0].url.raw_path == f"/v1/jobs/{raw}/accept".encode()


async def test_empty_path_parameter_is_refused_before_sending() -> None:
    rec = Recorder(lambda _: json_response(200, {}))
    async with GateClient("http://gate.test", transport=rec.transport) as c:
        with pytest.raises(ValueError, match="empty"):
            await c.job("")
        with pytest.raises(ValueError, match="empty"):
            await _drain(c.stream(""))
    assert rec.requests == []


async def test_base_url_path_prefix_is_kept() -> None:
    rec = Recorder(lambda _: json_response(200, minimal(m.Me)))
    async with GateClient("https://h.test/gate/", token=static("t"), transport=rec.transport) as c:
        assert c.base_url == "https://h.test/gate"
        await c.me()
    assert str(rec.requests[0].url) == "https://h.test/gate/v1/me"


async def test_workers_takes_an_offering_ref_as_the_catalog_does() -> None:
    """An offering name is two path segments (party, name), and the version resolves as
    a catalog path's: ``latest``, partial or exact."""
    rec = Recorder(lambda _: json_response(200, minimal(m.WorkersView)))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.workers("acme/enrich")
        await c.workers("acme/enrich@1")
        await c.workers("acme/enrich@1.2.0")
        with pytest.raises(NameError_):
            await c.workers("acme/enrich/1.2.0")
    assert [r.url.raw_path.decode() for r in rec.requests] == [
        "/v1/offerings/acme/enrich/versions/latest/workers",
        "/v1/offerings/acme/enrich/versions/1/workers",
        "/v1/offerings/acme/enrich/versions/1.2.0/workers",
    ]


@pytest.mark.parametrize(
    ("ref", "path"),
    [
        ("acme/enrich", "/v1/catalog/acme/enrich/latest"),
        ("acme/enrich@latest", "/v1/catalog/acme/enrich/latest"),
        ("acme/enrich@1", "/v1/catalog/acme/enrich/1"),
        ("acme/enrich@1.2", "/v1/catalog/acme/enrich/1.2"),
        ("acme/enrich@1.2.0", "/v1/catalog/acme/enrich/1.2.0"),
    ],
)
async def test_offering_ref_maps_to_catalog_path(ref: str, path: str) -> None:
    rec = Recorder(
        lambda r: json_response(
            200, {} if r.url.path.endswith("/card") else minimal(m.CatalogEntry)
        )
    )
    async with GateClient("http://gate.test", transport=rec.transport) as c:
        await c.get_offering(ref)
        await c.card(ref)
    assert [r.url.path for r in rec.requests] == [path, path + "/card"]


@pytest.mark.parametrize(
    "ref",
    ["acme/enrich\n", "Acme/enrich", "acme/enrich@1.2.3.4", "../x", "acme/enrich@", "acme"],
)
async def test_bad_offering_refs_never_reach_the_wire(ref: str) -> None:
    rec = Recorder(lambda _: json_response(200, {}))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        with pytest.raises(NameError_):
            await c.get_offering(ref)
        with pytest.raises(NameError_):
            await c.card(ref)
        with pytest.raises(NameError_):
            await c.quote(ref, WP)
    assert rec.requests == []


# --------------------------------------------------------------------------- queries


async def test_query_parameters() -> None:
    def respond(r: httpx.Request) -> httpx.Response:
        model = BY_NAME[
            {"/v1/catalog": "search", "/v1/jobs": "jobs", "/v1/balance": "balance"}[r.url.path]
        ].response
        assert model is not None
        return json_response(200, minimal(model))

    rec = Recorder(respond)
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.search()
        await c.search("enrich", cursor="c1", limit=5)
        await c.jobs()
        await c.jobs(as_="seller", state="working", cursor="c2", limit=7)
        await c.balance("USDC")
    assert [query(r) for r in rec.requests] == [
        {"limit": "20"},
        {"q": "enrich", "cursor": "c1", "limit": "5"},
        {"as": "buyer", "limit": "50"},
        {"as": "seller", "state": "working", "cursor": "c2", "limit": "7"},
        {"currency": "USDC"},
    ]


async def test_request_bodies() -> None:
    rec = Recorder(
        lambda r: json_response(
            200,
            minimal(m.HireView)
            if r.url.path == "/v1/jobs"
            else minimal(m.TemporalToken)
            if r.url.path.endswith("temporal-token")
            else minimal(m.QuoteView),
        )
    )
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.quote("acme/enrich@1", WP)
        await c.hire("q1", buyer_key="age1job")
        await c.temporal_token()
    from ._support import body

    assert [body(r) for r in rec.requests] == [
        {"offering": "acme/enrich@1", "input": {"b64": "AAAA"}},
        {"quote_id": "q1", "max_price_minor": None, "buyer_key": "age1job"},
        {"role": "worker"},
    ]
    assert all(r.headers["content-type"] == "application/json" for r in rec.requests)


async def test_temporal_token_role_is_checked_locally() -> None:
    rec = Recorder(lambda _: json_response(200, {}))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        with pytest.raises(ValueError, match="worker"):
            await c.temporal_token("publisher")
    assert rec.requests == []


async def test_job_key_queries() -> None:
    """``for`` (a Python keyword) is sent as ``for``; ``lacking`` always, the rest when given."""

    def respond(r: httpx.Request) -> httpx.Response:
        model = m.JobKeyList if r.url.path == "/v1/me/job-keys" else m.JobKey
        return json_response(200, minimal(model))

    rec = Recorder(respond)
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.job_key(JOB)
        await c.job_key(JOB, for_=AGE)
        await c.job_keys(AGE)
        await c.job_keys(AGE, for_=AGE, cursor=JOB, limit=7)
    assert [query(r) for r in rec.requests] == [
        {},
        {"for": AGE},
        {"lacking": AGE, "limit": "100"},
        {"lacking": AGE, "for": AGE, "cursor": JOB, "limit": "7"},
    ]


async def test_disputes_can_be_paged() -> None:
    """The gate pages the resolver's queue (cursor/limit, JobList.next_cursor); without
    them a resolver could never see past the first 50 disputes."""
    rec = Recorder(lambda _: json_response(200, minimal(m.JobList)))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.disputes()
        await c.disputes(cursor="c3", limit=10)
    assert [query(r) for r in rec.requests] == [{"limit": "50"}, {"cursor": "c3", "limit": "10"}]


def test_the_stream_replaces_the_progress_routes() -> None:
    """#52 (decisions I31): the job's stream is ``GET /v1/jobs/{id}/stream`` (SSE of
    StreamEvent, the job's parties), and the bespoke progress channel is gone, both verbs."""
    from kontra.gate_client.routes import BY_NAME, ROUTES

    stream = BY_NAME["stream"]
    assert (stream.method, stream.path, stream.auth) == ("GET", "/v1/jobs/{job_id}/stream", "party")
    assert stream.stream is True
    assert stream.response is m.StreamEvent
    assert "progress" not in BY_NAME
    assert "post_progress" not in BY_NAME
    assert not [r for r in ROUTES if r.path.endswith("/progress")]
    assert not hasattr(m, "ProgressEvent")
    assert not hasattr(m, "ProgressPost")


# --------------------------------------------------------------------------- the engine (seam ⑥)


def test_publish_request_engine_defaults_to_temporal_and_names_only_engines() -> None:
    """``PublishRequest.engine`` (decisions I32): what the version runs on, beside the
    document. Absent -- every client before #55 -- it is ``temporal``; a name that is no
    engine is refused by the model, before any gate code runs."""
    doc = {"nexusrpc": "1.0.0"}
    assert m.PublishRequest(document=doc).engine == "temporal"
    assert m.PublishRequest.model_validate({"document": doc}).engine == "temporal"
    assert m.PublishRequest.model_validate({"document": doc, "engine": "temporal"}).engine == (
        "temporal"
    )
    for bad in ("golem", "Temporal", "", None, 1):
        with pytest.raises(ValueError, match="engine"):
            m.PublishRequest.model_validate({"document": doc, "engine": bad})
    assert m.ENGINE_NAMES == ("temporal",)
    assert m.DEFAULT_ENGINE == "temporal"
    schema = m.PublishRequest.model_json_schema()["properties"]["engine"]
    assert (schema.get("const") or schema["enum"][0], schema["default"]) == (
        "temporal",
        "temporal",
    )


async def test_the_client_omits_the_default_engine() -> None:
    """Owner decision OD11 of #55: a client of this version sends ``engine`` only when it is
    not the default, so it still publishes to a gate that predates the field (which refuses
    an unknown member)."""
    rec = Recorder(lambda _: json_response(200, minimal(m.CatalogEntry)))
    async with GateClient("http://gate.test", token=static("t"), transport=rec.transport) as c:
        await c.publish({"nexusrpc": "1.0.0"})
        await c.publish({"nexusrpc": "1.0.0"}, engine="temporal")
    from ._support import body

    assert [body(r) for r in rec.requests] == [
        {"document": {"nexusrpc": "1.0.0"}, "streams": []},
        {"document": {"nexusrpc": "1.0.0"}, "streams": []},
    ]
    assert "engine" not in m.PublishRequest(document={}).model_dump(mode="json")


def test_the_pydantic_floor_has_what_the_client_relies_on() -> None:
    """OD11's omitted default engine (and ``buyer_key``'s) is ``Field(exclude_if=)``, and the
    client reads the gate's answers with ``model_validate_json(extra=)``: both pydantic 2.12.
    On 2.9-2.11 the first is swallowed as an unknown keyword, so ``engine`` is always sent
    and a gate that predates it refuses the publish. CI installs only the lock, so the
    floor in pyproject.toml is pinned here."""
    root = Path(__file__).resolve().parents[2]
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]
    [spec] = [d for d in project["dependencies"] if re.match(r"pydantic\b", d)]
    floor = re.search(r">=\s*(\d+)\.(\d+)", spec)
    assert floor is not None, spec
    assert (int(floor[1]), int(floor[2])) >= (2, 12), spec


def _properties(schema: Any) -> set[str]:
    """Every property name anywhere in a JSON schema (``$defs`` included)."""
    found: set[str] = set()
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key == "properties" and isinstance(value, dict):
                found |= set(value)
            found |= _properties(value)
    elif isinstance(schema, list):
        for item in schema:
            found |= _properties(item)
    return found


#: Every route a buyer reads (and the publish answer, a CatalogEntry): never the engine.
BUYER_VIEWS = (
    m.CatalogEntry,
    m.CatalogSummary,
    m.CatalogList,
    m.QuoteView,
    m.HireView,
    m.JobView,
    m.JobList,
    m.PartyView,
    m.ResultView,
    m.SellerJobView,
    m.WorkersView,
    m.StreamEvent,
)


@pytest.mark.parametrize("model", BUYER_VIEWS, ids=lambda c: c.__name__)
def test_no_buyer_facing_model_carries_the_engine(model: type[Any]) -> None:
    """The engine is a property of the offering that buyers never see (decisions I32)."""
    assert "engine" not in model.model_fields
    assert "engine" not in _properties(model.model_json_schema(mode="serialization"))


def test_no_route_answers_with_the_engine() -> None:
    """Walk every modelled response schema: ``engine`` is in none (only PublishRequest, a
    request, carries it). A route without a response model (the A2A card) is checked on the
    served bytes (``tests/testing/test_engine_dispatch.py``); the gate's OpenAPI snapshot in
    kontra-cloud asserts the same."""
    carried = [
        r.name
        for r in ROUTES
        if r.response is not None
        and "engine" in _properties(r.response.model_json_schema(mode="serialization"))
    ]
    assert carried == []
    requests = [
        r.name
        for r in ROUTES
        if r.request is not None
        and "engine" in _properties(r.request.model_json_schema(mode="validation"))
    ]
    assert requests == ["publish"]
