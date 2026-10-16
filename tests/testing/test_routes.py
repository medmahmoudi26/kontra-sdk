"""Seam ②, fake-gate side: every row of ROUTES is served with its method, path, models and
query parameters, every row answers the real GateClient, and every row enforces its
``auth`` exactly as the table says."""

from __future__ import annotations

import base64
import hashlib
import re
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from fastapi.routing import APIRoute

from kontra.artifacts import download, upload
from kontra.codec import GateBlobStore
from kontra.codec.age import encrypt, generate_identity
from kontra.contract.names import job_workflow_id
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.gate_client.routes import BY_NAME, ROUTES, Route
from kontra.testing import SoftAuthenticator, fake_gate
from kontra.testing.fake_gate import FakeGate

from .conftest import World
from .support import json_payload, ok_quote, wire

#: A buyer key for a direct `GateClient.hire` call. Real hires make one per job
#: (`Profile.new_pending_job_identity`); a test that drives the route itself only needs a
#: well-formed recipient (decisions I7).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


QUERY = {
    "search": {"q", "cursor", "limit"},
    "jobs": {"as", "state", "cursor", "limit"},
    "balance": {"currency"},
    "progress": {"after"},
    "disputes": {"cursor", "limit"},
}


def _served(gate: FakeGate) -> dict[str, APIRoute]:
    return {r.name: r for r in gate.app.routes if isinstance(r, APIRoute)}


def test_every_row_is_served_with_its_method_path_and_models() -> None:
    served = _served(fake_gate())
    for row in ROUTES:
        api = served.get(row.name)
        assert api is not None, f"{row.name} is not served"
        assert api.path == row.path, row.name
        assert api.methods == {row.method}, row.name
        assert api.operation_id == row.name
        body = api.body_field
        if row.request is None:
            assert body is None, row.name
        else:
            assert body is not None, row.name
            assert body.field_info.annotation is row.request, row.name
        if row.response is not None and not row.stream:
            assert api.response_model is row.response, row.name
        assert api.status_code == row.status, row.name
        names = {p.alias for p in api.dependant.query_params}
        assert names == QUERY.get(row.name, set()), row.name
    # the only other routes are the gate's own signed blob URLs, not part of the API
    extra = {r.path for r in served.values() if r.name not in BY_NAME}
    assert all(p.startswith("/_blob/") for p in extra), extra


def test_openapi_lists_every_row_by_operation_id() -> None:
    doc = fake_gate().app.openapi()
    ops = {
        (method.upper(), path): op["operationId"]
        for path, item in doc["paths"].items()
        for method, op in item.items()
    }
    assert {(r.method, r.path): r.name for r in ROUTES} == ops


# --------------------------------------------------------------------------- every row, through GateClient


def _matcher(route: Route) -> re.Pattern[str]:
    return re.compile(re.sub(r"\\\{[a-z_0-9]+\\\}", "[^/]+", re.escape(route.path)) + "$")


class Recording(httpx.AsyncBaseTransport):
    """Records which route rows answered 2xx."""

    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.ok: set[str] = set()
        self._rows = [(r, _matcher(r)) for r in ROUTES]

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        resp = await self.inner.handle_async_request(request)
        if resp.status_code < 300:
            for row, pattern in self._rows:
                if row.method == request.method and pattern.match(request.url.path):
                    self.ok.add(row.name)
        return resp


async def test_every_row_answers_the_real_client(world: World, tmp_path: Path) -> None:
    rec = Recording(world.transport)
    world.transport = rec  # type: ignore[assignment]
    gate, control = world.gate, world.control
    anon = GateClient(gate.base_url, transport=rec)

    assert (await anon.health())["status"] == "ok"
    assert (await anon.jwks())["keys"]
    assert (await anon.info()).dev  # a dev gate: dev login, play money
    start = await anon.device_start()
    dev_tokens = await anon.token(
        m.TokenRequest(
            grant_type="urn:ietf:params:oauth:grant-type:device_code",
            device_code=start.device_code,
            handle="devicer",
        )
    )
    assert dev_tokens.party == "devicer"
    # a sign-in link for a pending device (a dev gate's sign-up is open): one mail, no code in it
    linked = await anon.device_start()
    assert (await anon.magic_link("Devicer@Example.com", linked.user_code)).status == "sent"
    [mail] = gate.outbox
    assert mail.to == "devicer@example.com"
    assert linked.user_code not in mail.text

    seller = await world.party()  # dev_login, me, set_key, then token (refresh) on first use
    buyer = await world.party()
    judge = await world.party("judge")
    entry = await world.publish(seller, max_concurrent_jobs=5)
    assert (await anon.search("upper")).items[0].offering == entry.offering
    assert (await anon.get_offering(entry.name)).endpoint == entry.endpoint
    assert (await anon.card(entry.offering))["name"] == entry.name
    assert (await anon.by_endpoint(entry.endpoint)).offering == entry.offering
    assert (await anon.party(seller.profile.party)).recipients == [seller.profile.public_key]

    control.responder = lambda p, h: ok_quote(500)

    async def hired() -> m.HireView:
        q = await buyer.gate.quote(entry.name, wire(json_payload({"text": "hi"})))
        assert q.quote_id is not None
        return await buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY, max_price_minor=500)

    worker = world.bearer((await seller.gate.temporal_token()).token)  # kontra serve's credential
    assert (await worker.workers(entry.offering)).task_queue == entry.task_queue

    # job 1: started through the gate, progress, an output artifact, delivered, rejected, resolved
    one = await hired()
    await buyer.gate.start(one.job_id, wire(json_payload({"text": "hi"})))
    assert control.starts[-1].operation_id == one.operation_id
    sealed = encrypt(b'{"percent": 10}', [buyer.profile.public_key or ""])
    await worker.post_progress(
        one.job_id,
        m.ProgressPost(
            at=gate.now(), activity="fetch", ciphertext_b64=base64.b64encode(sealed).decode()
        ),
    )
    src = tmp_path / "out.txt"
    src.write_text("records\n")
    art = await upload(
        worker,
        src,
        recipients=[buyer.profile.public_key or "", seller.profile.public_key or ""],
        job_id=one.job_id,
    )  # create_artifact + complete_artifact
    blobs = GateBlobStore(worker)
    digest = hashlib.sha256(b"blob").hexdigest()
    await blobs.put(digest, b"blob")  # create_blob
    assert await blobs.get(digest) == b"blob"  # get_blob

    control.observe_as(
        seller.profile.party, job_workflow_id(one.job_id), ObservedStatus.COMPLETED, b"cipher"
    )
    await gate.sweep()
    assert (await buyer.gate.job(one.job_id)).state is m.JobState.COMPLETED
    got = await download(
        buyer.gate, art, buyer.identity(), tmp_path / "got.txt"
    )  # download_artifact: the buyer's, once delivered
    assert got.read_text() == "records\n"
    assert (await worker.seller_job(one.job_id)).buyer_public_key == _A_BUYER_KEY
    assert [j.job_id for j in (await worker.jobs(as_="seller")).items] == [one.job_id]
    events = [e async for e in buyer.gate.progress(one.job_id)]
    assert events[0].activity == "fetch"
    assert (await buyer.gate.result(one.job_id)).size == len(b"cipher")
    await buyer.gate.reject(one.job_id, "wrong case")
    assert [j.job_id for j in (await judge.gate.disputes()).items] == [one.job_id]
    resolved = await judge.gate.resolve(
        one.job_id, m.ResolveRequest(buyer_minor=200, seller_minor=300)
    )
    assert resolved.escrow is m.Escrow.RESOLVED

    # job 2: accepted; job 3: canceled
    two = await hired()
    control.observe_as(
        seller.profile.party, job_workflow_id(two.job_id), ObservedStatus.COMPLETED, b"x"
    )
    await gate.sweep()
    assert (await buyer.gate.accept(two.job_id)).escrow is m.Escrow.RELEASED
    three = await hired()
    assert (await buyer.gate.cancel(three.job_id)).state is m.JobState.CANCELED
    assert len((await buyer.gate.jobs()).items) == 3

    topped = await buyer.gate.topup(1000)
    assert topped.credited
    assert (await buyer.gate.balance()).available_minor == topped.balance.available_minor  # type: ignore[union-attr]
    assert (await worker.me()).party == seller.profile.party

    made = await buyer.gate.create_api_key("ci")  # a person's session mints, lists, revokes
    assert [k.id for k in (await buyer.gate.api_keys()).keys] == [made.id]
    await buyer.gate.revoke_api_key(made.id)
    fleet = generate_identity()[1]  # one more recipient for the party, then not
    assert fleet in [k.public_key for k in (await buyer.gate.add_recipient("fleet", fleet)).keys]
    await buyer.gate.revoke_recipient(fleet)
    await seller.gate.revoke()
    # a passkey: added (the sign-in is minutes old), listed, confirming, revoked
    device = SoftAuthenticator(origin=gate.base_url)
    adding = await buyer.gate.passkey_options()
    added = await buyer.gate.add_passkey(
        adding.ceremony_id, "laptop", device.create(adding.options)
    )
    assert [p.id for p in (await buyer.gate.passkeys()).passkeys] == [added.id]
    asking = await buyer.gate.confirm_passkey_options()
    confirmed = await buyer.gate.confirm_passkey(asking.ceremony_id, device.get(asking.options))
    assert confirmed.passkey_id == added.id
    await buyer.gate.revoke_passkey(added.id)
    started = await buyer.gate.confirm_start()  # a dev login confirms at once
    assert (started.channel, started.status) == ("dev", "confirmed")
    polled = await buyer.gate.confirm_status(started.confirm_id)
    assert (polled.status, polled.confirmed_until) == ("confirmed", started.confirmed_until)
    [mine] = (await buyer.gate.sessions()).sessions  # people's sign-ins; never a key's
    assert mine.current
    await buyer.gate.end_session(mine.id)  # the buyer signs out this way
    revoked = await anon.revocations()  # the key's session and both logins
    assert len(revoked.sessions) == 3

    missing = {r.name for r in ROUTES} - rec.ok
    assert not missing, f"never answered 2xx: {sorted(missing)}"


# --------------------------------------------------------------------------- auth, row by row

PATH_VALUES = {
    "party": "nobody",
    "name": "enrich",
    "version": "latest",
    "endpoint": "k" + "0" * 32,
    "job_id": "0" * 32,
    "key": "0" * 32,
    "sha256": "0" * 64,
    "key_id": "key_" + "0" * 16,
    "session_id": "0" * 32,
    "public_key": "age1" + "q" * 58,
    "confirm_id": "cfm_" + "0" * 32,
    "passkey_id": "pky_" + "0" * 32,
}


def _url(route: Route) -> str:
    return route.path.format(**PATH_VALUES)


async def _call(gate: FakeGate, route: Route, token: str | None) -> httpx.Response:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=gate.app), base_url=gate.base_url
    ) as http:
        if route.stream:
            async with http.stream(route.method, _url(route), headers=headers) as resp:
                await resp.aread()
                return resp
        return await http.request(route.method, _url(route), headers=headers)


@pytest.fixture
async def tokens(world: World) -> dict[str, str]:
    """A party's access token, a resolver's, the party's Temporal worker token, and access
    tokens exchanged for an API key of the party's and of the resolver's (made a moment
    ago)."""
    alice = await world.party("alice")
    judge = await world.party("judge")

    async def exchanged(key: str) -> str:
        req = m.TokenRequest(grant_type="api_key", api_key=key)
        return (await world.anon().token(req)).access_token

    return {
        "party": await alice.access_token(),
        "resolver": await judge.access_token(),
        "worker": (await alice.gate.temporal_token()).token,
        "api_key": await exchanged((await alice.gate.create_api_key("probe")).key),
        "resolver_key": await exchanged((await judge.gate.create_api_key("bot")).key),
    }


@pytest.mark.parametrize("row", ROUTES, ids=[r.name for r in ROUTES])
async def test_each_row_enforces_its_auth(world: World, tokens: dict[str, str], row: Route) -> None:
    gate = world.gate
    # The rule is the window, not its length: a short one keeps the tokens of the fixture
    # (15 minutes) alive past it.
    gate.confirm_ttl = timedelta(minutes=10)
    denied = {401, 403}
    need = m.ConfirmationRequired(confirm_with="dev", confirm_ttl_seconds=600)

    async def answer(token: str | None) -> tuple[int, str, object]:
        resp = await _call(gate, row, token)
        body = (
            resp.json()
            if resp.headers.get("content-type", "").startswith("application/json")
            else {}
        )
        if not isinstance(body, dict):
            body = {}
        return resp.status_code, str(body.get("code", "")), body.get("detail")

    async def status(token: str | None) -> tuple[int, str]:
        return (await answer(token))[:2]

    async def later(token: str) -> tuple[int, str, object]:
        """The answer once the sign-in is older than the window (and nothing confirmed)."""
        gate.advance(timedelta(minutes=11))
        try:
            return await answer(token)
        finally:
            gate.advance(-timedelta(minutes=11))

    anonymous = await status(None)
    with_party = await status(tokens["party"])
    with_worker = await status(tokens["worker"])
    with_key = await status(tokens["api_key"])
    if row.auth in ("none", "dev"):
        assert anonymous[0] not in denied, anonymous
        return
    assert anonymous == (401, "unauthenticated")
    if row.auth == "party|worker":
        assert with_party[0] not in denied, with_party
        assert with_worker[0] not in denied, with_worker
        assert with_key[0] not in denied, with_key  # a program serves and reads with a key
        return
    # a worker token is never a party's access token
    assert with_worker[0] == 401, with_worker
    if row.auth == "party":
        assert with_party[0] not in denied, with_party
        assert with_key[0] not in denied, with_key  # a program hires with a key
    elif row.auth == "human":
        assert with_party[0] not in denied, with_party
        # Keys and sign-ins are a person's to see, and a key never confirms.
        assert with_key == (403, "fresh_login_required"), with_key
        stale = await later(tokens["party"])
        assert stale[0] not in denied, stale  # a read asks for no confirmation
    elif row.auth == "fresh":
        assert with_party[0] not in denied, with_party
        # Acceptance 6: an API key's session never makes a change, though the key is new.
        assert with_key == (403, "fresh_login_required"), with_key
        stale = await later(tokens["party"])
        assert stale[:2] == (403, "confirmation_required"), stale
        assert m.ConfirmationRequired.model_validate(stale[2]) == need
    elif row.auth == "recent":
        assert with_party[0] not in denied, with_party
        assert with_key == (403, "fresh_login_required"), with_key
        # Within the hour (the window here is ten minutes) but past five minutes: refused,
        # with fresh's detail, where a fresh row would go on (decisions I24).
        gate.advance(timedelta(minutes=6))
        try:
            past = await answer(tokens["party"])
        finally:
            gate.advance(-timedelta(minutes=6))
        assert past[:2] == (403, "confirmation_required"), past
        assert m.ConfirmationRequired.model_validate(past[2]) == need
        stale = await later(tokens["party"])
        assert stale[:2] == (403, "confirmation_required"), stale
    elif row.auth in ("resolver", "resolver_human"):
        assert with_party == (403, "not_resolver")
        assert with_key == (403, "not_resolver")
        assert (await status(tokens["resolver"]))[0] not in denied
        # The queue and its decisions are the resolver's own, never a key of theirs (the
        # review's F4); splitting escrow is a change, confirmed (a stolen refresh token).
        assert await status(tokens["resolver_key"]) == (403, "fresh_login_required")
        stale = await later(tokens["resolver"])
        if row.auth == "resolver_human":
            assert stale[0] not in denied, stale
        else:
            assert stale[:2] == (403, "confirmation_required"), stale
            assert m.ConfirmationRequired.model_validate(stale[2]) == need
    else:  # pragma: no cover - a new auth kind needs a rule here
        pytest.fail(f"unknown auth {row.auth}")


async def test_dev_rows_are_absent_from_a_production_gate() -> None:
    gate = FakeGate(None, dev=False, sweep_interval=0)
    for row in ROUTES:
        if row.auth == "dev":
            resp = await _call(gate, row, None)
            assert (resp.status_code, resp.json()["code"]) == (404, "not_found")


async def test_unknown_routes_and_methods_answer_error_bodies(world: World) -> None:
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.get("/v1/nope")
        assert (r.status_code, r.json()["code"]) == (404, "not_found")
        r = await http.delete("/v1/catalog")
        assert (r.status_code, r.json()["code"]) == (405, "method_not_allowed")
        r = await http.post("/v1/auth/dev-login", json={"handle": 5})
        assert (r.status_code, r.json()["code"]) == (422, "invalid_request")


async def test_errors_reach_the_client_as_gate_errors(world: World) -> None:
    anon = world.anon()
    with pytest.raises(GateError) as e:
        await anon.get_offering("acme/enrich@1.2.0")
    assert (e.value.status, e.value.code) == (404, "offering_not_found")
    with pytest.raises(GateError) as e:
        await anon.dev_login("kontra")
    assert (e.value.status, e.value.code) == (422, "invalid_handle")
