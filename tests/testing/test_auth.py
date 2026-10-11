"""Identity on the fake gate behaves like the platform's (PRD §10.3, decisions I3): dev
login, the device flow, rotating refresh tokens with reuse detection, ES256 tokens in a
JWKS, the one Temporal role, freshness, revocation, all on the gate's own clock."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import httpx
import jwt
import pytest

from kontra.auth import Session, login_device
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import NotLoggedIn
from kontra.testing.fake_gate import AUD_API, AUD_TEMPORAL, FakeGate

from .conftest import World
from .support import ScriptedControl

DEVICE = "urn:ietf:params:oauth:grant-type:device_code"


def _verify(world: World, token: str, audience: str) -> dict[str, object]:
    jwks = jwt.PyJWKSet.from_dict(world.gate.keys.jwks())
    kid = jwt.get_unverified_header(token)["kid"]
    key = next(k for k in jwks.keys if k.key_id == kid)
    return jwt.decode(
        token, key.key, algorithms=["ES256"], audience=audience, issuer=world.gate.issuer
    )


async def test_dev_login_creates_the_party_namespace_and_grant(world: World) -> None:
    tok = await world.anon().dev_login("carol")
    assert tok.party == "carol"
    assert world.control.namespaces["carol"] == timedelta(days=3)
    claims = _verify(world, tok.access_token, AUD_API)
    assert claims["sub"] == "carol"
    assert claims["exp"] - claims["iat"] == 900  # type: ignore[operator]
    bal = await world.bearer(tok.access_token).balance()
    assert (bal.available_minor, bal.available) == (10000, "100.00")
    again = await world.anon().dev_login("carol")  # a second login: same party, no new grant
    assert (await world.bearer(again.access_token).balance()).available_minor == 10000


@pytest.mark.parametrize("bad", ["Carol", "kontra", "kontra-world", "a" * 33, "carol\n", "-x", ""])
async def test_dev_login_refuses_bad_and_reserved_handles(world: World, bad: str) -> None:
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.post("/v1/auth/dev-login", json={"handle": bad})
    assert (r.status_code, r.json()["code"]) == (422, "invalid_handle")


async def test_refresh_tokens_rotate_and_a_reused_one_revokes_the_session(world: World) -> None:
    first = await world.anon().dev_login("dave")
    anon = world.anon()
    second = await anon.token(
        m.TokenRequest(grant_type="refresh_token", refresh_token=first.refresh_token)
    )
    assert second.refresh_token != first.refresh_token
    assert second.auth_time == first.auth_time  # the human's login time never moves
    third = await anon.token(
        m.TokenRequest(grant_type="refresh_token", refresh_token=second.refresh_token)
    )
    with pytest.raises(GateError) as e:  # the copy someone kept
        await anon.token(
            m.TokenRequest(grant_type="refresh_token", refresh_token=first.refresh_token)
        )
    assert (e.value.status, e.value.code) == (400, "invalid_grant")
    with pytest.raises(GateError) as e:  # ...revoked the whole session, newest token included
        await anon.token(
            m.TokenRequest(grant_type="refresh_token", refresh_token=third.refresh_token)
        )
    assert e.value.code == "invalid_grant"
    with pytest.raises(GateError) as e:
        await world.bearer(third.access_token).me()
    assert (e.value.status, e.value.code) == (401, "invalid_token")


@pytest.mark.parametrize(
    "req",
    [
        {"grant_type": "refresh_token"},
        {"grant_type": "refresh_token", "refresh_token": "x", "device_code": "y"},
        {"grant_type": "refresh_token", "refresh_token": "x", "handle": "eve"},
        {"grant_type": DEVICE},
        {"grant_type": DEVICE, "device_code": "y", "refresh_token": "x"},
    ],
)
async def test_token_grants_take_exactly_their_own_fields(
    world: World, req: dict[str, str]
) -> None:
    with pytest.raises(GateError) as e:
        await world.anon().token(m.TokenRequest.model_validate(req))
    assert (e.value.status, e.value.code) == (400, "invalid_request")


async def test_unknown_or_expired_refresh_tokens_are_invalid_grants(world: World) -> None:
    anon = world.anon()
    for token in ["nope", "x" * 43]:
        with pytest.raises(GateError) as e:
            await anon.token(m.TokenRequest(grant_type="refresh_token", refresh_token=token))
        assert e.value.code == "invalid_grant"
    tok = await anon.dev_login("erin")
    world.gate.advance(timedelta(days=31))
    with pytest.raises(GateError) as e:
        await anon.token(
            m.TokenRequest(grant_type="refresh_token", refresh_token=tok.refresh_token)
        )
    assert "expired" in e.value.message


async def test_a_session_refreshes_across_the_gates_clock(world: World) -> None:
    s = await world.party("fran")
    first = await s.access_token()
    world.gate.advance(timedelta(hours=2))  # every token the session holds is now expired
    me = await s.gate.me()  # 401 token_expired -> refresh -> retry
    assert me.party == "fran"
    assert await s.access_token() != first


async def test_revoke_ends_the_session(world: World) -> None:
    s = await world.party("gail")
    worker = world.bearer((await s.gate.temporal_token()).token)
    await s.gate.revoke()
    with pytest.raises(GateError) as e:
        await worker.me()  # the worker token names the same session
    assert e.value.status == 401
    with pytest.raises(NotLoggedIn):
        await s.gate.me()  # the refresh is refused too: log in again


async def test_temporal_tokens_are_worker_tokens_for_the_own_namespace(world: World) -> None:
    s = await world.party("hana")
    tok = await s.gate.temporal_token()
    assert (tok.namespace, tok.role) == ("hana", "worker")
    assert tok.temporal_address == world.gate.temporal_address
    claims = _verify(world, tok.token, AUD_TEMPORAL)
    assert claims["permissions"] == ["hana:worker"]
    assert claims["sub"] == "hana"
    assert claims["exp"] - claims["iat"] == 900  # type: ignore[operator]
    # it opens party|worker routes and nothing else
    worker = world.bearer(tok.token)
    assert (await worker.me()).party == "hana"
    with pytest.raises(GateError) as e:
        await worker.balance()
    assert e.value.status == 401
    with pytest.raises(GateError) as e:
        await worker.temporal_token()  # a worker token cannot mint more Temporal tokens
    assert e.value.status == 401


async def test_forged_and_misaddressed_tokens_are_refused(world: World) -> None:
    s = await world.party("ivy")
    good = await s.access_token()
    claims = jwt.decode(good, options={"verify_signature": False})
    from cryptography.hazmat.primitives.asymmetric import ec

    other_key = ec.generate_private_key(ec.SECP256R1())
    header = {"kid": world.gate.keys.kid}
    forged = jwt.encode(claims, other_key, algorithm="ES256", headers=header)
    hold_aud = world.gate.keys.sign({**claims, "aud": "kontra:hold"})
    other_ns = world.gate.keys.sign(
        {**claims, "aud": AUD_TEMPORAL, "permissions": ["someone-else:worker"]}
    )
    unknown_sid = world.gate.keys.sign({**claims, "sid": "nope"})
    no_kid = jwt.encode(claims, other_key, algorithm="ES256")
    for token in (forged, hold_aud, other_ns, unknown_sid, no_kid, "not.a.jwt", good + "x"):
        with pytest.raises(GateError) as e:
            await world.bearer(token).me()
        assert (e.value.status, e.value.code) == (401, "invalid_token"), token


async def test_set_key_needs_a_fresh_session_and_a_real_key(world: World) -> None:
    s = await world.party("jade")
    with pytest.raises(GateError) as e:
        await s.gate.set_key("age1notakey")
    assert (e.value.status, e.value.code) == (422, "invalid_key")
    world.gate.advance(timedelta(minutes=11))
    with pytest.raises(GateError) as e:
        await s.gate.set_key(s.profile.public_key or "")
    assert (e.value.status, e.value.code) == (403, "stale_session")


# --------------------------------------------------------------------------- the device flow


async def test_device_flow_auto_approves_the_requested_handle(world: World) -> None:
    tok = await login_device(
        world.gate.base_url,
        "kim",
        open_browser=False,
        transport=world.transport,
        prompt=lambda start: None,
    )
    assert tok.party == "kim"
    assert "kim" in world.gate.parties


async def test_device_flow_by_hand(scripted: ScriptedControl) -> None:
    gate = FakeGate(scripted, sweep_interval=0, device_auto_approve=False, device_interval=5)
    async with gate:
        t = httpx.ASGITransport(app=gate.app)
        from kontra.gate_client import GateClient

        anon = GateClient(gate.base_url, transport=t)
        start = await anon.device_start()
        assert start.verification_uri == f"{gate.base_url}/device"
        assert start.user_code in start.verification_uri_complete
        req = m.TokenRequest(grant_type=DEVICE, device_code=start.device_code, handle="lee")

        async def code() -> str:
            try:
                await anon.token(req)
            except GateError as e:
                return e.code
            return "ok"

        assert await code() == "authorization_pending"
        assert await code() == "slow_down"  # polled again before the interval
        gate.advance(timedelta(seconds=11))
        assert await code() == "authorization_pending"
        assert await gate.approve_device(start.user_code, "lee")
        tok = await anon.token(req)
        assert tok.party == "lee"
        assert await code() == "invalid_grant"  # consumed

        denied = await anon.device_start()
        assert gate.deny_device(denied.user_code)
        assert not await gate.approve_device(denied.user_code, "lee")
        with pytest.raises(GateError) as e:
            await anon.token(m.TokenRequest(grant_type=DEVICE, device_code=denied.device_code))
        assert e.value.code == "access_denied"

        late = await anon.device_start()
        gate.advance(timedelta(minutes=11))
        with pytest.raises(GateError) as e:
            await anon.token(m.TokenRequest(grant_type=DEVICE, device_code=late.device_code))
        assert e.value.code == "expired_token"


async def test_auto_approval_needs_a_handle(world: World) -> None:
    anon = world.anon()
    start = await anon.device_start()
    with pytest.raises(GateError) as e:
        await anon.token(m.TokenRequest(grant_type=DEVICE, device_code=start.device_code))
    assert e.value.code == "invalid_request"
    with pytest.raises(GateError) as e:
        await anon.token(
            m.TokenRequest(grant_type=DEVICE, device_code=start.device_code, handle="Bad Handle")
        )
    assert e.value.code == "invalid_request"


async def test_session_survives_profile_reload(world: World, tmp_path: Path) -> None:
    s = await world.party("mona")
    await s.gate.me()
    reloaded = Session.load(tmp_path / "mona", transport=world.transport)
    try:
        assert (await reloaded.gate.me()).party == "mona"  # the rotated token was written back
    finally:
        await reloaded.aclose()


async def test_a_gate_that_is_not_dev_never_approves_a_device_by_itself(
    scripted: ScriptedControl,
) -> None:
    """Approving whatever handle the device asked for is logging in as anyone: only a dev
    gate does it unless told to."""
    gate = FakeGate(scripted, sweep_interval=0, dev=False)
    async with gate:
        await gate.ensure_party("alice")
        anon = GateClient(gate.base_url, transport=httpx.ASGITransport(app=gate.app))
        start = await anon.device_start()
        with pytest.raises(GateError) as e:
            await anon.token(
                m.TokenRequest(grant_type=DEVICE, device_code=start.device_code, handle="alice")
            )
        assert e.value.code == "authorization_pending"
        assert not gate.sessions
        await anon.aclose()
