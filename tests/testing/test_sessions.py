"""A party's sign-ins (``GET /v1/sessions``, ``DELETE /v1/sessions/{id}``) on the fake gate:
how a person finds a login they did not make -- a device code they were tricked into
approving (the auth review's F1) -- and ends it."""

from __future__ import annotations

import hashlib

import pytest

from kontra.gate_client import GateError
from kontra.gate_client import models as m

from .conftest import World

pytestmark = pytest.mark.asyncio


async def _device_login(world: World, party: str) -> m.TokenResponse:
    """A device login of ``party`` approved by its person (the phished one, say)."""
    anon = world.anon()
    start = await anon.device_start()
    assert await world.gate.approve_device(start.user_code, party, via="device:email")
    req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
    return await anon.token(req)


async def test_a_person_finds_and_ends_a_login_they_did_not_make(world: World) -> None:
    alice = await world.party("alice")
    stolen = await _device_login(world, "alice")
    key = await alice.gate.create_api_key("ci")
    listed = (await alice.gate.sessions()).sessions
    assert len(listed) == 2  # her own login and the stolen one; the key is not a sign-in
    assert [s.current for s in listed].count(True) == 1
    other = next(s for s in listed if not s.current)
    assert other.via == "device:email"

    await alice.gate.end_session(other.id)
    assert [s.current for s in (await alice.gate.sessions()).sessions] == [True]
    # The stolen login can neither refresh nor use the tokens it holds...
    with pytest.raises(GateError) as e:
        await world.anon().token(
            m.TokenRequest(grant_type="refresh_token", refresh_token=stolen.refresh_token)
        )
    assert (e.value.status, e.value.code) == (400, "invalid_grant")
    with pytest.raises(GateError) as e:
        await world.bearer(stolen.access_token).me()
    assert e.value.status == 401
    # ...and Temporal is told (the revocation list), while the key still works.
    digest = hashlib.sha256(other.id.encode()).hexdigest()
    assert digest in (await world.anon().revocations()).sessions
    req = m.TokenRequest(grant_type="api_key", api_key=key.key)
    assert (await world.anon().token(req)).party == "alice"


async def test_only_the_partys_own_sign_ins_can_be_ended(world: World) -> None:
    alice = await world.party("alice")
    bob = await world.party("bob")
    key = await alice.gate.create_api_key("ci")
    bobs = (await bob.gate.sessions()).sessions[0].id
    key_session = next(k for k in world.gate.api_keys.values() if k.id == key.id).session_id
    for session_id in (bobs, key_session, "0" * 32):
        with pytest.raises(GateError) as e:
            await alice.gate.end_session(session_id)
        assert (e.value.status, e.value.code) == (404, "not_found")
    assert len((await bob.gate.sessions()).sessions) == 1
    # Ending the current one is signing out.
    mine = (await alice.gate.sessions()).sessions[0]
    await alice.gate.end_session(mine.id)
    assert world.gate.sessions[mine.id].revoked
