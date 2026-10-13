"""API keys on the fake gate, as the production contract states them (decisions I19): minted,
listed and revoked by a fresh human session, shown once and stored as a hash, exchanged for
the key's own session -- which hires, serves and reads but never counts as fresh -- and dead
on the next request once revoked."""

from __future__ import annotations

import re
from datetime import timedelta

import httpx
import pytest

from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.gate_client.routes import ROUTES
from kontra.testing.fake_gate import AUD_API

from .conftest import World
from .support import json_payload, ok_quote, wire


async def _exchange(world: World, key: str) -> m.TokenResponse:
    return await world.anon().token(m.TokenRequest(grant_type="api_key", api_key=key))


async def _code(call: object) -> tuple[int, str]:
    try:
        await call  # type: ignore[misc]
    except GateError as e:
        return e.status, e.code
    return 200, "ok"


async def test_a_key_is_shown_once_and_stored_as_a_hash(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    assert re.fullmatch(r"kontra_sk_[A-Za-z0-9_-]{43}", made.key)
    assert re.fullmatch(r"key_[0-9a-f]{16}", made.id)
    assert made.id[4:] not in made.key  # the id says nothing about the key
    assert made.label == "ci"
    assert abs(made.created_at - world.gate.now()) < timedelta(seconds=5)
    [view] = (await alice.gate.api_keys()).keys
    assert (view.id, view.label, view.last_used_at, view.revoked_at) == (made.id, "ci", None, None)
    assert made.key not in view.model_dump_json()
    # The gate keeps the key's sha256, never the key.
    state = repr(world.gate.api_keys) + repr(world.gate._api_key_ids)  # pyright: ignore[reportPrivateUsage]
    assert made.key not in state
    assert made.key[len(m.API_KEY_PREFIX) :] not in state


async def test_create_answers_201_and_no_store(world: World) -> None:
    alice = await world.party("alice")
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.post(
            "/v1/keys",
            json={"label": "ci"},
            headers={"Authorization": f"Bearer {await alice.access_token()}"},
        )
    assert r.status_code == 201
    assert r.headers["cache-control"] == "no-store"


async def test_an_exchange_is_the_keys_own_session(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    world.gate.advance(timedelta(minutes=30))  # the key's age does not matter
    tok = await _exchange(world, made.key)
    assert (tok.party, tok.refresh_token, tok.auth_time) == ("alice", None, made.created_at)
    claims = world.gate.keys.verify(
        tok.access_token, audience=AUD_API, issuer=world.gate.issuer, now=world.gate.now()
    )
    session = world.gate.sessions[claims["sid"]]
    assert (session.via, session.party) == ("api_key", "alice")
    assert claims["sid"] != (await _human_sid(world, alice.gate))  # not the human's session
    again = await _exchange(world, made.key)
    assert again.access_token != tok.access_token  # a new token, the same session
    assert (
        world.gate.keys.verify(
            again.access_token, audience=AUD_API, issuer=world.gate.issuer, now=world.gate.now()
        )["sid"]
        == claims["sid"]
    )


async def _human_sid(world: World, gate: GateClient) -> str:
    me = await gate.me()
    sids = [
        s.id for s in world.gate.sessions.values() if s.party == me.party and s.via != "api_key"
    ]
    return sids[0]


async def test_a_key_hires_and_serves_but_never_counts_as_fresh(world: World) -> None:
    """Acceptance 6 for the routes a program uses; ``tests/testing/test_routes.py`` checks
    every ``fresh`` row of the table."""
    seller = await world.party("seller")
    entry = await world.publish(seller)
    buyer = await world.party("buyer")
    made = await buyer.gate.create_api_key("program")
    program = world.bearer((await _exchange(world, made.key)).access_token)

    world.control.responder = lambda p, h: ok_quote(500)
    q = await program.quote(entry.name, wire(json_payload({"text": "hi"})))
    assert q.quote_id is not None
    hired = await program.hire(q.quote_id, max_price_minor=500)
    assert (await program.job(hired.job_id)).buyer == "buyer"
    assert (await program.balance()).held_minor == 500
    worker = await program.temporal_token()  # what `kontra serve` with a key needs
    assert worker.namespace == "buyer"
    assert (await world.bearer(worker.token).me()).party == "buyer"

    assert (await world.publish(seller, version="1.0.1")).version == "1.0.1"  # a person can
    for call in (
        program.publish({"nexusrpc": "1.0.0"}),
        program.set_key(buyer.profile.public_key or ""),
        program.create_api_key("another"),
        program.api_keys(),
        program.revoke_api_key(made.id),
    ):
        assert await _code(call) == (403, "fresh_login_required")
    assert {r.name for r in ROUTES if r.auth == "fresh"} == {
        "set_key",
        "publish",
        "create_api_key",
        "api_keys",
        "revoke_api_key",
    }


async def test_an_old_human_login_is_stale_not_key_shaped(world: World) -> None:
    alice = await world.party("alice")
    world.gate.advance(timedelta(minutes=11))
    assert await _code(alice.gate.create_api_key("late")) == (403, "stale_session")
    assert await _code(alice.gate.api_keys()) == (403, "stale_session")


async def test_unknown_and_revoked_keys_get_the_same_answer(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    await alice.gate.revoke_api_key(made.id)
    unknown = m.API_KEY_PREFIX + "A" * 43
    answers: list[tuple[int, str, str]] = []
    for key in (made.key, unknown):
        with pytest.raises(GateError) as e:
            await _exchange(world, key)
        answers.append((e.value.status, e.value.code, e.value.message))
    assert answers[0] == answers[1] == (400, "invalid_grant", "the API key is not valid")


async def test_revoking_a_key_ends_its_tokens_on_their_next_request(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    program = world.bearer((await _exchange(world, made.key)).access_token)
    worker = world.bearer((await program.temporal_token()).token)
    assert (await program.me()).party == (await worker.me()).party == "alice"

    await alice.gate.revoke_api_key(made.id)
    for client in (program, worker):
        with pytest.raises(GateError) as e:
            await client.me()
        assert (e.value.status, e.value.code) == (401, "invalid_token")
    [view] = (await alice.gate.api_keys()).keys
    assert view.revoked_at is not None
    await alice.gate.revoke_api_key(made.id)  # again: nothing changes
    assert (await alice.gate.api_keys()).keys[0].revoked_at == view.revoked_at
    assert (await alice.gate.me()).party == "alice"  # the human session is untouched


async def test_a_key_that_logs_out_is_revoked(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    program = world.bearer((await _exchange(world, made.key)).access_token)
    await program.revoke()  # POST /v1/auth/revoke with the key's session
    assert (await alice.gate.api_keys()).keys[0].revoked_at is not None
    assert await _code(_exchange(world, made.key)) == (400, "invalid_grant")


async def test_unknown_and_foreign_ids_are_not_found(world: World) -> None:
    alice = await world.party("alice")
    bob = await world.party("bob")
    theirs = await bob.gate.create_api_key("bob's")
    for key_id in (theirs.id, "key_" + "0" * 16, "nonsense", ".."):
        assert await _code(alice.gate.revoke_api_key(key_id)) == (404, "not_found")
    assert (await bob.gate.api_keys()).keys[0].revoked_at is None
    assert [k.id for k in (await alice.gate.api_keys()).keys] == []


async def test_at_most_25_live_keys(world: World) -> None:
    alice = await world.party("alice")
    made = [await alice.gate.create_api_key(f"k{i}") for i in range(m.MAX_API_KEYS)]
    assert await _code(alice.gate.create_api_key("one too many")) == (409, "too_many_keys")
    await alice.gate.revoke_api_key(made[0].id)
    await alice.gate.create_api_key("in its place")
    listed = (await alice.gate.api_keys()).keys
    assert len(listed) == 26
    assert [k.revoked_at is None for k in listed] == [True] * 25 + [False]  # live ones first
    assert listed[0].label == "in its place"
    assert (await (await world.party("bob")).gate.create_api_key("bob's own")).label == "bob's own"


async def test_last_used_moves_at_most_once_a_minute(world: World) -> None:
    alice = await world.party("alice")
    made = await alice.gate.create_api_key("ci")
    await _exchange(world, made.key)
    first = (await alice.gate.api_keys()).keys[0].last_used_at
    assert first is not None
    world.gate.advance(timedelta(seconds=30))
    await _exchange(world, made.key)
    assert (await alice.gate.api_keys()).keys[0].last_used_at == first
    world.gate.advance(timedelta(seconds=31))
    await _exchange(world, made.key)
    later = (await alice.gate.api_keys()).keys[0].last_used_at
    assert later is not None
    assert later > first


@pytest.mark.parametrize(
    "body",
    [
        {"grant_type": "api_key"},
        {"grant_type": "api_key", "api_key": m.API_KEY_PREFIX + "A" * 43, "refresh_token": "r"},
        {"grant_type": "api_key", "api_key": m.API_KEY_PREFIX + "A" * 43, "handle": "alice"},
        {
            "grant_type": "refresh_token",
            "refresh_token": "r",
            "api_key": m.API_KEY_PREFIX + "A" * 43,
        },
    ],
)
async def test_the_api_key_grant_takes_its_key_only(world: World, body: dict[str, str]) -> None:
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.post("/v1/auth/token", json=body)
    assert (r.status_code, r.json()["code"]) == (400, "invalid_request")


@pytest.mark.parametrize(
    "key", [m.API_KEY_PREFIX + "A" * 42, m.API_KEY_PREFIX + "A" * 43 + "\n", "sk_" + "A" * 43]
)
async def test_a_misshapen_key_is_a_malformed_body(world: World, key: str) -> None:
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.post("/v1/auth/token", json={"grant_type": "api_key", "api_key": key})
    assert (r.status_code, r.json()["code"]) == (422, "invalid_request")
    assert key.strip() not in r.text  # the body is not echoed


async def test_labels_are_checked_by_the_gate_too(world: World) -> None:
    alice = await world.party("alice")
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        for label in ("", "x" * 65, "\x1b[2J"):
            r = await http.post(
                "/v1/keys",
                json={"label": label},
                headers={"Authorization": f"Bearer {await alice.access_token()}"},
            )
            assert (r.status_code, r.json()["code"]) == (422, "invalid_request")
