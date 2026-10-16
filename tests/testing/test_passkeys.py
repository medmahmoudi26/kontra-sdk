"""Passkeys on the fake gate (decisions I24): every scenario of
:mod:`kontra.testing.passkey_contract` -- the platform runs the same ones against its gate --
and what only this gate shows: the switch, how it derives the RP ID and the origins, the
stored use counts and user handles, the operator's ``revoke-passkeys``, the purge."""

from __future__ import annotations

import base64
import itertools
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta

import httpx
import pytest

from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing import Marketplace, SoftAuthenticator
from kontra.testing.fake_gate import SUBJECT_PASSKEY_REMOVED, FakeGate, SentMail
from kontra.testing.passkey_contract import SCENARIOS, Scenario, run_passkey_contract

pytestmark = pytest.mark.asyncio

ORIGIN = "http://localhost"
_ADDRESSES = itertools.count(1)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


class _Tokens:
    """A sign-in's tokens, refreshed on the gate's clock when the gate refuses one."""

    def __init__(self, anon: GateClient, tokens: m.TokenResponse) -> None:
        self._anon = anon
        self._access, self._refresh = tokens.access_token, tokens.refresh_token
        self._stale = False

    async def __call__(self) -> str:
        if self._stale and self._refresh is not None:
            req = m.TokenRequest(grant_type="refresh_token", refresh_token=self._refresh)
            tok = await self._anon.token(req)
            self._access, self._refresh, self._stale = tok.access_token, tok.refresh_token, False
        return self._access

    def invalidate(self, token: str) -> None:
        self._stale = True


class Harness:
    """:class:`~kontra.testing.passkey_contract.PasskeyHarness` for a :class:`FakeGate`:
    each sign-in comes from its own address, so mail limits never meet across scenarios."""

    origin = ORIGIN
    rp_id = "localhost"

    def __init__(self, gate: FakeGate) -> None:
        self.gate = gate

    def _transport(self) -> httpx.ASGITransport:
        n = next(_ADDRESSES)
        return httpx.ASGITransport(
            app=self.gate.app, client=(f"10.{n // 256 % 256}.{n % 256}.7", 1)
        )

    def address(self, party: str) -> str:
        return f"{party}@example.com"

    async def person(self, party: str, *, email: bool = True) -> GateClient:
        transport = self._transport()
        anon = GateClient(self.gate.base_url, transport=transport)
        start = await anon.device_start()
        if email:
            address = self.gate.allow_email(self.address(party))
            await anon.magic_link(address, start.user_code)
            link = self.gate.outbox[-1].link
            outcome = await self.gate.use_magic_link(link, start.user_code, handle=party)
            assert outcome == "approved"
        else:
            assert await self.gate.approve_device(start.user_code, party, via="device:github")
        req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
        tokens = _Tokens(anon, await anon.token(req))
        return GateClient(self.gate.base_url, token=tokens, transport=transport)

    def _bearer(self, token: str) -> GateClient:
        async def provider() -> str:
            return token

        return GateClient(self.gate.base_url, token=provider, transport=self._transport())

    async def key(self, party: str) -> GateClient:
        person = await self.person(party)
        made = await person.create_api_key("probe")
        req = m.TokenRequest(grant_type="api_key", api_key=made.key)
        return self._bearer((await person.token(req)).access_token)

    async def worker(self, party: str) -> GateClient:
        person = await self.person(party)
        return self._bearer((await person.temporal_token()).token)

    async def advance(self, seconds: float) -> None:
        self.gate.advance(seconds)

    async def now(self) -> datetime:
        return self.gate.now()

    async def mails(self) -> Sequence[SentMail]:
        return list(self.gate.outbox)

    async def confirm_by_email(self, client: GateClient) -> None:
        start = await client.confirm_start()
        assert start.user_code is not None
        assert await self.gate.use_magic_link(self.gate.outbox[-1].link, start.user_code) == (
            "confirmed"
        )

    async def require_confirmation(self, party: str) -> None:
        assert self.gate.require_confirmation(party)


@pytest.fixture
async def harness() -> AsyncIterator[Harness]:
    """A gate as production runs it (no dev login), its console on ``http://localhost``."""
    async with FakeGate(None, dev=False, sweep_interval=0) as gate:
        yield Harness(gate)


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.__name__)
async def test_the_contract(scenario: Scenario, harness: Harness) -> None:
    await scenario(harness)


async def test_the_whole_contract_on_one_gate(harness: Harness) -> None:
    """Scenarios share a gate: each keeps to parties of its own."""
    await run_passkey_contract(harness)


# --------------------------------------------------------------------------- this gate only


async def _registered(h: Harness, party: str = "alice") -> tuple[GateClient, SoftAuthenticator]:
    alice = await h.person(party)
    auth = SoftAuthenticator(origin=ORIGIN)
    ceremony = await alice.passkey_options()
    await alice.add_passkey(ceremony.ceremony_id, "laptop", auth.create(ceremony.options))
    return alice, auth


async def _code(call: object) -> tuple[int, str]:
    try:
        await call  # type: ignore[misc]
    except GateError as e:
        return e.status, e.code
    return 200, "ok"


async def test_passkeys_switched_off_keep_what_is_stored(harness: Harness) -> None:
    alice, auth = await _registered(harness)
    harness.gate.passkeys = False
    listed = await alice.passkeys()
    assert (listed.rp_id, len(listed.passkeys)) == (None, 1)  # the list still shows them
    off = (409, "passkeys_unavailable")
    assert await _code(alice.passkey_options()) == off
    assert await _code(alice.confirm_passkey_options()) == off
    assert await _code(alice.add_passkey("wac_" + "0" * 32, "x", {"id": "x"})) == off
    assert await _code(alice.confirm_passkey("wac_" + "0" * 32, {"id": "x"})) == off
    assert await _code(alice.revoke_passkey("pky_" + "0" * 32)) == (
        404,
        "not_found",
    )  # still served
    harness.gate.passkeys = True
    ceremony = await alice.confirm_passkey_options()
    await alice.confirm_passkey(ceremony.ceremony_id, auth.get(ceremony.options))


async def test_a_ceremony_started_before_the_switch_is_refused_after_it(harness: Harness) -> None:
    alice, auth = await _registered(harness)
    ceremony = await alice.confirm_passkey_options()
    harness.gate.passkeys = False
    answer = auth.get(ceremony.options)
    assert await _code(alice.confirm_passkey(ceremony.ceremony_id, answer)) == (
        409,
        "passkeys_unavailable",
    )


async def test_the_rp_id_and_origins_are_derived_as_the_platform_does() -> None:
    prod = FakeGate(None, base_url="https://app.kontra.run")
    assert (prod.webauthn_rp_id, prod.passkey_origins) == (
        "app.kontra.run",
        ("https://app.kontra.run",),
    )
    assert FakeGate(None, base_url="http://127.0.0.1:8080").webauthn_rp_id is None
    assert FakeGate(None, base_url="https://app.kontra.run", passkeys=False).webauthn_rp_id is None
    # No console origin on the RP ID (plain http, the console elsewhere, an RP ID of another
    # host): the platform takes no passkeys (409 passkeys_unavailable), rather than issuing
    # ceremonies no answer can complete.
    for nowhere in (
        FakeGate(None, base_url="http://app.kontra.run"),
        FakeGate(None, base_url="https://app.kontra.run", passkey_origins=["https://x.example"]),
        FakeGate(None, base_url="https://app.kontra.run", passkey_rp_id="localhost"),
    ):
        assert (nowhere.webauthn_rp_id, nowhere.passkey_origins) == (None, ())
    console = FakeGate(
        None,
        base_url="http://127.0.0.1:8080",
        passkey_rp_id="localhost",
        passkey_origins=["http://localhost:5173", "https://evil.example", "http://127.0.0.1:8080"],
    )
    assert (console.webauthn_rp_id, console.passkey_origins) == (
        "localhost",
        ("http://localhost:5173",),
    )
    for bad in ("127.0.0.1", "Kontra.run", "kontra", "app.kontra.run:443"):
        with pytest.raises(ValueError, match="KONTRA_PASSKEY_RP_ID"):
            FakeGate(None, passkey_rp_id=bad)
    with pytest.raises(ValueError, match="printable"):
        FakeGate(None, passkey_rp_name="")


async def test_a_served_gate_on_an_ip_takes_passkeys_only_when_told_its_console() -> None:
    async with Marketplace(temporal=False) as plain:
        assert plain.gate.webauthn_rp_id is None  # served on 127.0.0.1: no RP ID to derive
    async with Marketplace(
        temporal=False, passkey_rp_id="localhost", passkey_origins=["http://localhost:5173"]
    ) as mp:
        assert (mp.gate.webauthn_rp_id, mp.gate.passkey_origins) == (
            "localhost",
            ("http://localhost:5173",),
        )


async def test_what_is_stored(harness: Harness) -> None:
    """The use count and the backup state move with each confirmation; the user handle is
    the party's, random, one for all its passkeys; the user's name is its handle."""
    gate = harness.gate
    alice = await harness.person("alice")
    first = await alice.passkey_options()
    second = await alice.passkey_options()
    handle = gate._passkey_users["alice"].user_handle  # pyright: ignore[reportPrivateUsage]
    assert len(handle) == 32
    for ceremony in (first, second):
        assert ceremony.options["user"] == {
            "id": _b64(handle),
            "name": "alice",
            "displayName": "alice",
        }
    auth = SoftAuthenticator(origin=ORIGIN, backup_eligible=True)
    view = await alice.add_passkey(first.ceremony_id, "synced", auth.create(first.options))
    row = gate._passkey_rows[view.id]  # pyright: ignore[reportPrivateUsage]
    assert (row.sign_count, row.backup_eligible, row.backed_up, row.algorithm) == (
        0,
        True,
        False,
        -7,
    )
    assert row.credential_id == auth.credentials[0].credential_id
    auth.backed_up = True
    for _ in range(2):
        ceremony = await alice.confirm_passkey_options()
        await alice.confirm_passkey(ceremony.ceremony_id, auth.get(ceremony.options))
    assert (row.sign_count, row.backed_up) == (auth.credentials[0].sign_count, True)
    assert (await alice.passkeys()).passkeys[0].backed_up


async def test_the_operator_revokes_passkeys(harness: Harness) -> None:
    gate = harness.gate
    alice, _ = await _registered(harness)
    other = await harness.person("alice")
    ceremony = await alice.passkey_options()
    spare = SoftAuthenticator(origin=ORIGIN)
    kept = await alice.add_passkey(ceremony.ceremony_id, "spare", spare.create(ceremony.options))
    for party, prefix, says in (
        ("nobody", None, "no party"),
        ("alice", "pky_", "8 to 36"),
        ("alice", "pky_zzzz", "8 to 36"),
        ("alice", "pky_" + "0" * 32, "no passkey"),
    ):
        with pytest.raises(ValueError, match=says):
            gate.revoke_passkeys(party, prefix)
    sent = len(gate.outbox)
    assert gate.revoke_passkeys("alice", kept.id[:10]) == [kept.id[:12]]
    with pytest.raises(ValueError, match="revoked already"):
        gate.revoke_passkeys("alice", kept.id)
    for client in (alice, other):  # every sign-in, the operator's own reason
        assert await _code(client.revoke_api_key("key_" + "0" * 16)) == (
            403,
            "confirmation_required",
        )
    revoked = [p for p in (await alice.passkeys()).passkeys if p.id == kept.id]
    assert revoked[0].revoked_by == "operator"
    assert len(gate.revoke_passkeys("alice")) == 1  # every live one: the first
    assert gate.revoke_passkeys("alice") == []
    # The party is mailed each passkey removed, in the operator's words (no sign-in named).
    removed = gate.outbox[sent:]
    assert [x.subject for x in removed] == [SUBJECT_PASSKEY_REMOVED] * 2
    assert all(x.to == "alice@example.com" and x.link == "" for x in removed)
    assert removed[0].text.startswith('A passkey named "spare" was removed')
    assert "by Kontra's operators" in removed[0].text
    assert await _code(alice.confirm_passkey_options()) == (409, "no_passkeys")


async def test_settled_ceremonies_are_purged_a_day_after_they_expire(harness: Harness) -> None:
    gate = harness.gate
    alice, _ = await _registered(harness)
    await alice.confirm_passkey_options()
    assert len(gate._ceremonies) == 2  # pyright: ignore[reportPrivateUsage]
    gate.advance(timedelta(days=1, minutes=6))
    await harness.confirm_by_email(alice)
    await alice.confirm_passkey_options()
    assert len(gate._ceremonies) == 1  # pyright: ignore[reportPrivateUsage]


async def test_the_notices_go_to_the_outbox_with_their_subjects(harness: Harness) -> None:
    alice, auth = await _registered(harness, "alice")
    [added] = [x for x in harness.gate.outbox if x.subject.startswith("A passkey was added")]
    assert (added.to, added.link) == ("alice@example.com", "")
    assert "(a link by email, signed in " in added.text
    ceremony = await alice.confirm_passkey_options()
    copy = auth.clone()
    await alice.confirm_passkey(ceremony.ceremony_id, auth.get(ceremony.options))
    ceremony = await alice.confirm_passkey_options()
    assert await _code(alice.confirm_passkey(ceremony.ceremony_id, copy.get(ceremony.options))) == (
        403,
        "passkey_cloned",
    )
    assert harness.gate.outbox[-1].subject == "A passkey of yours was revoked"


async def test_a_dev_login_is_recent_again_once_it_confirms() -> None:
    """``recent`` counts from the later of the sign-in and the last confirmation; a dev
    gate confirms a development login at once."""
    gate = FakeGate(None, sweep_interval=0)
    async with gate:
        transport = httpx.ASGITransport(app=gate.app)
        anon = GateClient(gate.base_url, transport=transport)
        tok = await anon.dev_login("dana")

        async def token() -> str:
            return tok.access_token

        dana = GateClient(gate.base_url, token=token, transport=transport)
        gate.advance(301)
        assert await _code(dana.passkey_options()) == (403, "confirmation_required")
        assert (await dana.confirm_start()).status == "confirmed"
        await dana.passkey_options()
