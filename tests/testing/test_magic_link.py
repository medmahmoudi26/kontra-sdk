"""Magic links on the fake gate (decisions I19): mail only to an invited address with a
pending device code, the same answer either way, a link that approves only the device whose
code the human types, once, and that burns after three wrong codes."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta

import httpx
import pytest

from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import FakeGate

from .support import ScriptedControl

pytestmark = pytest.mark.asyncio


class Invite:
    """An allowlist-only, non-dev gate (production's ``KONTRA_SIGNUP=allowlist``)."""

    def __init__(self, gate: FakeGate) -> None:
        self.gate = gate
        self.transport = httpx.ASGITransport(app=gate.app)
        self.anon = GateClient(gate.base_url, transport=self.transport)

    async def device(self, handle: str | None = None) -> tuple[m.DeviceStart, m.TokenRequest]:
        start = await self.anon.device_start()
        req = m.TokenRequest(
            grant_type=m.DEVICE_GRANT, device_code=start.device_code, handle=handle
        )
        return start, req

    async def poll(self, req: m.TokenRequest) -> m.TokenResponse | str:
        self.gate.advance(timedelta(seconds=10))  # past the polling interval
        try:
            return await self.anon.token(req)
        except GateError as e:
            return e.code


@pytest.fixture
async def invite() -> AsyncIterator[Invite]:
    gate = FakeGate(ScriptedControl(), dev=False, sweep_interval=0)
    async with gate:
        yield Invite(gate)


async def test_a_production_gate_signs_up_by_invitation(invite: Invite) -> None:
    assert invite.gate.signup == "allowlist"
    assert FakeGate(None, sweep_interval=0).signup == "open"  # a dev gate: anyone


async def test_only_an_invited_address_with_a_pending_code_gets_mail(invite: Invite) -> None:
    gate = invite.gate
    start, _ = await invite.device("ann")
    for email, code in (
        ("ann@example.com", start.user_code),  # not invited (yet)
        ("Ann@Example.com", "BCDF-GHJK"),  # invited, but no such code
    ):
        if email == "Ann@Example.com":
            assert gate.allow_email("ANN@example.com") == "ann@example.com"
        sent = await invite.anon.magic_link(email, code)
        assert sent == m.MagicLinkSent()
        assert gate.outbox == []
    assert await invite.anon.magic_link("  ANN@EXAMPLE.COM ", start.user_code) == m.MagicLinkSent()
    [mail] = gate.outbox
    assert mail.to == "ann@example.com"
    assert mail.link.startswith(f"{gate.base_url}/auth/magic?t=")
    assert mail.link in mail.text
    # The code is only on the requester's screen: a mail someone else asked for approves nothing.
    assert start.user_code not in mail.text
    assert start.user_code.replace("-", "") not in mail.text


async def test_the_answer_is_identical_and_202_whoever_asks(invite: Invite) -> None:
    invite.gate.allow_email("ann@example.com")
    start, _ = await invite.device()
    answers: set[tuple[int, bytes, str | None]] = set()
    async with httpx.AsyncClient(transport=invite.transport, base_url=invite.gate.base_url) as http:
        for email, code in (
            ("ann@example.com", start.user_code),
            ("nobody@example.com", start.user_code),
            ("ann@example.com", "BCDF-GHJK"),
        ):
            r = await http.post("/v1/auth/magic", json={"email": email, "device_user_code": code})
            answers.add((r.status_code, r.content, r.headers.get("content-type")))
    assert answers == {(202, b'{"status":"sent"}', "application/json")}
    assert len(invite.gate.outbox) == 1


@pytest.mark.parametrize(
    "body",
    [
        {"email": "Ann <ann@example.com>", "device_user_code": "BCDF-GHJK"},
        {"email": "ann@[127.0.0.1]", "device_user_code": "BCDF-GHJK"},
        {"email": "ann@example.com", "device_user_code": "bcdf-ghjk"},
        {"email": "ann@example.com"},
        {"device_user_code": "BCDF-GHJK"},
        {"email": "ann@example.com", "device_user_code": "BCDF-GHJK", "handle": "ann"},
    ],
)
async def test_a_malformed_request_is_422(invite: Invite, body: dict[str, str]) -> None:
    async with httpx.AsyncClient(transport=invite.transport, base_url=invite.gate.base_url) as http:
        r = await http.post("/v1/auth/magic", json=body)
    assert (r.status_code, r.json()["code"]) == (422, "invalid_request")


async def test_rate_limits_count_every_request_invited_or_not(invite: Invite) -> None:
    """Per address and per client. A limit that counted only mail sent would tell an
    attacker who is invited."""
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, _ = await invite.device()
    for email in ("ann@example.com", "eve@example.com"):
        for _ in range(gate.magic_link_rate_per_email):
            await invite.anon.magic_link(email, start.user_code)
        with pytest.raises(GateError) as e:
            await invite.anon.magic_link(email, start.user_code)
        assert (e.value.status, e.value.code) == (429, "rate_limited")
    gate.advance(gate.magic_link_rate_window)
    await invite.anon.magic_link("ann@example.com", start.user_code)  # the window moved on

    many = FakeGate(None, dev=False, sweep_interval=0, magic_link_rate_per_client=3)
    anon = GateClient(many.base_url, transport=httpx.ASGITransport(app=many.app))
    for i in range(3):
        await anon.magic_link(f"user{i}@example.com", "BCDF-GHJK")
    with pytest.raises(GateError) as e:
        await anon.magic_link("user9@example.com", "BCDF-GHJK")
    assert e.value.status == 429


async def test_the_link_signs_up_an_invited_address_then_logs_it_in(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, req = await invite.device("ann")
    assert await invite.poll(req) == "authorization_pending"  # the CLI's first poll names "ann"
    await invite.anon.magic_link("ann@example.com", start.user_code)
    [mail] = gate.outbox
    assert gate.pending_user_codes() == [start.user_code]

    assert await gate.use_magic_link(mail.link, start.user_code.lower()) == "approved"
    tok = await invite.poll(req)
    assert isinstance(tok, m.TokenResponse)
    assert tok.party == "ann"
    assert gate.identities[("email", "ann@example.com")] == "ann"
    assert [s.via for s in gate.sessions.values()] == ["device:email"]
    assert await gate.use_magic_link(mail.link, start.user_code) == "gone"  # single use

    # The next sign-in of the address is the same party, whatever handle a device asks for.
    again, req2 = await invite.device("someone-else")
    await invite.poll(req2)
    await invite.anon.magic_link("ann@example.com", again.user_code)
    assert await gate.use_magic_link(gate.outbox[-1].link, again.user_code) == "approved"
    tok2 = await invite.poll(req2)
    assert isinstance(tok2, m.TokenResponse)
    assert tok2.party == "ann"
    assert "someone-else" not in gate.parties


async def test_a_link_approves_only_the_device_whose_code_is_typed(invite: Invite) -> None:
    """The attack the code defeats: Eve starts a device login and asks for a link to Ann's
    address. Ann gets a genuine mail, but no code: whatever she types, Eve's device stays
    pending."""
    gate = invite.gate
    gate.allow_email("ann@example.com")
    eves, eve_req = await invite.device("eve")
    anns, _ = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", eves.user_code)
    [mail] = gate.outbox
    assert await gate.use_magic_link(mail.link, anns.user_code) == "wrong_code"
    assert await invite.poll(eve_req) == "authorization_pending"
    assert eves.user_code in gate.pending_user_codes()


async def test_three_wrong_codes_burn_the_link(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, req = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", start.user_code)
    link = gate.outbox[0].link
    for typed in ("BCDF-GHJK", "not a code", "x" * 10_000):
        assert await gate.use_magic_link(link, typed) == "wrong_code"
    assert await gate.use_magic_link(link, start.user_code) == "gone"
    assert await invite.poll(req) == "authorization_pending"


async def test_a_link_expires_with_its_ten_minutes(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, _ = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", start.user_code)
    gate.advance(timedelta(minutes=10))
    assert await gate.use_magic_link(gate.outbox[0].link, start.user_code) == "gone"


@pytest.mark.parametrize(
    "link", ["", "https://evil.example/auth/magic?t=", "?t=a&t=b", "t=" + "A" * 43]
)
async def test_an_unknown_link_is_gone(invite: Invite, link: str) -> None:
    assert await invite.gate.use_magic_link(link, "BCDF-GHJK") == "gone"


async def test_two_racing_submits_approve_once(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, req = await invite.device("ann")
    await invite.poll(req)
    await invite.anon.magic_link("ann@example.com", start.user_code)
    link = gate.outbox[0].link
    outcomes = await asyncio.gather(
        gate.use_magic_link(link, start.user_code), gate.use_magic_link(link, start.user_code)
    )
    assert sorted(outcomes) == ["approved", "gone"]
    assert list(gate.parties) == ["ann"]


async def test_a_taken_handle_leaves_the_link_for_another(invite: Invite) -> None:
    gate = invite.gate
    await gate.ensure_party("ann")  # someone else's party already
    gate.allow_email("ann@example.com")
    start, req = await invite.device("ann")
    await invite.poll(req)
    await invite.anon.magic_link("ann@example.com", start.user_code)
    link = gate.outbox[0].link
    assert await gate.use_magic_link(link, start.user_code) == "handle_taken"
    assert await gate.use_magic_link(link, start.user_code, handle="ann-2") == "approved"
    tok = await invite.poll(req)
    assert isinstance(tok, m.TokenResponse)
    assert tok.party == "ann-2"


async def test_an_address_taken_off_the_list_cannot_use_its_link(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, _ = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", start.user_code)
    gate.disallow_email("ANN@example.com")
    assert await gate.use_magic_link(gate.outbox[0].link, start.user_code) == "gone"


async def test_a_dev_gate_mails_anyone(scripted: ScriptedControl) -> None:
    gate = FakeGate(scripted, sweep_interval=0, device_auto_approve=False)
    anon = GateClient(gate.base_url, transport=httpx.ASGITransport(app=gate.app))
    start = await anon.device_start()
    await anon.magic_link("anyone@example.com", start.user_code)
    assert [mail.to for mail in gate.outbox] == ["anyone@example.com"]
