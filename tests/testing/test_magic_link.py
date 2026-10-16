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
    """Per address (from one client) and per client. A limit that counted only mail sent
    would tell an attacker who is invited."""
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


def _from(gate: FakeGate, address: str) -> GateClient:
    """An anonymous client of ``gate`` connecting from ``address``."""
    transport = httpx.ASGITransport(app=gate.app, client=(address, 4242))
    return GateClient(gate.base_url, transport=transport)


async def test_strangers_cannot_spend_an_addresses_budget(invite: Invite) -> None:
    """The review's F2: five requests for Ann's address, from anywhere and with any code,
    used to lock Ann out of sign-in for an hour. An address's budget is now counted per
    requesting network (an IPv4 /24, an IPv6 /48): the stranger spends his own, and Ann, from
    her own network, still gets her link."""
    gate = invite.gate
    gate.allow_email("ann@example.com")
    mallory = _from(gate, "192.0.2.66")
    for _ in range(gate.magic_link_rate_per_email):
        await mallory.magic_link("ann@example.com", "BCDF-GHJK")
    for again in (mallory, _from(gate, "192.0.2.67")):  # another address of his network too
        with pytest.raises(GateError) as e:
            await again.magic_link("ann@example.com", "BCDF-GHJK")
        assert e.value.status == 429  # he is limited, from his network
    assert gate.outbox == []
    ann = _from(gate, "203.0.113.5")
    start = await ann.device_start()
    assert await ann.magic_link("ann@example.com", start.user_code) == m.MagicLinkSent()
    [mail] = gate.outbox
    assert mail.to == "ann@example.com"


async def test_a_strangers_pending_codes_do_not_use_up_the_addresses_mail(invite: Invite) -> None:
    """With pending codes of his own, a stranger makes the gate mail Ann links; the mails he
    causes count against his network, so Ann's own request still gets one. Only past the
    address's cap over every network together does mail stop (distributed abuse)."""
    gate = invite.gate
    gate.allow_email("ann@example.com")
    mallory = _from(gate, "192.0.2.66")
    for _ in range(gate.magic_link_rate_per_email):
        start = await mallory.device_start()
        await mallory.magic_link("ann@example.com", start.user_code)
    assert len(gate.outbox) == gate.magic_link_rate_per_email  # unsolicited, and capped
    ann = _from(gate, "203.0.113.5")
    start = await ann.device_start()
    await ann.magic_link("ann@example.com", start.user_code)
    assert len(gate.outbox) == gate.magic_link_rate_per_email + 1
    # Many networks together reach the address's own cap, and then nothing more goes out.
    capped = FakeGate(None, dev=False, sweep_interval=0, magic_link_max_per_email=3)
    capped.allow_email("ann@example.com")
    for i in range(5):
        c = _from(capped, f"2001:db8:{i + 1}::1")
        start = await c.device_start()
        assert await c.magic_link("ann@example.com", start.user_code) == m.MagicLinkSent()
    assert len(capped.outbox) == 3


async def test_one_machines_ipv6_prefix_is_one_budget() -> None:
    """Rotating through the /64s of one routed /48 buys no more requests for an address."""
    gate = FakeGate(None, dev=False, sweep_interval=0, magic_link_rate_per_email=2)
    statuses = []
    for i in range(4):
        try:
            await _from(gate, f"2001:db8:66:{i}::1").magic_link("ann@example.com", "BCDF-GHJK")
            statuses.append(202)
        except GateError as e:
            statuses.append(e.status)
    assert statuses == [202, 202, 429, 429]
    other = _from(gate, "2001:db8:67::1")
    assert await other.magic_link("ann@example.com", "BCDF-GHJK") == m.MagicLinkSent()


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
    wrong = "BCDF-GHJK" if start.user_code != "BCDF-GHJK" else "BCDF-GHJL"
    for typed in (wrong, "not a code", "x" * 10_000, "", wrong.lower(), wrong):
        assert await gate.use_magic_link(link, typed) == "wrong_code"  # non-codes cost nothing
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


async def test_the_link_page_signs_in_its_own_browser_too(invite: Invite) -> None:
    """After the code is typed, the browser that holds the link lands signed in: the page
    sends it to the console with a one-time handoff, traded for that browser's own session
    (seen in production: people typed the code and were left on a page that sent them
    nowhere). The device that asked still gets its session; the handoff works once."""
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, req = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", start.user_code)
    link = gate.outbox[0].link
    assert gate.handoffs == []
    assert await gate.use_magic_link(link, "BCDF-GHJK") == "wrong_code"
    assert gate.handoffs == []  # a wrong code hands nothing over
    assert await gate.use_magic_link(link, start.user_code, handle="ann") == "approved"
    [handoff] = gate.handoffs
    trade = m.TokenRequest(grant_type="handoff", handoff=handoff)
    browser = await invite.anon.token(trade)
    assert browser.party == "ann"
    assert browser.refresh_token is not None
    assert gate.now() - browser.auth_time < timedelta(minutes=1)  # fresh: a key can be set
    with pytest.raises(GateError) as e:
        await invite.anon.token(trade)
    assert e.value.code == "invalid_grant"  # once
    device = await invite.poll(req)
    assert isinstance(device, m.TokenResponse)
    assert device.party == "ann"
    assert sorted(s.via for s in gate.sessions.values()) == ["device:email", "device:email"]


async def test_a_handoff_waits_two_minutes_at_most(invite: Invite) -> None:
    gate = invite.gate
    gate.allow_email("ann@example.com")
    start, _ = await invite.device("ann")
    await invite.anon.magic_link("ann@example.com", start.user_code)
    assert (
        await gate.use_magic_link(gate.outbox[0].link, start.user_code, handle="ann") == "approved"
    )
    gate.advance(timedelta(seconds=m.HANDOFF_TTL_SECONDS))
    with pytest.raises(GateError) as e:
        await invite.anon.token(m.TokenRequest(grant_type="handoff", handoff=gate.handoffs[0]))
    assert e.value.code == "invalid_grant"
    with pytest.raises(GateError) as e:  # never issued
        await invite.anon.token(m.TokenRequest(grant_type="handoff", handoff="A" * 43))
    assert e.value.code == "invalid_grant"
