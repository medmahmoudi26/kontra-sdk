""" "Confirm it's you" on the fake gate, as the platform does it (decisions I23): reads need a
person, changes need the session confirmed within the hour (a sign-in counts); a
confirmation is a mailed link plus the code only the asking screen shows, consumed once, burnt
by three wrong codes, bound to the session that asked and to nothing else, never a sign-in;
an API key never confirms; a dev gate confirms a development login at once; and the mail
shares every limit of sign-in mail."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from datetime import timedelta

import httpx
import pytest

from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.testing.fake_gate import AUD_API, FakeGate, SentMail

from .support import ScriptedControl

pytestmark = pytest.mark.asyncio

ANN = "ann@example.com"
HOUR = timedelta(hours=1)
_LINK = re.compile(r"http://localhost/auth/magic\?t=[A-Za-z0-9_-]{43}")


class Person:
    """One sign-in of a person: a gate client whose access token is refreshed on the gate's
    clock (a token provider that rotates its refresh token on a 401)."""

    def __init__(self, gate: FakeGate, tokens: m.TokenResponse, client: str) -> None:
        self.gate = gate
        self._access = tokens.access_token
        self._refresh = tokens.refresh_token
        self._stale = False
        transport = httpx.ASGITransport(app=gate.app, client=(client, 1234))
        self._anon = GateClient(gate.base_url, transport=transport)
        self.client = GateClient(gate.base_url, token=self, transport=transport)
        claims = gate.keys.verify(
            tokens.access_token, audience=AUD_API, issuer=gate.issuer, now=gate.now()
        )
        self.session_id: str = claims["sid"]

    async def __call__(self) -> str:
        if self._stale:
            assert self._refresh is not None
            req = m.TokenRequest(grant_type="refresh_token", refresh_token=self._refresh)
            tok = await self._anon.token(req)
            self._access, self._refresh, self._stale = tok.access_token, tok.refresh_token, False
        return self._access

    def invalidate(self, token: str) -> None:
        self._stale = True


class Gate:
    """A production-like gate (not dev, sign-up by invitation, Ann invited) and the ways in."""

    def __init__(self, gate: FakeGate) -> None:
        self.gate = gate

    def anon(self, client: str = "203.0.113.5") -> GateClient:
        transport = httpx.ASGITransport(app=self.gate.app, client=(client, 1234))
        return GateClient(self.gate.base_url, transport=transport)

    async def email_sign_in(
        self, address: str = ANN, handle: str = "ann", client: str = "203.0.113.5"
    ) -> Person:
        """``kontra login --email``: a device login approved through the mailed link."""
        anon = self.anon(client)
        start = await anon.device_start()
        await anon.magic_link(address, start.user_code)
        mail = self.gate.outbox[-1]
        assert await self.gate.use_magic_link(mail.link, start.user_code, handle=handle) == (
            "approved"
        )
        req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
        return Person(self.gate, await anon.token(req), client)

    def mails(self, to: str = ANN) -> list[SentMail]:
        return [
            x for x in self.gate.outbox if x.subject == "Confirm it's you on Kontra" and x.to == to
        ]


@pytest.fixture
async def prod() -> AsyncIterator[Gate]:
    gate = FakeGate(ScriptedControl(), dev=False, sweep_interval=0)
    gate.allow_email(ANN)
    async with gate:
        yield Gate(gate)


async def _code(call: object) -> tuple[int, str]:
    try:
        await call  # type: ignore[misc]
    except GateError as e:
        return e.status, e.code
    return 200, "ok"


async def _need(call: object) -> m.ConfirmationRequired:
    """What the gate needs before ``call``, a change it must refuse for want of it."""
    with pytest.raises(GateError) as e:
        await call  # type: ignore[misc]
    assert (e.value.status, e.value.code) == (403, "confirmation_required"), e.value
    return m.ConfirmationRequired.model_validate(e.value.detail)


async def _confirm(prod: Gate, ann: Person) -> m.ConfirmStart:
    """Ask, then type the code this screen shows into the mailed link's page."""
    start = await ann.client.confirm_start()
    assert start.user_code is not None
    assert await prod.gate.use_magic_link(prod.mails()[-1].link, start.user_code) == "confirmed"
    return start


# --------------------------------------------------------------------------- the rule


async def test_reads_need_a_person_and_changes_a_confirmation(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    assert (await ann.client.create_api_key("now")).label == "now"  # the sign-in counts
    prod.gate.advance(HOUR - timedelta(seconds=1))
    assert (await ann.client.create_api_key("just in time")).label == "just in time"
    prod.gate.advance(timedelta(seconds=1))  # exactly an hour after the sign-in
    assert await _need(ann.client.create_api_key("late")) == m.ConfirmationRequired(
        confirm_with="email", confirm_ttl_seconds=3600
    )
    assert len((await ann.client.api_keys()).keys) == 2  # reads ask for nothing
    assert len((await ann.client.sessions()).sessions) == 1
    assert prod.mails() == []  # and a refusal mails nobody


async def test_a_confirmation_counts_from_its_own_time(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(timedelta(minutes=50))
    await _confirm(prod, ann)
    prod.gate.advance(timedelta(minutes=59))  # 109 minutes after the sign-in
    assert (await ann.client.create_api_key("still")).label == "still"
    prod.gate.advance(timedelta(minutes=1))
    await _need(ann.client.create_api_key("over"))


async def test_the_whole_confirmation(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    await ann.client.me()  # a new access token, on the gate's clock
    asked = prod.gate.now()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=prod.gate.app), base_url=prod.gate.base_url
    ) as http:
        r = await http.post("/v1/auth/confirm", headers={"Authorization": f"Bearer {await ann()}"})
    assert (r.status_code, r.headers["cache-control"]) == (201, "no-store")
    start = m.ConfirmStart.model_validate_json(r.content)
    assert (start.channel, start.status, start.expires_in, start.interval) == (
        "email",
        "pending",
        600,
        3,
    )
    assert start.user_code is not None
    assert m.is_user_code(start.user_code)
    assert abs(start.asked_at - asked) < timedelta(seconds=5)
    assert start.user_code in prod.gate.pending_confirm_codes()

    [mail] = prod.mails()
    assert mail.to == ANN
    assert _LINK.fullmatch(mail.link)
    assert mail.link in mail.text
    assert start.user_code not in mail.text  # the code is only on the asking screen
    assert start.user_code.replace("-", "") not in mail.text
    assert ann.session_id[:12] in mail.text  # which sign-in, so a stranger's can be ended
    assert ann.session_id not in mail.text
    assert "a link by email" in mail.text
    assert f"asked at {start.asked_at:%H:%M} UTC" in mail.text

    polled = await ann.client.confirm_status(start.confirm_id)
    assert (polled.status, polled.confirmed_until) == ("pending", None)
    handoffs = list(prod.gate.handoffs)
    assert await prod.gate.use_magic_link(mail.link, start.user_code.lower()) == "confirmed"
    assert prod.gate.handoffs == handoffs  # a confirmation is never a sign-in
    polled = await ann.client.confirm_status(start.confirm_id)
    assert polled.status == "confirmed"
    assert polled.confirmed_until is not None
    assert abs(polled.confirmed_until - (prod.gate.now() + HOUR)) < timedelta(seconds=5)
    assert (await ann.client.create_api_key("confirmed")).label == "confirmed"
    assert await prod.gate.use_magic_link(mail.link, start.user_code) == "gone"  # once


async def test_only_the_session_that_asked_is_confirmed(prod: Gate) -> None:
    laptop = await prod.email_sign_in()
    phone = await prod.email_sign_in(client="198.51.100.7")
    prod.gate.advance(HOUR)
    start = await _confirm(prod, laptop)
    assert (await laptop.client.create_api_key("laptop")).label == "laptop"
    await _need(phone.client.create_api_key("phone"))
    # The confirmation is the laptop's alone: the phone cannot even see it.
    assert await _code(phone.client.confirm_status(start.confirm_id)) == (404, "not_found")
    for other in ("cfm_" + "0" * 32, "cfm_nope", "..", "cfm_" + "A" * 32):
        assert await _code(laptop.client.confirm_status(other)) == (404, "not_found")


async def test_several_pending_confirmations_and_every_one_resumes(prod: Gate) -> None:
    """Two tabs of one sign-in (or a person and a thief of it): neither cancels the other,
    each link checks its own code, and every poll resumes once the session is confirmed."""
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    first = await ann.client.confirm_start()
    second = await ann.client.confirm_start()
    assert first.user_code != second.user_code
    first_mail, second_mail = prod.mails()
    assert second.user_code is not None
    assert await prod.gate.use_magic_link(second_mail.link, first.user_code or "") == "wrong_code"
    assert await prod.gate.use_magic_link(second_mail.link, second.user_code) == "confirmed"
    one = await ann.client.confirm_status(first.confirm_id)
    two = await ann.client.confirm_status(second.confirm_id)
    assert (one.status, two.status) == ("pending", "confirmed")
    assert one.confirmed_until == two.confirmed_until is not None
    assert first_mail.link != second_mail.link


# --------------------------------------------------------------------------- the link


async def test_three_wrong_codes_burn_it_and_a_non_code_costs_nothing(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    link = prod.mails()[-1].link
    for blind in ("", "not a code", "x" * 10_000, "AEIO-UAEI"):  # a scanner's blind submit
        assert await prod.gate.use_magic_link(link, blind) == "wrong_code"
    candidates = ("BCDF-GHJK", "BCDF-GHJL", "ZZZZ-ZZZZ", "XXXX-XXXX")
    first, second, third = [c for c in candidates if c != start.user_code][:3]
    for typed in (first, second):
        assert await prod.gate.use_magic_link(link, typed) == "wrong_code"
    assert (await ann.client.confirm_status(start.confirm_id)).status == "pending"
    assert await prod.gate.use_magic_link(link, third) == "wrong_code"
    assert (await ann.client.confirm_status(start.confirm_id)).status == "burned"
    assert await prod.gate.use_magic_link(link, start.user_code or "") == "gone"
    await _need(ann.client.create_api_key("burned"))


async def test_a_confirmation_expires_with_its_ten_minutes(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    prod.gate.advance(timedelta(minutes=10))
    assert (await ann.client.confirm_status(start.confirm_id)).status == "expired"
    assert await prod.gate.use_magic_link(prod.mails()[-1].link, start.user_code or "") == "gone"
    assert prod.gate.pending_confirm_codes() == []


async def test_an_ended_sign_in_has_nothing_to_confirm(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    await ann.client.revoke()
    assert await prod.gate.use_magic_link(prod.mails()[-1].link, start.user_code or "") == "gone"
    assert prod.gate.sessions[ann.session_id].confirmed_at is None


async def test_an_ended_sign_ins_link_is_used_up_whatever_is_typed(prod: Gate) -> None:
    """The platform's order: the session is checked before the code, so even a wrong code
    (or none) on an ended sign-in's link uses the link up, and nothing is confirmed."""
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    link = prod.mails()[-1].link
    await ann.client.revoke()
    wrong = "BCDF-GHJK" if start.user_code != "BCDF-GHJK" else "BCDF-GHJL"
    assert await prod.gate.use_magic_link(link, wrong) == "gone"
    [rec] = [r for r in prod.gate._magic.values() if r.confirmation_id == start.confirm_id]
    assert (rec.consumed_at is not None, rec.attempts) == (True, 0)
    assert await prod.gate.use_magic_link(link, start.user_code or "") == "gone"
    assert prod.gate.sessions[ann.session_id].confirmed_at is None


async def test_an_address_taken_off_the_list_cannot_confirm(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    prod.gate.disallow_email(ANN)
    assert await prod.gate.use_magic_link(prod.mails()[-1].link, start.user_code or "") == "gone"
    assert await _need(ann.client.create_api_key("x")) == m.ConfirmationRequired(
        confirm_with="sign_in", confirm_ttl_seconds=3600
    )


async def test_sign_in_and_confirmation_codes_never_cross(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    prod.gate.advance(HOUR)
    start = await ann.client.confirm_start()
    confirm_link = prod.mails()[-1].link
    anon = prod.anon()
    device = await anon.device_start()
    await anon.magic_link(ANN, device.user_code)
    sign_in_link = prod.gate.outbox[-1].link
    # A device's code on a confirmation link, a confirmation's code on a sign-in link.
    assert await prod.gate.use_magic_link(confirm_link, device.user_code) == "wrong_code"
    assert await prod.gate.use_magic_link(sign_in_link, start.user_code or "") == "wrong_code"
    assert device.user_code in prod.gate.pending_user_codes()
    assert (await ann.client.confirm_status(start.confirm_id)).status == "pending"


# --------------------------------------------------------------------------- who can


async def test_an_api_key_never_confirms(prod: Gate) -> None:
    ann = await prod.email_sign_in()
    made = await ann.client.create_api_key("ci")
    tok = await prod.anon().token(m.TokenRequest(grant_type="api_key", api_key=made.key))
    program = Person(prod.gate, tok, "203.0.113.5")
    for call in (
        program.client.confirm_start(),
        program.client.confirm_status("cfm_" + "0" * 32),
        program.client.api_keys(),
        program.client.sessions(),
        program.client.create_api_key("another"),
    ):
        assert await _code(call) == (403, "fresh_login_required")
    assert prod.mails() == []


async def test_a_party_with_no_address_can_only_sign_in_again() -> None:
    gate = FakeGate(ScriptedControl(), dev=False, github=True, sweep_interval=0)
    async with gate:
        anon = Gate(gate).anon()
        start = await anon.device_start()
        assert await gate.approve_device(start.user_code, "gh", via="device:github")
        req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
        gh = Person(gate, await anon.token(req), "203.0.113.5")
        gate.advance(HOUR)
        need = await _need(gh.client.create_api_key("x"))
        assert need.confirm_with == "sign_in"
        with pytest.raises(GateError) as e:
            await gh.client.confirm_start()
        assert (e.value.status, e.value.code) == (409, "confirm_unavailable")
        assert e.value.detail == {"confirm_with": "sign_in"}
        assert gate.outbox == []


async def test_a_dev_gate_confirms_a_development_login_at_once(scripted: ScriptedControl) -> None:
    gate = FakeGate(scripted, sweep_interval=0)  # dev, and devices approve themselves
    async with gate:
        anon = Gate(gate).anon()
        dev = Person(gate, await anon.dev_login("dana"), "203.0.113.5")
        start = await anon.device_start()
        req = m.TokenRequest(
            grant_type=m.DEVICE_GRANT, device_code=start.device_code, handle="dev2"
        )
        device = Person(gate, await anon.token(req), "203.0.113.5")
        assert gate.sessions[device.session_id].via == "device:dev"  # as the platform says
        gate.advance(HOUR)
        for who in (dev, device):
            assert (await _need(who.client.create_api_key("x"))).confirm_with == "dev"
            done = await who.client.confirm_start()
            assert (done.channel, done.status, done.user_code) == ("dev", "confirmed", None)
            assert done.confirmed_until is not None
            assert (await who.client.create_api_key("x")).label == "x"
        assert gate.outbox == []  # no mail, no code


async def test_an_email_login_on_a_dev_gate_still_confirms_by_mail(
    scripted: ScriptedControl,
) -> None:
    gate = FakeGate(scripted, sweep_interval=0, device_auto_approve=False)
    async with gate:
        ann = await Gate(gate).email_sign_in()
        gate.advance(HOUR)
        assert (await _need(ann.client.create_api_key("x"))).confirm_with == "email"
        assert (await ann.client.confirm_start()).channel == "email"


# --------------------------------------------------------------------------- limits


async def test_confirmation_mail_shares_the_sign_in_budget(prod: Gate) -> None:
    ann = await prod.email_sign_in()  # one sign-in link asked from this network
    anon = prod.anon()
    for _ in range(3):
        await anon.magic_link(ANN, "BCDF-GHJK")  # three more (no mail: no such device)
    await ann.client.confirm_start()  # the fifth: asking needs no stale session
    with pytest.raises(GateError) as e:
        await ann.client.confirm_start()
    with pytest.raises(GateError) as same:
        await anon.magic_link(ANN, "BCDF-GHJK")
    assert (e.value.status, e.value.code, e.value.message, e.value.detail) == (
        same.value.status,
        same.value.code,
        same.value.message,
        same.value.detail,
    )
    assert e.value.status == 429


async def test_one_sign_in_asks_at_most_five_times_an_hour() -> None:
    gate = FakeGate(ScriptedControl(), dev=False, sweep_interval=0, magic_link_rate_per_email=100)
    gate.allow_email(ANN)
    async with gate:
        ann = await Gate(gate).email_sign_in()
        gate.advance(HOUR)
        for _ in range(5):
            await ann.client.confirm_start()
        gate.advance(timedelta(minutes=20))
        with pytest.raises(GateError) as e:
            await ann.client.confirm_start()
        assert (e.value.status, e.value.code) == (429, "rate_limited")
        assert e.value.detail == {"retry_after_seconds": 40 * 60}
        gate.advance(timedelta(minutes=40))
        assert (await ann.client.confirm_start()).status == "pending"


async def test_the_mail_caps_count_confirmation_links() -> None:
    gate = FakeGate(ScriptedControl(), dev=False, sweep_interval=0, magic_link_max_per_email=2)
    gate.allow_email(ANN)
    async with gate:
        prod = Gate(gate)
        ann = await prod.email_sign_in()  # mail 1
        await ann.client.confirm_start()  # mail 2
        third = await ann.client.confirm_start()  # answered as usual, and no mail goes out
        assert third.status == "pending"
        assert len(prod.mails()) == 1


async def test_the_refusal_says_the_gates_own_window() -> None:
    """The 403's words follow ``confirm_ttl``, as the platform's follow KONTRA_CONFIRM_TTL."""
    gate = FakeGate(ScriptedControl(), dev=False, sweep_interval=0, confirm_ttl=2 * HOUR)
    gate.allow_email(ANN)
    async with gate:
        ann = await Gate(gate).email_sign_in()
        gate.advance(2 * HOUR)
        with pytest.raises(GateError) as e:
            await ann.client.create_api_key("x")
    assert e.value.code == "confirmation_required"
    assert "(once every 2 hours per sign-in)" in e.value.message
    assert e.value.detail == {"confirm_with": "email", "confirm_ttl_seconds": 7200}


async def test_freshness_is_confirm_ttl_by_its_old_name() -> None:
    with pytest.warns(DeprecationWarning, match="confirm_ttl"):
        gate = FakeGate(None, sweep_interval=0, freshness=timedelta(minutes=5))
    assert gate.confirm_ttl == gate.freshness == timedelta(minutes=5)
    assert FakeGate(None, sweep_interval=0).confirm_ttl == HOUR
