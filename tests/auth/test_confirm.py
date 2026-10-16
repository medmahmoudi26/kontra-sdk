"""Confirm it's you from the SDK (decisions I23): :func:`kontra.auth.confirm_session` shows
the code and nothing else, polls on the gate's interval, and says plainly why a confirmation
did not complete; a :class:`~kontra.auth.Session` confirms by itself only where that asks
nothing of a person (a dev login), one confirmation at a time, and never for a key."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from kontra.auth import (
    ConfirmError,
    Session,
    confirm_if_instant,
    confirm_in_place,
    confirm_prompt,
    confirm_session,
    never_confirm,
)
from kontra.gate_client import GateClient, GateError
from kontra.gate_client import models as m
from kontra.profile import Profile
from kontra.testing.fake_gate import FakeGate

pytestmark = pytest.mark.asyncio

CODE = "BCDF-GHJK"
CID = "cfm_" + "ab" * 16
ASKED = datetime(2026, 10, 16, 14, 3, tzinfo=UTC)
UNTIL = datetime(2026, 10, 16, 15, 4, tzinfo=UTC)
HOUR = timedelta(hours=1)


def _start(**fields: Any) -> dict[str, Any]:
    return {
        "confirm_id": CID,
        "channel": "email",
        "status": "pending",
        "user_code": CODE,
        "asked_at": ASKED.isoformat(),
        "expires_in": 600,
        "interval": 3,
        **fields,
    }


def _status(status: str = "pending", until: datetime | None = None) -> httpx.Response:
    body = {"confirm_id": CID, "status": status, "confirmed_until": until and until.isoformat()}
    return httpx.Response(200, json=body)


class Scripted:
    """A gate that answers ``POST /v1/auth/confirm`` with ``start`` and each poll with the
    next of ``polls`` (an exception is raised as the transport's)."""

    def __init__(self, start: httpx.Response, polls: list[httpx.Response | Exception]) -> None:
        self.start, self.polls = start, list(polls)
        self.requests: list[httpx.Request] = []
        self.transport = httpx.MockTransport(self._handle)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method == "POST":
            return self.start
        assert request.url.path == f"/v1/auth/confirm/{CID}"
        answer = self.polls.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def client(self) -> GateClient:
        async def token() -> str:
            return "t"

        return GateClient("http://gate.test", token=token, transport=self.transport)


class Ticks:
    """An injected sleep that moves a monotonic clock instead of waiting."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def clock(self) -> float:
        return self.now


async def _confirm(gate: Scripted, **kw: Any) -> tuple[datetime, list[m.ConfirmStart], Ticks]:
    ticks = Ticks()
    shown: list[m.ConfirmStart] = []
    async with gate.client() as c:
        until = await confirm_session(
            c, show=shown.append, sleep=ticks.sleep, clock=ticks.clock, **kw
        )
    return until, shown, ticks


# --------------------------------------------------------------------------- confirm_session


async def test_it_shows_the_code_polls_and_returns_until_when() -> None:
    gate = Scripted(
        httpx.Response(201, json=_start()), [_status(), _status(), _status("confirmed", UNTIL)]
    )
    until, shown, ticks = await _confirm(gate)
    assert until == UNTIL
    assert [s.user_code for s in shown] == [CODE]
    assert ticks.slept == [3, 3, 3]
    assert all(r.headers["authorization"] == "Bearer t" for r in gate.requests)


async def test_the_prompt_says_where_and_when_and_never_shows_a_link() -> None:
    start = m.ConfirmStart.model_validate(_start())
    text = confirm_prompt(start, "create an API key")
    assert text.startswith("Confirm it's you to create an API key: Kontra emailed a link")
    assert "the address you sign in with" in text
    assert "(asked at 14:03 UTC)" in text
    assert f"\n    {CODE}\n" in text
    assert "the mail never carries it" in text
    assert "10 minutes" in text
    assert "http" not in text
    assert confirm_prompt(start).startswith("Confirm it's you: ")


async def test_it_resumes_once_the_session_is_confirmed_by_any_confirmation() -> None:
    """Another tab's confirmation confirms the session: this one's poll says so and the
    change goes on, though its own link was never used."""
    gate = Scripted(httpx.Response(201, json=_start()), [_status("pending", UNTIL)])
    until, _, _ = await _confirm(gate)
    assert until == UNTIL


async def test_extend_waits_for_its_own_confirmation() -> None:
    later = UNTIL + timedelta(minutes=30)
    gate = Scripted(
        httpx.Response(201, json=_start()),
        [_status("pending", UNTIL), _status("confirmed", later)],
    )
    until, _, ticks = await _confirm(gate, extend=True)
    assert until == later
    assert len(ticks.slept) == 2


async def test_a_dev_login_is_confirmed_at_once_with_nothing_shown() -> None:
    done = _start(
        channel="dev",
        status="confirmed",
        user_code=None,
        expires_in=0,
        confirmed_until=UNTIL.isoformat(),
    )
    gate = Scripted(httpx.Response(201, json=done), [])
    until, shown, ticks = await _confirm(gate)
    assert (until, shown, ticks.slept) == (UNTIL, [], [])
    assert len(gate.requests) == 1


@pytest.mark.parametrize(
    ("polls", "code"),
    [
        ([_status(), _status("burned")], "burned"),
        ([_status("expired")], "expired"),
        ([_status("confirmed", None)], "expired"),  # confirmed, and its hour already over
    ],
)
async def test_what_did_not_complete_says_so(polls: list[httpx.Response], code: str) -> None:
    gate = Scripted(httpx.Response(201, json=_start()), list(polls))
    with pytest.raises(ConfirmError) as e:
        await _confirm(gate)
    assert e.value.code == code
    assert "nothing was changed" in e.value.message


async def test_it_gives_up_when_the_link_runs_out_here_too() -> None:
    gate = Scripted(httpx.Response(201, json=_start(expires_in=7)), [_status(), _status()])
    with pytest.raises(ConfirmError) as e:
        await _confirm(gate)
    assert e.value.code == "expired"
    assert len(gate.requests) == 3  # the start and two polls, then the deadline


async def test_a_dropped_connection_is_retried_less_often() -> None:
    gate = Scripted(
        httpx.Response(201, json=_start()),
        [httpx.ConnectError("down"), httpx.ConnectError("down"), _status("confirmed", UNTIL)],
    )
    until, _, ticks = await _confirm(gate)
    assert until == UNTIL
    assert ticks.slept == [3, 6, 12]


@pytest.mark.parametrize(
    ("answer", "code", "says"),
    [
        (
            httpx.Response(
                409,
                json={"code": "confirm_unavailable", "message": "no", "detail": None},
            ),
            "unavailable",
            "kontra login",
        ),
        (
            httpx.Response(
                429,
                json={
                    "code": "rate_limited",
                    "message": "too many sign-in links asked for; try again later",
                    "detail": {"retry_after_seconds": 3600},
                },
            ),
            "rate_limited",
            "too many sign-in links asked for; try again later; you can ask again in 60",
        ),
        (
            httpx.Response(
                403, json={"code": "fresh_login_required", "message": "no", "detail": None}
            ),
            "api_key",
            "unset KONTRA_API_KEY",
        ),
    ],
)
async def test_a_refused_start_is_a_confirm_error(
    answer: httpx.Response, code: str, says: str
) -> None:
    with pytest.raises(ConfirmError) as e:
        await _confirm(Scripted(answer, []))
    assert e.value.code == code
    assert says in e.value.message


async def test_other_refusals_stay_gate_errors() -> None:
    gate = Scripted(
        httpx.Response(401, json={"code": "invalid_token", "message": "revoked", "detail": None}),
        [],
    )
    with pytest.raises(GateError) as e:
        await _confirm(gate)
    assert e.value.code == "invalid_token"


# --------------------------------------------------------------------------- Session


@pytest.fixture
async def dev_gate() -> AsyncIterator[FakeGate]:
    gate = FakeGate(None, sweep_interval=0)
    async with gate:
        yield gate


@pytest.fixture
async def prod_gate() -> AsyncIterator[FakeGate]:
    gate = FakeGate(None, dev=False, sweep_interval=0)
    gate.allow_email("ann@example.com")
    async with gate:
        yield gate


def _session(gate: FakeGate, tok: m.TokenResponse, **kw: Any) -> Session:
    profile = Profile(
        gate_url=gate.base_url,
        temporal_address="temporal.invalid:7233",
        party=tok.party,
        namespace=tok.party,
        refresh_token=tok.refresh_token,
        auth_time=tok.auth_time,
        tls=False,
        dev=gate.dev,
    )
    return Session(profile, transport=httpx.ASGITransport(app=gate.app), **kw)


async def _dev_session(gate: FakeGate, handle: str = "dana", **kw: Any) -> Session:
    anon = GateClient(gate.base_url, transport=httpx.ASGITransport(app=gate.app))
    await gate.ensure_party(handle)
    return _session(gate, await anon.dev_login(handle), **kw)


async def _email_session(gate: FakeGate, **kw: Any) -> Session:
    anon = GateClient(gate.base_url, transport=httpx.ASGITransport(app=gate.app))
    start = await anon.device_start()
    await anon.magic_link("ann@example.com", start.user_code)
    assert await gate.use_magic_link(gate.outbox[-1].link, start.user_code, handle="ann") == (
        "approved"
    )
    req = m.TokenRequest(grant_type=m.DEVICE_GRANT, device_code=start.device_code)
    return _session(gate, await anon.token(req), **kw)


def _confirmations(gate: FakeGate) -> int:
    return len(gate._confirmations)  # pyright: ignore[reportPrivateUsage]


async def test_a_dev_login_confirms_by_itself_past_the_hour(dev_gate: FakeGate) -> None:
    async with await _dev_session(dev_gate) as dana:
        dev_gate.advance(HOUR)
        assert (await dana.gate.create_api_key("past the hour")).label == "past the hour"
    assert dev_gate.outbox == []


async def test_by_default_nobody_is_mailed(prod_gate: FakeGate) -> None:
    """Library code against a real gate gets the gate's answer: a program nobody watches
    must never make the gate mail a person who did not ask."""
    async with await _email_session(prod_gate) as ann:
        prod_gate.advance(HOUR)
        with pytest.raises(GateError) as e:
            await ann.gate.create_api_key("unconfirmed")
        assert e.value.code == "confirmation_required"
    assert _confirmations(prod_gate) == 0
    assert [x.subject for x in prod_gate.outbox] == ["Your Kontra sign-in link"]


async def test_never_confirm_leaves_even_a_dev_login_refused(dev_gate: FakeGate) -> None:
    async with await _dev_session(dev_gate, confirm=never_confirm) as dana:
        dev_gate.advance(HOUR)
        with pytest.raises(GateError) as e:
            await dana.gate.create_api_key("x")
        assert e.value.code == "confirmation_required"
    assert _confirmations(dev_gate) == 0


async def test_the_default_confirms_only_the_dev_channel() -> None:
    asked: list[str] = []

    class Gate:
        async def confirm_start(self) -> m.ConfirmStart:
            asked.append("start")
            raise AssertionError("never for email or sign_in")

    for how in ("email", "sign_in"):
        need = m.ConfirmationRequired(confirm_with=how, confirm_ttl_seconds=3600)  # type: ignore[arg-type]
        assert await confirm_if_instant(Gate(), "publish", need) is False  # type: ignore[arg-type]
    assert asked == []


async def test_one_confirmation_serves_changes_made_at_once(dev_gate: FakeGate) -> None:
    calls: list[str] = []

    async def counting(gate: GateClient, route: str, need: m.ConfirmationRequired) -> bool:
        calls.append(route)
        await asyncio.sleep(0.01)  # the other change is refused meanwhile
        return await confirm_if_instant(gate, route, need)

    async with await _dev_session(dev_gate, confirm=counting) as dana:
        dev_gate.advance(HOUR)
        made = await asyncio.gather(
            dana.gate.create_api_key("one"), dana.gate.create_api_key("two")
        )
    assert sorted(k.label for k in made) == ["one", "two"]
    assert len(calls) == 1
    assert _confirmations(dev_gate) == 1


async def test_confirm_in_place_waits_for_the_person(prod_gate: FakeGate) -> None:
    typed: list[asyncio.Task[str]] = []

    def show(start: m.ConfirmStart) -> None:  # the person reads the code, opens the mail
        link = prod_gate.outbox[-1].link
        assert start.user_code is not None
        typed.append(asyncio.create_task(prod_gate.use_magic_link(link, start.user_code)))

    async def no_wait(_: float) -> None:
        await asyncio.sleep(0)

    async with await _email_session(
        prod_gate, confirm=confirm_in_place(show, sleep=no_wait)
    ) as ann:
        prod_gate.advance(HOUR)
        assert (await ann.gate.create_api_key("in place")).label == "in place"
    assert [await t for t in typed] == ["confirmed"]


async def test_confirm_in_place_says_when_only_a_sign_in_will_do() -> None:
    need = m.ConfirmationRequired(confirm_with="sign_in", confirm_ttl_seconds=3600)
    with pytest.raises(ConfirmError) as e:
        await confirm_in_place()(object(), "publish", need)  # type: ignore[arg-type]
    assert e.value.code == "unavailable"


async def test_session_confirm_starts_a_new_hour(prod_gate: FakeGate) -> None:
    shown: list[m.ConfirmStart] = []

    def show(start: m.ConfirmStart) -> None:
        shown.append(start)
        link = prod_gate.outbox[-1].link
        asyncio.create_task(prod_gate.use_magic_link(link, start.user_code or ""))  # noqa: RUF006

    async def no_wait(_: float) -> None:
        await asyncio.sleep(0)

    async with await _email_session(prod_gate, sleep=no_wait) as ann:
        prod_gate.advance(timedelta(minutes=50))  # confirmed by the sign-in, ten minutes left
        until = await asyncio.wait_for(ann.confirm(show=show), 30)
    assert until - prod_gate.now() > timedelta(minutes=59)
    assert len(shown) == 1


async def test_a_key_never_confirms_and_never_asks(dev_gate: FakeGate) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    key = m.API_KEY_PREFIX + "A" * 43
    session = Session(
        api_key=key, gate_url="https://gate.test", transport=httpx.MockTransport(handler)
    )
    async with session:
        with pytest.raises(ConfirmError) as e:
            await session.confirm()
    assert e.value.code == "api_key"
    assert requests == []
