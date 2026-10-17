"""``kontra login --email``: the device flow, approved through the link the gate mails. The
code is printed here and typed on the link's page, never mailed (decisions I19)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from kontra.auth import LoginError, login, login_device
from kontra.gate_client import models as m
from kontra.profile import Profile
from kontra.testing.fake_gate import FakeGate


@pytest.fixture
async def gate() -> AsyncIterator[FakeGate]:
    g = FakeGate(None, dev=False, sweep_interval=0)  # sign-ups by invitation
    g.allow_email("ann@example.com")
    async with g:
        yield g


async def _the_human(gate: FakeGate, codes: list[str], handle: str | None = None) -> str:
    """Wait for the mail, open the link, type the code the terminal showed."""
    while not gate.outbox or not codes:  # noqa: ASYNC110 -- the gate signals nothing
        await asyncio.sleep(0.01)
    return await gate.use_magic_link(gate.outbox[-1].link, codes[-1], handle=handle)


async def test_login_by_email_writes_the_profile(gate: FakeGate, tmp_path: Path) -> None:
    codes: list[str] = []
    human = asyncio.create_task(_the_human(gate, codes, handle="ann"))  # types it in the form
    profile = await login(
        gate.base_url,
        email=" Ann@Example.com ",
        handle="ann",
        home=tmp_path,
        transport=httpx.ASGITransport(app=gate.app),
        prompt=lambda start: codes.append(start.user_code),
    )
    assert await human == "approved"
    assert (profile.party, profile.tls, profile.dev) == ("ann", True, False)
    assert Profile.load(tmp_path).party == "ann"
    assert [mail.to for mail in gate.outbox] == ["ann@example.com"]
    assert codes[0] not in gate.outbox[0].text


async def test_the_terminal_shows_the_code_and_opens_no_browser(
    gate: FakeGate, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    import webbrowser

    opened: list[str] = []
    monkeypatch.setattr(webbrowser, "open", lambda url, *a, **k: opened.append(url))
    codes: list[str] = []

    async def human() -> str:
        while not gate.pending_user_codes() or not gate.outbox:  # noqa: ASYNC110
            await asyncio.sleep(0.01)
        codes.extend(gate.pending_user_codes())
        return await _the_human(gate, codes, handle="ann")

    task = asyncio.create_task(human())
    tok = await login_device(
        gate.base_url,
        "ann",
        email="ann@example.com",
        transport=httpx.ASGITransport(app=gate.app),
    )
    assert await task == "approved"
    assert tok.party == "ann"
    err = capsys.readouterr().err
    assert f"Check ann@example.com for a sign-in link; when the page asks, type {codes[0]}" in err
    assert opened == []


@pytest.mark.parametrize("email", ["ann", "Ann <ann@example.com>", "ann@[127.0.0.1]"])
async def test_a_bad_address_stops_before_anything_is_sent(email: str) -> None:
    sent: list[httpx.Request] = []
    transport = httpx.MockTransport(lambda r: sent.append(r) or httpx.Response(500))
    with pytest.raises(LoginError) as e:
        await login_device("https://gate.test", email=email, transport=transport)
    assert e.value.code == "invalid_email"
    assert sent == []


async def test_dev_login_takes_no_email(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="email"):
        await login("http://127.0.0.1:1", dev=True, handle="ann", email="a@b.co", home=tmp_path)


async def test_an_unused_link_expires_with_a_hint(gate: FakeGate) -> None:
    """No mail came (not invited, a typo, spam): the code runs out, and the message says
    what to check without the gate having said anything."""
    now = [0.0]

    async def sleep(delay: float) -> None:
        now[0] += delay
        await asyncio.sleep(0)

    with pytest.raises(LoginError) as e:
        await login_device(
            gate.base_url,
            "eve",
            email="eve@example.com",
            transport=httpx.ASGITransport(app=gate.app),
            prompt=lambda start: None,
            sleep=sleep,
            clock=lambda: now[0],
        )
    assert e.value.code == "expired_token"
    assert "by invitation" in e.value.message
    assert gate.outbox == []


async def test_the_request_names_the_devices_code() -> None:
    seen: list[dict[str, str]] = []

    def answer(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/auth/device":
            start = m.DeviceStart(
                device_code="d" * 43,
                user_code="BCDF-GHJK",
                verification_uri="https://gate.test/device",
                verification_uri_complete="https://gate.test/device?user_code=BCDF-GHJK",
                interval=1,
                expires_in=1,
            )
            return httpx.Response(200, json=start.model_dump(mode="json"))
        if request.url.path == "/v1/auth/magic":
            import json

            seen.append(json.loads(request.content))
            return httpx.Response(202, json={"status": "sent"})
        return httpx.Response(400, json={"code": "expired_token", "message": "gone"})

    with pytest.raises(LoginError):
        await login_device(
            "https://gate.test",
            email="ann@example.com",
            transport=httpx.MockTransport(answer),
            prompt=lambda start: None,
            sleep=lambda _: asyncio.sleep(0),
        )
    assert seen == [{"email": "ann@example.com", "device_user_code": "BCDF-GHJK"}]


def _refused_link(retry: int, message: str) -> httpx.MockTransport:
    """A gate whose ``POST /v1/auth/magic`` answers 429 ``rate_limited``."""

    def answer(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/auth/device":
            start = m.DeviceStart(
                device_code="d" * 43,
                user_code="BCDF-GHJK",
                verification_uri="https://gate.test/device",
                verification_uri_complete="https://gate.test/device?user_code=BCDF-GHJK",
                interval=1,
                expires_in=600,
            )
            return httpx.Response(200, json=start.model_dump(mode="json"))
        assert request.url.path == "/v1/auth/magic"
        return httpx.Response(
            429,
            json={
                "code": "rate_limited",
                "message": message,
                "detail": {"retry_after_seconds": retry},
            },
            headers={"Retry-After": str(retry)},
        )

    return httpx.MockTransport(answer)


@pytest.mark.parametrize(
    ("retry", "says"),
    [
        (
            1800,
            "too many sign-in links asked for; try again later; you can ask again in 30 minutes",
        ),
        (
            3600,
            "too many sign-in links asked for; try again later; you can ask again in 60 minutes",
        ),
        (61, "too many sign-in links asked for; try again later; you can ask again in 2 minutes"),
        (11, "too many sign-in links asked for; try again later; you can ask again in 1 minute"),
    ],
)
async def test_a_refused_link_says_how_long_to_wait(retry: int, says: str) -> None:
    """Past the limits the gate answers 429 with ``retry_after_seconds``: the login says the
    wait, in minutes (kontra-cloud#36), and shows no code nobody can use."""
    shown: list[m.DeviceStart] = []
    with pytest.raises(LoginError) as e:
        await login_device(
            "https://gate.test",
            email="ann@example.com",
            transport=_refused_link(retry, "too many sign-in links asked for; try again later"),
            prompt=shown.append,
        )
    assert (e.value.code, e.value.message) == ("rate_limited", says)
    assert shown == []


@pytest.mark.parametrize("retry", [1, 5, 10])
async def test_a_busy_gate_says_only_that(retry: int) -> None:
    """A 429 for a few seconds is the gate being busy: no wait in minutes after it."""
    with pytest.raises(LoginError) as e:
        await login_device(
            "https://gate.test",
            email="ann@example.com",
            transport=_refused_link(retry, "the gate is busy; try again in a few seconds"),
            prompt=lambda start: None,
        )
    assert (e.value.code, e.value.message) == (
        "rate_limited",
        "the gate is busy; try again in a few seconds",
    )
