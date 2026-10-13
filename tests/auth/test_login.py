"""Login: the RFC 8628 device-flow state machine, dev login, and writing the profile + key."""

from __future__ import annotations

import webbrowser
from pathlib import Path
from typing import Any

import httpx
import pytest

import kontra.codec.age as age
from kontra.auth import DEVICE_GRANT, LoginError, dev_login, login, login_device
from kontra.contract.names import NameError_
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.profile import KEY_FILE, PROFILE_FILE, Profile

from ._fakes import FakeClock, FakeGate, err

TOKEN = "/v1/auth/token"


class Mono:
    """A monotonic clock driven by the fake sleep."""

    def __init__(self) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.t += delay


@pytest.fixture
def gate() -> FakeGate:
    return FakeGate(FakeClock())


@pytest.fixture
def mono() -> Mono:
    return Mono()


@pytest.fixture(autouse=True)
def no_browser(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    opened: list[str] = []

    def fake_open(url: str, *a: Any, **k: Any) -> bool:
        opened.append(url)
        return True

    monkeypatch.setattr(webbrowser, "open", fake_open)
    return opened


def pending() -> httpx.Response:
    return err(400, "authorization_pending", "waiting")


def _quiet(_: m.DeviceStart) -> None:
    pass


async def device(gate: FakeGate, mono: Mono, **kw: Any) -> m.TokenResponse:
    kw.setdefault("prompt", _quiet)
    return await login_device(
        "https://gate.test", transport=gate.transport, sleep=mono.sleep, clock=mono, **kw
    )


async def test_pending_slow_down_then_success(
    gate: FakeGate, mono: Mono, no_browser: list[str]
) -> None:
    gate.device_script = [pending(), err(400, "slow_down"), pending()]
    shown: list[m.DeviceStart] = []
    tok = await device(gate, mono, handle="alice", prompt=shown.append)
    assert tok.access_token == "acc1"
    assert mono.sleeps == [5, 5, 10, 10]  # slow_down adds 5 s for the rest of the flow
    polls = [r for r in gate.requests if r.url.path == TOKEN]
    assert len(polls) == 4
    assert {r.content for r in polls} == {
        m.TokenRequest(grant_type=DEVICE_GRANT, device_code="dev-code", handle="alice")
        .model_dump_json()
        .encode()
    }
    assert shown == [gate.device]
    assert no_browser == [gate.device.verification_uri_complete]


async def test_a_gate_that_signs_in_by_email_only_is_told_at_once(
    gate: FakeGate, mono: Mono, no_browser: list[str]
) -> None:
    """A production gate's device page approves nothing (no GitHub, no dev login, and it
    mails no links: the review's F1). Plain `kontra login` says what works instead of
    opening that page and polling for ten minutes."""
    gate.github, gate.dev = False, False
    with pytest.raises(LoginError) as ei:
        await device(gate, mono)
    assert ei.value.code == "email_sign_in"
    assert "kontra login --email" in ei.value.message
    assert gate.calls("/v1/auth/device") == 0
    assert no_browser == []


async def test_a_gate_without_the_info_route_is_tried_as_before(gate: FakeGate, mono: Mono) -> None:
    gate.github = None
    tok = await device(gate, mono, open_browser=False)
    assert tok.access_token == "acc1"


async def test_slow_down_honours_a_larger_interval_from_the_gate(
    gate: FakeGate, mono: Mono
) -> None:
    gate.device_script = [err(400, "slow_down", detail={"interval": 30})]
    await device(gate, mono, open_browser=False)
    assert mono.sleeps == [5, 30]


@pytest.mark.parametrize("code", ["access_denied", "expired_token"])
async def test_terminal_errors(gate: FakeGate, mono: Mono, code: str) -> None:
    gate.device_script = [pending(), err(400, code, "over")]
    with pytest.raises(LoginError) as ei:
        await device(gate, mono)
    assert (ei.value.code, ei.value.message) == (code, "over")
    assert gate.calls(TOKEN) == 2


async def test_the_code_expires_locally(gate: FakeGate, mono: Mono) -> None:
    gate.device = gate.device.model_copy(update={"expires_in": 12})
    gate.device_script = [pending()] * 10
    with pytest.raises(LoginError) as ei:
        await device(gate, mono)
    assert ei.value.code == "expired_token"
    assert gate.calls(TOKEN) == 2  # polled at t+5 and t+10, gave up at t+15
    assert mono.sleeps == [5, 5, 5]


async def test_connection_errors_back_off_exponentially(gate: FakeGate, mono: Mono) -> None:
    script: list[Any] = ["drop", "drop", pending()]

    def handle(r: httpx.Request) -> httpx.Response:
        if r.url.path == TOKEN and script:
            step = script.pop(0)
            if step == "drop":
                raise httpx.ConnectError("refused", request=r)
            return step
        return gate.handle(r)

    tok = await login_device(
        "https://gate.test",
        transport=httpx.MockTransport(handle),
        sleep=mono.sleep,
        clock=mono,
        prompt=lambda _: None,
        open_browser=False,
    )
    assert tok.party == "alice"
    assert mono.sleeps == [5, 10, 20, 20]


async def test_unexpected_errors_propagate(gate: FakeGate, mono: Mono) -> None:
    gate.device_script = [err(400, "invalid_grant", "device code is not valid")]
    with pytest.raises(GateError) as ei:
        await device(gate, mono)
    assert ei.value.code == "invalid_grant"


@pytest.mark.parametrize("handle", ["Alice", "alice\n", "a--b", "kontra", "x" * 33])
async def test_handle_is_validated_before_anything_is_sent(
    gate: FakeGate, mono: Mono, handle: str
) -> None:
    with pytest.raises(NameError_):
        await device(gate, mono, handle=handle)
    with pytest.raises(NameError_):
        await dev_login("http://localhost:8400", handle, transport=gate.transport)
    assert gate.requests == []


async def test_only_http_urls_are_opened(gate: FakeGate, mono: Mono, no_browser: list[str]) -> None:
    gate.device = gate.device.model_copy(update={"verification_uri_complete": "file:///etc/passwd"})
    await device(gate, mono)
    assert no_browser == []


@pytest.mark.parametrize(
    ("url", "ok"),
    [
        ("https://gate.example", True),
        ("http://localhost:8400", True),
        ("http://127.0.0.1:8400", True),
        ("http://gate.example", False),
        ("ftp://gate.example", False),
        ("gate.example", False),
    ],
)
async def test_tokens_only_travel_over_tls_or_to_this_machine(
    gate: FakeGate, mono: Mono, url: str, ok: bool
) -> None:
    run = login_device(
        url, transport=gate.transport, sleep=mono.sleep, clock=mono, prompt=lambda _: None
    )
    if ok:
        await run
    else:
        with pytest.raises(LoginError) as ei:
            await run
        assert ei.value.code == "insecure_gate"
        assert gate.requests == []


async def test_default_prompt_goes_to_stderr(
    gate: FakeGate, mono: Mono, capsys: pytest.CaptureFixture[str]
) -> None:
    await login_device(
        "https://gate.test",
        transport=gate.transport,
        sleep=mono.sleep,
        clock=mono,
        open_browser=False,
    )
    out = capsys.readouterr()
    assert out.out == ""
    assert "ABCD-EFGH" in out.err
    assert "https://gate.test/device" in out.err


async def test_dev_login(gate: FakeGate) -> None:
    tok = await dev_login("http://localhost:8400", "bob", transport=gate.transport)
    assert tok.party == "bob"
    assert [r.url.path for r in gate.requests] == ["/v1/auth/dev-login"]


# --------------------------------------------------------------------------- login()


async def test_dev_login_writes_profile_and_key(tmp_path: Path, gate: FakeGate) -> None:
    profile = await login(
        "http://localhost:8400", handle="alice", dev=True, home=tmp_path, transport=gate.transport
    )
    secret = Profile.identity(tmp_path)
    assert profile.public_key == age.public_key_of(secret)
    assert gate.public_key == profile.public_key
    assert Profile.load(tmp_path) == profile
    assert (profile.party, profile.namespace, profile.temporal_address) == (
        "alice",
        "alice",
        "temporal.test:7400",
    )
    assert (profile.refresh_token, profile.dev, profile.tls) == ("r1", True, False)
    assert profile.auth_time == gate.auth_time
    for name in (PROFILE_FILE, KEY_FILE):
        assert (tmp_path / name).stat().st_mode & 0o777 == 0o600

    # logging in again reuses the key and does not re-register it
    before = gate.calls("/v1/me/key")
    again = await login(
        "http://localhost:8400", handle="alice", dev=True, home=tmp_path, transport=gate.transport
    )
    assert again.public_key == profile.public_key
    assert gate.calls("/v1/me/key") == before
    assert Profile.identity(tmp_path) == secret


async def test_device_login_writes_a_tls_profile(tmp_path: Path, gate: FakeGate) -> None:
    gate.dev = False
    gate.device = gate.device.model_copy(update={"interval": 1})  # login() sleeps for real
    profile = await login(
        "https://gate.test",
        home=tmp_path,
        transport=gate.transport,
        prompt=lambda _: None,
        open_browser=False,
    )
    assert (profile.tls, profile.dev) == (True, False)
    assert Profile.load(tmp_path).gate_url == "https://gate.test"


async def test_a_different_registered_key_stops_the_login(tmp_path: Path, gate: FakeGate) -> None:
    _, elsewhere = age.generate_identity()
    gate.public_key = elsewhere
    with pytest.raises(LoginError) as ei:
        await login(
            "http://localhost:8400",
            handle="alice",
            dev=True,
            home=tmp_path,
            transport=gate.transport,
        )
    assert ei.value.code == "key_mismatch"
    assert gate.public_key == elsewhere
    assert not (tmp_path / PROFILE_FILE).exists()

    profile = await login(
        "http://localhost:8400",
        handle="alice",
        dev=True,
        home=tmp_path,
        transport=gate.transport,
        replace_key=True,
    )
    assert gate.public_key == profile.public_key != elsewhere


async def test_dev_login_needs_a_handle(tmp_path: Path, gate: FakeGate) -> None:
    with pytest.raises(ValueError, match="handle"):
        await login("http://localhost:8400", dev=True, home=tmp_path, transport=gate.transport)
