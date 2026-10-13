"""A program's session (``KONTRA_API_KEY``, :meth:`Session.from_api_key`): it exchanges the
key, exchanges it again when the access token runs out, learns who it is from the gate,
writes nothing anywhere, and never shows the key in an error. Against the fake gate."""

from __future__ import annotations

import shutil
import traceback
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

from kontra.auth import (
    API_KEY_ENV,
    DEFAULT_GATE_URL,
    GATE_ENV,
    LoginError,
    Session,
    api_key_from_env,
    login,
)
from kontra.gate_client import models as m
from kontra.hire import Kontra, KontraError
from kontra.profile import KEY_FILE, NotLoggedIn, Profile, ProfileError
from kontra.testing.fake_gate import FakeGate

from ._fakes import T0, FakeClock

KEY = m.API_KEY_PREFIX + "S" * 43


class Counting(httpx.AsyncBaseTransport):
    def __init__(self, inner: httpx.AsyncBaseTransport) -> None:
        self.inner = inner
        self.paths: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        return await self.inner.handle_async_request(request)


@pytest.fixture
async def gate() -> AsyncIterator[FakeGate]:
    g = FakeGate(None, sweep_interval=0)
    async with g:
        yield g


@pytest.fixture
def transport(gate: FakeGate) -> Counting:
    return Counting(httpx.ASGITransport(app=gate.app))


@pytest.fixture(autouse=True)
def no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(API_KEY_ENV, raising=False)
    monkeypatch.delenv(GATE_ENV, raising=False)


async def _human(gate: FakeGate, home: Path, transport: httpx.AsyncBaseTransport) -> Session:
    profile = await login(gate.base_url, handle="prog", dev=True, home=home, transport=transport)
    return Session(profile, home=home, transport=transport)


async def _program_home(tmp_path: Path, human_home: Path) -> Path:
    """What a program's machine holds: the party's key file and nothing else."""
    home = tmp_path / "program"
    home.mkdir(mode=0o700)
    shutil.copy2(human_home / KEY_FILE, home / KEY_FILE)
    return home


async def test_from_api_key_learns_who_it_is_and_writes_nothing(
    gate: FakeGate, transport: Counting, tmp_path: Path
) -> None:
    async with await _human(gate, tmp_path / "human", transport) as human:
        made = await human.gate.create_api_key("program")
    home = await _program_home(tmp_path, tmp_path / "human")

    async with await Session.from_api_key(
        gate.base_url, made.key, home=home, transport=transport
    ) as s:
        p = s.profile
        assert (p.party, p.namespace, p.dev, p.tls, p.refresh_token) == (
            "prog",
            "prog",
            True,
            False,  # a dev gate: no TLS to its Temporal, as a login there would say
            None,
        )
        assert p.auth_time == made.created_at
        assert p.public_key == Profile.load(tmp_path / "human").public_key
        assert (s.uses_api_key, s.resolved) == (True, True)
        assert s.identity() == Profile.identity(home)
        assert (await s.gate.balance()).party == "prog"
        assert (await s.temporal_token()).namespace == "prog"
        assert made.key not in repr(s.profile)
    assert sorted(x.name for x in home.iterdir()) == [KEY_FILE]  # no profile, no lock, nothing


async def test_the_key_is_exchanged_again_when_its_token_runs_out(
    gate: FakeGate, transport: Counting, tmp_path: Path
) -> None:
    async with await _human(gate, tmp_path / "human", transport) as human:
        made = await human.gate.create_api_key("program")
    clock = FakeClock(T0)
    async with await Session.from_api_key(
        gate.base_url, made.key, transport=transport, clock=clock
    ) as s:
        transport.paths.clear()
        await s.gate.me()
        assert transport.paths == ["/v1/me"]  # the exchange's token is still good

        clock.advance(900 - 59)  # ours: inside the last minute, so exchanged before use
        await s.gate.me()
        assert transport.paths[1:] == ["/v1/auth/token", "/v1/me"]

        transport.paths.clear()
        gate.advance(timedelta(hours=2))  # the gate's: every token we hold has expired
        assert (await s.gate.me()).party == "prog"  # 401, exchanged again, retried
        assert transport.paths == ["/v1/me", "/v1/auth/token", "/v1/me"]


async def test_a_revoked_key_means_a_new_key(
    gate: FakeGate, transport: Counting, tmp_path: Path
) -> None:
    async with await _human(gate, tmp_path / "human", transport) as human:
        made = await human.gate.create_api_key("program")
        s = await Session.from_api_key(gate.base_url, made.key, transport=transport)
        await human.gate.revoke_api_key(made.id)
    try:
        with pytest.raises(NotLoggedIn) as e:
            await s.gate.me()  # the token dies with the key; the exchange is refused
        assert "kontra keys create" in str(e.value)
        assert made.key not in "".join(traceback.format_exception(e.value))
    finally:
        await s.aclose()
    with pytest.raises(NotLoggedIn):
        await Session.from_api_key(gate.base_url, made.key, transport=transport)


async def test_from_env_prefers_the_key_and_reads_no_profile(
    gate: FakeGate, transport: Counting, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    human_home = tmp_path / "human"
    async with await _human(gate, human_home, transport) as human:
        made = await human.gate.create_api_key("program")

    s = Session.from_env(human_home, transport=transport)  # no key: the profile
    assert (s.uses_api_key, s.profile.party) == (False, "prog")
    await s.aclose()

    monkeypatch.setenv(API_KEY_ENV, made.key)
    s = Session.from_env(tmp_path / "nowhere", transport=transport)
    assert s.gate.base_url == DEFAULT_GATE_URL  # the profile's gate is not consulted
    assert not s.resolved
    with pytest.raises(RuntimeError, match="resolve"):
        _ = s.profile
    await s.aclose()

    monkeypatch.setenv(GATE_ENV, gate.base_url)
    async with Session.from_env(tmp_path / "nowhere", transport=transport) as s:
        assert (await s.resolve()).party == "prog"
        assert await s.resolve() is s.profile

    monkeypatch.setenv(API_KEY_ENV, "")  # empty is unset
    assert api_key_from_env() is None


@pytest.mark.parametrize(
    "value",
    [KEY + "\n", f'"{KEY}"', " " + KEY, KEY[:-1], KEY + "=", "Bearer " + KEY, "kontra_sk_"],
)
def test_a_malformed_key_is_refused_without_being_shown(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, value: str
) -> None:
    monkeypatch.setenv(API_KEY_ENV, value)
    for attempt in (api_key_from_env, lambda: Session.from_env(tmp_path)):
        with pytest.raises(ProfileError) as e:
            attempt()
        shown = "".join(traceback.format_exception(e.value))
        assert API_KEY_ENV in str(e.value)
        assert KEY[len(m.API_KEY_PREFIX) : -1] not in shown
    with pytest.raises(ProfileError) as e:
        Session(api_key=value)
    assert KEY[len(m.API_KEY_PREFIX) : -1] not in "".join(traceback.format_exception(e.value))


def test_a_key_only_travels_over_tls_or_to_this_machine() -> None:
    with pytest.raises(LoginError) as e:
        Session(api_key=KEY, gate_url="http://gate.example")
    assert e.value.code == "insecure_gate"
    Session(api_key=KEY, gate_url="http://127.0.0.1:8000")  # loopback: fine
    assert Session(api_key=KEY).gate.base_url == DEFAULT_GATE_URL


def test_a_session_takes_one_credential(tmp_path: Path) -> None:
    profile = Profile(
        gate_url="https://gate.test",
        temporal_address="t:7233",
        party="alice",
        namespace="alice",
        refresh_token="r",
        auth_time=T0,
    )
    with pytest.raises(TypeError):
        Session()
    with pytest.raises(TypeError):
        Session(profile, api_key=KEY)
    with pytest.raises(TypeError):
        Session(profile, gate_url="https://other.test")


async def test_a_profile_without_a_session_is_not_logged_in() -> None:
    profile = Profile(
        gate_url="https://gate.test",
        temporal_address="t:7233",
        party="alice",
        namespace="alice",
        refresh_token=None,
        auth_time=T0,
    )
    async with Session(profile, transport=httpx.MockTransport(lambda r: httpx.Response(500))) as s:
        with pytest.raises(NotLoggedIn, match="kontra login"):
            await s.access_token()


async def test_a_gate_that_changes_the_keys_party_is_refused() -> None:
    parties = iter(["alice", "mallory"])

    def answer(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/auth/token":
            tok = m.TokenResponse(access_token="a", expires_in=1, party=next(parties), auth_time=T0)
            return httpx.Response(200, json=tok.model_dump(mode="json"))
        return httpx.Response(401, json={"code": "token_expired", "message": "x"})

    s = Session(api_key=KEY, gate_url="https://gate.test", transport=httpx.MockTransport(answer))
    try:
        await s.access_token()
        s._access = None  # pyright: ignore[reportPrivateUsage]
        with pytest.raises(ProfileError, match="mallory"):
            await s.access_token()
    finally:
        await s.aclose()


# --------------------------------------------------------------------------- Kontra()


async def test_kontra_with_a_key_binds_on_first_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``Kontra()`` with ``KONTRA_API_KEY``: no profile anywhere, the key file in
    ``KONTRA_HOME``, the party learned from the gate on ``async with``."""
    gate = FakeGate(None, sweep_interval=0)
    url = await gate.serve()
    try:
        async with await _human(gate, tmp_path / "human", httpx.AsyncHTTPTransport()) as human:
            made = await human.gate.create_api_key("program")
        home = await _program_home(tmp_path, tmp_path / "human")
        monkeypatch.setenv(API_KEY_ENV, made.key)
        monkeypatch.setenv(GATE_ENV, url)
        monkeypatch.setenv("KONTRA_HOME", str(home))

        k = Kontra()
        assert not k.session.resolved
        with pytest.raises(RuntimeError):
            _ = k.party
        async with k:
            assert k.party == "prog"
            assert (await k.balance()).party == "prog"
        assert sorted(x.name for x in home.iterdir()) == [KEY_FILE]

        lazy = Kontra()  # without `async with`, the first call binds
        try:
            assert (await lazy.jobs()) == []
            assert lazy.party == "prog"
        finally:
            await lazy.aclose()

        other = tmp_path / "other"
        other.mkdir()
        Profile.ensure_identity(other)  # a key file that is not the party's
        monkeypatch.setenv("KONTRA_HOME", str(other))
        with pytest.raises(KontraError, match="not the one prog registered"):
            async with Kontra():
                pass
    finally:
        await gate.stop()
