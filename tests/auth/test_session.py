"""Session: gate access-token refresh, refresh-token rotation, Temporal token caching and the
background api_key swap."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from kontra.auth import Session
from kontra.gate_client import GateError
from kontra.profile import NotLoggedIn, Profile, ProfileError

from ._fakes import T0, FakeClock, FakeGate, FakeSleep, make_profile

TOKEN = "/v1/auth/token"
TEMPORAL = "/v1/auth/temporal-token"


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def gate(clock: FakeClock) -> FakeGate:
    return FakeGate(clock)


@pytest.fixture
def home(tmp_path: Path) -> Path:
    make_profile().save(tmp_path)
    return tmp_path


def session(home: Path, gate: FakeGate, clock: FakeClock, **kw: Any) -> Session:
    return Session(Profile.load(home), transport=gate.transport, clock=clock, **kw)


# --------------------------------------------------------------------------- access token


async def test_refreshes_once_then_reuses_until_60s_before_expiry(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        await s.gate.me()
        await s.gate.me()
        assert gate.calls(TOKEN) == 1
        clock.advance(900 - 61)
        await s.gate.me()
        assert gate.calls(TOKEN) == 1
        clock.advance(1)  # exactly 60 s before expiry
        await s.gate.me()
        assert gate.calls(TOKEN) == 2
    assert [r.headers["authorization"] for r in gate.requests if r.url.path == "/v1/me"] == [
        "Bearer acc1",
        "Bearer acc1",
        "Bearer acc1",
        "Bearer acc2",
    ]


async def test_short_lived_tokens_refresh_at_half_life(home: Path, clock: FakeClock) -> None:
    gate = FakeGate(clock, access_ttl=60)
    async with session(home, gate, clock) as s:
        await s.access_token()
        clock.advance(29)
        await s.access_token()
        assert gate.calls(TOKEN) == 1
        clock.advance(1)
        await s.access_token()
        assert gate.calls(TOKEN) == 2


async def test_rotated_refresh_token_is_persisted(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        await s.access_token()
        assert s.profile.refresh_token == "r1"
    on_disk = Profile.load(home)
    assert on_disk.refresh_token == "r1"
    assert on_disk.auth_time == gate.auth_time
    assert (home / "profile").stat().st_mode & 0o777 == 0o600


async def test_concurrent_callers_share_one_refresh(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        tokens = await asyncio.gather(*(s.access_token() for _ in range(20)))
    assert set(tokens) == {"acc1"}
    assert gate.calls(TOKEN) == 1  # a second refresh with r0 would revoke the session


async def test_401_forces_a_refresh_and_the_call_is_retried(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        await s.gate.me()
        gate.access.clear()  # e.g. the gate rotated its signing key
        me = await s.gate.me()
    assert me.party == "alice"
    assert gate.calls(TOKEN) == 2
    assert gate.calls("/v1/me") == 3


async def test_a_spent_refresh_token_means_log_in_again(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    gate.refresh_token = "someone-else-rotated-it"
    async with session(home, gate, clock) as s:
        with pytest.raises(NotLoggedIn, match="kontra login") as ei:
            await s.gate.me()
    assert isinstance(ei.value.__cause__, GateError)
    assert ei.value.__cause__.code == "invalid_grant"


async def test_other_gate_errors_on_refresh_propagate(home: Path, clock: FakeClock) -> None:
    import httpx

    def down(r: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"code": "unavailable", "message": "later"})

    s = Session(Profile.load(home), transport=httpx.MockTransport(down), clock=clock)
    async with s:
        with pytest.raises(GateError) as ei:
            await s.access_token()
    assert ei.value.code == "unavailable"


async def test_adopts_a_token_rotated_by_another_process(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    s = session(home, gate, clock)
    async with s:
        await s.access_token()  # r0 -> r1, persisted
        # another process sharing the profile refreshes: r1 -> r2, and writes it back
        async with session(home, gate, clock) as other:
            await other.access_token()
        assert Profile.load(home).refresh_token == "r2"
        clock.advance(900)
        await s.access_token()  # must present r2, not its own spent r1
    assert Profile.load(home).refresh_token == "r3"


async def test_does_not_overwrite_a_profile_that_now_belongs_to_someone_else(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        make_profile(party="bob", namespace="bob", refresh_token="bobs").save(home)
        await s.access_token()
        assert s.profile.refresh_token == "r1"
    assert Profile.load(home).party == "bob"
    assert Profile.load(home).refresh_token == "bobs"


async def test_in_memory_profile_is_not_written_anywhere(
    tmp_path: Path, gate: FakeGate, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "default"))
    async with Session(make_profile(), transport=gate.transport, clock=clock) as s:
        await s.access_token()
        assert s.profile.refresh_token == "r1"
    assert not (tmp_path / "default").exists()


async def test_explicit_home_persists_an_in_memory_profile(
    tmp_path: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with Session(make_profile(), transport=gate.transport, clock=clock, home=tmp_path) as s:
        await s.access_token()
    assert Profile.load(tmp_path).refresh_token == "r1"


async def test_refusing_a_token_for_another_party(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    gate.answer_party = "mallory"
    async with session(home, gate, clock) as s:
        with pytest.raises(ProfileError, match="mallory"):
            await s.access_token()
    assert Profile.load(home).refresh_token == "r0"


async def test_load_and_identity(home: Path, gate: FakeGate) -> None:
    secret, _ = Profile.ensure_identity(home)
    async with Session.load(home, transport=gate.transport) as s:
        assert s.profile.party == "alice"
        assert s.identity() == secret


# --------------------------------------------------------------------------- Temporal token


async def test_temporal_token_is_cached_until_60s_before_expiry(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        a = await s.temporal_token()
        b = await s.temporal_token("worker")
        assert a is b
        assert a.token == "jwt1"
        clock.advance(900 - 61)
        assert (await s.temporal_token()).token == "jwt1"
        clock.advance(1)
        assert (await s.temporal_token()).token == "jwt2"
    assert gate.calls(TEMPORAL) == 2


async def test_temporal_token_concurrent_callers_share_one_fetch(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        got = await asyncio.gather(*(s.temporal_token() for _ in range(10)))
    assert {t.token for t in got} == {"jwt1"}
    assert gate.calls(TEMPORAL) == 1


async def test_temporal_token_role_is_validated(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    async with session(home, gate, clock) as s:
        with pytest.raises(ValueError, match="worker"):
            await s.temporal_token("publisher")
    assert gate.calls(TEMPORAL) == 0


# --------------------------------------------------------------------------- api_key swap


class FakeClient:
    def __init__(self, api_key: str) -> None:
        self.api_key: str | None = api_key
        self.history = [api_key]

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name == "api_key" and hasattr(self, "history"):
            self.history.append(value)


async def test_keep_fresh_swaps_the_api_key_before_expiry(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    sleep = FakeSleep(clock, budget=2)
    async with session(home, gate, clock, sleep=sleep) as s:
        tok = await s.temporal_token()
        client = FakeClient(tok.token)
        task = s.keep_fresh(client)
        await asyncio.wait_for(sleep.parked.wait(), 5)
        assert client.history == ["jwt1", "jwt2", "jwt3"]
        assert sleep.delays[:2] == [840.0, 840.0]
        assert clock() == T0 + timedelta(seconds=2 * 840)
    assert task.cancelled()
    assert not s._tasks


async def test_keep_fresh_backs_off_on_failure_and_keeps_the_old_key(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    sleep = FakeSleep(clock, budget=3)
    async with session(home, gate, clock, sleep=sleep) as s:
        tok = await s.temporal_token()
        client = FakeClient(tok.token)
        gate.temporal_failures = 2
        s.keep_fresh(client)
        await asyncio.wait_for(sleep.parked.wait(), 5)
    assert sleep.delays[:3] == [840.0, 2.0, 4.0]
    assert client.history == ["jwt1", "jwt2"]


async def test_connect_temporal_real_client_gets_its_api_key_swapped(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    """temporalio's real Client, lazily connected to nothing: the swap reaches the
    bridge's config, which is what every subsequent RPC sends."""
    sleep = FakeSleep(clock, budget=1)
    async with session(home, gate, clock, sleep=sleep) as s:
        client = await s.connect_temporal(lazy=True)
        assert client.api_key == "jwt1"
        assert client.namespace == "alice"
        await asyncio.wait_for(sleep.parked.wait(), 5)
        assert client.api_key == "jwt2"
        assert client.service_client.config.api_key == "jwt2"
        assert client.service_client.config.tls is False


async def test_connect_temporal_arguments(
    home: Path, gate: FakeGate, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    from temporalio.client import Client
    from temporalio.converter import DataConverter

    seen: list[tuple[str, dict[str, Any]]] = []

    async def fake_connect(target: str, **kwargs: Any) -> FakeClient:
        seen.append((target, kwargs))
        return FakeClient(kwargs["api_key"])

    monkeypatch.setattr(Client, "connect", fake_connect)
    conv = DataConverter.default
    sleep = FakeSleep(clock, budget=0)
    async with session(home, gate, clock, sleep=sleep) as s:
        await s.connect_temporal(namespace="other", data_converter=conv, identity="w1")
        await s.connect_temporal(tls=True)
        with pytest.raises(TypeError, match="api_key"):
            await s.connect_temporal(api_key="mine")
    assert seen == [
        (
            "127.0.0.1:1",
            {
                "namespace": "other",
                "api_key": "jwt1",
                "tls": False,
                "data_converter": conv,
                "identity": "w1",
            },
        ),
        ("127.0.0.1:1", {"namespace": "alice", "api_key": "jwt1", "tls": True}),
    ]


def _refreshers() -> list[asyncio.Task[Any]]:
    """The api_key refreshers alive in this event loop, by asyncio's own bookkeeping."""
    return [
        t
        for t in asyncio.all_tasks()
        if t.get_name().startswith("kontra-temporal-token-") and not t.done()
    ]


async def test_release_stops_that_clients_refresher_only(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    """FIXUPS #23: whoever is done with a client stops its refresher, while the session
    (shared with others) lives on; nothing is left running for the released client."""
    sleep = FakeSleep(clock, budget=0)
    async with session(home, gate, clock, sleep=sleep) as s:
        mine, theirs = FakeClient("x"), FakeClient("y")
        task = s.keep_fresh(mine)
        other = s.keep_fresh(theirs)
        await asyncio.sleep(0)
        assert set(_refreshers()) == {task, other}
        await s.release(mine)
        assert task.cancelled()
        assert _refreshers() == [other]
        assert list(s._tasks.values()) == [theirs]
        await s.release(mine)  # idempotent
        await s.release(FakeClient("never connected"))
        assert _refreshers() == [other]
    assert other.cancelled()
    assert _refreshers() == []


async def test_connect_temporal_then_release_leaves_no_task(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    sleep = FakeSleep(clock, budget=0)
    async with session(home, gate, clock, sleep=sleep) as s:
        client = await s.connect_temporal(lazy=True)
        assert len(_refreshers()) == 1
        await s.release(client)
        assert _refreshers() == []
        assert not s._tasks


async def test_aclose_cancels_every_refresher(home: Path, gate: FakeGate, clock: FakeClock) -> None:
    sleep = FakeSleep(clock, budget=0)
    s = session(home, gate, clock, sleep=sleep)
    tasks = [s.keep_fresh(FakeClient("x")) for _ in range(3)]
    await asyncio.sleep(0)
    await s.aclose()
    assert all(t.cancelled() for t in tasks)
    assert s.gate._client.is_closed


# --------------------------------------------------------------------------- review regressions


async def test_a_failed_save_never_brings_back_the_spent_token(
    home: Path, gate: FakeGate, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The gate rotated r0 -> r1 but r1 could not be written (disk full). The next refresh
    must present r1 from memory: re-reading r0 from disk and presenting it again would make
    the gate revoke the whole session, logging out every process that shares it."""
    real_save = Profile.save
    failures = [OSError(28, "No space left on device")]

    def flaky_save(self: Profile, home: Path | str | None = None) -> Path:
        if failures:
            raise failures.pop()
        return real_save(self, home)

    monkeypatch.setattr(Profile, "save", flaky_save)
    async with session(home, gate, clock) as s:
        assert await s.access_token() == "acc1"  # the call that rotated still gets its token
        assert Profile.load(home).refresh_token == "r0"  # ... but the write failed
        clock.advance(900)
        assert await s.access_token() == "acc2"
    assert not gate.revoked
    assert gate.used == {"r0", "r1"}
    assert Profile.load(home).refresh_token == "r2"  # the next successful write catches up


async def test_a_stale_disk_copy_is_rewritten_even_when_the_gate_does_not_rotate(
    home: Path, gate: FakeGate, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_save = Profile.save
    failures = [OSError(30, "Read-only file system")]

    def flaky_save(self: Profile, home: Path | str | None = None) -> Path:
        if failures:
            raise failures.pop()
        return real_save(self, home)

    monkeypatch.setattr(Profile, "save", flaky_save)
    async with session(home, gate, clock) as s:
        await s.access_token()  # r0 -> r1, not written
        gate.rotate = False
        clock.advance(900)
        await s.access_token()  # r1 comes back unchanged; the stale r0 on disk is replaced
    assert Profile.load(home).refresh_token == "r1"


@pytest.mark.parametrize("skew", [-1000, -120, 120, 600])
async def test_temporal_token_refresh_is_immune_to_clock_skew(
    home: Path, clock: FakeClock, skew: int
) -> None:
    """``expires_at`` is on the gate's clock. Comparing it with ours refreshed too late when
    ours was behind (an expired api_key for up to the skew) and every second when ours was
    ahead by more than the lifetime. The JWT's own iat/exp give the lifetime instead."""
    gate = FakeGate(clock, skew=skew, jwt=True)
    sleep = FakeSleep(clock, budget=2)
    async with session(home, gate, clock, sleep=sleep) as s:
        first = await s.temporal_token()
        clock.advance(839)
        assert (await s.temporal_token()) is first
        clock.advance(-839)
        client = FakeClient(first.token)
        s.keep_fresh(client)
        await asyncio.wait_for(sleep.parked.wait(), 5)
        assert sleep.delays[:2] == [840.0, 840.0]
        assert len(client.history) == 3
    assert gate.calls(TEMPORAL) == 3


@pytest.mark.parametrize(
    "token",
    ["jwt1", "a.b.c", "a.e30.c", 'a.{"iat":1}.c', "a.WzFd.c", "a.eyJpYXQiOjIsImV4cCI6MX0.c"],
    ids=["opaque", "not-base64", "no-claims", "not-json", "a-list", "exp-before-iat"],
)
async def test_tokens_without_a_usable_lifetime_fall_back_to_expires_at(
    home: Path, gate: FakeGate, clock: FakeClock, token: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kontra.auth import _jwt_lifetime  # pyright: ignore[reportPrivateUsage]

    assert _jwt_lifetime(token) is None
    async with session(home, gate, clock) as s:
        await s.temporal_token()
        assert s._temporal["worker"][1] == T0 + timedelta(seconds=840)  # pyright: ignore[reportPrivateUsage]


def test_the_refresh_token_is_not_in_the_profile_repr() -> None:
    p = make_profile(refresh_token="kr_supersecret")
    assert "kr_supersecret" not in repr(p)
    assert "kr_supersecret" not in str(p)
    assert p.model_dump()["refresh_token"] == "kr_supersecret"  # still saved


async def test_a_closed_session_starts_no_refresher(
    home: Path, gate: FakeGate, clock: FakeClock
) -> None:
    """A refresher started after :meth:`Session.aclose` would run forever against a closed
    gate client: nobody would ever cancel it. A client connecting while the session closes
    (its token was cached), or a ``keep_fresh`` after it, is refused instead."""
    sleep = FakeSleep(clock, budget=0)
    s = session(home, gate, clock, sleep=sleep)
    await s.temporal_token()  # cached: connect_temporal needs no gate call
    await s.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        s.keep_fresh(FakeClient("x"))
    with pytest.raises(RuntimeError, match="closed"):
        await s.connect_temporal(lazy=True)
    assert _refreshers() == []
    assert not s._tasks  # pyright: ignore[reportPrivateUsage]


async def test_a_client_connected_while_the_session_closes_is_not_kept_fresh(
    home: Path, gate: FakeGate, clock: FakeClock, monkeypatch: pytest.MonkeyPatch
) -> None:
    from temporalio.client import Client

    sleep = FakeSleep(clock, budget=0)
    s = session(home, gate, clock, sleep=sleep)
    connecting, closed = asyncio.Event(), asyncio.Event()
    real_connect = Client.connect

    async def slow_connect(*args: Any, **kwargs: Any) -> Client:
        connecting.set()
        await closed.wait()
        return await real_connect(*args, **kwargs)

    monkeypatch.setattr(Client, "connect", slow_connect)
    pending = asyncio.ensure_future(s.connect_temporal(lazy=True))
    await connecting.wait()
    await s.aclose()
    closed.set()
    with pytest.raises(RuntimeError, match="closed"):
        await pending
    assert _refreshers() == []
