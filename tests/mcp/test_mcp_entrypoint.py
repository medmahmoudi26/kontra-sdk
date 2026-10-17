"""``kontra mcp`` (``kontra.mcp.main``) before any agent connects."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest


def test_kontra_mcp_refuses_to_start_without_a_profile(tmp_path: Path) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "kontra.mcp"],
        env={**os.environ, "KONTRA_HOME": str(tmp_path / "nobody")},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 1
    assert done.stdout == ""  # stdout is the protocol channel: nothing else goes there
    assert "kontra login" in done.stderr


# --------------------------------------------------------------------------- with an API key

KEY = "kontra_sk_" + "M" * 43


def test_a_key_and_a_key_file_are_enough_to_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kontra.mcp._server import startup_problem
    from kontra.profile import Profile

    monkeypatch.setenv("KONTRA_HOME", str(tmp_path))
    problem = startup_problem()  # nothing at all
    assert problem is not None
    assert "kontra login" in problem
    monkeypatch.setenv("KONTRA_API_KEY", KEY)
    problem = startup_problem()  # a key, but nothing to decrypt with
    assert problem is not None
    assert "no encryption key" in problem
    Profile.ensure_identity(tmp_path)
    assert startup_problem() is None  # no profile needed


def test_a_malformed_key_stops_it_without_showing_the_key(tmp_path: Path) -> None:
    done = subprocess.run(
        [sys.executable, "-m", "kontra.mcp"],
        env={**os.environ, "KONTRA_HOME": str(tmp_path), "KONTRA_API_KEY": KEY + "\n"},
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert done.returncode == 1
    assert done.stdout == ""
    assert "KONTRA_API_KEY is not a Kontra API key" in done.stderr
    assert "M" * 20 not in done.stderr


async def test_the_tools_buy_with_the_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from kontra.auth import Session, login
    from kontra.mcp._server import KontraTools
    from kontra.testing.fake_gate import FakeGate

    gate = FakeGate(None, sweep_interval=0)
    url = await gate.serve()
    try:
        profile = await login(url, handle="agent", dev=True, home=tmp_path)
        async with Session(profile, home=tmp_path) as human:
            made = await human.gate.create_api_key("agent")
        (tmp_path / "profile").unlink()  # the agent's machine: the key file and the API key
        monkeypatch.setenv("KONTRA_HOME", str(tmp_path))
        monkeypatch.setenv("KONTRA_API_KEY", made.key)
        monkeypatch.setenv("KONTRA_GATE", url)
        tools = KontraTools()
        try:
            assert (await tools.search("", 5, None)).offerings == []
            k = tools.kontra()
            assert k.session.uses_api_key
            assert (await k.balance()).party == "agent"
        finally:
            await tools.aclose()
    finally:
        await gate.stop()


# --------------------------------------------------------------------------- devices (I25)


async def test_kontra_mcp_keeps_the_devices_caught_up_whatever_happens() -> None:
    """Every online device serves the account's new devices: ``kontra mcp`` catches up now
    and then every few minutes, and a client that cannot be made is said and tried again."""
    import asyncio

    from kontra.mcp._server import _keep_devices_caught_up

    calls: list[str] = []

    class _Kontra:
        async def sync_job_keys(self) -> None:
            calls.append("sync")

    class _Tools:
        def __init__(self) -> None:
            self.made = 0

        def kontra(self) -> _Kontra:
            self.made += 1
            if self.made == 1:
                raise RuntimeError("not logged in yet")
            return _Kontra()

    task = asyncio.create_task(_keep_devices_caught_up(_Tools(), 0.01))  # type: ignore[arg-type]
    for _ in range(200):
        if len(calls) >= 2:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert calls[:2] == ["sync", "sync"]


async def test_kontra_mcp_shows_a_new_device_before_it_copies_anything_to_it() -> None:
    """L9: `kontra mcp` has no terminal the person reads, so a device it sees for the first
    time is held back -- nothing is copied to it from this machine -- and shown in the next
    tool answer (to the agent, for the person); only then does it count as shown."""
    from datetime import UTC, datetime

    from mcp.types import TextContent
    from pydantic import BaseModel

    from kontra.codec import age
    from kontra.gate_client import models as m
    from kontra.mcp._server import KontraTools, _answer

    shown: list[list[str]] = []

    class _Kontra:
        on_new_device = None

        async def devices_shown(self, keys: list[str]) -> None:
            shown.append(list(keys))

    tools = KontraTools()
    tools._kontra = _Kontra()  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
    _, key = age.generate_identity()
    device = m.DeviceView(
        public_key=key, label="Firefox on Linux", kind="browser", added_at=datetime.now(UTC)
    )
    assert tools.hold_notice(device) is False  # not shown yet: nothing copied to it from here
    assert tools.hold_notice(device) is False  # held once

    class _Out(BaseModel):
        ok: bool = True

    async def work() -> _Out:
        return _Out()

    answer = await _answer(work(), tools)
    texts = [c.text for c in answer.content if isinstance(c, TextContent)]
    assert len(texts) == 2
    assert "Kontra notice" in texts[1]
    assert "Firefox on Linux" in texts[1]
    assert "can now read your jobs" in texts[1]
    assert shown == [[key]]
    assert answer.structured_content == {"ok": True}  # the tool's own answer, unchanged
    again = await _answer(work(), tools)
    assert len(again.content) == 1
    assert shown == [[key]]
