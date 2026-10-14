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
