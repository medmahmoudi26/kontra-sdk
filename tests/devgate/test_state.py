"""``kontra.devgate.state``: ports, settings, the stack record, the registry, the flock."""

from __future__ import annotations

import dataclasses
import os
import socket
import stat
from pathlib import Path
from typing import Any

import pytest

from kontra.devgate import state
from kontra.devgate.state import Ports
from kontra.profile import ProfileError


@pytest.fixture(autouse=True)
def _tmp_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONTRA_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))


def test_resolve_ports_never_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """OD8: ``kontra dev``'s ports come from settings and flags only. Exporting
    ``KONTRA_DEV_GATE_PORT`` (the e2e stack's) does not move the dev gate."""
    monkeypatch.setenv("KONTRA_DEV_GATE_PORT", "18080")
    monkeypatch.setenv("KONTRA_DEV_TEMPORAL_PORT", "17233")
    assert state.resolve_ports().gate == 8080
    assert state.resolve_ports().temporal == 7233
    # A flag (an override) wins; settings are remembered.
    assert state.resolve_ports(overrides={"gate": 9000}).gate == 9000
    state.remember_ports(Ports(gate=9100, temporal=7300))
    assert state.resolve_ports().gate == 9100
    assert state.resolve_ports(overrides={"gate": 9200}).gate == 9200  # override still wins


def test_ignored_env_names_the_compose_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KONTRA_DEV_GATE_PORT", "18080")
    monkeypatch.setenv("KONTRA_GATE_IMAGE", "ghcr.io/x/kontra-gate:dev")
    ignored = state.ignored_env()
    assert "KONTRA_DEV_GATE_PORT" in ignored
    assert "KONTRA_GATE_IMAGE" in ignored


def test_settings_round_trip() -> None:
    s = state.read_settings()
    assert s.ports == {}
    assert s.persist is False
    s.persist = True
    s.ports = {"gate": 8090}
    state.write_settings(s)
    again = state.read_settings()
    assert again.persist is True
    assert again.ports == {"gate": 8090}
    assert stat.S_IMODE(state.settings_path().stat().st_mode) == 0o600


def _record(**over: object) -> state.StackRecord:
    base: dict[str, Any] = dict(
        pid=1234, pgid=1234, token="t" * 20, started_at="2026-10-20T00:00:00Z",
        sdk_version="0.3.0", cli={"path": "/x/temporal", "version": "1.9.1"},
        ports={f.name: getattr(Ports(), f.name) for f in dataclasses.fields(Ports)},
        temporal_http=40000, ui=False, metrics=False, persist=False,
    )  # fmt: skip
    base.update(over)
    return state.StackRecord(**base)  # type: ignore[arg-type]


def test_stack_record_round_trip_and_strictness() -> None:
    state.write_stack_record(_record())
    read = state.read_stack_record()
    assert read is not None
    assert read.pid == 1234
    assert read.port_set.gate == 8080
    assert stat.S_IMODE(state.stack_path().stat().st_mode) == 0o600
    # A wrong format is stale, never acted on.
    state.stack_path().write_text('{"format": 99, "pid": 1}')
    assert state.read_stack_record() is None
    # Malformed is stale too.
    state.stack_path().write_text("{not json")
    assert state.read_stack_record() is None


def test_a_world_writable_record_is_refused() -> None:
    state.write_stack_record(_record())
    state.stack_path().chmod(0o666)
    assert state.read_stack_record() is None  # _read_private refuses a world-writable file


def test_the_registry_maps_a_port_to_its_home() -> None:
    home = Path(os.environ["KONTRA_HOME"])
    assert state.registry_owner(8080) is None
    state.register_port(8080, home)
    assert state.registry_owner(8080) == home
    assert stat.S_IMODE(state.registry_path(8080).stat().st_mode) == 0o600
    assert state.unregister_port(8080, home) is True
    assert state.registry_owner(8080) is None


def test_a_home_never_unregisters_another_homes_stack(tmp_path: Path) -> None:
    """After a crash or a reboot, home A's stale record may name the gate port that home B
    has started on since: A's cleanup must leave B's entry, or no home can join B (OD7)."""
    home_a, home_b = tmp_path / "homeA", tmp_path / "homeB"
    state.register_port(18556, home_b)
    assert state.unregister_port(18556, home_a) is False
    assert state.registry_owner(18556) == home_b
    assert state.unregister_port(18556, home_b) is True
    assert state.registry_owner(18556) is None


def test_hello_proof_binds_the_token_and_the_nonce() -> None:
    nonce = "ab" * 16
    proof = state.hello_proof("t" * 40, nonce)
    assert proof == state.hello_proof("t" * 40, nonce)
    assert proof != state.hello_proof("u" * 40, nonce)
    assert proof != state.hello_proof("t" * 40, "cd" * 16)
    assert "t" * 40 not in proof  # a proof, never the token


# --------------------------------------------------------------------------- shared directories


def _victim(tmp_path: Path) -> Path:
    victim = tmp_path / "victim"
    victim.write_text("the user's own file\n")
    return victim


def test_a_planted_link_at_the_old_temp_name_is_never_written_through(tmp_path: Path) -> None:
    """The temp file of every state write is ``mkstemp``'s (``O_EXCL``, random): a link
    planted at the predictable ``.NAME.<pid>.<host8>`` the review used is not followed."""
    home = Path(os.environ["KONTRA_HOME"])
    victim = _victim(tmp_path)
    host8 = socket.gethostname()[:8]
    registry = state.registry_path(8080)
    (registry.parent / f".{registry.name}.{os.getpid()}.{host8}").symlink_to(victim)
    settings = state.settings_path(home)
    (settings.parent / f".{settings.name}.{os.getpid()}.{host8}").symlink_to(victim)
    state.register_port(8080, home)
    state.write_settings(state.Settings(persist=True), home)
    assert victim.read_text() == "the user's own file\n"
    assert state.registry_owner(8080) == home
    assert state.read_settings(home).persist is True


def test_a_dev_directory_others_may_write_is_made_private() -> None:
    home = Path(os.environ["KONTRA_HOME"])
    (home / "dev" / "data").mkdir(parents=True)
    (home / "dev").chmod(0o777)
    (home / "dev" / "data").chmod(0o777)
    state.data_dir(home)
    assert stat.S_IMODE((home / "dev").stat().st_mode) == 0o700
    assert stat.S_IMODE((home / "dev" / "data").stat().st_mode) == 0o700


@pytest.mark.skipif(not hasattr(os, "geteuid") or os.geteuid() != 0, reason="needs root to chown")
def test_a_dev_directory_another_user_made_is_refused(tmp_path: Path) -> None:
    """The review's reproduction: ``$KONTRA_HOME/dev`` (or ``dev-gates/``) pre-created by
    another user is not adopted -- every write of ours would land where they choose."""
    home = Path(os.environ["KONTRA_HOME"])
    (home / "dev").mkdir(parents=True)
    os.chown(home / "dev", 65534, 65534)
    with pytest.raises(ProfileError, match="not a directory of yours"):
        state.write_settings(state.Settings(persist=True), home)
    gates = tmp_path / "state" / "kontra-sdk" / "dev-gates"
    gates.mkdir(parents=True)
    os.chown(gates, 65534, 65534)
    with pytest.raises(ProfileError, match="not a directory of yours"):
        state.register_port(8080, home)


def test_the_flock_is_held_and_released() -> None:
    with state.lock():
        pass
    with state.lock():  # re-entrant across calls (released on exit)
        pass


def test_port_taken_sees_a_bound_socket() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        assert state.port_taken(port) is True
    assert state.port_taken(port) is False
