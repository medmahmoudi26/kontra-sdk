"""``kontra.devgate.temporal.TemporalChild``: it runs only the CLI the host took, and a child
that never comes up is killed rather than left holding the Temporal port. No Temporal here:
the "CLI" is a shell script."""

from __future__ import annotations

import os
import socket
from pathlib import Path

import pytest

from kontra.devgate.temporal import RESTART_SECONDS, TemporalChild


def _free() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _script(path: Path, body: str) -> Path:
    path.write_text(f"#!/bin/sh\n{body}\n")
    path.chmod(0o755)
    return path


def _child(tmp_path: Path, cli: Path) -> TemporalChild:
    return TemporalChild(
        cli=cli,
        port=_free(),
        http_port=_free(),
        db=tmp_path / "temporal.db",
        log_path=tmp_path / "temporal.log",
    )


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


async def test_a_cli_replaced_after_the_host_took_it_is_not_run(tmp_path: Path) -> None:
    """The review's swap: the verified binary renamed away and another planted at its path
    after ``ensure``. The child's next start (the first, or a ``--ui`` restart) refuses it."""
    ran = tmp_path / "planted-ran"
    cli = _script(tmp_path / "temporal", "exec sleep 60")
    child = _child(tmp_path, cli)
    cli.rename(tmp_path / "verified")
    _script(cli, f"touch {ran}; exec sleep 60")
    with pytest.raises(RuntimeError, match="not the Temporal CLI this stack started with"):
        await child.start(ready_timeout=1)
    assert not ran.exists()
    assert child.pid is None


async def test_a_child_that_never_answers_is_killed(tmp_path: Path) -> None:
    """``start`` raising must not orphan the child: it would keep the port after the host
    (or ``--foreground``) reported the failure."""
    pidfile = tmp_path / "pid"
    child = _child(tmp_path, _script(tmp_path / "temporal", f"echo $$ > {pidfile}; exec sleep 60"))
    with pytest.raises(RuntimeError, match="did not answer"):
        await child.start(ready_timeout=1)
    pid = int(pidfile.read_text())
    assert not _alive(pid)  # killed and reaped before start raised
    assert child.pid is None


def test_the_restart_budget_covers_a_stop_and_a_start() -> None:
    """What ``POST /_devgate/config`` may take (the CLI's timeout for it is at least this)."""
    assert RESTART_SECONDS >= 60 + 10
