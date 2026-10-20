"""The Temporal CLI dev server the host owns as a child (decisions I34).

:class:`TemporalChild` runs ``temporal server start-dev`` on loopback with a fixed frontend
port, a kernel-chosen HTTP API port fixed for the host's life, and a ``--db-filename`` so a UI
or metrics toggle is a one-or-two-second restart of the child that keeps every namespace,
endpoint, run and connected worker. The child is **not** started in a new session: it inherits
the host's process group, so ``kontra dev down`` can signal the group if the host was killed.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

__all__ = ["DYNAMIC_CONFIG", "READY_TIMEOUT", "RESTART_SECONDS", "STOP_GRACE", "TemporalChild"]

#: What standalone Nexus operations need on server 1.32 (decisions I4). Here, not in
#: :mod:`kontra.testing.devserver` (which re-exports it): the host never imports the testing
#: package (OD2).
DYNAMIC_CONFIG = ("nexusoperation.enableStandalone=true", "history.enableChasm=true")
#: Seconds :meth:`TemporalChild.stop` waits after SIGTERM before SIGKILL, then for the exit.
STOP_GRACE = 10.0
_KILL_WAIT = 5.0
#: Seconds :meth:`TemporalChild.start` waits for the frontend to answer.
READY_TIMEOUT = 60.0
#: The longest a ``--ui``/``--observability`` toggle (a :meth:`TemporalChild.restart`) takes:
#: what ``POST /_devgate/config`` may need before it answers.
RESTART_SECONDS = STOP_GRACE + _KILL_WAIT + READY_TIMEOUT


@dataclass
class TemporalChild:
    """A ``temporal server start-dev`` process. ``ui``/``metrics`` are mutable; a
    :meth:`restart` rebuilds the argv on the same db file."""

    cli: Path
    port: int
    http_port: int
    db: Path
    log_path: Path
    ui: bool = False
    ui_port: int = 8233
    codec_port: int = 8081
    #: Bound only with ``--observability``; otherwise the CLI picks its own metrics port.
    metrics_port: int | None = None
    _proc: subprocess.Popen[bytes] | None = field(default=None, init=False, repr=False)
    #: The verified binary as it was when the host took it: every start (a ``--ui`` restart
    #: too) runs it only if it is still that file.
    _cli_id: tuple[int, int, int, int] | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self._cli_id = _identity(self.cli)

    @property
    def address(self) -> str:
        return f"127.0.0.1:{self.port}"

    @property
    def http_base(self) -> str:
        return f"http://127.0.0.1:{self.http_port}"

    @property
    def pid(self) -> int | None:
        return None if self._proc is None else self._proc.pid

    def _argv(self) -> list[str]:
        argv = [
            str(self.cli), "server", "start-dev",
            "--ip", "127.0.0.1",
            "--port", str(self.port),
            "--http-port", str(self.http_port),
            "--db-filename", str(self.db),
            "--log-level", "warn",
            "--log-format", "text",
            "--color", "never",
        ]  # fmt: skip
        for value in DYNAMIC_CONFIG:
            argv += ["--dynamic-config-value", value]
        if self.ui:
            argv += [
                "--ui-ip", "127.0.0.1",
                "--ui-port", str(self.ui_port),
                "--ui-codec-endpoint", f"http://127.0.0.1:{self.codec_port}",
                "--ui-disable-news-fetch",
            ]  # fmt: skip
        else:
            argv += ["--headless"]
        if self.metrics_port is not None:
            argv += ["--metrics-port", str(self.metrics_port)]
        return argv

    async def start(self, *, ready_timeout: float = READY_TIMEOUT) -> None:
        """Spawn the child (inheriting the host's process group) and wait until its frontend
        answers ``GetSystemInfo``. Raises :class:`RuntimeError` with the tail of the log if the
        CLI is no longer the file the host took, or the child exits first or does not come up
        in ``ready_timeout`` seconds -- a child that never came up is killed, never left
        holding the port."""
        if self._proc is not None and self._proc.poll() is None:
            return
        if _identity(self.cli) != self._cli_id:
            raise RuntimeError(
                f"{self.cli} is not the Temporal CLI this stack started with (replaced since); "
                "refusing to run it: `kontra dev down`, then `kontra dev up` verifies it again"
            )
        log = self.log_path.open("ab")
        try:
            self._proc = subprocess.Popen(  # noqa: S603, ASYNC220 -- fixed argv, no shell; Popen does not block
                self._argv(), stdin=subprocess.DEVNULL, stdout=log, stderr=log
            )
        finally:
            log.close()
        try:
            await self._wait_ready(ready_timeout)
        except BaseException:
            self._kill()
            raise

    def _kill(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        if proc.poll() is None:
            proc.kill()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_KILL_WAIT)

    async def _wait_ready(self, ready_timeout: float) -> None:
        from temporalio.api.workflowservice.v1 import GetSystemInfoRequest
        from temporalio.client import Client

        assert self._proc is not None
        deadline = time.monotonic() + ready_timeout
        while True:
            if self._proc.poll() is not None:
                raise RuntimeError(
                    f"the Temporal dev server exited {self._proc.returncode}: {self._log_tail()}"
                )
            try:
                client = await Client.connect(self.address)
                await client.workflow_service.get_system_info(GetSystemInfoRequest())
                return
            except Exception:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"the Temporal dev server did not answer on {self.address} in "
                        f"{ready_timeout:.0f}s: {self._log_tail()}"
                    ) from None
                await asyncio.sleep(0.2)

    async def stop(self, *, grace: float = STOP_GRACE) -> None:
        """SIGTERM the child, SIGKILL it after ``grace`` seconds."""
        proc, self._proc = self._proc, None
        if proc is None or proc.poll() is not None:
            return
        proc.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + grace
        while proc.poll() is None:
            if time.monotonic() > deadline:
                proc.kill()
                break
            await asyncio.sleep(0.1)
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=_KILL_WAIT)

    async def restart(self, *, ready_timeout: float = READY_TIMEOUT) -> None:
        """Stop and start on the same db file (keeps namespaces, runs and workers)."""
        await self.stop()
        await self.start(ready_timeout=ready_timeout)

    def _log_tail(self, lines: int = 20) -> str:
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return f"(no {self.log_path})"
        tail = text.strip().splitlines()[-lines:]
        return " / ".join(tail) if tail else "(no output)"


def _identity(path: Path) -> tuple[int, int, int, int] | None:
    """``(st_dev, st_ino, st_size, st_mtime_ns)`` of ``path``, or None when it cannot be read."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)
