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
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from kontra.testing.devserver import DYNAMIC_CONFIG

__all__ = ["TemporalChild"]


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

    async def start(self, *, ready_timeout: float = 60.0) -> None:
        """Spawn the child (inheriting the host's process group) and wait until its frontend
        answers ``GetSystemInfo``. Raises :class:`RuntimeError` with the tail of the log if the
        child exits first or does not come up in ``ready_timeout`` seconds."""
        if self._proc is not None and self._proc.poll() is None:
            return
        log = self.log_path.open("ab")
        try:
            self._proc = subprocess.Popen(  # noqa: S603, ASYNC220 -- fixed argv, no shell; Popen does not block
                self._argv(), stdin=subprocess.DEVNULL, stdout=log, stderr=log
            )
        finally:
            log.close()
        await self._wait_ready(ready_timeout)

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

    async def stop(self, *, grace: float = 10.0) -> None:
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
            proc.wait(timeout=5)

    async def restart(self, *, ready_timeout: float = 60.0) -> None:
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
