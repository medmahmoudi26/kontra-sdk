"""Running the ``kontra`` command in tests.

:class:`Cli` invokes the real click command in-process (``CliRunner``) as one party: its
own ``KONTRA_HOME``, and nothing else from the environment that could point elsewhere.
Commands run ``asyncio.run`` themselves, so from an async test they run in a worker thread
(:meth:`Cli.run`) while the marketplace's gate and dev server keep their own threads.
:func:`serve_process` runs ``kontra serve`` as a real background process, as a seller does.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Generator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from click.testing import CliRunner, Result

from kontra.cli import cli

#: Variables a test must never inherit from the developer's shell.
_SCRUB = ("KONTRA_HOME", "KONTRA_GATE", "KONTRA_API_KEY", "KONTRA_NEXGEN", "KONTRA_DEBUG")


def handle(prefix: str) -> str:
    """A party handle no other test uses (namespaces outlive a test on a shared server)."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


_CWD_LOCK = threading.Lock()


@dataclass
class Cli:
    home: Path
    env: dict[str, str] = field(default_factory=dict[str, str])
    #: The working directory for the command (``kontra init`` writes into it).
    cwd: Path | None = None

    def __call__(self, *args: str, stdin: str | None = None, **env: str) -> Result:
        runner = CliRunner()
        merged: dict[str, str | None] = {k: None for k in _SCRUB}
        merged.update({"KONTRA_HOME": str(self.home), **self.env, **env})
        with _CWD_LOCK:
            before = Path.cwd()
            if self.cwd is not None:
                os.chdir(self.cwd)
            try:
                return runner.invoke(
                    cli, list(args), input=stdin, env=merged, catch_exceptions=True
                )
            finally:
                os.chdir(before)

    async def run(self, *args: str, stdin: str | None = None, **env: str) -> Result:
        return await asyncio.to_thread(self, *args, stdin=stdin, **env)

    async def ok(self, *args: str, stdin: str | None = None, **env: str) -> Result:
        """Run and insist on exit code 0, showing the output when not."""
        result = await self.run(*args, stdin=stdin, **env)
        assert result.exit_code == 0, explain(result)
        return result

    async def json(self, *args: str, **env: str) -> Any:
        result = await self.ok(*args, "--json", **env)
        return json.loads(result.stdout)


def explain(result: Result) -> str:
    exc = result.exception
    trace = ""
    if exc is not None and not isinstance(exc, SystemExit):
        import traceback

        trace = "".join(traceback.format_exception(exc))
    return f"exit {result.exit_code}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}\n{trace}"


@contextlib.contextmanager
def serve_process(
    directory: Path, home: Path, log: Path, *, timeout: float = 90
) -> Generator[subprocess.Popen[bytes]]:
    """``kontra serve --dir directory`` as its own process, until the block ends; then
    SIGINT, which a worker takes as a graceful shutdown (exit code 0)."""
    env: Mapping[str, str] = {
        **{k: v for k, v in os.environ.items() if k not in _SCRUB},
        "KONTRA_HOME": str(home),
    }
    with log.open("wb") as out:
        proc = subprocess.Popen(
            [sys.executable, "-m", "kontra.cli", "serve", "--dir", str(directory)],
            env=dict(env),
            stdout=out,
            stderr=subprocess.STDOUT,
        )
    try:
        deadline = time.monotonic() + timeout
        while b"serving " not in log.read_bytes():
            if proc.poll() is not None:
                raise AssertionError(f"kontra serve exited {proc.returncode}:\n{log.read_text()}")
            if time.monotonic() > deadline:
                raise AssertionError(f"kontra serve never served:\n{log.read_text()}")
            time.sleep(0.1)
        yield proc
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
