"""The host process ``kontra dev up`` spawns (detached) or ``--foreground`` runs.

It reads one JSON line on standard input -- ``{"token": ..., "config": {...}}`` -- stands up a
:class:`~kontra.devgate.stack.DevStack`, writes ``ready`` (or ``error: ...``) on the inherited
ready pipe, and serves until SIGTERM/SIGINT or ``POST /_devgate/stop``. The token travels on
stdin only, never argv or the environment (both readable in ``/proc``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from kontra.devgate.stack import DevStack, StackConfig
from kontra.devgate.state import Ports, logs_dir


def _setup_logging(home: Path) -> None:
    handler = RotatingFileHandler(
        logs_dir(home) / "gate.log", maxBytes=10 * 1024 * 1024, backupCount=1, encoding="utf-8"
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    for name in (
        "kontra.devgate",
        "kontra.devgate.access",
        "kontra.devgate.gate",
        "uvicorn",
        "uvicorn.error",
    ):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.propagate = False


def _write_fd(fd: int, data: bytes) -> None:
    try:
        os.write(fd, data)
        os.close(fd)
    except OSError:
        pass


async def _run(token: str, cfg: dict[str, Any], ready_fd: int) -> None:
    config = StackConfig(
        home=Path(cfg["home"]),
        ports=Ports(**cfg["ports"]),
        cli=Path(cfg["cli"]),
        ui=bool(cfg.get("ui", False)),
        metrics=bool(cfg.get("metrics", False)),
        persist=bool(cfg.get("persist", False)),
    )
    stack = DevStack(config, token=token)
    ready_written = False
    try:
        async with stack:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stack.request_stop)
            if ready_fd >= 0:
                _write_fd(ready_fd, b"ready\n")
                ready_written = True
            await stack.wait_closed()
    except BaseException as exc:
        logging.getLogger("kontra.devgate").exception("the dev stack failed")
        if ready_fd >= 0 and not ready_written:
            _write_fd(ready_fd, f"error: {exc}\n".encode())
        raise


def main(argv: list[str] | None = None) -> int:
    line = sys.stdin.readline()
    if not line.strip():
        print('a {"token", "config"} line is expected on standard input', file=sys.stderr)
        return 2
    try:
        message: Any = json.loads(line)
        token = str(message["token"])
        cfg = dict(message["config"])
    except (ValueError, KeyError, TypeError) as exc:
        print(f"malformed host message: {exc}", file=sys.stderr)
        return 2
    ready_fd = int(cfg.pop("ready_fd", -1))
    _setup_logging(Path(cfg["home"]))
    try:
        asyncio.run(_run(token, cfg, ready_fd))
    except KeyboardInterrupt:
        return 0
    except BaseException:  # already logged, and reported on the ready fd
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
