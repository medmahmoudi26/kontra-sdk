"""The e2e/acceptance stack: ``compose.yaml`` (the prod gate image, the Authorizer, Postgres,
SeaweedFS) driven by the Makefile (``make stack-up``/``stack-down``/``stack-logs``/``e2e``).

This is repository tooling, never shipped and never a ``kontra dev`` mode (decisions I34): the
dev path runs the in-process ``kontra.devgate`` with no containers. Here ``compose.yaml`` runs as
the project :data:`PROJECT` with one env file, ``.stack/stack.env`` (``KONTRA_DEV=1`` and random
local credentials, written once). The host ports come from ``KONTRA_DEV_*_PORT`` (the Makefile
exports the CI values 18080/17233/18333/15432/18233), which ``kontra dev`` does not read (OD8),
so ``make e2e`` and ``kontra dev up`` coexist on one machine.

``python -m tests.e2e.stack {up,down,logs}`` is what the Makefile runs; the ``stack`` fixture in
``conftest.py`` calls :func:`up`/:func:`down`.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "kontra-e2e"
STACK_DIR = ROOT / ".stack"
STACK_ENV = STACK_DIR / "stack.env"
COMPOSE = ROOT / "compose.yaml"
_HEALTH_TIMEOUT = 300.0
_TEMPORAL_TIMEOUT = 180.0


def stack_env() -> Path:
    """``.stack/stack.env`` (0600), written once with ``KONTRA_DEV=1`` and random credentials:
    the Postgres volume remembers the password it was created with, so it is kept."""
    if STACK_ENV.is_file():
        return STACK_ENV
    STACK_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    text = (
        "# Written by tests/e2e/stack.py: the e2e stack's settings and credentials.\n"
        "KONTRA_DEV=1\n"
        f"KONTRA_PG_PASSWORD={secrets.token_hex(24)}\n"
        f"KONTRA_S3_ACCESS_KEY=kontra-{secrets.token_hex(6)}\n"
        f"KONTRA_S3_SECRET_KEY={secrets.token_hex(24)}\n"
    )
    fd = os.open(STACK_ENV, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    return STACK_ENV


def read_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").split("\n"):
        name, sep, value = line.partition("=")
        if sep and not name.lstrip().startswith("#"):
            out[name.strip()] = value
    return out


def _docker() -> str:
    found = shutil.which("docker")
    if found is None:
        raise RuntimeError("the e2e stack needs Docker with the compose plugin (docker compose)")
    return found


def _argv(*args: str, ui: bool = False) -> list[str]:
    cmd = [_docker(), "compose", "-p", PROJECT, "-f", str(COMPOSE), "--env-file", str(stack_env())]
    if ui:
        cmd += ["--profile", "ui"]
    return [*cmd, *args]


def _compose(*args: str, ui: bool = False, capture: bool = False) -> str:
    proc = subprocess.run(_argv(*args, ui=ui), capture_output=capture, text=True, check=False)
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() if capture else ""
        raise RuntimeError(
            f"docker compose {args[0]} failed ({proc.returncode}){': ' + detail if detail else ''}"
        )
    return proc.stdout if capture else ""


def config(*, ui: bool = False) -> dict[str, Any]:
    out = _compose("config", "--format", "json", ui=ui, capture=True)
    parsed: Any = json.loads(out)
    if not isinstance(parsed, dict):
        raise RuntimeError("docker compose config answered something unexpected")
    return {str(k): v for k, v in parsed.items()}


def gate_env(cfg: dict[str, Any]) -> dict[str, str]:
    gate: Any = (cfg.get("services") or {}).get("gate") or {}
    env: Any = gate.get("environment") or {}
    return {str(k): "" if v is None else str(v) for k, v in env.items()}


def _public_url(cfg: dict[str, Any]) -> str:
    url = gate_env(cfg).get("KONTRA_PUBLIC_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("the compose file sets no KONTRA_PUBLIC_URL for the gate")
    return url


def _wait_healthy(url: str, timeout: float = _HEALTH_TIMEOUT) -> None:
    deadline = time.monotonic() + timeout
    while True:
        try:
            if httpx.get(f"{url}/healthz", timeout=5).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        if time.monotonic() > deadline:
            raise RuntimeError(f"the gate at {url} is not healthy after {timeout:.0f}s")
        time.sleep(1)


def _wait_temporal(url: str, timeout: float = _TEMPORAL_TIMEOUT) -> None:
    """Until Temporal's public frontend accepts a throwaway dev party's worker token (Temporal
    polls the gate's JWKS; a new namespace takes a moment to be served)."""
    import asyncio
    import tempfile

    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.service import RPCError

    from kontra.auth import Session, login

    async def _probe() -> None:
        home = Path(tempfile.mkdtemp(prefix="stack-probe-"))
        try:
            await login(
                url,
                handle=f"stack-probe-{secrets.token_hex(4)}",
                dev=True,
                home=home,
                open_browser=False,
            )
            async with Session.load(home) as session:
                deadline = asyncio.get_running_loop().time() + timeout
                client = None
                while asyncio.get_running_loop().time() < deadline:
                    try:
                        client = client or await session.connect_temporal()
                        await client.workflow_service.describe_namespace(
                            DescribeNamespaceRequest(namespace=session.profile.namespace)
                        )
                        return
                    except (RuntimeError, RPCError, httpx.TransportError):
                        await asyncio.sleep(2)
            raise RuntimeError(f"Temporal at {url} did not accept a token in {timeout:.0f}s")
        finally:
            shutil.rmtree(home, ignore_errors=True)

    asyncio.run(_probe())


def up(*, ui: bool = False) -> None:
    """Bring the stack up (idempotent) and wait until the gate is healthy and Temporal takes a
    dev party's token."""
    stack_env()
    _compose("up", "--detach", "--remove-orphans", ui=ui)
    url = _public_url(config(ui=ui))
    _wait_healthy(url)
    _wait_temporal(url)


def down() -> None:
    _compose("down", "--volumes", "--remove-orphans")
    STACK_ENV.unlink(missing_ok=True)


def logs(services: list[str], *, tail: int = 200, follow: bool = False) -> None:
    args = ["logs", "--tail", str(tail), *(["--follow"] if follow else []), "--", *services]
    _compose(*args)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tests.e2e.stack")
    sub = parser.add_subparsers(dest="cmd", required=True)
    up_p = sub.add_parser("up")
    up_p.add_argument("--ui", action="store_true")
    sub.add_parser("down")
    logs_p = sub.add_parser("logs")
    logs_p.add_argument("--tail", type=int, default=200)
    logs_p.add_argument("services", nargs="*")
    args = parser.parse_args(argv)
    try:
        if args.cmd == "up":
            up(ui=args.ui)
        elif args.cmd == "down":
            down()
        elif args.cmd == "logs":
            logs(args.services, tail=args.tail)
    except RuntimeError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
