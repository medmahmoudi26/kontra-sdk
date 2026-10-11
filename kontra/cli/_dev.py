"""``kontra dev``: the local stack, from the one environment definition (PRD §19.3).

The stack is the repository's root ``compose.yaml``, shipped in the wheel as
``kontra/_dev/compose.yaml``; there is no second way to bring it up. It runs as the compose
project :data:`PROJECT` with two env files: ``$KONTRA_HOME/dev.env`` (written once by this
command: ``KONTRA_DEV=1`` and random credentials for the local Postgres and bucket -- the
compose file itself has no default secrets, so production cannot inherit one) and any
``--env-file`` given after it. Variables in the environment win over both, as in compose.

``up`` then waits for the gate's ``/healthz``, logs in to it as a dev party (``--handle``,
else derived from the OS user), writes a dev profile, and waits until Temporal accepts the
party's worker token: the gate creates namespaces through Temporal's internal frontend, and
Temporal learns the gate's signing keys by polling its JWKS, so each side comes up without
the other and the order is settled by retries, not by ``depends_on`` alone.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import shutil
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import httpx

import kontra
from kontra.cli._common import default_handle, emit_json, fail, run

if TYPE_CHECKING:
    from kontra.profile import Profile

__all__ = ["DEV_ENV_FILE", "PROJECT", "compose_file", "dev", "dev_env"]

PROJECT = "kontra-dev"
DEV_ENV_FILE = "dev.env"
_HEALTH_TIMEOUT = 300.0
_TEMPORAL_TIMEOUT = 180.0


def compose_file() -> Path:
    """The packaged ``compose.yaml`` (wheel), else the checkout's root one."""
    pkg = Path(kontra.__file__).resolve().parent
    for candidate in (pkg / "_dev" / "compose.yaml", pkg.parent / "compose.yaml"):
        if candidate.is_file():
            return candidate
    fail("compose.yaml is missing from the kontra package; reinstall kontra-sdk")


def dev_env(home: Path) -> Path:
    """``$KONTRA_HOME/dev.env``, created (0600) with fresh credentials on first use and
    kept afterwards: the Postgres volume remembers the password it was created with."""
    path = home / DEV_ENV_FILE
    if path.is_file():
        return path
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    text = (
        "# Written by `kontra dev`: the local stack's settings and credentials. Keep it as\n"
        "# long as the kontra-dev volumes exist (`kontra dev down --volumes` drops them).\n"
        "KONTRA_DEV=1\n"
        f"KONTRA_PG_PASSWORD={secrets.token_hex(24)}\n"
        f"KONTRA_S3_ACCESS_KEY=kontra-{secrets.token_hex(6)}\n"
        f"KONTRA_S3_SECRET_KEY={secrets.token_hex(24)}\n"
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def _docker() -> str:
    found = shutil.which("docker")
    if found is None:
        fail("kontra dev needs Docker with the compose plugin (docker compose); install it first")
    return found


def _compose_cmd(env_files: tuple[Path, ...], *args: str) -> list[str]:
    from kontra.profile import kontra_home

    cmd = [_docker(), "compose", "-p", PROJECT, "-f", str(compose_file())]
    for env in (dev_env(kontra_home()), *env_files):
        cmd += ["--env-file", str(env)]
    return [*cmd, *args]


def _compose(env_files: tuple[Path, ...], *args: str, capture: bool = False) -> str:
    """Run ``docker compose`` on the dev project; its own output goes to the terminal
    unless ``capture``. A failure ends the command with compose's message."""
    cmd = _compose_cmd(env_files, *args)
    proc = subprocess.run(cmd, capture_output=capture, text=True, check=False)  # noqa: S603 -- argv, no shell
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() if capture else ""
        fail(
            f"docker compose {args[0]} failed (exit {proc.returncode}){': ' + detail if detail else ''}"
        )
    return proc.stdout if capture else ""


def _config(env_files: tuple[Path, ...]) -> dict[str, Any]:
    out = _compose(env_files, "config", "--format", "json", capture=True)
    try:
        parsed: object = json.loads(out)
    except ValueError:
        fail("docker compose config did not answer JSON; is the compose plugin v2.23 or newer?")
    if not isinstance(parsed, dict):
        fail("docker compose config answered something unexpected")
    return {str(k): v for k, v in parsed.items()}  # pyright: ignore[reportUnknownVariableType]


def _gate_env(config: dict[str, Any]) -> dict[str, str]:
    gate: Any = (config.get("services") or {}).get("gate") or {}
    env: Any = gate.get("environment") or {}
    return {str(k): "" if v is None else str(v) for k, v in env.items()}


def _published(config: dict[str, Any], service: str) -> str | None:
    svc: Any = (config.get("services") or {}).get(service) or {}
    for port in svc.get("ports") or []:
        if isinstance(port, dict) and port.get("published"):
            host = port.get("host_ip") or ""
            local = host in ("", "127.0.0.1", "0.0.0.0")  # noqa: S104 -- compares, binds nothing
            return f"{'localhost' if local else host}:{port['published']}"
    return None


def _wait_healthy(url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    said = 0.0
    start = time.monotonic()
    while True:
        try:
            r = httpx.get(f"{url}/healthz", timeout=5)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        now = time.monotonic()
        if now > deadline:
            fail(
                f"the gate at {url} is not healthy after {timeout:.0f}s; see `kontra dev logs "
                "gate`. (If this stack was first started with another KONTRA_HOME, its "
                "database has other credentials: `kontra dev down --volumes` resets it.)"
            )
        if now - said >= 15:
            click.echo(f"waiting for the gate at {url} ({now - start:.0f}s)...", err=True)
            said = now
        time.sleep(1)


async def _wait_temporal(home: Path, within: float) -> None:
    """Until Temporal's public frontend accepts the party's worker token in its namespace
    (Temporal polls the gate's JWKS; a new namespace takes a moment to be served)."""
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.client import Client
    from temporalio.service import RPCError

    from kontra.auth import Session

    loop = asyncio.get_running_loop()
    deadline = loop.time() + within
    last: BaseException | None = None
    async with Session.load(home) as session:
        client: Client | None = None
        while loop.time() < deadline:
            try:
                if client is None:
                    client = await session.connect_temporal()
                await client.workflow_service.describe_namespace(
                    DescribeNamespaceRequest(namespace=session.profile.namespace)
                )
                return
            except (RuntimeError, RPCError, httpx.TransportError) as e:
                last = e  # temporalio raises RuntimeError("Failed client connect: ...")
            await asyncio.sleep(2)
    fail(
        f"Temporal at {session.profile.temporal_address} does not accept {session.profile.party}'s "
        f"token after {within:.0f}s ({last}); see `kontra dev logs temporal`"
    )


def _check_profile(replace: bool) -> Profile | None:
    """The profile here, if ``up`` may replace it: a dev one, or any with
    ``--replace-profile``. ``up`` never writes over a real login without being asked."""
    from kontra.profile import Profile, ProfileError, kontra_home

    try:
        existing = Profile.load()
    except ProfileError:
        return None
    if existing.dev or replace:
        return existing
    fail(
        f"{kontra_home()} holds your login to {existing.gate_url} as {existing.party}, which "
        "`kontra dev` would replace. Use another home for dev (KONTRA_HOME=~/.kontra-dev "
        "kontra dev), or pass --replace-profile."
    )


_ENV_FILES = click.option(
    "--env-file",
    "env_files",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="More compose variables (after $KONTRA_HOME/dev.env; may repeat).",
)
_HANDLE = click.option(
    "--handle", metavar="H", help="Dev party handle (default: from your user name)."
)


def _given(ctx: click.Context, env_files: tuple[Path, ...]) -> tuple[Path, ...]:
    """``kontra dev --env-file A <cmd> --env-file B``: A, then B."""
    outer: tuple[Path, ...] = (ctx.obj or {}).get("env_files", ())
    return (*outer, *env_files)


@click.group(invoke_without_command=True)
@click.pass_context
@_HANDLE
@_ENV_FILES
def dev(ctx: click.Context, handle: str | None, env_files: tuple[Path, ...]) -> None:
    """The local Kontra stack: Temporal, the gate, Postgres and a bucket, with play money.

    `kontra dev` alone is `kontra dev up`."""
    ctx.obj = {"env_files": env_files, "handle": handle}
    if ctx.invoked_subcommand is None:
        ctx.invoke(up)


@dev.command()
@click.pass_context
@_HANDLE
@_ENV_FILES
@click.option(
    "--timeout", type=float, default=_HEALTH_TIMEOUT, show_default=True, help="Seconds to wait."
)
@click.option("--replace-profile", is_flag=True, help="Replace a non-dev profile with the dev one.")
@click.option("--no-login", is_flag=True, help="Only bring the stack up.")
def up(
    ctx: click.Context,
    handle: str | None = None,
    env_files: tuple[Path, ...] = (),
    timeout: float = _HEALTH_TIMEOUT,
    replace_profile: bool = False,
    no_login: bool = False,
) -> None:
    """Start the stack, wait for the gate, and log in to it as a dev party."""
    from kontra.auth import login
    from kontra.contract.names import validate_handle
    from kontra.profile import KEY_FILE, PROFILE_FILE, kontra_home

    env_files = _given(ctx, env_files)
    handle = handle if handle is not None else (ctx.obj or {}).get("handle")
    if handle is not None:
        validate_handle(handle)
    existing = None if no_login else _check_profile(replace_profile)
    config = _config(env_files)
    gate_env = _gate_env(config)
    url = gate_env.get("KONTRA_PUBLIC_URL", "").rstrip("/")
    if not url:
        fail("the compose file sets no KONTRA_PUBLIC_URL for the gate")
    _compose(env_files, "up", "--detach", "--remove-orphans")
    _wait_healthy(url, timeout)
    click.echo(f"kontra dev is up (compose project {PROJECT})")
    click.echo(f"  gate      {url}")
    click.echo(f"  temporal  {gate_env.get('KONTRA_TEMPORAL_PUBLIC', '?')}")
    click.echo(f"  bucket    {gate_env.get('KONTRA_S3_PUBLIC_ENDPOINT', '?')}")
    if (pg := _published(config, "postgres")) is not None:
        click.echo(f"  postgres  {pg}")
    if no_login:
        return
    if gate_env.get("KONTRA_DEV") not in ("1", "true"):
        click.echo("the gate is not a dev gate (KONTRA_DEV is not 1): log in with `kontra login`")
        return
    if handle is not None:
        party = handle
    elif existing is not None and existing.dev and existing.gate_url == url:
        party = existing.party  # `kontra dev` again: the same dev party as last time
    else:
        party = default_handle()
    home = kontra_home()
    profile = run(lambda: login(url, handle=party, dev=True, home=home, replace_key=True))
    run(lambda: _wait_temporal(home, _TEMPORAL_TIMEOUT))
    click.echo(f"logged in as {profile.party} (namespace {profile.namespace}), play money")
    click.echo(f"  profile   {home / PROFILE_FILE} (dev)")
    click.echo(f"  key       {home / KEY_FILE} ({profile.public_key})")
    click.echo("next: kontra init <name> && cd <name> && kontra serve")


@dev.command()
@click.pass_context
@_ENV_FILES
@click.option("--volumes", is_flag=True, help="Also delete the stack's data (Postgres, bucket).")
def down(ctx: click.Context, env_files: tuple[Path, ...], volumes: bool) -> None:
    """Stop the stack (its data is kept unless --volumes)."""
    env_files = _given(ctx, env_files)
    _compose(env_files, "down", "--remove-orphans", *(["--volumes"] if volumes else []))
    click.echo("kontra dev is down" + (" and its data deleted" if volumes else ""))


@dev.command()
@click.pass_context
@_ENV_FILES
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def status(ctx: click.Context, env_files: tuple[Path, ...], as_json: bool) -> None:
    """The stack's containers, the gate's health, and the profile."""
    from kontra.profile import Profile, ProfileError

    env_files = _given(ctx, env_files)
    raw = _compose(env_files, "ps", "--all", "--format", "json", capture=True)
    services: list[dict[str, Any]] = []
    for line in raw.splitlines():  # compose prints one JSON object per line (or one array)
        line = line.strip()
        if not line:
            continue
        try:
            parsed: Any = json.loads(line)
        except ValueError:
            fail(f"docker compose ps answered something unexpected: {line[:200]}")
        services += parsed if isinstance(parsed, list) else [parsed]
    url = _gate_env(_config(env_files)).get("KONTRA_PUBLIC_URL", "").rstrip("/")
    try:
        healthy = httpx.get(f"{url}/healthz", timeout=5).status_code == 200
    except httpx.HTTPError:
        healthy = False
    try:
        profile: Profile | None = Profile.load()
    except ProfileError:
        profile = None
    rows = [
        {
            "service": s.get("Service"),
            "state": s.get("State"),
            "health": s.get("Health") or None,
            "status": s.get("Status"),
        }
        for s in services
    ]
    if as_json:
        emit_json(
            {
                "project": PROJECT,
                "services": rows,
                "gate": {"url": url, "healthy": healthy},
                "profile": None
                if profile is None
                else {"party": profile.party, "gate_url": profile.gate_url, "dev": profile.dev},
            }
        )
        return
    if not rows:
        click.echo(f"kontra dev is not running (no containers in project {PROJECT})")
    for r in rows:
        click.echo(f"{r['service']:<10} {r['status'] or r['state']}")
    click.echo(f"gate {url}: {'healthy' if healthy else 'not answering'}")
    if profile is not None:
        click.echo(
            f"profile: {profile.party} at {profile.gate_url}{' (dev)' if profile.dev else ''}"
        )


@dev.command()
@click.pass_context
@_ENV_FILES
@click.option("-f", "--follow", is_flag=True, help="Keep printing new lines.")
@click.option("--tail", metavar="N", default="200", show_default=True, help="Lines per service.")
@click.argument("services", nargs=-1)
def logs(
    ctx: click.Context,
    env_files: tuple[Path, ...],
    follow: bool,
    tail: str,
    services: tuple[str, ...],
) -> None:
    """The stack's logs (all services, or the ones named)."""
    env_files = _given(ctx, env_files)
    if re.fullmatch(r"all|[0-9]{1,7}", tail, re.ASCII) is None:
        fail("--tail is a number of lines, or all")
    args = ["logs", "--tail", tail, *(["--follow"] if follow else []), "--", *services]
    _compose(env_files, *args)
