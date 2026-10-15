"""``kontra dev``: the local stack, from the one environment definition (PRD §19.3).

The stack is the repository's root ``compose.yaml``, shipped in the wheel as
``kontra/_dev/compose.yaml``; there is no second way to bring it up. It runs as the compose
project :data:`PROJECT` with two env files: ``$KONTRA_HOME/dev.env`` (written once by this
command: ``KONTRA_DEV=1`` and random credentials for the local Postgres and bucket -- the
compose file itself has no default secrets, so production cannot inherit one) and any
``--env-file`` given after it. Variables in the environment win over both, as in compose.

``up`` remembers the host ports it ran with (the ``KONTRA_DEV_*_PORT`` settings the compose
file lists under ``x-kontra-dev-settings``) in that same ``dev.env``, so ``status``, ``logs``
and ``down`` -- and the next ``up`` -- find the stack where it was put without being told
again. Never the bind addresses: they decide who on the network reaches a dev gate (dev
login, play money), so publishing beyond loopback is asked for on each ``up``. It then waits for the gate's ``/healthz``, logs in to it as a dev party
(``--handle``, else derived from the OS user), writes a dev profile, and waits until Temporal
accepts the party's worker token: the gate creates namespaces through Temporal's internal
frontend, and Temporal learns the gate's signing keys by polling its JWKS, so each side comes
up without the other and the order is settled by retries, not by ``depends_on`` alone.

Two compose profiles (:data:`PROFILES`) add dev tools, each started by its own flag and
only then: ``up`` runs exactly the profiles it is given and stops the containers of the
others, so ``kontra dev up`` alone is the stack it always was. ``--ui`` is
Temporal's own UI, which shows what is encrypted to this machine's key through a codec server
``up`` starts on 127.0.0.1 (:mod:`kontra.cli._codec_server`, in the background; ``down``
stops it). ``--observability`` is Prometheus and Grafana, with Temporal's metrics listener
turned on for Prometheus to scrape. ``status``, ``logs`` and ``down`` see every profile.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import httpx

import kontra
from kontra.cli._common import CliError, default_handle, emit_json, fail, run

if TYPE_CHECKING:
    from kontra.profile import Profile

__all__ = [
    "CODEC_LOG",
    "CODEC_STATE",
    "DEV_ENV_FILE",
    "PROFILES",
    "PROJECT",
    "compose_file",
    "dev",
    "dev_env",
]

PROJECT = "kontra-dev"
DEV_ENV_FILE = "dev.env"
#: The compose profiles, by the flag of ``up`` that starts each.
PROFILES = ("ui", "observability")
#: Where Temporal serves its metrics while the observability profile runs; the profile's
#: Prometheus scrapes ``temporal:8000`` (observability/prometheus.yml).
_METRICS_SETTING = "KONTRA_TEMPORAL_METRICS_ADDR"
_METRICS_ADDR = "0.0.0.0:8000"
#: The background codec server's pid, port and stop token (0600), and its output.
CODEC_STATE = "codec.json"
CODEC_LOG = "codec.log"
_CODEC_START_TIMEOUT = 15.0
#: The only codec endpoint the UI may be given: the server listens on 127.0.0.1 alone.
_CODEC_ENDPOINT = re.compile(r"http://127\.0\.0\.1:([0-9]{1,5})", re.ASCII)
_HEALTH_TIMEOUT = 300.0
_TEMPORAL_TIMEOUT = 180.0
#: The compose file's list of the settings ``up`` remembers, resolved by compose itself.
_SETTINGS_KEY = "x-kontra-dev-settings"
#: Host ports only: an address (KONTRA_DEV_BIND) remembered would quietly publish every later
#: stack wherever one ``up`` once did, so a compose file listing one is refused here too.
_SETTING_NAME = re.compile(r"KONTRA_DEV_[A-Z0-9_]{1,64}_PORT")
#: A port: nothing a dotenv file would read as quoting, a comment, an interpolation or a
#: second line.
_SETTING_VALUE = re.compile(r"[0-9]{1,5}")
_SETTINGS_HEADER = "# The host ports of the last `kontra dev up`:"


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


def _settings(config: dict[str, Any]) -> dict[str, str]:
    """The ``KONTRA_DEV_*`` settings as compose resolved them for this run (environment,
    then the env files, then ``dev.env``); empty means compose's default."""
    raw: Any = config.get(_SETTINGS_KEY) or {}
    if not isinstance(raw, dict):
        fail(f"compose.yaml's {_SETTINGS_KEY} is not a mapping")
    settings: dict[str, str] = {}
    for key, val in raw.items():
        name, value = str(key), "" if val is None else str(val)
        if _SETTING_NAME.fullmatch(name) is None:
            fail(f"compose.yaml's {_SETTINGS_KEY} lists {name!r}, which is not a KONTRA_DEV_*_PORT")
        if value and _SETTING_VALUE.fullmatch(value) is None:
            fail(f"{name}={value!r} is not a port")
        settings[name] = value
    return settings


def _remember(home: Path, settings: Mapping[str, str]) -> None:
    """Write ``settings`` into ``dev.env`` in place of what an earlier ``up`` wrote there.
    ``status``, ``logs`` and ``down`` pass ``dev.env`` to compose too, so they resolve the
    ports ``up`` used; a setting that is empty now (compose's default) is dropped. The
    credentials in the file are kept as they are."""
    path = dev_env(home)
    before = path.read_text(encoding="utf-8")
    kept: list[str] = []
    # "\n" only, as compose reads the file: str.splitlines would also split a value at a
    # form feed or U+2028 and join it back with a newline.
    for line in before.split("\n"):
        name, sep, _ = line.partition("=")
        if line == _SETTINGS_HEADER or (sep and name in settings):
            continue
        kept.append(line)
    while kept and not kept[-1].strip():
        kept.pop()
    chosen = [f"{name}={value}" for name, value in settings.items() if value]
    text = "\n".join([*kept, *([_SETTINGS_HEADER, *chosen] if chosen else [])]) + "\n"
    if text == before:
        return
    # A new file renamed over the old one: never a half-written dev.env, still 0600.
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(8)}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _docker() -> str:
    found = shutil.which("docker")
    if found is None:
        fail("kontra dev needs Docker with the compose plugin (docker compose); install it first")
    return found


def _compose_cmd(
    env_files: tuple[Path, ...], *args: str, profiles: Sequence[str] = ()
) -> list[str]:
    from kontra.profile import kontra_home

    cmd = [_docker(), "compose", "-p", PROJECT, "-f", str(compose_file())]
    for env in (dev_env(kontra_home()), *env_files):
        cmd += ["--env-file", str(env)]
    for profile in profiles:
        cmd += ["--profile", profile]
    return [*cmd, *args]


def _compose(
    env_files: tuple[Path, ...],
    *args: str,
    capture: bool = False,
    profiles: Sequence[str] = (),
    env: Mapping[str, str] | None = None,
) -> str:
    """Run ``docker compose`` on the dev project, with ``profiles`` enabled and ``env`` as
    its environment (default: ours); its own output goes to the terminal unless
    ``capture``. A failure ends the command with compose's message."""
    cmd = _compose_cmd(env_files, *args, profiles=profiles)
    proc = subprocess.run(  # noqa: S603 -- argv, no shell
        cmd,
        capture_output=capture,
        text=True,
        check=False,
        env=None if env is None else dict(env),
    )
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() if capture else ""
        fail(
            f"docker compose {args[0]} failed (exit {proc.returncode}){': ' + detail if detail else ''}"
        )
    return proc.stdout if capture else ""


def _config(
    env_files: tuple[Path, ...],
    profiles: Sequence[str] = (),
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    out = _compose(
        env_files, "config", "--format", "json", capture=True, profiles=profiles, env=env
    )
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


def _in_profiles(config: dict[str, Any], profiles: Sequence[str]) -> list[str]:
    """The services of ``config`` that belong to one of ``profiles``."""
    services: Any = config.get("services") or {}
    return [
        str(name)
        for name, svc in services.items()
        if isinstance(svc, dict) and set(svc.get("profiles") or ()) & set(profiles)  # pyright: ignore[reportUnknownArgumentType]
    ]


def _compose_env(observability: bool) -> dict[str, str] | None:
    """Compose's environment for ``up``: ours, plus Temporal's metrics listener when the
    observability profile runs (unless the environment sets where already)."""
    if not observability or os.environ.get(_METRICS_SETTING):
        return None
    return {**os.environ, _METRICS_SETTING: _METRICS_ADDR}


# --------------------------------------------------------------------------- the codec server


def _codec_http() -> httpx.Client:
    """A client for the codec server: never through a proxy (the stop token must not leave
    this machine)."""
    return httpx.Client(trust_env=False, timeout=5)


def _codec_answers(port: int) -> bool:
    """Whether a codec server answers on 127.0.0.1:``port`` (an empty decode)."""
    try:
        with _codec_http() as http:
            r = http.post(f"http://127.0.0.1:{port}/decode", json={"payloads": []})
    except httpx.HTTPError:
        return False
    return r.status_code == 200


def _codec_state(home: Path) -> dict[str, Any] | None:
    try:
        raw: Any = json.loads((home / CODEC_STATE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("port"), int):
        return None
    return {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownVariableType]


def _stop_codec(home: Path) -> bool:
    """Stop the codec server an earlier ``up`` started from this home; whether one was
    running. One that answers with another token is not ours, and is left alone."""
    state = _codec_state(home)
    if state is None:
        return False
    stopped = False
    try:
        with _codec_http() as http:
            r = http.post(
                f"http://127.0.0.1:{state['port']}/stop",
                headers={"Authorization": f"Bearer {state.get('token', '')}"},
            )
        stopped = r.status_code == 204
    except httpx.HTTPError:
        pass  # not running any more
    (home / CODEC_STATE).unlink(missing_ok=True)
    return stopped


def _codec_ports(config: dict[str, Any]) -> tuple[int, int]:
    """(the codec server's port, the UI's) from the UI service the compose file defines."""
    ui: Any = (config.get("services") or {}).get("ui") or {}
    endpoint = str((ui.get("environment") or {}).get("TEMPORAL_CODEC_ENDPOINT") or "")
    m = _CODEC_ENDPOINT.fullmatch(endpoint)
    if m is None:
        fail(
            f"the UI's TEMPORAL_CODEC_ENDPOINT is {endpoint!r}; the codec server listens on "
            "http://127.0.0.1:<port> only"
        )
    published = _published(config, "ui")
    if published is None:
        fail("the compose file publishes no port for the UI")
    return int(m.group(1)), int(published.rsplit(":", 1)[1])


def _start_codec(home: Path, config: dict[str, Any]) -> str:
    """Start the codec server in the background for the UI of ``config`` (the one an
    earlier ``up`` started is stopped first); its URL. Raises :class:`CliError` with the
    server's own words if it does not come up."""
    port, ui_port = _codec_ports(config)
    _stop_codec(home)
    token = secrets.token_urlsafe(32)
    log_path = home / CODEC_LOG
    log = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        proc = subprocess.Popen(  # noqa: S603 -- argv, no shell
            [
                sys.executable,
                *("-m", "kontra.cli._codec_server"),
                *("--port", str(port), "--ui-port", str(ui_port)),
            ],
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=log,
            start_new_session=True,  # outlives this command, and its terminal's Ctrl-C
        )
    finally:
        os.close(log)
    assert proc.stdin is not None
    proc.stdin.write(f"{token}\n".encode())
    proc.stdin.close()
    state = json.dumps({"pid": proc.pid, "port": port, "token": token})
    fd = os.open(home / CODEC_STATE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(state)
    deadline = time.monotonic() + _CODEC_START_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        if _codec_answers(port):
            return f"http://127.0.0.1:{port}"
        time.sleep(0.2)
    (home / CODEC_STATE).unlink(missing_ok=True)
    if proc.poll() is None:
        proc.terminate()
    lines = log_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
    fail(f"the codec server did not start: {lines[-1] if lines else 'no output'} ({log_path})")


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

    `kontra dev` alone is `kontra dev up`; `up --ui` and `up --observability` add Temporal's
    UI and Prometheus with Grafana."""
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
@click.option(
    "--ui",
    is_flag=True,
    help="Also Temporal's UI, on 127.0.0.1, decoding with this machine's key (dev only).",
)
@click.option(
    "--observability",
    is_flag=True,
    help="Also Prometheus and Grafana with Temporal's dashboards, on 127.0.0.1.",
)
def up(
    ctx: click.Context,
    handle: str | None = None,
    env_files: tuple[Path, ...] = (),
    timeout: float = _HEALTH_TIMEOUT,
    replace_profile: bool = False,
    no_login: bool = False,
    ui: bool = False,
    observability: bool = False,
) -> None:
    """Start the stack, wait for the gate, and log in to it as a dev party."""
    from kontra.contract.names import validate_handle
    from kontra.profile import kontra_home

    env_files = _given(ctx, env_files)
    handle = handle if handle is not None else (ctx.obj or {}).get("handle")
    if handle is not None:
        validate_handle(handle)
    existing = None if no_login else _check_profile(replace_profile)
    asked = [name for name, on in zip(PROFILES, (ui, observability), strict=True) if on]
    env = _compose_env(observability)
    config = _config(env_files, PROFILES, env)
    gate_env = _gate_env(config)
    url = gate_env.get("KONTRA_PUBLIC_URL", "").rstrip("/")
    if not url:
        fail("the compose file sets no KONTRA_PUBLIC_URL for the gate")
    home = kontra_home()
    # Before the stack starts: a failed `up` is exactly when `status` and `logs` are needed.
    _remember(home, _settings(config))
    # Exactly the profiles asked for: what an earlier `up --ui` started stops here.
    unasked = _in_profiles(config, [p for p in PROFILES if p not in asked])
    if unasked:
        _compose(
            env_files, "rm", "--stop", "--force", *unasked, capture=True, profiles=PROFILES, env=env
        )
    if not ui:
        _stop_codec(home)
    _compose(env_files, "up", "--detach", "--remove-orphans", profiles=asked, env=env)
    _wait_healthy(url, timeout)
    click.echo(f"kontra dev is up (compose project {PROJECT})")
    click.echo(f"  gate      {url}")
    click.echo(f"  temporal  {gate_env.get('KONTRA_TEMPORAL_PUBLIC', '?')}")
    click.echo(f"  bucket    {gate_env.get('KONTRA_S3_PUBLIC_ENDPOINT', '?')}")
    if (pg := _published(config, "postgres")) is not None:
        click.echo(f"  postgres  {pg}")
    for service, shown in (("ui", ui), ("prometheus", observability), ("grafana", observability)):
        if shown and (where := _published(config, service)) is not None:
            click.echo(f"  {service:<9} http://{where}")
    profile = None if no_login else _dev_login(gate_env, url, handle, existing, home)
    if ui:
        if profile is not None and (where := _published(config, "ui")) is not None:
            click.echo(f"  workflows http://{where}/namespaces/{profile.namespace}/workflows")
        _up_codec(home, config)
    if profile is not None:
        click.echo("next: kontra init <name> && cd <name> && kontra serve")


def _dev_login(
    gate_env: dict[str, str],
    url: str,
    handle: str | None,
    existing: Profile | None,
    home: Path,
) -> Profile | None:
    """``up``'s login as a dev party, once the gate answers; None when the gate is not a dev
    gate."""
    from kontra.auth import login
    from kontra.profile import KEY_FILE, PROFILE_FILE

    if gate_env.get("KONTRA_DEV") not in ("1", "true"):
        click.echo("the gate is not a dev gate (KONTRA_DEV is not 1): log in with `kontra login`")
        return None
    if handle is not None:
        party = handle
    elif existing is not None and existing.dev and existing.gate_url == url:
        party = existing.party  # `kontra dev` again: the same dev party as last time
    else:
        party = default_handle()
    profile = run(lambda: login(url, handle=party, dev=True, home=home))
    run(lambda: _wait_temporal(home, _TEMPORAL_TIMEOUT))
    click.echo(f"logged in as {profile.party} (namespace {profile.namespace}), play money")
    click.echo(f"  profile   {home / PROFILE_FILE} (dev)")
    click.echo(f"  key       {home / KEY_FILE} ({profile.public_key})")
    return profile


def _up_codec(home: Path, config: dict[str, Any]) -> None:
    """Start the UI's codec server. A failure is said and the stack stays up: the UI then
    shows payloads as ciphertext."""
    try:
        where = _start_codec(home, config)
    except CliError as e:
        click.echo(f"codec server not started: {e.format_message()}", err=True)
        return
    click.echo(f"codec server {where}: the UI decodes with this machine's key")


@dev.command()
@click.pass_context
@_ENV_FILES
@click.option("--volumes", is_flag=True, help="Also delete the stack's data (Postgres, bucket).")
def down(ctx: click.Context, env_files: tuple[Path, ...], volumes: bool) -> None:
    """Stop the stack (its data is kept unless --volumes)."""
    from kontra.profile import kontra_home

    env_files = _given(ctx, env_files)
    _stop_codec(kontra_home())
    # Every profile: compose leaves the containers of one not enabled running.
    _compose(
        env_files,
        "down",
        "--remove-orphans",
        *(["--volumes"] if volumes else []),
        profiles=PROFILES,
    )
    click.echo("kontra dev is down" + (" and its data deleted" if volumes else ""))


@dev.command()
@click.pass_context
@_ENV_FILES
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def status(ctx: click.Context, env_files: tuple[Path, ...], as_json: bool) -> None:
    """The stack's containers, the gate's health, the UI's codec server, and the profile."""
    from kontra.profile import Profile, ProfileError, kontra_home

    env_files = _given(ctx, env_files)
    raw = _compose(env_files, "ps", "--all", "--format", "json", capture=True, profiles=PROFILES)
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
    codec_state = _codec_state(kontra_home())
    codec = (
        None
        if codec_state is None
        else {
            "url": f"http://127.0.0.1:{codec_state['port']}",
            "running": _codec_answers(codec_state["port"]),
        }
    )
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
                "codec": codec,
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
    if codec is not None:
        click.echo(
            f"codec server {codec['url']}: {'running' if codec['running'] else 'not answering'}"
        )
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
    _compose(env_files, *args, profiles=PROFILES)
