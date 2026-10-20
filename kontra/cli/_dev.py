"""``kontra dev``: a local Kontra on this machine with no containers (decisions I34).

``kontra dev up`` spawns one host process (:mod:`kontra.devgate`) that runs the public dev gate
and a Temporal CLI dev server, loopback only, and mints a dev profile from the OS user. There is
no login verb and no ``--real`` mode: the compose stack with the prod gate image is the
repository's e2e tooling (``make stack-up``/``make e2e``), never a choice here. Only
``--observability`` runs containers (a shipped observability-only compose file).

``down``, ``status`` and ``logs`` talk to the host over three control routes and never import
FastAPI or uvicorn; ``up``'s ports come from ``$KONTRA_HOME/dev/settings.json`` and the flags
below, never the ``KONTRA_DEV_*_PORT`` the e2e stack reads (OD8).
"""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import httpx

import kontra
from kontra.cli._common import CliError, default_handle, emit_json, fail, run
from kontra.devgate import state
from kontra.devgate.state import Ports

if TYPE_CHECKING:
    from kontra.devgate.state import StackRecord
    from kontra.profile import Profile

__all__ = [
    "CODEC_LOG",
    "CODEC_STATE",
    "OBSERVABILITY_PROJECT",
    "OBSERVABILITY_SERVICES",
    "dev",
    "observability_compose_file",
]

#: The background codec server's pid, port and stop token (0600), and its output. The seam ⑤
#: codec tests key on these names (``dev_module.CODEC_STATE``/``CODEC_LOG``).
CODEC_STATE = "codec.json"
CODEC_LOG = "codec.log"
_CODEC_START_TIMEOUT = 15.0

#: ``--observability`` is the one flag that runs containers: a shipped compose file with only
#: the observability services, run as its own project with ``-p``/``-f`` and nothing a user
#: chooses (no ``--env-file``, no extra ``-f``), so it is not a back door to the prod gate (§7).
OBSERVABILITY_PROJECT = "kontra-dev-observability"
OBSERVABILITY_SERVICES = ("prometheus", "prometheus-agent", "grafana", "otelcol", "loki", "tempo")

_READY_TIMEOUT = 120.0
_STOP_WAIT = 20.0
_HOST_HTTP_TIMEOUT = 10.0


# --------------------------------------------------------------------------- talking to the host


def _host_http() -> httpx.Client:
    """A client for the host and the codec server: never through a proxy (the control token
    must not leave this machine)."""
    return httpx.Client(trust_env=False, timeout=_HOST_HTTP_TIMEOUT)


def _host_headers(port: int, token: str | None = None) -> dict[str, str]:
    headers = {"Host": f"127.0.0.1:{port}"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _ready_doc(port: int, token: str) -> dict[str, Any] | None:
    """``GET /_devgate/ready`` with the token, proving the process on ``port`` is this host's
    (not another home's, not a stale pid); the host's readiness document, or None."""
    try:
        with _host_http() as http:
            r = http.get(
                f"http://127.0.0.1:{port}/_devgate/ready", headers=_host_headers(port, token)
            )
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    try:
        doc = r.json()
    except ValueError:
        return None
    return doc if isinstance(doc, dict) else None


def _control_post(port: int, token: str, route: str, body: dict[str, Any]) -> dict[str, Any]:
    with _host_http() as http:
        r = http.post(
            f"http://127.0.0.1:{port}/_devgate/{route}",
            headers=_host_headers(port, token),
            json=body,
        )
    if r.status_code not in (200, 204):
        fail(f"the dev gate refused /_devgate/{route} ({r.status_code})")
    try:
        return r.json() if r.content else {}
    except ValueError:
        return {}


def _info_is_dev(port: int) -> bool:
    """``GET /v1/info`` says ``dev: true`` (``up`` exits non-zero otherwise, never suggesting a
    login): proof that what answers is a dev gate."""
    try:
        with _host_http() as http:
            r = http.get(f"http://127.0.0.1:{port}/v1/info", headers=_host_headers(port))
    except httpx.HTTPError:
        return False
    try:
        return r.status_code == 200 and bool(r.json().get("dev"))
    except ValueError:
        return False


# --------------------------------------------------------------------------- the codec server


def _codec_http() -> httpx.Client:
    return httpx.Client(trust_env=False, timeout=5)


def _codec_running(port: int, token: str) -> bool:
    try:
        with _codec_http() as http:
            r = http.get(
                f"http://127.0.0.1:{port}/ready", headers={"Authorization": f"Bearer {token}"}
            )
    except httpx.HTTPError:
        return False
    return r.status_code == 204


def _codec_state(home: Path) -> dict[str, Any] | None:
    try:
        raw: Any = json.loads((home / CODEC_STATE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict) or not isinstance(raw.get("port"), int):
        return None
    return {str(k): v for k, v in raw.items()}  # pyright: ignore[reportUnknownVariableType]


def _stop_codec(home: Path) -> bool:
    """Stop the codec server an earlier ``up`` started from this home; whether one was running.
    One that answers with another token is not ours, and is left alone."""
    st = _codec_state(home)
    if st is None:
        return False
    stopped = False
    try:
        with _codec_http() as http:
            r = http.post(
                f"http://127.0.0.1:{st['port']}/stop",
                headers={"Authorization": f"Bearer {st.get('token', '')}"},
            )
        stopped = r.status_code == 204
    except httpx.HTTPError:
        pass
    (home / CODEC_STATE).unlink(missing_ok=True)
    return stopped


def _start_codec(
    home: Path, *, codec_port: int, ui_port: int, grafana_url: str | None = None
) -> str:
    """Start the codec server in the background on ``codec_port`` for Temporal's UI on
    ``ui_port`` (the one an earlier ``up`` started is stopped first), linking the graph page to
    the UI and, with ``grafana_url``, to Grafana. Its URL; a failure raises :class:`CliError`
    with the server's own last words."""
    _stop_codec(home)
    token = secrets.token_urlsafe(32)
    log_path = home / CODEC_LOG
    argv = [
        sys.executable, "-m", "kontra.cli._codec_server",
        "--port", str(codec_port), "--ui-port", str(ui_port),
        "--temporal-ui", f"http://127.0.0.1:{ui_port}",
    ]  # fmt: skip
    if grafana_url is not None:
        argv += ["--grafana", grafana_url]
    log = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        proc = subprocess.Popen(  # noqa: S603 -- argv, no shell
            argv, stdin=subprocess.PIPE, stdout=log, stderr=log, start_new_session=True
        )
    finally:
        os.close(log)
    assert proc.stdin is not None
    proc.stdin.write(f"{token}\n".encode())
    proc.stdin.close()
    fd = os.open(home / CODEC_STATE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(json.dumps({"pid": proc.pid, "port": codec_port, "token": token}))
    deadline = time.monotonic() + _CODEC_START_TIMEOUT
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        if _codec_running(codec_port, token):
            return f"http://127.0.0.1:{codec_port}"
        time.sleep(0.2)
    (home / CODEC_STATE).unlink(missing_ok=True)
    if proc.poll() is None:
        proc.terminate()
    lines = log_path.read_text(encoding="utf-8", errors="replace").strip().splitlines()
    fail(f"the codec server did not start: {lines[-1] if lines else 'no output'} ({log_path})")


# --------------------------------------------------------------------------- observability compose


def _observability_file() -> Path | None:
    """The shipped observability-only ``compose.yaml`` (wheel: ``kontra/_dev/observability``;
    checkout: ``observability/``), or None when it is not packaged."""
    pkg = Path(kontra.__file__).resolve().parent
    for candidate in (
        pkg / "_dev" / "observability" / "compose.yaml",
        pkg.parent / "observability" / "compose.yaml",
    ):
        if candidate.is_file():
            return candidate
    return None


def observability_compose_file() -> Path:
    """:func:`_observability_file`, or fail: ``--observability`` needs it shipped."""
    found = _observability_file()
    if found is None:
        fail(
            "the observability compose file is missing from the kontra package; reinstall kontra-sdk"
        )
    return found


def _docker() -> str | None:
    return shutil.which("docker")


def _observability_cmd(*args: str) -> list[str]:
    docker = _docker()
    assert docker is not None
    return [
        docker, "compose", "-p", OBSERVABILITY_PROJECT, "-f", str(observability_compose_file()),
        *args,
    ]  # fmt: skip


def _observability_env(ports: Ports) -> dict[str, str]:
    """Only the port variables the observability file reads, beyond our own environment."""
    return {
        **os.environ,
        state.PORT_ENV["prometheus"]: str(ports.prometheus),
        state.PORT_ENV["grafana"]: str(ports.grafana),
        state.PORT_ENV["otlp"]: str(ports.otlp),
        state.PORT_ENV["temporal_metrics"]: str(ports.temporal_metrics),
    }


def _observability_up(ports: Ports) -> None:
    subprocess.run(  # noqa: S603 -- argv, no shell
        _observability_cmd("up", "-d", "--remove-orphans"),
        check=True,
        env=_observability_env(ports),
    )


def _observability_down(ports: Ports) -> None:
    # Best-effort: nothing to take down without Docker or the shipped file.
    if _docker() is None or _observability_file() is None:
        return
    subprocess.run(  # noqa: S603 -- argv, no shell
        _observability_cmd("down", "--remove-orphans"),
        check=False,
        env=_observability_env(ports),
    )


# --------------------------------------------------------------------------- profile & cleanup


def _check_profile(replace: bool) -> Profile | None:
    """The profile here, if ``up`` may replace it: a dev one, or any with ``--replace-profile``.
    ``up`` never writes over a real login without being asked, and never points at a login verb."""
    from kontra.profile import Profile, ProfileError, kontra_home

    try:
        existing = Profile.load()
    except ProfileError:  # NotLoggedIn is a ProfileError
        return None
    if existing.dev or replace:
        return existing
    fail(
        f"{kontra_home()} holds your login to {existing.gate_url} as {existing.party}, which "
        "`kontra dev` would replace. Use another home for dev (KONTRA_HOME=~/.kontra-dev "
        "kontra dev), or pass --replace-profile."
    )


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _group_is_ours(pid: int, pgid: int, cli_path: str) -> bool:
    """Whether the recorded process group is still this stack's: the leader is alive in it, or
    (a dead leader) a member's command line is the recorded Temporal CLI (Linux /proc)."""
    if _process_alive(pid):
        try:
            return os.getpgid(pid) == pgid
        except ProcessLookupError:
            return False
    proc = Path("/proc")
    if not proc.is_dir():
        return False
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if os.getpgid(int(entry.name)) != pgid:
                continue
            cmdline = (
                (entry / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace")
            )
        except (OSError, ProcessLookupError):
            continue
        if cli_path in cmdline:
            return True
    return False


def _signal_group(pgid: int, *, grace: float = 5.0) -> None:
    try:
        os.killpg(pgid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        return
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.1)
    with _suppress_process_errors():
        os.killpg(pgid, signal.SIGKILL)


class _suppress_process_errors:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: object, *_: object) -> bool:
        return exc_type is None or issubclass(exc_type, (ProcessLookupError, PermissionError))  # type: ignore[arg-type]


def _cleanup_stack(record: StackRecord, *, persist: bool, home: Path) -> None:
    """Stop the recorded stack's process group if it is still ours, then forget it and (unless
    persist) wipe the in-memory data."""
    if _group_is_ours(record.pid, record.pgid, record.cli.get("path", "")):
        _signal_group(record.pgid)
    state.remove_stack_record(home)
    state.unregister_port(record.port_set.gate)
    if not persist:
        shutil.rmtree(state.data_dir(home), ignore_errors=True)


# --------------------------------------------------------------------------- the host


def _spawn_host(
    home: Path,
    ports: Ports,
    cli: Path,
    *,
    ui: bool,
    metrics: bool,
    persist: bool,
    token: str,
    timeout: float,
) -> int:
    """Spawn ``python -m kontra.devgate`` detached (new session, the config and token on stdin,
    a ready pipe) and wait for ``ready``. Returns the host pid. On failure: kill the group,
    print the last log lines, and exit 1."""
    read_fd, write_fd = os.pipe()
    cfg = {
        "home": str(home),
        "ports": asdict(ports),
        "cli": str(cli),
        "ui": ui,
        "metrics": metrics,
        "persist": persist,
        "ready_fd": write_fd,
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "kontra.devgate"],
        stdin=subprocess.PIPE,
        # The detached host must not hold this command's stdout/stderr: a caller that captures
        # `kontra dev up`'s output (a test, CI, `$(...)`) would otherwise block on the open pipe
        # until the host exits. The host logs to dev/logs/gate.log; startup errors come back on
        # the ready pipe.
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        pass_fds=(write_fd,),
        start_new_session=True,
    )
    os.close(write_fd)
    assert proc.stdin is not None
    proc.stdin.write((json.dumps({"token": token, "config": cfg}) + "\n").encode())
    proc.stdin.close()
    signal_line = _await_ready(read_fd, proc, timeout)
    if signal_line != "ready":
        with _suppress_process_errors():
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        tails = _log_tails(home)
        detail = signal_line or "the host did not become ready"
        fail(f"kontra dev up failed: {detail}\n{tails}")
    return proc.pid


def _await_ready(read_fd: int, proc: subprocess.Popen[bytes], timeout: float) -> str:
    os.set_blocking(read_fd, False)
    deadline = time.monotonic() + timeout
    buf = b""
    try:
        while time.monotonic() < deadline:
            try:
                chunk = os.read(read_fd, 256)
                if chunk:
                    buf += chunk
                elif buf:
                    break
            except BlockingIOError:
                pass
            if b"\n" in buf:
                break
            if proc.poll() is not None and not buf:
                break
            time.sleep(0.05)
    finally:
        os.close(read_fd)
    return buf.decode("utf-8", "replace").strip()


def _log_tails(home: Path, lines: int = 15) -> str:
    out: list[str] = []
    for name in ("gate.log", "temporal.log"):
        path = state.logs_dir(home) / name
        try:
            tail = path.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-lines:]
        except OSError:
            continue
        if tail:
            out.append(f"--- {name} ---\n" + "\n".join(tail))
    return "\n".join(out) or "(no logs)"


# --------------------------------------------------------------------------- port flags


def _port_option(name: str, field: str, *, hidden: bool = False) -> Any:
    return click.option(
        f"--{name}",
        f"port_{field}",
        type=int,
        default=None,
        hidden=hidden,
        help=f"Bind the {field.replace('_', ' ')} on this loopback port (remembered).",
    )


def _port_overrides(**kwargs: int | None) -> dict[str, int | None]:
    return {
        field[len("port_") :]: value for field, value in kwargs.items() if field.startswith("port_")
    }


_PORT_FLAGS = [
    _port_option("gate-port", "gate"),
    _port_option("temporal-port", "temporal"),
    _port_option("ui-port", "ui"),
    _port_option("codec-port", "codec"),
    _port_option("metrics-port", "temporal_metrics", hidden=True),
    _port_option("prometheus-port", "prometheus", hidden=True),
    _port_option("grafana-port", "grafana", hidden=True),
    _port_option("otlp-port", "otlp", hidden=True),
]


def _with_port_flags(fn: Any) -> Any:
    for option in reversed(_PORT_FLAGS):
        fn = option(fn)
    return fn


# --------------------------------------------------------------------------- the CLI


_HANDLE = click.option(
    "--handle", metavar="H", help="Dev party handle (default: from your user name)."
)


@click.group(invoke_without_command=True)
@click.pass_context
@_HANDLE
def dev(ctx: click.Context, handle: str | None) -> None:
    """A local Kontra on this machine, no containers: the dev gate and a Temporal dev server.

    `kontra dev` alone is `kontra dev up`; `up --ui` adds Temporal's UI and the graph page,
    `up --observability` adds Prometheus, Grafana, Loki and Tempo (the one part that uses
    Docker)."""
    ctx.obj = {"handle": handle}
    if ctx.invoked_subcommand is None:
        ctx.invoke(up)


@dev.command()
@click.pass_context
@_HANDLE
@click.option(
    "--ui",
    is_flag=True,
    help="Also Temporal's UI and the graph page on 127.0.0.1, decoding with this machine's key (dev only).",
)
@click.option(
    "--observability",
    is_flag=True,
    help="Also Prometheus, Grafana, Loki and Tempo (Docker with the compose plugin).",
)
@click.option(
    "--persist/--no-persist",
    default=None,
    help="Keep the stack's state across down and reboots (remembered).",
)
@click.option("--replace-profile", is_flag=True, help="Replace a non-dev profile with the dev one.")
@click.option(
    "--foreground",
    is_flag=True,
    help="Run the stack in this terminal (Ctrl-C stops it) instead of detached.",
)
@click.option(
    "--timeout",
    type=float,
    default=_READY_TIMEOUT,
    show_default=True,
    help="Seconds to wait for the stack (the CLI download excluded).",
)
@_with_port_flags
def up(
    ctx: click.Context,
    handle: str | None = None,
    ui: bool = False,
    observability: bool = False,
    persist: bool | None = None,
    replace_profile: bool = False,
    foreground: bool = False,
    timeout: float = _READY_TIMEOUT,
    **port_kwargs: int | None,
) -> None:
    """Run the dev gate and Temporal on this machine, and log in as a dev party."""
    from kontra.contract.names import validate_handle
    from kontra.profile import kontra_home

    handle = handle if handle is not None else (ctx.obj or {}).get("handle")
    if handle is not None:
        validate_handle(handle)
    existing = _check_profile(replace_profile)
    home = kontra_home()

    ignored = state.ignored_env()
    if ignored:
        click.echo(
            f"note: {', '.join(ignored)} set but ignored (kontra dev runs no containers and its "
            "ports come from its settings and flags); the e2e stack reads them (make stack-up).",
            err=True,
        )
    if observability and _docker() is None:
        fail(
            "--observability needs Docker with the compose plugin; the rest of `kontra dev` does not"
        )

    settings = state.read_settings(home)
    want_persist = settings.persist if persist is None else persist
    overrides = _port_overrides(**port_kwargs)
    ports = state.resolve_ports(home, overrides)

    with state.lock(home):
        record = state.read_stack_record(home)
        running = record is not None and _ready_doc(record.port_set.gate, record.token) is not None
        if running:
            assert record is not None
            if record.port_set != ports and any(v is not None for v in overrides.values()):
                fail(
                    f"kontra dev is already up on 127.0.0.1:{record.port_set.gate}; "
                    "`kontra dev down` first (it drops its in-memory state), then up with new ports"
                )
            ports = record.port_set
            _control_post(
                ports.gate,
                record.token,
                "config",
                {"ui": ui, "metrics": observability, "persist": want_persist},
            )
        else:
            if record is not None:  # a stale record: a crash or a reboot
                _cleanup_stack(record, persist=want_persist, home=home)
            _start_fresh(
                home,
                ports,
                overrides,
                want_persist,
                ui,
                observability,
                timeout,
                foreground,
                handle,
                existing,
            )
            if foreground:
                return  # _start_fresh ran it in the foreground and has returned on Ctrl-C

    _finish(home, ports, handle, existing, ui, observability, want_persist, fresh=not running)


def _start_fresh(
    home: Path,
    ports: Ports,
    overrides: dict[str, int | None],
    persist: bool,
    ui: bool,
    observability: bool,
    timeout: float,
    foreground: bool,
    handle: str | None,
    existing: Profile | None,
) -> None:
    """Bind-check the ports, ensure the CLI, and start the host (detached, or in the
    foreground). Joins a sibling home's stack if this user already runs one on the gate port."""
    from kontra.devgate import temporal_cli

    for field in ("gate", "temporal", "ui", "codec"):
        port = getattr(ports, field)
        if state.port_taken(port):
            owner = state.registry_owner(ports.gate) if field == "gate" else None
            if field == "gate" and owner is not None and owner != home:
                fail(
                    f"127.0.0.1:{ports.gate} is another dev stack of yours (KONTRA_HOME={owner}); "
                    "`kontra dev up` there, or `kontra dev down` there first"
                )
            holder = state.port_holder(port)
            fail(
                f"127.0.0.1:{port} is taken{f' ({holder})' if holder else ''}; "
                f"free it or pass --{field.replace('_', '-')}-port"
            )
    cli = temporal_cli.ensure(say=lambda m: click.echo(m, err=True))
    if foreground:
        _run_foreground(
            home,
            ports,
            cli,
            ui=ui,
            metrics=observability,
            persist=persist,
            handle=handle,
            existing=existing,
        )
        return
    token = secrets.token_urlsafe(32)
    pid = _spawn_host(
        home,
        ports,
        cli,
        ui=ui,
        metrics=observability,
        persist=persist,
        token=token,
        timeout=timeout,
    )
    doc = _ready_doc(ports.gate, token)
    if doc is None:
        with _suppress_process_errors():
            os.killpg(os.getpgid(pid), signal.SIGKILL)
        fail("the dev gate did not answer its control token after starting")
    record = state.StackRecord(
        pid=pid,
        pgid=os.getpgid(pid),
        token=token,
        started_at=datetime.now(UTC).isoformat(),
        sdk_version=_sdk_version(),
        cli={"path": str(cli), "version": temporal_cli.VERSION},
        ports=asdict(ports),
        temporal_http=int(doc.get("temporal_http") or 0),
        ui=ui,
        metrics=observability,
        persist=persist,
    )
    state.write_stack_record(record, home)
    state.register_port(ports.gate, home)
    state.remember_ports(ports, home)
    settings = state.read_settings(home)
    settings.persist = persist
    state.write_settings(settings, home)


def _run_foreground(
    home: Path,
    ports: Ports,
    cli: Path,
    *,
    ui: bool,
    metrics: bool,
    persist: bool,
    handle: str | None,
    existing: Profile | None,
) -> None:
    """Run the stack in this process until Ctrl-C (for a container entrypoint, or to watch the
    log live). The profile is minted once serving, as detached ``up`` does."""
    import asyncio

    from kontra.auth import login
    from kontra.devgate.stack import DevStack, StackConfig

    config = StackConfig(home=home, ports=ports, cli=cli, ui=ui, metrics=metrics, persist=persist)
    token = secrets.token_urlsafe(32)
    if handle is not None:
        party = handle
    elif existing is not None and existing.dev:
        party = existing.party
    else:
        party = default_handle()

    async def _serve() -> None:
        async with DevStack(config, token=token) as stack:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, stack.request_stop)
            profile = await login(stack.gate_url, handle=party, dev=True, home=home)
            codec = None
            if ui:
                grafana = f"http://localhost:{ports.grafana}" if metrics else None
                try:
                    codec = _start_codec(
                        home, codec_port=ports.codec, ui_port=ports.ui, grafana_url=grafana
                    )
                except CliError as e:
                    click.echo(f"codec server not started: {e.format_message()}", err=True)
            if metrics:
                _observability_up(ports)
            _print_summary(
                home,
                ports,
                profile,
                ui=ui,
                observability=metrics,
                persist=persist,
                codec=codec,
                fresh=True,
            )
            click.echo("serving in the foreground; Ctrl-C to stop")
            try:
                await stack.wait_closed()
            finally:
                _stop_codec(home)
                if metrics:
                    _observability_down(ports)

    run(lambda: _serve())


def _finish(
    home: Path,
    ports: Ports,
    handle: str | None,
    existing: Profile | None,
    ui: bool,
    observability: bool,
    persist: bool,
    *,
    fresh: bool,
) -> None:
    """Check the gate is a dev gate, mint the profile, start/stop the codec and observability,
    and print. Exit 0 means a dev profile was written; every other outcome is 1."""
    if not _info_is_dev(ports.gate):
        fail(f"the server on 127.0.0.1:{ports.gate} is not this home's dev gate")
    profile = _mint_profile(home, ports, handle, existing)
    if ui:
        grafana = f"http://localhost:{ports.grafana}" if observability else None
        codec = _up_codec(home, ports, grafana)
    else:
        _stop_codec(home)
        codec = None
    settings = state.read_settings(home)
    if observability:
        _observability_up(ports)
        settings.observability = True
    elif settings.observability:
        # It was on last time and is not now: take the project down (the only time a plain
        # `up` touches Docker). A stack that never ran --observability never calls docker.
        _observability_down(ports)
        settings.observability = False
    state.write_settings(settings, home)
    _print_summary(
        home,
        ports,
        profile,
        ui=ui,
        observability=observability,
        persist=persist,
        codec=codec,
        fresh=fresh,
    )


def _mint_profile(
    home: Path, ports: Ports, handle: str | None, existing: Profile | None
) -> Profile:
    from kontra.auth import login

    if handle is not None:
        party = handle
    elif existing is not None and existing.dev:
        party = existing.party  # the same dev party as last time, even on a new port
    else:
        party = default_handle()
    url = f"http://127.0.0.1:{ports.gate}"
    return run(lambda: login(url, handle=party, dev=True, home=home))


def _up_codec(home: Path, ports: Ports, grafana_url: str | None) -> str | None:
    from kontra.profile import Profile, ProfileError

    try:
        profile: Profile | None = Profile.load(home)
    except ProfileError:
        profile = None
    if profile is None or not profile.dev:
        click.echo(
            f"codec server not started: {home} holds no dev login, and it would decrypt with "
            "its key for anyone on this machine; the UI shows payloads as ciphertext.",
            err=True,
        )
        return None
    try:
        return _start_codec(home, codec_port=ports.codec, ui_port=ports.ui, grafana_url=grafana_url)
    except CliError as e:
        click.echo(f"codec server not started: {e.format_message()}", err=True)
        return None


def _print_summary(
    home: Path,
    ports: Ports,
    profile: Profile,
    *,
    ui: bool,
    observability: bool,
    persist: bool,
    codec: str | None,
    fresh: bool,
) -> None:
    from kontra.profile import KEY_FILE, PROFILE_FILE

    kept = "`kontra dev up --persist` keeps it" if not persist else "kept with --persist"
    click.echo(f"kontra dev is up: no containers, state in memory ({kept})")
    click.echo(f"  gate      http://127.0.0.1:{ports.gate}")
    click.echo(f"  temporal  127.0.0.1:{ports.temporal}   (Temporal CLI dev server: no authorizer)")
    click.echo(f"logged in as {profile.party} (namespace {profile.namespace}), play money")
    click.echo(f"  profile   {home / PROFILE_FILE} (dev)")
    click.echo(f"  key       {home / KEY_FILE} ({profile.public_key})")
    if ui:
        click.echo(
            f"  ui        http://127.0.0.1:{ports.ui}/namespaces/{profile.namespace}/workflows"
        )
        if codec is not None:
            click.echo(f"  graph     {codec}/graph")
    if observability:
        click.echo(
            f"  grafana   http://localhost:{ports.grafana}   otlp http://127.0.0.1:{ports.otlp} (kontra serve --otlp)"
        )
    click.echo("next: kontra init <name> && cd <name> && kontra serve")


# --------------------------------------------------------------------------- down


@dev.command()
@click.option("--reset", is_flag=True, help="Also delete persisted data and the logs.")
@click.option("--volumes", is_flag=True, hidden=True, help="Deprecated alias of --reset.")
def down(reset: bool, volumes: bool) -> None:
    """Stop the stack (its in-memory state is dropped; --persist keeps a snapshot)."""
    from kontra.profile import kontra_home

    reset = reset or volumes
    home = kontra_home()
    _stop_codec(home)
    joined = state.joined_path(home)
    if joined.exists():
        try:
            owner = json.loads(joined.read_text(encoding="utf-8")).get("owner_home")
        except (OSError, ValueError):
            owner = None
        fail(f"this home joined the stack of KONTRA_HOME={owner}; run `kontra dev down` there")

    with state.lock(home):
        record = state.read_stack_record(home)
        settings = state.read_settings(home)
        if record is not None:
            if _ready_doc(record.port_set.gate, record.token) is not None:
                _control_post(record.port_set.gate, record.token, "stop", {})
                _wait_for_exit(record.pid, _STOP_WAIT)
            _cleanup_stack(record, persist=settings.persist and not reset, home=home)
        # Only touch Docker when a previous `up --observability` left a project (T1/OD11).
        if settings.observability:
            _observability_down(state.resolve_ports(home))
            settings.observability = False
            state.write_settings(settings, home)
        if reset:
            shutil.rmtree(state.data_dir(home), ignore_errors=True)
            shutil.rmtree(state.logs_dir(home), ignore_errors=True)
            (home / "dev.env").unlink(missing_ok=True)
    click.echo("kontra dev is down" + (" and its kept state deleted" if reset else ""))


def _wait_for_exit(pid: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _process_alive(pid):
            return
        time.sleep(0.1)


# --------------------------------------------------------------------------- status


@dev.command()
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def status(as_json: bool) -> None:
    """The gate, Temporal, the codec server and the profile; never starts anything."""
    from kontra.profile import Profile, ProfileError, kontra_home

    home = kontra_home()
    record = state.read_stack_record(home)
    doc = None if record is None else _ready_doc(record.port_set.gate, record.token)
    running = doc is not None
    stale = record is not None and not running
    joined = state.joined_path(home).exists()
    codec = _codec_state(home)
    codec_view = (
        None
        if codec is None
        else {
            "url": f"http://127.0.0.1:{codec['port']}",
            "running": _codec_running(codec["port"], str(codec.get("token", ""))),
        }
    )
    try:
        profile: Profile | None = Profile.load()
    except ProfileError:
        profile = None
    prof_view = (
        None
        if profile is None
        else {"party": profile.party, "gate_url": profile.gate_url, "dev": profile.dev}
    )
    if as_json:
        emit_json(
            {
                "running": running,
                "stale": stale,
                "joined": joined,
                "pid": record.pid if record else None,
                "ports": record.ports if record else None,
                "temporal_http": doc.get("temporal_http") if doc else None,
                "ui": doc.get("ui") if doc else None,
                "metrics": doc.get("metrics") if doc else None,
                "persist": doc.get("persist") if doc else None,
                "counts": doc.get("counts") if doc else None,
                "codec": codec_view,
                "profile": prof_view,
            }
        )
        return
    if record is None:
        click.echo("kontra dev is not running (no stack recorded for this home)")
    elif stale:
        click.echo(
            "kontra dev: a stale record (the host is gone); `kontra dev up` or `kontra dev down`"
        )
    else:
        assert doc is not None
        counts = doc.get("counts") or {}
        click.echo(f"kontra dev: running (pid {record.pid}, kontra-sdk {record.sdk_version})")
        click.echo(
            f"  gate      http://127.0.0.1:{record.port_set.gate}   "
            f"{counts.get('parties', 0)} parties, {counts.get('offerings', 0)} offerings, "
            f"{counts.get('open_jobs', 0)} open jobs"
        )
        click.echo(
            f"  temporal  127.0.0.1:{record.port_set.temporal}   HTTP :{doc.get('temporal_http')}, "
            f"UI {'on' if doc.get('ui') else 'off'}"
        )
    click.echo(f"  codec     {'not running' if codec_view is None else codec_view['url']}")
    if profile is not None:
        click.echo(
            f"profile: {profile.party} at {profile.gate_url}{' (dev)' if profile.dev else ''}"
        )


# --------------------------------------------------------------------------- logs


@dev.command()
@click.option("-f", "--follow", is_flag=True, help="Keep printing new lines.")
@click.option("--tail", metavar="N", default="200", show_default=True, help="Lines per source.")
@click.argument("sources", nargs=-1)
def logs(follow: bool, tail: str, sources: tuple[str, ...]) -> None:
    """The stack's logs: gate, temporal, codec (files), or named observability services."""
    from kontra.profile import kontra_home

    if re.fullmatch(r"all|[0-9]{1,7}", tail, re.ASCII) is None:
        fail("--tail is a number of lines, or all")
    home = kontra_home()
    chosen = list(sources) or ["gate", "temporal", "codec"]
    files = {
        "gate": state.logs_dir(home) / "gate.log",
        "temporal": state.logs_dir(home) / "temporal.log",
        "codec": home / CODEC_LOG,
    }
    file_sources = [s for s in chosen if s in files]
    obs_sources = [s for s in chosen if s in OBSERVABILITY_SERVICES]
    unknown = [s for s in chosen if s not in files and s not in OBSERVABILITY_SERVICES]
    if unknown:
        fail(
            f"unknown log source(s): {', '.join(unknown)}; try gate, temporal, codec or an observability service"
        )
    n = None if tail == "all" else int(tail)
    for name in file_sources:
        _print_file(name, files[name], n)
    if obs_sources:
        docker = _docker()
        if docker is None:
            fail("the observability services need Docker to read their logs")
        args = ["logs", "--tail", tail, *(["--follow"] if follow else []), *obs_sources]
        subprocess.run(  # noqa: S603 -- argv, no shell
            _observability_cmd(*args),
            check=False,
            env=_observability_env(state.resolve_ports(home)),
        )
    if follow and file_sources and not obs_sources:
        _follow_files({name: files[name] for name in file_sources})


def _print_file(name: str, path: Path, n: int | None) -> None:
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        click.echo(f"== {name}: no log yet ==")
        return
    click.echo(f"== {name} ==")
    for line in lines if n is None else lines[-n:]:
        click.echo(line)


def _follow_files(paths: dict[str, Path]) -> None:
    handles = {
        name: p.open(encoding="utf-8", errors="replace") for name, p in paths.items() if p.exists()
    }
    for f in handles.values():
        f.seek(0, os.SEEK_END)
    try:
        while True:
            printed = False
            for name, f in handles.items():
                line = f.readline()
                while line:
                    click.echo(f"[{name}] {line.rstrip()}")
                    printed = True
                    line = f.readline()
            if not printed:
                time.sleep(0.3)
    except KeyboardInterrupt:
        pass
    finally:
        for f in handles.values():
            f.close()


def _sdk_version() -> str:
    try:
        from importlib.metadata import version

        return version("kontra-sdk")
    except Exception:
        return "unknown"
