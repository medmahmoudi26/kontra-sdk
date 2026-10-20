"""Where ``kontra dev`` keeps what it must remember between commands, and how it resolves ports.

Everything lives under ``$KONTRA_HOME/dev/`` (0700). ``settings.json`` is what an ``up``
remembers (ports and ``--persist``); ``stack.json`` is the running host's record; ``joined.json``
marks a home that joined another's stack; ``lock`` serialises ``up``/``down``. The per-user
registry (under ``$XDG_STATE_HOME``) maps a gate port to the home that owns the stack on it, so
a second home of the same user can *join* rather than collide.

Ports come from ``settings.json`` and ``kontra dev up`` flags, **never** the environment
(decisions I34, OD8): ``compose.yaml``'s ``KONTRA_DEV_*_PORT`` are the e2e stack's, and the
acceptance doc tells maintainers to export them, so reading them here would move two stacks at
once. :func:`ignored_env` is what ``up`` warns about when one is set.

This module never imports FastAPI or uvicorn: ``down``, ``status`` and ``logs`` go through it.
"""

from __future__ import annotations

import contextlib
import json
import os
import shutil
import socket
import subprocess
from collections.abc import Generator, Mapping
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from kontra.profile import ProfileError, _read_private, kontra_home

if TYPE_CHECKING:
    from typing import IO

__all__ = [
    "PORT_ENV",
    "Ports",
    "StackRecord",
    "data_dir",
    "dev_dir",
    "ignored_env",
    "joined_path",
    "lock",
    "logs_dir",
    "port_holder",
    "port_taken",
    "read_settings",
    "read_stack_record",
    "register_port",
    "registry_owner",
    "registry_path",
    "resolve_ports",
    "settings_path",
    "stack_path",
    "unregister_port",
    "write_settings",
    "write_stack_record",
]

#: ``kontra dev``'s record format; a file of another version is treated as stale, never acted on.
STACK_FORMAT = 1


@dataclass(frozen=True, slots=True)
class Ports:
    """Every loopback port the dev stack binds. The Temporal HTTP API's port is not here: it
    is kernel-chosen per host start and recorded on :class:`StackRecord`."""

    gate: int = 8080
    temporal: int = 7233
    ui: int = 8233
    codec: int = 8081
    #: Only bound with ``--observability``; 9464-9473 are the workers' own metrics ports.
    temporal_metrics: int = 9463
    prometheus: int = 9090
    grafana: int = 3000
    otlp: int = 4317


#: The environment variable the e2e stack reads for each port. ``kontra dev`` does **not** read
#: these (OD8); the mapping is only so ``up`` can warn that setting one does not move its gate.
PORT_ENV = {
    "gate": "KONTRA_DEV_GATE_PORT",
    "temporal": "KONTRA_DEV_TEMPORAL_PORT",
    "ui": "KONTRA_DEV_UI_PORT",
    "codec": "KONTRA_DEV_CODEC_PORT",
    "temporal_metrics": "KONTRA_DEV_TEMPORAL_METRICS_PORT",
    "prometheus": "KONTRA_DEV_PROMETHEUS_PORT",
    "grafana": "KONTRA_DEV_GRAFANA_PORT",
    "otlp": "KONTRA_DEV_OTLP_PORT",
}
#: Variables the compose stack used that mean nothing to ``kontra dev`` (it runs no containers,
#: no images, and binds loopback only); ``up`` warns when one is set.
_IGNORED_OTHER = (
    "KONTRA_DEV_S3_PORT",
    "KONTRA_DEV_PG_PORT",
    "KONTRA_DEV_BIND",
    "KONTRA_DEV_PG_BIND",
    "KONTRA_GATE_IMAGE",
    "KONTRA_TEMPORAL_IMAGE",
    "KONTRA_TEMPORAL_METRICS_ADDR",
    "KONTRA_CONTROL_NET",
)

_PORT_FIELDS = tuple(f.name for f in fields(Ports))


def ignored_env() -> list[str]:
    """The ``KONTRA_DEV_*`` and image/bind variables set in the environment that ``kontra dev``
    ignores (the compose stack's), newest-popular first, so ``up`` can say so once."""
    return [name for name in (*PORT_ENV.values(), *_IGNORED_OTHER) if os.environ.get(name)]


# --------------------------------------------------------------------------- the dev/ layout


def dev_dir(home: Path | None = None) -> Path:
    """``$KONTRA_HOME/dev`` (created 0700)."""
    d = (home or kontra_home()) / "dev"
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    return d


def settings_path(home: Path | None = None) -> Path:
    return dev_dir(home) / "settings.json"


def stack_path(home: Path | None = None) -> Path:
    return dev_dir(home) / "stack.json"


def joined_path(home: Path | None = None) -> Path:
    return dev_dir(home) / "joined.json"


def data_dir(home: Path | None = None) -> Path:
    d = dev_dir(home) / "data"
    d.mkdir(mode=0o700, exist_ok=True)
    return d


def logs_dir(home: Path | None = None) -> Path:
    d = dev_dir(home) / "logs"
    d.mkdir(mode=0o700, exist_ok=True)
    return d


@contextlib.contextmanager
def lock(home: Path | None = None) -> Generator[None, None, None]:
    """``flock`` on ``dev/lock`` for the length of an ``up`` or a ``down`` (two cannot race on
    one home). Advisory and best-effort: a platform without ``fcntl`` (Windows) just proceeds."""
    path = dev_dir(home) / "lock"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
    try:
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
        except (ImportError, OSError):
            pass
        yield
    finally:
        os.close(fd)


# --------------------------------------------------------------------------- settings.json


class Settings(BaseModel):
    """What ``up`` remembers: the ports it ran with, whether ``--persist`` is on, and whether
    the last ``up`` ran ``--observability`` (so a plain ``up``/``down`` touches Docker only when
    there is an observability project to take down)."""

    model_config = ConfigDict(extra="forbid")

    ports: dict[str, int] = Field(default_factory=dict)
    persist: bool = False
    observability: bool = False


def read_settings(home: Path | None = None) -> Settings:
    """``dev/settings.json``, or empty defaults when absent or unreadable."""
    try:
        raw = _read_private(settings_path(home))
    except (FileNotFoundError, ProfileError):
        return Settings()
    try:
        return Settings.model_validate_json(raw)
    except ValueError:
        return Settings()


def write_settings(settings: Settings, home: Path | None = None) -> None:
    _atomic_private(settings_path(home), settings.model_dump_json().encode())


def resolve_ports(
    home: Path | None = None, overrides: Mapping[str, int | None] | None = None
) -> Ports:
    """The stack's ports: an ``overrides`` value (a ``kontra dev up`` flag) wins, else what
    ``settings.json`` remembers, else the default. The environment is never read (OD8)."""
    remembered = read_settings(home).ports
    chosen: dict[str, int] = {}
    overrides = overrides or {}
    for name in _PORT_FIELDS:
        given = overrides.get(name)
        if given is not None:
            chosen[name] = given
        elif name in remembered:
            chosen[name] = int(remembered[name])
    return Ports(**chosen)


def remember_ports(ports: Ports, home: Path | None = None) -> None:
    """Record ``ports`` (all of them) so the next command finds the stack where ``up`` put it."""
    settings = read_settings(home)
    settings.ports = asdict(ports)
    write_settings(settings, home)


# --------------------------------------------------------------------------- stack.json


class StackRecord(BaseModel):
    """The running host, written 0600 by ``up`` and read by every other command. A malformed
    or wrong-``format`` file is stale: it is never acted on, only cleaned up."""

    model_config = ConfigDict(extra="forbid")

    format: int = STACK_FORMAT
    pid: int
    pgid: int
    #: The control-route bearer, on stdin only at spawn; proves the port answers *this* host.
    token: str
    started_at: str
    sdk_version: str
    cli: dict[str, str]  # {"path", "version"}
    ports: dict[str, int]
    #: The Temporal HTTP API's kernel-chosen port, fixed for the host's life.
    temporal_http: int
    ui: bool = False
    metrics: bool = False
    persist: bool = False

    @property
    def port_set(self) -> Ports:
        return Ports(**{k: int(v) for k, v in self.ports.items() if k in _PORT_FIELDS})


def read_stack_record(home: Path | None = None) -> StackRecord | None:
    """``dev/stack.json`` as a :class:`StackRecord`, or None when absent, unreadable (another
    user, group-writable: :func:`kontra.profile._read_private` refuses it), malformed, or of a
    format this version does not know -- every one of which ``up``/``down`` treat as stale."""
    try:
        raw = _read_private(stack_path(home))
    except (FileNotFoundError, ProfileError):
        return None
    try:
        record = StackRecord.model_validate_json(raw)
    except ValueError:
        return None
    return record if record.format == STACK_FORMAT else None


def write_stack_record(record: StackRecord, home: Path | None = None) -> None:
    _atomic_private(stack_path(home), record.model_dump_json().encode())


def remove_stack_record(home: Path | None = None) -> None:
    stack_path(home).unlink(missing_ok=True)


# --------------------------------------------------------------------------- the per-user registry


def registry_path(gate_port: int) -> Path:
    """``$XDG_STATE_HOME/kontra-sdk/dev-gates/<gate port>``: the home that owns the stack on
    that gate port, so another home of this user joins it rather than colliding."""
    base = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    d = Path(base) / "kontra-sdk" / "dev-gates"
    d.mkdir(mode=0o700, parents=True, exist_ok=True)
    return d / str(gate_port)


def register_port(gate_port: int, home: Path) -> None:
    _atomic_private(registry_path(gate_port), json.dumps({"home": str(home)}).encode())


def registry_owner(gate_port: int) -> Path | None:
    """The home that owns the stack on ``gate_port`` (this user's), or None."""
    try:
        raw = _read_private(registry_path(gate_port))
    except (FileNotFoundError, ProfileError):
        return None
    try:
        doc = json.loads(raw)
        home = doc["home"]
    except (ValueError, KeyError, TypeError):
        return None
    return Path(home) if isinstance(home, str) else None


def unregister_port(gate_port: int) -> None:
    registry_path(gate_port).unlink(missing_ok=True)


# --------------------------------------------------------------------------- ports in use


def port_taken(port: int, host: str = "127.0.0.1") -> bool:
    """Whether something already listens on ``host:port`` (a bind that fails means taken)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, port))
        except OSError:
            return True
    return False


def port_holder(port: int) -> str | None:
    """A short description of what holds ``port`` (``pid 1234 docker-proxy``), best-effort via
    ``ss``; None when it cannot be told. Never raises."""
    ss = shutil.which("ss")
    if ss is None:
        return None
    try:
        out = subprocess.run(  # noqa: S603 -- fixed argv, no shell
            [ss, "-H", "-ltnp", f"sport = :{port}"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    for line in out.splitlines():
        if "users:" in line:
            # users:(("docker-proxy",pid=1234,fd=4))
            frag = line.split("users:", 1)[1]
            name = frag.split('"')[1] if '"' in frag else "?"
            pid = ""
            if "pid=" in frag:
                pid = frag.split("pid=", 1)[1].split(",", 1)[0].strip(") ")
            return f"pid {pid} {name}".strip()
    return None


# --------------------------------------------------------------------------- helpers


def _atomic_private(path: Path, data: bytes) -> None:
    """Write ``data`` to ``path`` as a 0600 file, atomically (temp file, fsync, rename)."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{socket.gethostname()[:8]}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    f: IO[bytes]
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
