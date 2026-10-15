"""The process's Kontra runtime: who we are, our key, our gate client, and (in a seller
activity) which job we are working for.

``kontra.serve.Worker`` installs one when it starts; ``kontra.hire.Kontra`` installs one
when it is created. ``Artifact`` methods and the progress forwarder read it. Seller
workflow and activity code never touch it directly (D1).
"""

from __future__ import annotations

import contextvars
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from kontra.gate_client import GateClient

__all__ = [
    "JobContext",
    "NoRuntime",
    "Runtime",
    "current",
    "current_job",
    "install",
    "job_context",
    "uninstall",
]


class NoRuntime(RuntimeError):
    def __init__(self) -> None:
        super().__init__(
            "no Kontra runtime in this process: Artifact I/O runs inside `kontra serve` "
            "(seller activities) or after creating `kontra.hire.Kontra()` (buyer)"
        )


@dataclass(frozen=True, slots=True)
class JobContext:
    """Set for the duration of a seller activity that belongs to a job."""

    job_id: str
    activity: str | None = None


_job: contextvars.ContextVar[JobContext | None] = contextvars.ContextVar("kontra_job", default=None)


class KeyLookup(Protocol):
    async def buyer_key(self, job_id: str) -> str: ...


@dataclass
class Runtime:
    party: str
    gate: GateClient
    #: ``AGE-SECRET-KEY-1...``: this machine's key, the one thing that decrypts.
    identity: str
    #: ``age1...``, this machine's own public key.
    recipient: str
    #: The party's whole recipient set (``age1...``), this machine's included: what "to
    #: ourselves" means for a seller running more than one deployment (decisions I7).
    #: Empty means "just this machine", for a buyer that registered nothing.
    recipients: list[str] = field(default_factory=list[str])
    #: More secret keys to decrypt with, read at each call because the set grows: a buyer
    #: makes one per job at hire and shreds it at accept (decisions I7). Encryption never
    #: consults these -- only :attr:`recipients` says who may read.
    more_identities: Callable[[], Sequence[str]] | None = None
    #: seller side: job id -> buyer key (filled by the Nexus handler, gate on miss)
    jobs: KeyLookup | None = None
    extra_recipients: list[str] = field(default_factory=list[str])

    def all_identities(self) -> list[str]:
        """Every secret key this process may decrypt with: this machine's, then the job
        keys. Artifacts, claim-checks and heartbeats all read it, so a buyer opens a
        delivery encrypted to the job's key and not to any key it registered."""
        out = [self.identity]
        if self.more_identities is not None:
            for extra in self.more_identities():
                if extra not in out:
                    out.append(extra)
        return out

    def current_job_id(self) -> str | None:
        ctx = _job.get()
        return ctx.job_id if ctx else None

    async def recipients_for_current_job(self) -> list[str]:
        """Seller side: ourselves and the job's buyer (job directory, gate on miss).
        Outside a job: ourselves only."""
        out = [self.recipient, *self.recipients, *self.extra_recipients]
        ctx = _job.get()
        if ctx is not None and self.jobs is not None:
            out.append(await self.jobs.buyer_key(ctx.job_id))
        return list(dict.fromkeys(out))


_current: Runtime | None = None


def install(rt: Runtime) -> None:
    global _current
    _current = rt


def uninstall(rt: Runtime | None = None) -> None:
    global _current
    if rt is None or _current is rt:
        _current = None


def current() -> Runtime:
    if _current is None:
        raise NoRuntime()
    return _current


def current_job() -> JobContext | None:
    return _job.get()


class job_context:
    """``with job_context(JobContext(job_id, activity)): ...``"""

    def __init__(self, ctx: JobContext | None) -> None:
        self._ctx = ctx
        self._token: contextvars.Token[JobContext | None] | None = None

    def __enter__(self) -> JobContext | None:
        self._token = _job.set(self._ctx)
        return self._ctx

    def __exit__(self, *exc: object) -> None:
        if self._token is not None:
            _job.reset(self._token)
