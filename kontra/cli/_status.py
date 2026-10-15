"""``kontra status``: how a seller's offerings are doing.

``--workers`` (kontra-cloud#6) is whether the workers keep up with each version: Temporal's
task-queue stats and pollers and the verdict :mod:`kontra.workers` reads from them, as the
gate answers ``GET /v1/offerings/{party}/{name}/versions/{version}/workers``. It is the one
section so far, so ``kontra status`` alone shows it too. A version the gate refuses (one not
published yet, Temporal not answering) is an ``error:`` line on stderr and exit status 1; the
other versions are shown all the same (``--json`` lists theirs).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import click

from kontra.cli._common import describe, emit_json, fail, printable, run

if TYPE_CHECKING:
    from kontra.gate_client import models as m

__all__ = ["status"]


def _rate(value: float) -> str:
    return f"{value:.2f}/s"


def _change(value: float) -> str:
    """``backlog_increase_rate``: ``+0.80/s`` while the backlog grows, ``-0.30/s`` while it
    drains (the console's "Backlog change")."""
    shown = value if abs(value) >= 0.005 else 0.0
    return f"{'+' if shown > 0 else ''}{shown:.2f}/s"


def _when(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%SZ")


def _worker(p: m.PollerInfo) -> str:
    """What a poller said of itself: build id, deployment, SDK (each when it said it)."""
    parts = [f"build {printable(p.build_id)}"] if p.build_id else []
    if p.deployment_name:
        parts.append(f"deployment {printable(p.deployment_name)}")
    sdk = " ".join(printable(v) for v in (p.sdk_name, p.sdk_version) if v)
    if sdk:
        parts.append(f"sdk {sdk}")
    return "".join(f", {part}" for part in parts)


def _show(view: m.WorkersView) -> None:
    """One version: the verdict, a line per task-queue type, the pollers by identity."""
    from kontra.gate_client.models import TaskQueueType
    from kontra.workers import STALE_NOTE, format_age

    # The gate's answer: printed, never trusted to be printable.
    click.echo(f"{printable(view.offering)}  {printable(view.verdict)}")
    click.echo(f"  task queue  {printable(view.task_queue)}")
    #: identity -> its most recent listing and the types it polls
    seen: dict[str, tuple[m.PollerInfo, list[str]]] = {}
    for kind in TaskQueueType:
        info = view.types_info.get(kind)
        if info is None:
            continue
        s = info.stats
        backlog = f"backlog {s.approximate_backlog_count}"
        if s.approximate_backlog_count:
            backlog += f" (oldest {format_age(s.approximate_backlog_age)})"
        pollers = f"{len(info.pollers)} poller" + ("" if len(info.pollers) == 1 else "s")
        click.echo(
            f"  {kind.value:<10}  {backlog}, add {_rate(s.tasks_add_rate)}, "
            f"dispatch {_rate(s.tasks_dispatch_rate)}, "
            f"change {_change(s.backlog_increase_rate)}, {pollers}"
        )
        for p in info.pollers:
            latest, kinds = seen.get(p.identity, (p, []))
            if p.last_access_time > latest.last_access_time:
                latest = p
            seen[p.identity] = (latest, [*kinds, kind.value])
    label = "pollers"
    for latest, kinds in sorted(seen.values(), key=lambda i: i[0].last_access_time, reverse=True):
        click.echo(
            f"  {label:<10}  {printable(latest.identity)} ({', '.join(kinds)}), "
            f"last poll {_when(latest.last_access_time)}{_worker(latest)}"
        )
        label = ""
    if view.stale:
        click.echo(f"  {'note':<10}  {STALE_NOTE}")


@click.command()
@click.argument("offerings", nargs=-1, metavar="[OFFERING]...")
@click.option(
    "--workers",
    "_workers",
    is_flag=True,
    help="Whether the workers keep up: backlog, rates and pollers per task-queue type, "
    "and a verdict (the one section so far, shown by default).",
)
@click.option(
    "--dir",
    "directory",
    default=".",
    show_default=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Without OFFERING: the offering directory whose worker.py names the versions.",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def status(offerings: tuple[str, ...], _workers: bool, directory: Path, as_json: bool) -> None:
    """How your offering versions are doing. OFFERING is acme/enrich@1.2.0 (or acme/enrich,
    @1, @1.2: the latest of yours that matches); without one, every offering worker.py binds."""
    from kontra.auth import Session
    from kontra.cli._project import WORKER_FILE, load_worker
    from kontra.contract.names import OfferingRef
    from kontra.gate_client import GateError

    refs = list(offerings)
    for ref in refs:
        OfferingRef.parse(ref)  # a malformed reference is one line, before any login
    if not refs:
        if not (directory / WORKER_FILE).is_file():
            fail(
                f"name an offering (acme/enrich@1.2.0), or run this from an offering "
                f"directory or pass --dir: there is no {WORKER_FILE} in {directory.resolve()}"
            )
        with load_worker(directory) as worker:
            refs = [b.key for b in worker.offerings]

    async def go() -> list[m.WorkersView | GateError]:
        answers: list[m.WorkersView | GateError] = []
        async with Session.from_env() as session:
            for ref in refs:
                try:
                    answers.append(await session.gate.workers(ref))
                except GateError as e:
                    answers.append(e)
        return answers

    answers = run(go)
    views = [a for a in answers if not isinstance(a, GateError)]
    if as_json:
        emit_json(views)
    else:
        for i, view in enumerate(views):
            if i:
                click.echo()
            _show(view)
    refused = [(ref, a) for ref, a in zip(refs, answers, strict=True) if isinstance(a, GateError)]
    for ref, err in refused:
        click.echo(f"error: {printable(ref)}: {printable(describe(err))}", err=True)
    if refused:
        raise click.exceptions.Exit(1)
