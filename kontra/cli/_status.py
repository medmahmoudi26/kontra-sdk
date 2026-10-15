"""``kontra status``: how a seller's offerings are doing.

``--workers`` (kontra-cloud#6) is whether the workers keep up with each version: Temporal's
task-queue stats and pollers and the verdict :mod:`kontra.workers` reads from them, as the
gate answers ``GET /v1/offerings/{party}/{name}/versions/{version}/workers``. It is the one
section so far, so ``kontra status`` alone shows it too.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import click

from kontra.cli._common import emit_json, fail, printable, run

if TYPE_CHECKING:
    from kontra.gate_client import models as m

__all__ = ["status"]


def _rate(value: float) -> str:
    return f"{value:.2f}/s"


def _when(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%SZ")


def _show(view: m.WorkersView) -> None:
    """One version: the verdict, a line per task-queue type, the pollers by identity."""
    from kontra.gate_client.models import TaskQueueType
    from kontra.workers import STALE_NOTE, format_age

    # The gate's answer: printed, never trusted to be printable.
    click.echo(f"{printable(view.offering)}  {printable(view.verdict)}")
    click.echo(f"  task queue  {printable(view.task_queue)}")
    seen: dict[str, tuple[datetime, list[str], str | None]] = {}
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
            f"dispatch {_rate(s.tasks_dispatch_rate)}, {pollers}"
        )
        for p in info.pollers:
            last, kinds, build = seen.get(p.identity, (p.last_access_time, [], p.build_id))
            if p.last_access_time > last:
                last, build = p.last_access_time, p.build_id
            seen[p.identity] = (last, [*kinds, kind.value], build)
    label = "pollers"
    for identity, (last, kinds, build) in sorted(seen.items(), key=lambda i: i[1][0], reverse=True):
        version = f", build {printable(build)}" if build else ""
        click.echo(
            f"  {label:<10}  {printable(identity)} ({', '.join(kinds)}), "
            f"last poll {_when(last)}{version}"
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

    async def go() -> list[m.WorkersView]:
        async with Session.from_env() as session:
            return [await session.gate.workers(ref) for ref in refs]

    views = run(go)
    if as_json:
        emit_json(views)
        return
    for i, view in enumerate(views):
        if i:
            click.echo()
        _show(view)
