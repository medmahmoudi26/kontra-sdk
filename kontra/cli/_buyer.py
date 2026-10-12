"""The buyer's commands (PRD §7.1, §12): ``quote``, ``hire``, ``jobs``, ``job``. Each is
one call on :class:`kontra.hire.Kontra`, which validates the input against the published
contract, encrypts it, and decrypts what comes back."""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from kontra.cli._common import emit_json, fail, printable, read_input, run, to_jsonable
from kontra.gate_client.models import JobState

if TYPE_CHECKING:
    from kontra.gate_client import models as m
    from kontra.hire import Job, Kontra, QuoteResult

__all__ = ["hire", "job", "jobs", "quote"]

_INPUT = click.option(
    "-f",
    "--file",
    "source",
    required=True,
    metavar="FILE",
    help="The input as a JSON object (- reads standard input).",
)
_JSON = click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")


def _quote_dict(q: QuoteResult) -> dict[str, Any]:
    return {
        "status": q.status.value,
        "offering": q.offering,
        "quote_id": q.quote_id,
        "price": None if q.price is None else str(q.price),
        "currency": q.currency,
        "amount_minor": q.amount_minor,
        "max_duration_seconds": None
        if q.max_duration is None
        else int(q.max_duration.total_seconds()),
        "reason": q.reason,
        "eta_seconds": None if q.eta is None else int(q.eta.total_seconds()),
        "expires_at": q.expires_at,
        "contract_hash": q.contract_hash,
    }


@click.command()
@click.argument("offering")
@_INPUT
@_JSON
def quote(offering: str, source: str, as_json: bool) -> None:
    """Ask OFFERING (acme/enrich, acme/enrich@1.2.0) for its price on an input."""
    from kontra.hire import Kontra

    value = read_input(source)

    async def go() -> dict[str, Any]:
        async with Kontra() as k:
            return _quote_dict(await k.quote(offering, value))

    q = run(go)
    if as_json:
        emit_json(q)
        return
    if q["status"] == "ok":
        click.echo(f"{q['offering']}: {q['price']} {q['currency']}")
        click.echo(f"  max duration  {q['max_duration_seconds']}s")
        click.echo(f"  quote         {q['quote_id']} (valid until {to_jsonable(q['expires_at'])})")
    elif q["status"] == "rejected":
        click.echo(f"{q['offering']} rejected this input: {printable(q['reason'])}")
    else:
        eta = q["eta_seconds"]
        why = f": {printable(q['reason'])}" if q["reason"] else ""
        click.echo(f"{q['offering']} is busy{why}" + (f"; try again in {eta}s" if eta else ""))


def _max_price_option(
    _ctx: click.Context, _param: click.Parameter, value: str | None
) -> str | None:
    return _max_price(value)


def _max_price(value: str | None) -> str | None:
    """A non-negative decimal, checked before anything is quoted."""
    from decimal import Decimal, InvalidOperation

    if value is None:
        return None
    try:
        d = Decimal(value)
    except InvalidOperation:
        raise click.BadParameter(f"{value!r} is not a number") from None
    if not d.is_finite() or d < 0:
        raise click.BadParameter(f"{value!r} must be a number >= 0")
    return value


def _job_line(v: m.JobView) -> str:
    return (
        f"{v.job_id}  {v.offering}  {v.state.value}/{v.escrow.value}  "
        f"{v.price} {v.currency}  {v.created_at.isoformat(timespec='seconds')}"
    )


def _show_job(v: m.JobView) -> None:
    click.echo(f"job {v.job_id}")
    click.echo(f"  offering  {v.offering} (seller {v.seller}, buyer {v.buyer})")
    click.echo(f"  state     {v.state.value}, escrow {v.escrow.value}")
    click.echo(f"  price     {v.price} {v.currency}")
    click.echo(f"  deadline  {v.deadline.isoformat(timespec='seconds')}")
    if v.delivery is not None:
        click.echo(f"  delivered {v.delivery.delivered_at.isoformat(timespec='seconds')}")
    if v.review_deadline is not None:
        click.echo(f"  review    until {v.review_deadline.isoformat(timespec='seconds')}")
    if v.reason:
        click.echo(f"  reason    {printable(v.reason)}")


@click.command()
@click.argument("offering")
@_INPUT
@click.option(
    "--max-price",
    metavar="X",
    callback=_max_price_option,
    help="Refuse to lock more than this (in the quote's currency).",
)
@click.option("--wait", is_flag=True, help="Wait for the delivery and print it.")
@click.option("--accept", "accept_", is_flag=True, help="Accept the delivery (implies --wait).")
@_JSON
def hire(
    offering: str,
    source: str,
    max_price: str | None,
    wait: bool,
    accept_: bool,
    as_json: bool,
) -> None:
    """Quote OFFERING on an input, lock the price, and start the job."""
    from kontra.hire import Kontra

    value = read_input(source)

    async def go() -> dict[str, Any]:
        async with Kontra() as k:
            j = await k.hire(offering, value, max_price=max_price)
            out: dict[str, Any] = {
                "job_id": j.job_id,
                "offering": j.offering,
                "price": str(j.price),
                "currency": j.currency,
            }
            if not as_json:
                click.echo(f"hired {j.offering}: job {j.job_id} at {j.price} {j.currency}")
            if wait or accept_:
                out["result"] = to_jsonable(await j.result())
                if accept_:
                    out["state"] = to_jsonable(await j.accept())
            return out

    out = run(go)
    if as_json:
        emit_json(out)
        return
    if "result" in out:
        emit_json(out["result"])
    if "state" in out:
        click.echo(f"accepted: escrow {out['state']['escrow']}")


@click.command()
@click.option(
    "--as",
    "as_",
    type=click.Choice(["buyer", "seller"]),
    default="buyer",
    show_default=True,
    help="The jobs you hired, or the jobs you sell.",
)
@click.option(
    "--state",
    type=click.Choice([s.value for s in JobState]),
    help="Only jobs in this A2A state.",
)
@click.option("--limit", type=click.IntRange(1, 1000), default=50, show_default=True)
@_JSON
def jobs(as_: str, state: str | None, limit: int, as_json: bool) -> None:
    """Your jobs, newest first."""
    from kontra.hire import Kontra

    async def go() -> list[m.JobView]:
        async with Kontra() as k:
            return await k.jobs(as_=as_, state=state, limit=limit)

    found = run(go)
    if as_json:
        emit_json(found)
        return
    if not found:
        click.echo(f"no jobs as {as_}")
    for v in found:
        click.echo(_job_line(v))


def _private_dirs(directory: Path) -> None:
    """Create ``directory`` and its missing parents, each exactly 0700 (``mkdir -p`` would
    make the parents 0755 and filter the leaf's mode through the umask). Directories that
    already exist are left as they are."""
    missing: list[Path] = []
    here = directory
    while not here.exists() and here != here.parent:
        missing.append(here)
        here = here.parent
    for made in reversed(missing):
        try:
            made.mkdir(mode=0o700)
        except FileExistsError:  # made meanwhile by someone else: not ours to change
            continue
        os.chmod(made, 0o700)


def _write_private(path: Path, text: str) -> None:
    """Write ``text`` to ``path`` atomically, as a 0600 file: a new file under a temporary
    name, renamed over ``path`` (never written through a file or link already there)."""
    tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.part")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(tmp, flags, 0o600)
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)  # exactly 0600, whatever the umask
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


async def _download_all(value: Any, directory: Path) -> list[Path]:
    """Every artifact in a delivery, into ``directory`` (named by its key: never by a name
    the seller chose)."""
    from kontra.types import Artifact

    if isinstance(value, Artifact):
        return [await value.download(directory)]
    items: list[Any]
    if isinstance(value, dict):
        items = list(value.values())  # pyright: ignore[reportUnknownArgumentType]
    elif isinstance(value, list | tuple):
        items = list(value)  # pyright: ignore[reportUnknownArgumentType]
    else:
        from kontra.hire import Record

        items = list(value.values()) if isinstance(value, Record) else []
    found: list[Path] = []
    for item in items:
        found += await _download_all(item, directory)
    return found


@click.command()
@click.argument("job_id")
@click.option("--accept", "accept_", is_flag=True, help="Release the escrow to the seller now.")
@click.option("--reject", metavar="REASON", help="Dispute the delivery, saying why.")
@click.option("--cancel", is_flag=True, help="Cancel before delivery (the hold refunds).")
@click.option("--progress", is_flag=True, help="Follow the job's progress until it closes.")
@click.option(
    "--result",
    "result_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Wait for the delivery; write result.json and every artifact into this directory.",
)
@_JSON
def job(
    job_id: str,
    accept_: bool,
    reject: str | None,
    cancel: bool,
    progress: bool,
    result_dir: Path | None,
    as_json: bool,
) -> None:
    """Show job JOB_ID, or act on it (one of the options)."""
    import asyncio
    import json

    from kontra.contract.names import is_job_id
    from kontra.hire import Kontra

    actions = [accept_, reject is not None, cancel, progress, result_dir is not None]
    if sum(actions) > 1:
        fail("pick one of --accept, --reject, --cancel, --progress, --result")
    if not is_job_id(job_id):
        fail(f"{job_id!r} is not a job id (32 hex characters)")

    async def act(k: Kontra, j: Job) -> Any:
        if accept_:
            return await j.accept()
        if reject is not None:
            return await j.reject(reject)
        if cancel:
            return await j.cancel()
        if progress:
            events: list[Any] = []
            async for p in j.progress():
                if as_json:
                    events.append(p)
                else:
                    click.echo(f"{p.at.isoformat(timespec='seconds')}  {p}")
            return events
        if result_dir is not None:
            value = await j.result()
            # What lands here is the decrypted delivery: directories made for it are the
            # buyer's alone (0700) and so is every file (0600), whatever the umask and
            # whatever the mode of a directory that already exists.
            await asyncio.to_thread(_private_dirs, result_dir)
            files = await _download_all(value, result_dir)
            target = result_dir / "result.json"
            text = json.dumps(to_jsonable(value), indent=2) + "\n"
            await asyncio.to_thread(_write_private, target, text)
            return {"result": str(target), "artifacts": [str(f) for f in files]}
        return await j.status()

    async def go() -> Any:
        async with Kontra() as k:
            return await act(k, await k.job(job_id))

    out = run(go)
    if as_json:
        if progress:
            emit_json([_progress_dict(p) for p in out])
        else:
            emit_json(out)
        return
    if progress:
        return
    if result_dir is not None:
        click.echo(f"wrote {out['result']}")
        for f in out["artifacts"]:
            click.echo(f"  artifact {f}")
        return
    _show_job(out)


def _progress_dict(p: Any) -> dict[str, Any]:
    return {
        "seq": p.seq,
        "at": p.at.isoformat(),
        "source": p.source.value,
        "activity": p.activity,
        "percent": p.percent,
        "message": p.message,
        "state": None if p.state is None else p.state.value,
        "text": str(p),
    }
