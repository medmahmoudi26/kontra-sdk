"""The seller's commands: ``init`` (§6.1), ``publish`` (§11.4), ``serve`` (§6.5) and
``contract`` (the document ``publish`` would send, or its nexgen view)."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click

from kontra.cli._common import cli_session, default_handle, emit_json, fail, printable, run
from kontra.cli._project import load_worker

__all__ = ["contract", "init", "publish", "serve"]

_DIR = click.option(
    "--dir",
    "directory",
    default=".",
    show_default=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="The offering directory (the one with worker.py).",
)
_OFFERING = click.option(
    "--offering",
    "only",
    metavar="NAME@VERSION",
    help="Only this offering of the worker (default: every bound offering).",
)


def _pick(bound: tuple[Any, ...], only: str | None) -> list[Any]:
    if only is None:
        return list(bound)
    chosen = [b for b in bound if only in (b.key, b.offering_id.full_name)]
    if not chosen:
        keys = ", ".join(b.key for b in bound)
        fail(f"worker.py binds no offering {only!r} (it binds {keys})")
    return chosen


@click.command()
@click.argument("name")
@click.option(
    "--party",
    metavar="P",
    help="Your party handle (default: the profile's, else derived from your user name).",
)
def init(name: str, party: str | None) -> None:
    """Create ./NAME/: an offering project (§6.1) with a test that runs its whole lifecycle."""
    from kontra.cli._scaffold import scaffold
    from kontra.contract.names import validate_handle
    from kontra.profile import Profile, ProfileError

    validate_handle(name, what="offering name", allow_reserved=True)
    if party is None:
        try:
            party = Profile.load().party
        except ProfileError:
            party = default_handle()
    validate_handle(party, what="party")
    target = Path(name)
    try:
        written = scaffold(target, party, name)
    except FileExistsError as e:
        fail(str(e))
    click.echo(f"created {party}/{name} in {target}/ ({len(written)} files)")
    click.echo(f"  cd {name} && pytest          # the whole lifecycle, no containers")
    click.echo("  kontra dev && kontra serve    # a local Kontra, then sell it there")


@click.command()
@_DIR
@_OFFERING
@click.option("--nexgen", is_flag=True, help="The nexgen view: the document without x-kontra.")
@click.option("--json", "as_json", is_flag=True, help="JSON instead of YAML.")
@click.option(
    "-o",
    "--output",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write to this file instead of standard output.",
)
def contract(
    directory: Path,
    only: str | None,
    nexgen: bool,
    as_json: bool,
    output: Path | None,
) -> None:
    """Print the contract document derived from worker.py's offerings (what `publish` sends)."""
    import json

    from kontra.contract import nexgen_view, to_yaml

    with load_worker(directory) as worker:
        chosen = _pick(worker.offerings, only)
        if output is not None and len(chosen) > 1:
            fail(f"worker.py binds {len(chosen)} offerings; pick one with --offering for -o")
        docs = [b.document() for b in chosen]
    if nexgen:
        docs = [nexgen_view(d) for d in docs]
    if as_json:
        text = "\n".join(json.dumps(d, indent=2, ensure_ascii=False) + "\n" for d in docs)
    else:
        text = "---\n".join(to_yaml(d) for d in docs)
    if output is None:
        sys.stdout.write(text)
        return
    output.write_text(text, encoding="utf-8")
    click.echo(f"wrote {output}", err=True)


@click.command()
@_DIR
@_OFFERING
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def publish(directory: Path, only: str | None, as_json: bool) -> None:
    """Publish worker.py's offerings to the catalog (needs your login, confirmed within the
    hour: a terminal asks in place)."""
    from kontra.cli._seller_code import check_project
    from kontra.contract import contract_hash
    from kontra.gate_client import models as m

    root = directory.resolve()
    refused = "\n".join("  " + f.render(root) for f in check_project(root))
    with load_worker(directory) as worker:
        chosen = _pick(worker.offerings, only)

        async def go() -> list[m.CatalogEntry]:
            async with cli_session() as session:
                profile = await session.resolve()
                if refused and not profile.dev:  # kontra dev is the seller's own
                    fail("seller code must stay behind the kontra façade:\n" + refused)
                # No key is sealed into a document any more (spec v2, decisions I7), so
                # publishing needs none: a seller may add or revoke a recipient without
                # republishing.
                for b in chosen:
                    if b.offering_id.party != profile.party:
                        fail(
                            f"{b.source} names the party {b.offering_id.party!r} but you are "
                            f"logged in as {profile.party!r}; change `name:` there"
                        )
                entries: list[m.CatalogEntry] = []
                for b in chosen:
                    doc = b.document()
                    entry = await session.gate.publish(doc)
                    ours = contract_hash(doc)
                    if (entry.offering, entry.contract_hash) != (b.key, ours):
                        fail(
                            f"the catalog recorded {printable(entry.offering)} with contract "
                            f"{printable(entry.contract_hash)} for {b.key} {ours}; `kontra serve` "
                            "would refuse it -- check the gate"
                        )
                    entries.append(entry)
                return entries

        entries = run(go)
    if as_json:
        emit_json(entries)
        return
    for e in entries:  # the gate's answer: printed, never trusted to be printable
        click.echo(f"published {printable(e.offering)}")
        click.echo(f"  contract  {printable(e.contract_hash)}")
        click.echo(f"  endpoint  {printable(e.endpoint)}")
        click.echo(f"  queue     {printable(e.task_queue)}")


@click.command()
@_DIR
@click.option(
    "--metrics",
    metavar="IP:PORT",
    help="Serve the Temporal SDK's Prometheus metrics at http://IP:PORT/metrics (default: off).",
)
@click.option(
    "--otlp",
    metavar="URL",
    help="Export Temporal's traces over OTLP/gRPC to URL, e.g. http://localhost:4317 "
    "(default: off; needs kontra-sdk[opentelemetry]).",
)
def serve(directory: Path, metrics: str | None, otlp: str | None) -> None:
    """Run worker.py's worker with this machine's profile, or KONTRA_API_KEY (Ctrl-C stops
    it gracefully)."""
    import logging

    from kontra.serve.telemetry import TelemetryError, metrics_runtime, otlp_tracing

    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    # One line per HTTP request (token refreshes, progress posts) drowns what matters.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    log = logging.getLogger("kontra.serve")
    try:
        tracing = otlp_tracing(otlp) if otlp is not None else None
        runtime = metrics_runtime(metrics) if metrics is not None else None
    except TelemetryError as e:
        fail(str(e))
    if runtime is not None:
        log.info("serving the Temporal SDK's metrics at http://%s/metrics", metrics)
    if tracing is not None:
        log.info("exporting traces to %s", otlp)
    try:
        with load_worker(directory) as worker:
            worker.run(
                runtime=runtime, interceptors=[tracing.interceptor] if tracing is not None else []
            )
    finally:
        if tracing is not None:
            tracing.shutdown()
    log.info("stopped")
