"""The ``kontra`` command (PRD §12).

```
kontra login [--gate URL] [--email ADDR] [--dev --handle H] [--no-device]
                                               device flow (or a mailed link); key registered,
                                               this machine one of your devices (I25)
kontra confirm                                 confirm it's you: an hour of changes (I23)
kontra keys create|list|revoke                 API keys for programs (KONTRA_API_KEY)
kontra recipients add|list|revoke              keys others encrypt to you (I7)
kontra devices list|add [--new-key]|remove      machines and browsers that read your jobs (I25)
kontra init <name> [--party P]                 scaffold an offering (§6.1)
kontra dev [up|down|status|logs]               the local stack (compose.yaml)
kontra publish | serve | contract [--dir D]    sell worker.py's offerings (§11.4, §6.5)
kontra graph [--dir D] [--runs N] [--mermaid|--json]
                                               worker.py's workflows as a graph, from mock
                                               runs (I27)
kontra status [--workers] [<offering>...]      whether your workers keep up (Temporal's stats)
kontra pull <offering> --lang go|python|typescript|java
kontra quote|hire <offering> -f in.json        buy (§7.1)
kontra jobs | job <id> [--accept|--reject R|--cancel|--progress|--result DIR]
                                               a job hired in the console or elsewhere reads
                                               here too: its key reaches every device
kontra balance | topup <amount> | mcp
```

Every command is a thin layer over :mod:`kontra.auth`, :mod:`kontra.hire`,
:mod:`kontra.serve` and :mod:`kontra.contract`: no business rule lives here. A failure
prints one line (``error: ...``: the gate's code and message, or the contract path at
fault) and exits 1; read commands take ``--json``.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
from typing import NoReturn

import click

from kontra import __version__
from kontra.cli._account import balance, confirm, login, topup
from kontra.cli._buyer import hire, job, jobs, quote
from kontra.cli._common import KontraGroup, fail
from kontra.cli._dev import dev
from kontra.cli._devices import devices
from kontra.cli._graph import graph
from kontra.cli._keys import keys
from kontra.cli._pull import pull
from kontra.cli._recipients import recipients
from kontra.cli._seller import contract, init, publish, serve
from kontra.cli._status import status

__all__ = ["cli", "main"]


@click.group(cls=KontraGroup, context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="kontra")
def cli() -> None:
    """Sell and hire durable, quoted jobs: plain Temporal workers behind Nexus, with escrow."""


@click.command()
def mcp() -> None:
    """Serve the MCP tools on stdio, for an agent on this machine (it holds your key)."""
    try:
        from kontra.mcp import main as serve_stdio
    except ModuleNotFoundError as err:
        fail(str(err))
    serve_stdio()


for _command in (
    login,
    confirm,
    keys,
    recipients,
    devices,
    init,
    dev,
    publish,
    serve,
    contract,
    graph,
    status,
    pull,
    quote,
    hire,
    jobs,
    job,
    balance,
    topup,
    mcp,
):
    cli.add_command(_command)


def main() -> NoReturn:
    """The console script ``kontra`` (and ``python -m kontra.cli``).

    It leaves through :func:`os._exit`, skipping interpreter finalization: with temporalio
    1.34, the Rust core's threads can crash finalization (SIGSEGV) after a command has
    finished cleanly -- measured at about one ``kontra serve`` in fifteen -- which would
    turn a graceful stop into exit status 139. Everything a command writes is flushed and
    closed before it returns, so nothing is lost by not finalizing. An unexpected exception
    still propagates normally, with its traceback.
    """
    try:
        cli.main(prog_name="kontra")  # standalone mode: always ends in SystemExit
    except SystemExit as stop:
        code = stop.code
    else:  # pragma: no cover -- click's standalone mode always raises
        code = 0
    if not isinstance(code, int):
        if code is not None:
            print(code, file=sys.stderr)
        code = 0 if code is None else 1
    logging.shutdown()
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.flush()
    os._exit(code)
