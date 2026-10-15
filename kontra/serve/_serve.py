"""``serve(Flow)``: the whole of a seller's ``worker.py``.

```python
from kontra import serve
from workflows import Triage

serve(Triage)                                   # or: serve(Triage, max_concurrent_activities=50)
```

It builds the :class:`~kontra.serve.Worker` a seller would otherwise write by hand:

* the offering is ``offering.yaml`` beside the calling file, bound to the **first** workflow;
  further workflows are registered on the worker unbound (the children the first starts);
* ``quote`` is the ``quote`` function of ``quote.py`` beside the calling file unless one is
  passed (imported the way ``from quote import quote`` in ``worker.py`` would import it);
* the activities are every function decorated with :func:`kontra.activity.defn` whose module
  is imported in this process -- an activity nothing imports cannot be referenced by a
  workflow either -- except Kontra's own (``Worker`` registers those) and methods (they need
  their instance: pass ``activities=[obj.method]``);
* with the ``[agents]`` extra installed, ``OpenAIAgentsPlugin`` goes on the worker
  (:mod:`kontra._agents`), serving ``mcp_servers``;
* every other keyword is ``temporalio.worker.Worker``'s, passed to ``Worker`` verbatim;
  ``activities``, ``workflows`` and ``plugins`` given there are added after serve()'s own.

Run as ``python worker.py`` (the calling module is ``__main__``), serve() runs the worker
until it stops. Imported -- by ``kontra serve``, ``kontra publish`` or a test -- it only
builds the worker and returns it; the CLI finds it with :func:`capture`, so neither command
starts anything by importing ``worker.py``.
"""

from __future__ import annotations

import contextlib
import importlib
import sys
from collections.abc import Generator, Mapping
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Unpack

from temporalio.worker import WorkerConfig

from kontra import _agents, activity
from kontra.serve.offering import BindingError, QuoteFunction, offering
from kontra.serve.worker import Worker

__all__ = ["OFFERING_FILE", "QUOTE_FILE", "capture", "serve", "serve_from"]

#: What serve() binds, beside the calling file.
OFFERING_FILE = "offering.yaml"
#: Where serve() finds ``quote`` when none is passed, beside the calling file.
QUOTE_FILE = "quote.py"

#: The Workers serve() built while :func:`capture` was open (``kontra serve`` / ``publish``
#: importing ``worker.py``); None outside it.
_captured: ContextVar[list[Worker] | None] = ContextVar("kontra_serve_captured", default=None)


@contextlib.contextmanager
def capture() -> Generator[list[Worker]]:
    """Collect the Workers serve() builds inside the block (the CLI imports ``worker.py``
    in it). The list is filled as serve() returns; a second serve() in the block is refused."""
    built: list[Worker] = []
    token = _captured.set(built)
    try:
        yield built
    finally:
        _captured.reset(token)


def _base(caller: Mapping[str, Any]) -> Path:
    """The calling module's directory, else the cwd (a REPL)."""
    filename = caller.get("__file__")
    return Path(filename).resolve().parent if isinstance(filename, str) else Path.cwd()


def _default_quote(caller: Mapping[str, Any], base: Path) -> QuoteFunction:
    """``quote`` from ``quote.py`` beside the caller, imported as the caller would import it
    (``from quote import quote``, or ``from .quote import quote`` inside a package)."""
    path = base / QUOTE_FILE
    if not path.is_file():
        raise BindingError(
            f"serve() takes the quote function from {path}, which does not exist: write "
            "`async def quote(inp: <the workflow's input model>) -> Quote` there, or pass "
            "quote="
        )
    package = caller.get("__package__") or None
    name = f"{package}.quote" if isinstance(package, str) else "quote"
    module = importlib.import_module(name)
    found = getattr(module, "__file__", None)
    if not isinstance(found, str) or Path(found).resolve() != path.resolve():
        raise BindingError(
            f"`import {name}` loads {found}, not {path}; rename the module that shadows it, "
            "or pass quote="
        )
    fn = getattr(module, "quote", None)
    if fn is None:
        raise BindingError(f"{path} defines no `quote`; write `async def quote(inp) -> Quote`")
    return fn


def _is_method(fn: object) -> bool:
    """Defined in a class body (``Acts.fetch``), not at module level or in a function."""
    scopes = str(getattr(fn, "__qualname__", "")).split(".")[:-1]
    return bool(scopes) and scopes[-1] != "<locals>"


def _loaded(fn: object) -> bool:
    """Its module is the one imported now: not one ``kontra serve`` set aside to load another
    project's module of the same name."""
    name = str(getattr(fn, "__module__", ""))
    module = sys.modules.get(name)
    if module is None:
        return False
    namespace = getattr(fn, "__globals__", None)
    return namespace is None or namespace is vars(module)


def _imported_activities() -> list[Any]:
    """What serve() registers by itself: see the module docstring."""
    return [
        fn
        for fn in activity.defined()
        if str(getattr(fn, "__module__", "")).partition(".")[0] != "kontra"
        and not _is_method(fn)
        and _loaded(fn)
    ]


def _extend(ours: list[Any], given: object) -> list[Any]:
    """``ours``, then what the seller passed that is not already there (by identity)."""
    out = list(ours)
    for item in given if isinstance(given, list | tuple) else ():
        if not any(item is seen for seen in out):
            out.append(item)
    return out


def serve_from(
    caller: Mapping[str, Any],
    workflows: tuple[type, ...],
    *,
    quote: QuoteFunction | None,
    mcp_servers: _agents.MCPServers | None,
    options: Mapping[str, Any],
) -> Worker:
    """:func:`serve` for the module whose globals are ``caller`` (``kontra.serve(...)``, the
    callable package, calls this with its own caller's)."""
    if not workflows:
        raise TypeError("serve() needs the workflow that serves the offering: serve(MyFlow)")
    if "offerings" in options:
        raise TypeError(
            "serve() binds offering.yaml beside worker.py; for several offerings use "
            "Worker(offerings=[...])"
        )
    base = _base(caller)
    bound = offering(
        base / OFFERING_FILE,
        workflow=workflows[0],
        quote=quote if quote is not None else _default_quote(caller, base),
    )
    kwargs = dict(options)
    kwargs["activities"] = _extend(_imported_activities(), options.get("activities"))
    extra_workflows = _extend(list(workflows[1:]), options.get("workflows"))
    if extra_workflows:
        kwargs["workflows"] = extra_workflows
    plugins = _extend(_agents.plugins(mcp_servers), options.get("plugins"))
    if plugins:
        kwargs["plugins"] = plugins
    worker = Worker(offerings=[bound], **kwargs)
    if caller.get("__name__") == "__main__":
        worker.run()
        return worker
    built = _captured.get()
    if built is not None:
        if built:
            raise RuntimeError(
                "worker.py calls serve() twice; one serve() binds the offering beside it"
            )
        built.append(worker)
    return worker


def serve(
    *workflows: type,
    quote: QuoteFunction | None = None,
    mcp_servers: _agents.MCPServers | None = None,
    **temporal_worker_options: Unpack[WorkerConfig],
) -> Worker:
    """Serve ``offering.yaml`` beside the calling file with ``workflows[0]`` (see the module
    docstring): runs the worker when the caller is ``__main__``, else returns it unstarted.

    ``mcp_servers``: ``{"name": factory}`` for ``kontra.workflow.mcp_server("name")`` (the
    ``[agents]`` extra). ``temporal_worker_options``: ``temporalio.worker.Worker``'s keywords.
    """
    return serve_from(
        sys._getframe(1).f_globals,
        workflows,
        quote=quote,
        mcp_servers=mcp_servers,
        options=temporal_worker_options,
    )
