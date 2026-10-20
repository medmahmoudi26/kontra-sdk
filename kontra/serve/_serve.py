"""``serve(entry)``: the whole of a seller's ``worker.py``.

```python
from kontra import serve
from workflows import Triage

serve(Triage)                                   # or: serve(Triage, max_concurrent_activities=50)
serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])   # one handler of an agent
```

It builds the :class:`~kontra.serve.Worker` a seller would otherwise write by hand:

* the offering is ``offering.yaml`` beside the calling file, bound to ``entry``: a
  ``@workflow.defn`` class, or one ``@agent.accepts`` handler of an agent (one hire is one
  turn of it, :mod:`kontra.serve.turn`); passing the agent class itself is refused with a
  fix-it;
* ``quote`` is the ``quote`` function of ``quote.py`` beside the calling file unless one is
  passed (imported the way ``from quote import quote`` in ``worker.py`` would import it);
* the activities are every module-level function temporalio's ``@activity.defn`` stamped in
  a module loaded from under the calling file's directory (the project) -- its tests,
  virtual environments, build output and ``kontra`` itself left out -- in import, then
  definition order; then those a project module binds that a function of another project
  module made (``fetch = make_activity("fetch")``); then the ``activities=`` passed (by
  identity, none twice). A method needs its instance: pass ``activities=[obj.method]``; an
  activity made or defined outside the project (a library's) is passed the same way. Two
  activities with one Temporal name are refused, naming both;
* every other keyword is ``temporalio.worker.Worker``'s, passed to ``Worker`` verbatim:
  ``workflows=`` for further workflows (the children the entry starts), and ``plugins=``,
  which :class:`~kontra.serve.Worker` routes -- client plugins go on the client before
  Kontra's own plugin, which comes last (decisions I29).

Run as ``python worker.py`` (the calling module is ``__main__``), serve() runs the worker
until it stops, its :attr:`~kontra.serve.Worker.project` the calling file's directory (under
a dev profile the graph page's static graphs are drawn from it, decisions I35). Imported --
by ``kontra serve``, ``kontra publish`` or a test -- it only builds the worker and returns
it; the CLI finds it with :func:`capture`, so neither command starts anything by importing
``worker.py``.
"""

from __future__ import annotations

import contextlib
import importlib
import inspect
import sys
from collections.abc import Callable, Generator, Mapping
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Unpack

from temporalio.worker import WorkerConfig

from kontra.serve.offering import BindingError, QuoteFunction, offering
from kontra.serve.worker import Worker

__all__ = ["OFFERING_FILE", "QUOTE_FILE", "capture", "project_activities", "serve", "serve_from"]

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
            "`async def quote(inp: <the entry's input model>) -> Quote` there, or pass "
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


#: Directories under the project whose modules are never scanned for activities: tests,
#: environments and build output (and every directory whose name starts with a dot).
_SKIP_DIRS = frozenset(
    {"tests", "test", "venv", "node_modules", "__pycache__", "build", "dist", "site-packages"}
)


def _in_project(name: str, module: object, base: Path) -> bool:
    """A module loaded from a file under ``base`` (not under a skipped directory), and not
    ``kontra``'s own."""
    if name.partition(".")[0] == "kontra":
        return False
    origin = getattr(module, "__file__", None)
    if not isinstance(origin, str):
        return False
    try:
        rel = Path(origin).resolve().relative_to(base)
    except (OSError, ValueError):
        return False
    return not any(part.startswith(".") or part in _SKIP_DIRS for part in rel.parts[:-1])


def _activity_name(fn: object) -> str:
    definition = getattr(fn, "__temporal_activity_definition", None)
    return str(getattr(definition, "name", None) or "")  # "" is the dynamic activity


def project_activities(base: Path) -> list[Any]:
    """What serve() registers by itself (see the module docstring): every module-level
    function stamped by temporalio's ``activity.defn`` in a module of the project under
    ``base``, defined in that module (a function a module imports from another is found
    once, in its own), in import then definition order; then every such function a project
    module binds that was *made* in another project module -- by a factory, whose module
    never binds what it returns. Two of one Temporal name are a :class:`BindingError`
    naming both -- never the last one silently winning."""
    modules = [(n, m) for n, m in list(sys.modules.items()) if _in_project(n, m, base)]
    project = {n for n, _ in modules}
    found: list[Any] = []
    named: dict[str, tuple[Any, str]] = {}

    def add(obj: Any, where: str) -> None:
        if any(obj is seen for seen in found):
            return
        key = _activity_name(obj)
        other = named.get(key)
        if other is not None:
            what = f"activity {key!r}" if key else "a dynamic activity"
            raise BindingError(
                f"{what} is defined twice in the project: {other[1]} and {where}; Temporal "
                "registers one per name -- rename one (@activity.defn(name=...))"
            )
        named[key] = (obj, where)
        found.append(obj)

    made: list[tuple[Any, str]] = []
    for name, module in modules:
        for attr, obj in list(vars(module).items()):
            if not (
                inspect.isfunction(obj)
                and getattr(obj, "__temporal_activity_definition", None) is not None
            ):
                continue
            if obj.__module__ == name:
                add(obj, f"{name}.{obj.__qualname__}")
            elif obj.__module__ in project:  # imported, or made by a factory: see below
                made.append((obj, f"{name}.{attr} (made by {obj.__module__}.{obj.__qualname__})"))
    for obj, where in made:  # what no module defines at its own top level
        add(obj, where)
    return found


def _extend(ours: list[Any], given: object) -> list[Any]:
    """``ours``, then what the seller passed that is not already there (by identity)."""
    out = list(ours)
    for item in given if isinstance(given, list | tuple) else ():
        if not any(item is seen for seen in out):
            out.append(item)
    return out


def serve_from(
    caller: Mapping[str, Any],
    entry: type | Callable[..., Any],
    *,
    quote: QuoteFunction | None,
    options: Mapping[str, Any],
    engine: str | None = None,
) -> Worker:
    """:func:`serve` for the module whose globals are ``caller`` (``kontra.serve(...)``, the
    callable package, calls this with its own caller's)."""
    if "offerings" in options:
        raise TypeError(
            "serve() binds offering.yaml beside worker.py; for several offerings use "
            "Worker(offerings=[...])"
        )
    base = _base(caller)
    bound = offering(
        base / OFFERING_FILE,
        entry=entry,
        quote=quote if quote is not None else _default_quote(caller, base),
        engine=engine,
    )
    kwargs = dict(options)
    kwargs["activities"] = _extend(project_activities(base), options.get("activities"))
    worker = Worker(offerings=[bound], **kwargs)
    if caller.get("__name__") == "__main__":
        worker.project = base  # `python worker.py`: the project it serves (decisions I35)
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
    entry: type | Callable[..., Any],
    /,
    *,
    quote: QuoteFunction | None = None,
    engine: str | None = None,
    **worker_kwargs: Unpack[WorkerConfig],
) -> Worker:
    """Serve ``offering.yaml`` beside the calling file with ``entry`` -- a ``@workflow.defn``
    class, or an agent's ``@agent.accepts`` handler (see the module docstring): runs the
    worker when the caller is ``__main__``, else returns it unstarted.

    ``engine``: the engine the offering runs on (seam ⑥, decisions I32), picked from the
    entry's type when omitted -- ``temporal`` for everything serve() binds; an explicit one
    must agree (``ValueError``). ``worker_kwargs``: ``temporalio.worker.Worker``'s keywords,
    verbatim (``plugins=`` and ``workflows=`` included; ``activities=`` are added after the
    project's).
    """
    return serve_from(
        sys._getframe(1).f_globals, entry, quote=quote, options=worker_kwargs, engine=engine
    )
