"""Load an offering project's ``worker.py`` (the §6.5 convention): the Worker its
``serve(...)`` call built (:func:`kontra.serve._serve.capture`), or its module-level
``worker = Worker(...)``, is what ``kontra serve`` runs and where ``kontra publish`` finds the
offerings. Importing it starts nothing: serve() runs only under ``python worker.py``.

The project directory goes first on ``sys.path`` and ``worker`` is imported by name, so its
own ``from workflows import ...`` resolves exactly as ``python worker.py`` would, and the
workflow module stays importable by name for Temporal's sandbox. Top-level modules of the
same name loaded from somewhere else (another project loaded earlier in this process) are
set aside first and restored afterwards: two projects both have a ``models.py``.
"""

from __future__ import annotations

import contextlib
import importlib
import sys
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from typing import Any

from kontra.cli._common import fail

__all__ = ["WORKER_FILE", "load_worker"]

WORKER_FILE = "worker.py"


def _local_names(root: Path) -> set[str]:
    """Top-level module names the project defines: ``x.py`` and package directories."""
    names = {p.stem for p in root.glob("*.py")}
    names |= {p.name for p in root.iterdir() if p.is_dir() and (p / "__init__.py").is_file()}
    return names


def _owned_by(module: ModuleType, root: Path) -> bool:
    origin = getattr(module, "__file__", None)
    if not isinstance(origin, str):
        return False
    try:
        Path(origin).resolve().relative_to(root)
    except ValueError:
        return False
    return True


def _clashes(names: set[str]) -> dict[str, ModuleType]:
    """Loaded modules (and their submodules) whose top-level name the project reuses."""
    return {key: mod for key, mod in sys.modules.items() if key.split(".", 1)[0] in names}


def _installed(module: ModuleType) -> bool:
    """Built in, or from the standard library or site-packages: never set aside."""
    origin = getattr(module, "__file__", None)
    if not isinstance(origin, str):
        return True
    path = Path(origin).resolve()
    for prefix in {sys.prefix, sys.base_prefix, sys.exec_prefix}:
        with contextlib.suppress(ValueError):
            path.relative_to(Path(prefix).resolve())
            return True
    return False


@contextlib.contextmanager
def load_worker(directory: str | Path) -> Generator[Any]:
    """The :class:`kontra.serve.Worker` of ``<directory>/worker.py``: the one its serve()
    built, else its module-level ``worker``. The project's modules stay imported while the
    block runs (a worker serving needs them)."""
    from kontra.serve import Worker
    from kontra.serve._serve import capture

    root = Path(directory).expanduser().resolve()
    if not (root / WORKER_FILE).is_file():
        fail(
            f"no {WORKER_FILE} in {root}: run this from an offering directory "
            "(`kontra init <name>` makes one) or pass --dir"
        )
    names = _local_names(root)
    if "kontra" in names:
        fail(f"{root} has a kontra.py or kontra/ that would hide the kontra package; rename it")
    set_aside = {k: m for k, m in _clashes(names).items() if not _owned_by(m, root)}
    shadowed = sorted(k for k, m in set_aside.items() if "." not in k and _installed(m))
    if shadowed:
        fail(
            f"{root} has modules named like installed ones ({', '.join(shadowed)}); "
            "rename them so the project imports its own"
        )
    for key in set_aside:
        del sys.modules[key]
    sys.path.insert(0, str(root))
    importlib.invalidate_caches()
    loaded: set[str] = set()
    try:
        before = set(sys.modules)
        try:
            with capture() as served:
                module = importlib.import_module("worker")
        finally:
            loaded = {k for k in set(sys.modules) - before if k.split(".", 1)[0] in names}
        found = getattr(module, "worker", None)
        if served:
            if isinstance(found, Worker) and found is not served[0]:
                fail(
                    f"{root / WORKER_FILE} calls serve() and also defines another "
                    "`worker = Worker(...)`; keep one"
                )
            found = served[0]
        if not isinstance(found, Worker):
            fail(
                f"{root / WORKER_FILE} must call `serve(YourFlow)` or define a module-level "
                f"`worker = Worker(...)` (kontra.serve.Worker); found "
                f"{type(found).__name__ if found is not None else 'none'}"
            )
        yield found
    finally:
        with contextlib.suppress(ValueError):
            sys.path.remove(str(root))
        for key in loaded:
            sys.modules.pop(key, None)
        sys.modules.update(set_aside)
