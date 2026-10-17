"""Kontra: a marketplace of durable, quoted jobs.

A seller imports only from here -- ``from kontra import workflow, activity, serve`` -- and
Temporal stays behind the façade: :mod:`kontra.workflow` and :mod:`kontra.activity` are
Temporal's own objects re-exported, so the code still runs on any Temporal (decisions D1).
``worker.py`` binds the workflow to its offering with :func:`serve` (or the explicit
:class:`Worker`). Buyers hire with :mod:`kontra.hire` from a script, from inside their own
workflow, or from an agent through ``kontra mcp``.

``Worker`` and ``serve`` load the serving stack (gate client, codec, auth) on first use, not
on ``import kontra``: workflow code imports this package, and the sandbox should not have to
pass any of that through.
"""

from typing import TYPE_CHECKING, Any

from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from kontra import activity, workflow
from kontra._parallel import Page

if TYPE_CHECKING:
    from kontra.serve import Worker, serve

__version__ = "0.2.4"

__all__ = ["ApplicationError", "Page", "RetryPolicy", "Worker", "activity", "serve", "workflow"]


def __getattr__(name: str) -> Any:
    if name in ("Worker", "serve"):
        import kontra.serve

        return getattr(kontra.serve, name)
    raise AttributeError(f"module 'kontra' has no attribute {name!r}")
