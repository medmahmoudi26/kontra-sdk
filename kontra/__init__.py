"""Kontra: a marketplace of durable, quoted jobs.

Sellers write vanilla Temporal -- ``from temporalio import workflow, activity`` -- and the one
file of theirs that imports ``kontra`` is ``worker.py``, which binds the workflow (or an
agent's handler) to its offering with :func:`serve`, or puts :class:`KontraPlugin` on a
Temporal client of their own (decisions I29). Buyers hire with :mod:`kontra.hire` from a
script, from inside their own workflow, or from an agent through ``kontra mcp``.

This package exports only what Kontra defines. ``serve``, ``Worker`` and ``KontraPlugin``
load the serving stack (gate client, codec, auth, temporalio) on first use, not on
``import kontra``: the CLI starts without it, and a seller's ``models.py`` importing
:mod:`kontra.types` loads no Temporal.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kontra.plugin import KontraPlugin
    from kontra.serve import Worker, serve

__version__ = "0.2.4"

__all__ = ["KontraPlugin", "Worker", "__version__", "serve"]

#: Names this package exported before kontra-sdk#51, and what replaces each.
_GONE = {
    "workflow": "`from temporalio import workflow`",
    "activity": "`from temporalio import activity`",
    "RetryPolicy": "`from temporalio.common import RetryPolicy`",
    "ApplicationError": "`from temporalio.exceptions import ApplicationError`",
    "Page": "`from kontra.contrib.parallel import Page` (or a model of your own with "
    "`items` and `next`)",
}


def __getattr__(name: str) -> Any:
    if name in ("Worker", "serve"):
        import kontra.serve

        return getattr(kontra.serve, name)
    if name == "KontraPlugin":
        from kontra.plugin import KontraPlugin

        return KontraPlugin
    if name in _GONE:
        raise AttributeError(
            f"`kontra.{name}` is gone: {_GONE[name]} -- seller code imports Temporal directly "
            "(kontra-sdk#51, decisions I29)"
        )
    raise AttributeError(f"module 'kontra' has no attribute {name!r}")
