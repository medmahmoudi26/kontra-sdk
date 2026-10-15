"""Temporal's ``temporalio.activity``, as a seller imports it: ``from kontra import activity``.

Aliases, except :func:`defn`, which calls Temporal's ``activity.defn`` and returns exactly
what it returns -- the same function, stamped -- after noting it, so that
:func:`kontra.serve` registers every activity the process imported without a list to keep
(an activity nothing imports cannot be referenced by any workflow either). Code written
against this module runs on any Temporal with ``from temporalio import activity``.

Throughput without per-item history: one activity doing its own concurrency and
heartbeating its cursor (``activity.heartbeat(i)``, resumed from
``activity.info().heartbeat_details`` on retry) -- see docs/sellers.md.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar, overload

from temporalio import activity as _temporal
from temporalio.activity import (
    Info,
    cancellation_details,
    heartbeat,
    info,
    is_cancelled,
    is_worker_shutdown,
    logger,
    wait_for_cancelled,
)

__all__ = [
    "Info",
    "cancellation_details",
    "defined",
    "defn",
    "heartbeat",
    "info",
    "is_cancelled",
    "is_worker_shutdown",
    "logger",
    "wait_for_cancelled",
]

CallableT = TypeVar("CallableT", bound=Callable[..., Any])

#: Every activity defined through :func:`defn` in this process, by Temporal name, in
#: definition order. Written when an activities module is imported, never by workflow code
#: (workflows only reference activities), so the sandbox passthrough shares nothing a run
#: could change.
_DEFINED: dict[str, Callable[..., Any]] = {}


def _note(fn: CallableT) -> CallableT:
    definition = getattr(fn, "__temporal_activity_definition")  # stamped by Temporal's defn
    _DEFINED[definition.name or ""] = fn  # "": the one dynamic activity a worker may have
    return fn


@overload
def defn(fn: CallableT) -> CallableT: ...


@overload
def defn(
    *,
    name: str | None = None,
    no_thread_cancel_exception: bool = False,
    dynamic: bool = False,
) -> Callable[[CallableT], CallableT]: ...


def defn(
    fn: CallableT | None = None,
    *,
    name: str | None = None,
    no_thread_cancel_exception: bool = False,
    dynamic: bool = False,
) -> CallableT | Callable[[CallableT], CallableT]:
    """``temporalio.activity.defn``, with the same arguments and the same result."""
    if fn is not None:
        return _note(_temporal.defn(fn))
    # Temporal's overloads take a name or dynamic=True, never both.
    if dynamic:
        decorator = _temporal.defn(
            no_thread_cancel_exception=no_thread_cancel_exception, dynamic=True
        )
    else:
        decorator = _temporal.defn(name=name, no_thread_cancel_exception=no_thread_cancel_exception)

    def noted(f: CallableT) -> CallableT:
        return _note(decorator(f))

    return noted


def defined() -> list[Callable[..., Any]]:
    """The activities :func:`defn` has seen in this process, in definition order (what
    :func:`kontra.serve` registers)."""
    return list(_DEFINED.values())
