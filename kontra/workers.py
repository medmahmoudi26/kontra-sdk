"""The Workers view's rules (kontra-cloud#6), as pure functions.

Whether a seller's workers keep up is Temporal metadata: ``DescribeTaskQueue`` reports, per
task-queue type, the approximate backlog and the age of its oldest task, the rates tasks are
added and dispatched at, and the workers polling. :func:`verdict` reads one offering
version's numbers and says in one line what they mean; :func:`stale` says when they may mean
nothing. The gate (platform) and the fake gate (:mod:`kontra.testing`) both answer
``GET /v1/offerings/.../workers`` with :func:`workers_view`, so the console, ``kontra status
--workers`` and anything else reading the route show the same verdict.

Nothing here does I/O or keeps state. The verdict is the numbers' alone, read once: "growing
for 5 min" is the age of the oldest backlogged task (Temporal's
``approximate_backlog_age``), not a history of earlier reads.
"""

from __future__ import annotations

from collections.abc import Mapping

from kontra.gate_client.models import (
    TaskQueueStats,
    TaskQueueType,
    TaskQueueTypeInfo,
    WorkersView,
)

__all__ = ["STALE_NOTE", "format_age", "offline", "stale", "verdict", "workers_view"]

#: What a client shows next to a :attr:`~kontra.gate_client.models.WorkersView.stale` view.
STALE_NOTE = (
    "Temporal reports zeros and no pollers for a task queue it unloaded after about five "
    "idle minutes, so these numbers may not be a measurement; they are again once a worker "
    "polls."
)

_TypesInfo = Mapping[TaskQueueType, TaskQueueTypeInfo]


def format_age(seconds: float) -> str:
    """``42 s``, ``5 min``, ``3 h``, ``2 d``: whole units, rounded down."""
    whole = max(0, int(seconds))
    if whole < 60:
        return f"{whole} s"
    if whole < 2 * 3600:
        return f"{whole // 60} min"
    if whole < 2 * 86400:
        return f"{whole // 3600} h"
    return f"{whole // 86400} d"


def _pollers(count: int) -> str:
    return f"{count} poller" if count == 1 else f"{count} pollers"


def _backlog(kind: TaskQueueType, info: TaskQueueTypeInfo) -> tuple[int, float, str] | None:
    """How bad one type's backlog is (higher is worse), its age, and the line that says
    so; None without a backlog."""
    s = info.stats
    count = s.approximate_backlog_count
    if count <= 0:
        return None
    age = s.approximate_backlog_age
    since = format_age(age)
    polling = len(info.pollers)
    if polling == 0:
        line = f"{kind} backlog of {count} for {since}, no {kind} pollers -- start a worker"
        return 3, age, line
    if s.backlog_increase_rate > 0:
        return 2, age, f"{kind} backlog growing for {since}, {_pollers(polling)} -- scale up"
    if s.tasks_dispatch_rate <= 0:
        line = f"{kind} backlog of {count} not moving for {since}, {_pollers(polling)}"
        return 2, age, f"{line} -- check the workers"
    return 1, age, f"{kind} backlog of {count} draining, {_pollers(polling)}"


def verdict(types_info: _TypesInfo) -> str:
    """One line on whether the workers keep up, worst first:

    * a type with a backlog and none of its pollers: ``start a worker`` (one that serves
      that kind of task);
    * a backlog growing (``backlog_increase_rate > 0``): ``scale up``;
    * a backlog nothing is dispatched from: ``check the workers`` (every slot busy, or
      workers gone within Temporal's poller listing);
    * a backlog shrinking: ``draining``;
    * no backlog: ``no pollers -- start a worker`` without any poller, else ``keeping up``
      while tasks flow, else ``idle``.

    Among backlogs equally bad, the oldest speaks. Pollers are counted as Temporal lists
    them, per type for a backlog and by identity across types otherwise."""
    worst: tuple[int, float, str] | None = None
    for kind in TaskQueueType:
        info = types_info.get(kind)
        found = _backlog(kind, info) if info is not None else None
        if found is not None and (worst is None or found[:2] > worst[:2]):
            worst = found
    if worst is not None:
        return worst[2]
    identities = {p.identity for info in types_info.values() for p in info.pollers}
    if not identities:
        return "no pollers -- start a worker"
    stats = [i.stats for i in types_info.values()]
    if any(s.tasks_add_rate > 0 or s.tasks_dispatch_rate > 0 for s in stats):
        return f"keeping up, {_pollers(len(identities))}"
    return f"idle, {_pollers(len(identities))}"


def stale(types_info: _TypesInfo) -> bool:
    """Temporal reported nothing: every number zero and no poller, of every type. That is
    what it reports for a task queue it unloaded after about five idle minutes as well as
    for one never used, so a client shows :data:`STALE_NOTE` with it."""
    empty = TaskQueueStats()
    return all(not i.pollers and i.stats == empty for i in types_info.values())


def offline() -> dict[TaskQueueType, TaskQueueTypeInfo]:
    """Every type with nothing reported: what a gate with no Temporal behind it describes
    (its sellers are all offline)."""
    return {kind: TaskQueueTypeInfo() for kind in TaskQueueType}


def workers_view(offering: str, task_queue: str, types_info: _TypesInfo) -> WorkersView:
    """The answer of ``GET /v1/offerings/{party}/{name}/versions/{version}/workers`` for
    the exact version ``offering`` and what ``DescribeTaskQueue`` said of ``task_queue``."""
    return WorkersView(
        offering=offering,
        task_queue=task_queue,
        types_info=dict(types_info),
        verdict=verdict(types_info),
        stale=stale(types_info),
    )
