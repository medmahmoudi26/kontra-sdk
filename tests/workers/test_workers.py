"""kontra.workers: the Workers view's verdict and stale flag, from Temporal's numbers alone."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kontra.gate_client.models import (
    PollerInfo,
    TaskQueueStats,
    TaskQueueType,
    TaskQueueTypeInfo,
)
from kontra.workers import STALE_NOTE, format_age, offline, stale, verdict, workers_view

WF, ACT, NEX = TaskQueueType.WORKFLOW, TaskQueueType.ACTIVITY, TaskQueueType.NEXUS
NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def poller(identity: str = "4242@host-a") -> PollerInfo:
    return PollerInfo(identity=identity, last_access_time=NOW, rate_per_second=100_000.0)


def info(
    pollers: int | list[PollerInfo] = 0,
    count: int = 0,
    age: float = 0.0,
    add: float = 0.0,
    dispatch: float = 0.0,
) -> TaskQueueTypeInfo:
    listed = pollers if isinstance(pollers, list) else [poller(f"{i}@host") for i in range(pollers)]
    return TaskQueueTypeInfo(
        pollers=listed,
        stats=TaskQueueStats(
            approximate_backlog_count=count,
            approximate_backlog_age=age,
            tasks_add_rate=add,
            tasks_dispatch_rate=dispatch,
            backlog_increase_rate=add - dispatch,
        ),
    )


def served(**types: TaskQueueTypeInfo) -> dict[TaskQueueType, TaskQueueTypeInfo]:
    """Every type with one poller and nothing queued, but for ``types``."""
    out = {kind: info(pollers=[poller()]) for kind in TaskQueueType}
    out.update({TaskQueueType(k): v for k, v in types.items()})
    return out


@pytest.mark.parametrize(
    ("seconds", "text"),
    [
        (0, "0 s"),
        (59.9, "59 s"),
        (60, "1 min"),
        (300, "5 min"),
        (7199, "119 min"),
        (7200, "2 h"),
        (47 * 3600, "47 h"),
        (2 * 86400, "2 d"),
        (-3, "0 s"),
    ],
)
def test_format_age(seconds: float, text: str) -> None:
    assert format_age(seconds) == text


def test_a_growing_backlog_says_scale_up_for_as_long_as_its_oldest_task() -> None:
    """The issue's own example: growing for 5 min with 2 pollers. "For" is the age of the
    oldest backlogged task, the one stateless reading of how long it has been there."""
    types = served(activity=info(pollers=2, count=120, age=300, add=1.2, dispatch=0.4))
    assert verdict(types) == "activity backlog growing for 5 min, 2 pollers -- scale up"
    assert not stale(types)


def test_a_backlog_without_its_pollers_says_start_a_worker() -> None:
    """Workflow pollers alone do not take activity tasks: a worker that registered no
    activities leaves them queued however many workflow pollers there are."""
    types = served(activity=info(count=7, age=90, add=0.5))
    assert (
        verdict(types) == "activity backlog of 7 for 1 min, no activity pollers -- start a worker"
    )


def test_a_backlog_nothing_is_dispatched_from_says_check_the_workers() -> None:
    """Listed pollers that take nothing: every slot busy, or workers gone within Temporal's
    five-minute poller listing."""
    types = served(workflow=info(pollers=1, count=3, age=40))
    assert (
        verdict(types) == "workflow backlog of 3 not moving for 40 s, 1 poller -- check the workers"
    )


def test_a_shrinking_backlog_is_draining() -> None:
    types = served(nexus=info(pollers=3, count=12, age=20, add=1.0, dispatch=4.0))
    assert verdict(types) == "nexus backlog of 12 draining, 3 pollers"


def test_the_worst_backlog_speaks_and_among_equals_the_oldest() -> None:
    growing = info(pollers=1, count=5, age=600, add=2, dispatch=1)
    unserved = info(count=1, age=5)
    assert verdict(served(workflow=growing, activity=unserved)).startswith(
        "activity backlog of 1 for 5 s, no activity pollers"
    )
    older = info(pollers=1, count=2, age=900, add=2, dispatch=1)
    assert verdict(served(workflow=growing, nexus=older)).startswith(
        "nexus backlog growing for 15 min"
    )


def test_without_a_backlog() -> None:
    assert verdict(served()) == "idle, 1 poller"  # one worker polls all three types
    flowing = served(workflow=info(pollers=[poller(), poller("b@host")], add=3, dispatch=3))
    assert verdict(flowing) == "keeping up, 2 pollers"
    assert verdict(offline()) == "no pollers -- start a worker"
    assert verdict({}) == "no pollers -- start a worker"


def test_stale_is_temporal_reporting_nothing_at_all() -> None:
    """Zeros and no pollers are also what Temporal reports for a task queue it unloaded
    after five idle minutes: the client shows the caveat with them."""
    assert stale(offline())
    assert stale({})
    assert not stale(served())
    assert not stale({**offline(), ACT: info(count=1)})
    assert not stale({**offline(), WF: info(add=0.1)})
    assert "unloaded" in STALE_NOTE


def test_workers_view_carries_the_numbers_and_what_they_mean() -> None:
    types = served(activity=info(pollers=2, count=120, age=300, add=1.2, dispatch=0.4))
    view = workers_view("acme/enrich@1.2.0", "acme/enrich@1.2.0", types)
    assert view.offering == view.task_queue == "acme/enrich@1.2.0"
    assert view.types_info == types
    assert view.verdict == verdict(types)
    assert view.stale is False
    assert workers_view("acme/enrich@1.2.0", "q", offline()).stale is True
    # the wire form keys the types by Temporal's names
    assert set(view.model_dump(mode="json")["types_info"]) == {"workflow", "activity", "nexus"}
    assert {WF, ACT, NEX} == set(TaskQueueType)
