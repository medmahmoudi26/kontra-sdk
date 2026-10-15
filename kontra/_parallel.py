"""``execute_activities_in_parallel``: one activity per item over a paged source, with
continue-as-new hidden in a child workflow, ``kontra.batch.v1`` (:class:`_Batch`).

A seller's workflow awaits it like any ``workflow`` function. It starts the child on the
parent's task queue and returns what the child returns. The child takes pages -- from
``pages=``, an activity ``(cursor) -> Page``, or ``items=``, one page held in memory --,
schedules the item activity for each item with never more than ``max_concurrent`` of them
open, and folds each result into an accumulator with ``reduce=`` (default: list-append) in
the order they complete. When the server suggests continue-as-new, the child lets what is
in flight finish and then carries only a :class:`_Cursor` into the next run: the next
page's cursor, the accumulator, and the current page's items not yet started. History is
bounded by ``max_concurrent``, never by the number of items; results never reach it, only
the accumulator does.

Functions cross into the child by reference (module and qualified name, resolved there by
import), never by value. Activities are scheduled by their Temporal name; the reference
is only read for their type hints, so results reach ``reduce=`` as the type the activity
declares and the accumulator is rebuilt as the type ``reduce=`` takes after every
continue-as-new. All three must therefore be module-level (an activity may also be a
method of a module-level class); a lambda, a closure or a nested function is refused when
the call is made, not in the child.

Workflow code imports this module and the worker passes ``kontra`` through the sandbox:
nothing here keeps module-level state.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import importlib
import inspect
import types
import typing
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Generic, TypeVar, overload

from temporalio import workflow
from temporalio.common import Priority, RetryPolicy
from temporalio.exceptions import ApplicationError

# Passed through even under a sandbox that does not pass ``kontra`` through (a seller's own
# workflow_runner): the ``kontra.contract`` package touches the filesystem when imported.
with workflow.unsafe.imports_passed_through():
    from kontra.contract.names import batch_workflow_id

__all__ = ["BATCH_WORKFLOW", "Page", "execute_activities_in_parallel"]

T = TypeVar("T")
A = TypeVar("A")

#: The child workflow's Temporal name. The version changes when :class:`_Spec` or
#: :class:`_Cursor` changes shape: a run in flight carries the old shape into its next run.
BATCH_WORKFLOW = "kontra.batch.v1"


@dataclass
class Page(Generic[T]):
    """What a ``pages=`` activity returns: this page's items, and the cursor to fetch the
    next page with (None: this page was the last)."""

    items: list[T]
    next: Any = None


@dataclass(frozen=True)
class _Ref:
    """A module-level function, or a method of a module-level class, by where it is."""

    module: str
    qualname: str


@dataclass
class _Options:
    """The activity options every item activity and every page activity is scheduled with
    (Temporal's names). Typed field by field, not a dict: the pydantic converter rebuilds a
    ``timedelta`` or a ``RetryPolicy`` only where the type says so."""

    start_to_close_timeout: timedelta | None = None
    schedule_to_close_timeout: timedelta | None = None
    schedule_to_start_timeout: timedelta | None = None
    heartbeat_timeout: timedelta | None = None
    retry_policy: RetryPolicy | None = None
    cancellation_type: workflow.ActivityCancellationType = (
        workflow.ActivityCancellationType.TRY_CANCEL
    )
    summary: str | None = None
    priority: Priority = Priority.default


_OPTIONS = tuple(f.name for f in dataclasses.fields(_Options))


@dataclass
class _Spec:
    """Everything the child needs, fixed for the whole batch."""

    activity: str
    activity_ref: _Ref
    pages: str | None
    pages_ref: _Ref | None
    items: list[Any] | None
    max_concurrent: int
    options: _Options
    reduce: _Ref | None


@dataclass
class _Cursor:
    """The only state that crosses continue-as-new. ``started``: a page was taken (so a
    ``next`` of None means exhausted, not "fetch the first"); ``rest``: the current page's
    items not yet started."""

    next: Any = None
    acc: Any = None
    started: bool = False
    rest: list[Any] = field(default_factory=list[Any])


# --------------------------------------------------------------------------- references


def _resolve(ref: _Ref) -> Any:
    """The object ``ref`` names. In a workflow, its module is passed through the sandbox:
    imported once per process (the worker has it already: it registered the activities)
    rather than once per batch run, and without the sandbox's late-import warning."""
    with workflow.unsafe.imports_passed_through():
        obj: Any = importlib.import_module(ref.module)
    for part in ref.qualname.split("."):
        obj = getattr(obj, part)
    return obj


def _ref(fn: Any, what: str) -> _Ref:
    """``fn``'s reference, refused unless importing it gives back ``fn`` itself."""
    target = getattr(fn, "__func__", fn)  # a bound method: its class's function
    module = getattr(target, "__module__", None)
    qualname = getattr(target, "__qualname__", None)
    if isinstance(module, str) and isinstance(qualname, str) and "<" not in qualname:
        ref = _Ref(module, qualname)
        try:
            found = _resolve(ref)
        except (ImportError, AttributeError):
            found = None
        if found is target:
            return ref
    raise TypeError(
        f"{what} must be a module-level function (or a method of a module-level class), "
        f"got {fn!r}: the batch workflow finds it by importing it, so a lambda, a closure "
        "or a nested function cannot be used"
    )


def _activity_name(fn: Any, what: str) -> str:
    definition = getattr(fn, "__temporal_activity_definition", None)
    name = getattr(definition, "name", None)
    if not isinstance(name, str) or not name:
        raise TypeError(f"{what} must be an @activity.defn activity with a name, got {fn!r}")
    return name


def _hints(fn: Any) -> dict[str, Any]:
    """``fn``'s resolved annotations, or none when they do not resolve."""
    try:
        return typing.get_type_hints(getattr(fn, "__func__", fn))
    except Exception:
        return {}


def _acc_type(fold: Callable[..., Any]) -> Any:
    """The accumulator's type: what ``reduce=`` takes first, else what it returns."""
    hints = _hints(fold)
    first = next(iter(inspect.signature(fold).parameters), None)
    return hints.get(first or "", hints.get("return"))


def _list_of(item: Any) -> Any:
    return None if item is None else types.GenericAlias(list, (item,))


def _append(acc: list[Any], result: Any) -> list[Any]:
    acc.append(result)
    return acc


# --------------------------------------------------------------------------- the call


@overload
async def execute_activities_in_parallel(
    activity: Callable[..., Any],
    *,
    pages: Callable[..., Any] | None = None,
    items: Sequence[Any] | None = None,
    max_concurrent: int = 100,
    reduce: None = None,
    init: None = None,
    **activity_options: Any,
) -> list[Any]: ...


@overload
async def execute_activities_in_parallel(
    activity: Callable[..., Any],
    *,
    pages: Callable[..., Any] | None = None,
    items: Sequence[Any] | None = None,
    max_concurrent: int = 100,
    reduce: Callable[[A, Any], A],
    init: A,
    **activity_options: Any,
) -> A: ...


async def execute_activities_in_parallel(
    activity: Callable[..., Any],
    *,
    pages: Callable[..., Any] | None = None,
    items: Sequence[Any] | None = None,
    max_concurrent: int = 100,
    reduce: Callable[[Any, Any], Any] | None = None,
    init: Any = None,
    **activity_options: Any,
) -> Any:
    """Run ``activity`` once per item, at most ``max_concurrent`` at a time, in a child
    workflow that continues as new on its own; return the fold of the results.

    ``pages=``: an activity ``(cursor) -> Page``, first called with None, then with each
    page's ``next`` until one is None. ``items=``: the items themselves, as one page; they
    travel in the child's input, so they must fit one payload. Exactly one of the two.

    ``reduce=``: a pure module-level function ``(acc, result) -> acc`` run in the workflow,
    starting from ``init``; the accumulator is the only result state kept, so keep it
    small. Without it the results are returned as a list, in completion order -- fine for a
    small batch, a payload-size failure for a large one.

    ``**activity_options``: Temporal's activity options, applied to every item activity and
    page activity (``start_to_close_timeout`` or ``schedule_to_close_timeout`` required).
    The task queue is the parent's, never an option.

    An item activity that fails after its retries fails the batch -- what is in flight is
    cancelled first -- and this call raises Temporal's ``ChildWorkflowError`` with the
    ``ActivityError`` and the activity's own error below it.
    """
    if (pages is None) == (items is None):
        raise ValueError("execute_activities_in_parallel needs exactly one of pages= or items=")
    if isinstance(items, str | bytes):
        raise TypeError("items= must be a sequence of items, not a string")
    if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError(f"max_concurrent must be an int, got {max_concurrent!r}")
    if max_concurrent < 1:
        raise ValueError(f"max_concurrent must be at least 1, got {max_concurrent}")
    for key in activity_options:
        if key == "task_queue":
            raise TypeError("task_queue is not an option: batch activities run on the parent's")
        if key not in _OPTIONS:
            raise TypeError(
                f"execute_activities_in_parallel got an unexpected keyword argument {key!r}; "
                f"its activity options are {', '.join(_OPTIONS)}"
            )
    options = _Options(**activity_options)
    if options.start_to_close_timeout is None and options.schedule_to_close_timeout is None:
        raise ValueError("start_to_close_timeout or schedule_to_close_timeout is required")
    name = _activity_name(activity, "activity")
    activity_ref = _ref(activity, "activity")
    pages_name = pages_ref = None
    if pages is not None:
        pages_name, pages_ref = _activity_name(pages, "pages="), _ref(pages, "pages=")
    reduce_ref = None
    if reduce is None:
        if init is not None:
            raise TypeError("init= is the starting value of reduce=; pass reduce= too")
        acc, result_type = [], _list_of(_hints(activity).get("return"))
    else:
        if not inspect.isfunction(reduce):
            raise TypeError(f"reduce= must be a module-level function, got {reduce!r}")
        try:
            inspect.signature(reduce).bind(init, init)
        except TypeError as e:
            raise TypeError(f"reduce= must take (acc, result): {e}") from e
        reduce_ref = _ref(reduce, "reduce=")
        acc, result_type = init, _hints(reduce).get("return")
    spec = _Spec(
        activity=name,
        activity_ref=activity_ref,
        pages=pages_name,
        pages_ref=pages_ref,
        items=None if items is None else list(items),
        max_concurrent=max_concurrent,
        options=options,
        reduce=reduce_ref,
    )
    return await workflow.execute_child_workflow(
        BATCH_WORKFLOW,
        args=[spec, _Cursor(acc=acc)],
        id=batch_workflow_id(workflow.info().workflow_id, name, str(workflow.uuid4())),
        result_type=result_type,
    )


# --------------------------------------------------------------------------- the child


def _typed(value: Any, type_hint: Any) -> Any:
    """``value`` (as decoded without a type) as ``type_hint``, through the worker's own
    payload converter."""
    if type_hint is None:
        return value
    converter = workflow.payload_converter()
    return converter.from_payload(converter.to_payload(value), type_hint)


async def _settle(pending: dict[asyncio.Task[None], None]) -> None:
    """Cancel what is in flight and wait for it, in the order it was started (a set's
    order would differ on replay, and so would the cancel commands)."""
    tasks = list(pending)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@workflow.defn(name=BATCH_WORKFLOW)
class _Batch:
    """The child behind :func:`execute_activities_in_parallel`; registered on every
    :class:`kontra.serve.Worker`."""

    @workflow.run
    async def run(self, spec: _Spec, cur: _Cursor) -> Any:
        result_type = _hints(_resolve(spec.activity_ref)).get("return")
        if spec.reduce is None:
            fold: Callable[[Any, Any], Any] = _append
            acc_type = _list_of(result_type)
        else:
            fold = _resolve(spec.reduce)
            acc_type = _acc_type(fold)
        page_type = (
            Page if spec.pages_ref is None else _hints(_resolve(spec.pages_ref)).get("return", Page)
        )
        options = {name: getattr(spec.options, name) for name in _OPTIONS}

        acc = _typed(cur.acc, acc_type)
        cursor, started, rest = cur.next, cur.started, collections.deque(cur.rest)
        pending: dict[asyncio.Task[None], None] = {}
        # An item activity that failed for good (ActivityError: the batch fails), or a
        # result that would not decode or reduce= raising (a bug: the task fails, retried).
        failures: list[BaseException] = []
        progressed = False  # a run the server already wants rolled must still move on

        async def one(item: Any) -> None:
            nonlocal acc
            try:
                result = await workflow.execute_activity(
                    spec.activity, item, result_type=result_type, **options
                )
                acc = fold(acc, result)
            except Exception as e:  # never dropped: run() raises the first as it came
                failures.append(e)

        def start(item: Any) -> None:
            task = asyncio.create_task(one(item))
            pending[task] = None
            task.add_done_callback(lambda t: pending.pop(t, None))

        try:
            while True:
                # Backpressure: nothing is fetched or scheduled past max_concurrent open.
                await workflow.wait_condition(
                    lambda: len(pending) < spec.max_concurrent or bool(failures)
                )
                if failures:
                    break
                if progressed and workflow.info().is_continue_as_new_suggested():
                    break
                progressed = True
                if rest:
                    start(rest.popleft())
                    continue
                if started and cursor is None:
                    break  # exhausted
                if spec.items is not None:
                    rest.extend(spec.items)
                    started, cursor = True, None
                    continue
                assert spec.pages is not None
                page = await workflow.execute_activity(
                    spec.pages, cursor, result_type=page_type, **options
                )
                if not isinstance(page, Page):
                    raise TypeError(f"{spec.pages} returned {type(page).__name__}, not a Page")
                page = typing.cast(Page[Any], page)
                if not page.items and page.next is not None and page.next == cursor:
                    raise ApplicationError(
                        f"{spec.pages} returned an empty page whose next is the cursor it "
                        f"was given ({cursor!r}): it would be called with it forever",
                        type="KontraBatchStuck",
                        non_retryable=True,
                    )
                rest.extend(page.items)
                started, cursor = True, page.next
            await workflow.wait_condition(lambda: not pending or bool(failures))
        except BaseException:  # cancelled, or a page that failed: nothing is left running
            await _settle(pending)
            raise
        if failures:
            await _settle(pending)
            raise failures[0]
        if started and cursor is None and not rest:
            return acc
        workflow.continue_as_new(
            args=[
                # items= is in rest now: the next run must not carry it twice.
                spec if spec.items is None else dataclasses.replace(spec, items=[]),
                _Cursor(next=cursor, acc=acc, started=started, rest=list(rest)),
            ]
        )
