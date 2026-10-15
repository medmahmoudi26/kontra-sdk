"""Temporal's ``temporalio.workflow``, as a seller imports it: ``from kontra import workflow``.

Aliases, never wrappers. Temporal's decorators stamp ``__temporal_*`` attributes on the
objects they return and validate ``@workflow.run`` signatures with ``inspect``; a wrapper
returning another object would break both and the sandbox's class identity. So every name
here *is* Temporal's object, and a workflow written against this module runs unchanged on
any Temporal with ``from temporalio import workflow`` (decisions D1).

Added by Kontra: :func:`execute_activities_in_parallel` and :class:`Page`
(:mod:`kontra._parallel`), which never need ``continue_as_new`` in seller code.

The worker passes ``kontra`` through the workflow sandbox (``kontra.Worker``), so this
module and everything it imports is loaded once per process, never per run: nothing in
``kontra`` keeps mutable module-level state that a workflow could see.
"""

from temporalio.workflow import (
    ActivityCancellationType,
    ActivityHandle,
    ChildWorkflowCancellationType,
    ChildWorkflowHandle,
    ParentClosePolicy,
    as_completed,
    defn,
    deprecate_patch,
    execute_activity,
    execute_child_workflow,
    execute_local_activity,
    info,
    init,
    logger,
    now,
    patched,
    query,
    random,
    run,
    signal,
    sleep,
    start_activity,
    start_child_workflow,
    start_local_activity,
    time,
    unsafe,
    update,
    uuid4,
    wait,
    wait_condition,
)

from kontra._parallel import Page, execute_activities_in_parallel

__all__ = [
    "ActivityCancellationType",
    "ActivityHandle",
    "ChildWorkflowCancellationType",
    "ChildWorkflowHandle",
    "Page",
    "ParentClosePolicy",
    "as_completed",
    "defn",
    "deprecate_patch",
    "execute_activities_in_parallel",
    "execute_activity",
    "execute_child_workflow",
    "execute_local_activity",
    "info",
    "init",
    "logger",
    "now",
    "patched",
    "query",
    "random",
    "run",
    "signal",
    "sleep",
    "start_activity",
    "start_child_workflow",
    "start_local_activity",
    "time",
    "unsafe",
    "update",
    "uuid4",
    "wait",
    "wait_condition",
]

# The OpenAI Agents SDK through the façade, when the [agents] extra is installed: the agent
# loop runs as workflow code, every model call, tool activity and MCP operation as an activity.
from kontra import _agents

if _agents.AVAILABLE:
    from kontra._agents import activity_as_tool, mcp_server

    __all__ += ["activity_as_tool", "mcp_server"]
