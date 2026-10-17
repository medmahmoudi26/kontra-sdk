"""Temporal's OpenAI Agents integration, for a workflow that runs an agent.

``temporalio-openai-agents`` (import name ``temporalio.openai_agents``, the ``[agents]``
extra) runs an OpenAI Agents SDK loop as workflow code and every model call,
``activity_as_tool`` call and MCP operation as an activity. :mod:`kontra.workflow` re-exports
its ``activity_as_tool`` and ``temporal_mcp_server`` (as ``mcp_server``), imported when a
workflow first names them, so importing the façade never imports ``agents``.

There is no switch. The workflow does what it wants (decisions I25): one that imports OpenAI's
``agents`` package runs an agent, and its worker needs the plugin and the plugin's payload
converter; one that does not must get neither -- the plugin's converter validates more
leniently than the pydantic one. So the worker reads the fact from the import itself: once
the seller's ``worker.py`` imported its workflows, OpenAI's ``agents`` in ``sys.modules``
(:func:`imported`) puts :func:`plugins` on the worker -- the one :func:`kontra.serve` builds
and a ``Worker(...)`` written out alike, since ``kontra publish`` lets seller code import no
``temporalio`` -- and the worker's converter follows its plugins
(:func:`payload_converter_class`). ``agents`` imported without the extra is a ``TypeError``
naming it. The import counts when the workflow's module, or one it passes through, is
imported: one inside ``run`` comes after the worker was built.

Everything else is worker config, as Temporal has it: ``mcp_servers`` (the factories that
hold an MCP server's processes and credentials) and the model endpoint, which is the Agents
SDK's own default client and API -- ``set_default_openai_client(AsyncOpenAI(base_url=...))``
and ``set_default_openai_api("chat_completions")`` in ``worker.py``, before ``serve()``, point
every model activity at Ollama, vLLM, OpenRouter or Mistral. The plugin's own default would
not: it builds ``AsyncOpenAI()`` when the worker is built, from ``OPENAI_API_KEY`` and
``OPENAI_BASE_URL`` alone, so :func:`plugins` gives it the Agents SDK's ``OpenAIProvider()``,
which reads the default API when it is made, with the worker, and takes the default client
(else one from the environment) at the first model call.

"Installed" means the module can be found; a broken installation fails on import, loudly,
rather than reading as absent.
"""

from __future__ import annotations

import importlib.metadata
import importlib.util
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import temporalio.worker
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import PayloadConverter

__all__ = [
    "AVAILABLE",
    "DISTRIBUTION",
    "EXTRA",
    "FACADE",
    "facade",
    "imported",
    "payload_converter_class",
    "plugins",
    "uses_agents",
]

#: Whether ``temporalio.openai_agents`` is installed (``pip install 'kontra-sdk[agents]'``).
AVAILABLE: bool = importlib.util.find_spec("temporalio.openai_agents") is not None
#: What installs it.
EXTRA = "pip install 'kontra-sdk[agents]'"
#: The distribution OpenAI's Agents SDK comes in, the one whose ``agents`` is the signal.
DISTRIBUTION = "openai-agents"
#: The names :mod:`kontra.workflow` takes from ``temporalio.openai_agents.workflow``.
FACADE: Mapping[str, str] = {
    "activity_as_tool": "activity_as_tool",
    "mcp_server": "temporal_mcp_server",
}

#: ``mcp_servers=``: a name the workflow passes to ``mcp_server(name)``, and the factory
#: that makes that server on the worker (an ``agents.mcp.MCPServer``).
MCPServers = Mapping[str, Callable[..., Any]]


def facade(name: str) -> Any:
    """``kontra.workflow.<name>`` for a name in :data:`FACADE`: Temporal's own object,
    imported now (which imports ``agents``)."""
    if not AVAILABLE:
        raise AttributeError(f"kontra.workflow.{name} needs the extra: {EXTRA}")
    from temporalio.openai_agents import workflow as agents_workflow

    return getattr(agents_workflow, FACADE[name])


def imported() -> bool:
    """Whether this process imported OpenAI's Agents SDK: a workflow that runs an agent imports
    it, through its own module or anything that module passed through.

    The ``agents`` in ``sys.modules`` must be the one the ``openai-agents`` distribution
    installed: a seller's own ``agents.py``, or another distribution's ``agents``, runs no
    agent."""
    origin = getattr(sys.modules.get("agents"), "__file__", None)
    if not isinstance(origin, str):
        return False
    try:
        installed = importlib.metadata.distribution(DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError:
        return False
    shipped = Path(str(installed.locate_file("agents/__init__.py")))
    return Path(origin).resolve() == shipped.resolve()


def plugins(
    *, mcp_servers: MCPServers | None = None, given: object = None
) -> list[temporalio.worker.Plugin]:
    """The plugins a worker needs for what its workflows imported: ``[OpenAIAgentsPlugin]``
    when they imported ``agents`` (:func:`imported`), else none -- ``mcp_servers`` included,
    which is config a workflow may or may not use. ``agents`` without the extra: a
    ``TypeError`` naming it. None either when the seller's own ``plugins=`` (``given``)
    already hold an ``OpenAIAgentsPlugin``: theirs is Temporal's config, and two would
    register the same activities.

    The plugin's model activity gets the Agents SDK's ``OpenAIProvider()``, so the default
    client and API ``worker.py`` set before this are what every model call dials (see the
    module docstring); without a default client it is the SDK's own, from ``OPENAI_API_KEY``
    and ``OPENAI_BASE_URL``, read at the first model call rather than when the worker starts."""
    if not imported():
        return []
    if not AVAILABLE:
        raise TypeError(
            "the workflow imports OpenAI's Agents SDK (`agents`), which runs on Temporal "
            f"through the extra: {EXTRA}"
        )
    if uses_agents(given):
        if mcp_servers:
            raise TypeError(
                "mcp_servers and an OpenAIAgentsPlugin in plugins: give that plugin the "
                "MCP servers instead"
            )
        return []
    from agents import OpenAIProvider
    from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

    return [
        OpenAIAgentsPlugin(
            model_params=ModelActivityParameters(),
            model_provider=OpenAIProvider(),
            mcp_servers=dict(mcp_servers or {}) or None,
        )
    ]


def uses_agents(worker_plugins: object) -> bool:
    """Whether a worker's ``plugins=`` include the OpenAI Agents plugin."""
    if not AVAILABLE or not isinstance(worker_plugins, list | tuple):
        return False
    from temporalio.openai_agents import OpenAIAgentsPlugin

    return any(isinstance(p, OpenAIAgentsPlugin) for p in worker_plugins)  # pyright: ignore[reportUnknownVariableType]


def payload_converter_class(worker_plugins: object = None) -> type[PayloadConverter]:
    """The payload converter a seller's client needs for a worker with ``worker_plugins``:
    ``OpenAIPayloadConverter`` when they include the agents plugin (:func:`uses_agents`),
    else temporalio's pydantic one.

    The plugin goes on the worker (``plugins=``), which leaves the client -- and so its data
    converter -- as the worker made it. The model activity's input (``ActivityModelInput``)
    does not pass temporalio's strict pydantic decoding: OpenAI's request types drift from
    what the SDK sends, and ``OpenAIPayloadConverter`` (pydantic, ``exclude_unset``, a
    lenient fallback when validation fails) is the converter the integration is built on."""
    if not uses_agents(worker_plugins):
        return PydanticPayloadConverter
    from temporalio.openai_agents import OpenAIPayloadConverter

    return OpenAIPayloadConverter
