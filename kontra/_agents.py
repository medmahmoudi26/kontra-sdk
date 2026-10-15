"""Temporal's OpenAI Agents integration, when the ``[agents]`` extra installed it.

``temporalio-openai-agents`` (import name ``temporalio.openai_agents``) runs an OpenAI Agents
SDK loop as workflow code and every model call, ``activity_as_tool`` call and MCP operation
as an activity. A seller reaches it only through Kontra: :mod:`kontra.workflow` re-exports
:func:`activity_as_tool` and ``temporal_mcp_server`` as :func:`mcp_server` from here, and
:func:`kontra.serve` adds :func:`plugins` to the worker when the seller asks for it
(``serve(Flow, agents=True)``, or ``mcp_servers=``). Opt-in, not "whenever installed": the
plugin builds an OpenAI client when the worker starts (no ``OPENAI_API_KEY``, no worker), and
its payload converter is laxer than the pydantic one -- neither belongs on a seller who does
not use agents but has the extra installed. Without the extra, :data:`AVAILABLE` is False and
the two names do not exist.

"Installed" means the module can be found; a broken installation fails on import, loudly,
rather than reading as absent. Importing it loads the ``agents`` and ``openai`` packages,
outside the workflow sandbox (``kontra`` is passed through it).
"""

from __future__ import annotations

import importlib.util
from collections.abc import Callable, Mapping
from typing import Any

import temporalio.worker
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import PayloadConverter

__all__ = ["AVAILABLE", "payload_converter_class", "plugins", "uses_agents"]

#: Whether ``temporalio.openai_agents`` is installed (``pip install 'kontra-sdk[agents]'``).
AVAILABLE: bool = importlib.util.find_spec("temporalio.openai_agents") is not None

if AVAILABLE:
    from temporalio.openai_agents.workflow import activity_as_tool
    from temporalio.openai_agents.workflow import temporal_mcp_server as mcp_server

    __all__ += ["activity_as_tool", "mcp_server"]

#: ``mcp_servers=``: a name the workflow passes to ``mcp_server(name)``, and the factory
#: that makes that server on the worker (an ``agents.mcp.MCPServer``).
MCPServers = Mapping[str, Callable[..., Any]]


def plugins(
    *, agents: bool = False, mcp_servers: MCPServers | None = None
) -> list[temporalio.worker.Plugin]:
    """The worker plugins agents need when asked for (``agents`` or ``mcp_servers``):
    ``[OpenAIAgentsPlugin]`` with default ``ModelActivityParameters`` and ``mcp_servers``.
    Not asked for: none. Asked for without the extra: a ``TypeError`` naming it."""
    if not (agents or mcp_servers):
        return []
    if not AVAILABLE:
        raise TypeError("agents need the extra: pip install 'kontra-sdk[agents]'")
    from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

    return [
        OpenAIAgentsPlugin(
            model_params=ModelActivityParameters(), mcp_servers=dict(mcp_servers or {}) or None
        )
    ]


def uses_agents(worker_plugins: object) -> bool:
    """Whether a worker's ``plugins=`` include the OpenAI Agents plugin."""
    if not AVAILABLE or not isinstance(worker_plugins, list | tuple):
        return False
    from temporalio.openai_agents import OpenAIAgentsPlugin

    return any(isinstance(p, OpenAIAgentsPlugin) for p in worker_plugins)  # pyright: ignore[reportUnknownVariableType]


def payload_converter_class(*, agents: bool = False) -> type[PayloadConverter]:
    """The payload converter a seller's client needs: ``OpenAIPayloadConverter`` when the
    worker runs the agents plugin (``agents``), else temporalio's pydantic one.

    The plugin goes on the worker (``plugins=``), which leaves the client -- and so its data
    converter -- as the worker made it. The model activity's input (``ActivityModelInput``)
    does not pass temporalio's strict pydantic decoding: OpenAI's request types drift from
    what the SDK sends, and ``OpenAIPayloadConverter`` (pydantic, ``exclude_unset``, a
    lenient fallback when validation fails) is the converter the integration is built on."""
    if not (agents and AVAILABLE):
        return PydanticPayloadConverter
    from temporalio.openai_agents import OpenAIPayloadConverter

    return OpenAIPayloadConverter
