"""Temporal's OpenAI Agents integration, when the ``[agents]`` extra installed it.

``temporalio-openai-agents`` (import name ``temporalio.openai_agents``) runs an OpenAI Agents
SDK loop as workflow code and every model call, ``activity_as_tool`` call and MCP operation
as an activity. A seller reaches it only through Kontra: :mod:`kontra.workflow` re-exports
:func:`activity_as_tool` and ``temporal_mcp_server`` as :func:`mcp_server` from here, and
:func:`kontra.serve` adds :func:`plugins` to the worker. Without the extra, :data:`AVAILABLE`
is False, the two names do not exist, and :func:`plugins` adds nothing.

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

__all__ = ["AVAILABLE", "payload_converter_class", "plugins"]

#: Whether ``temporalio.openai_agents`` is installed (``pip install 'kontra-sdk[agents]'``).
AVAILABLE: bool = importlib.util.find_spec("temporalio.openai_agents") is not None

if AVAILABLE:
    from temporalio.openai_agents.workflow import activity_as_tool
    from temporalio.openai_agents.workflow import temporal_mcp_server as mcp_server

    __all__ += ["activity_as_tool", "mcp_server"]

#: ``mcp_servers=``: a name the workflow passes to ``mcp_server(name)``, and the factory
#: that makes that server on the worker (an ``agents.mcp.MCPServer``).
MCPServers = Mapping[str, Callable[..., Any]]


def plugins(mcp_servers: MCPServers | None = None) -> list[temporalio.worker.Plugin]:
    """The worker plugins agents need: ``[OpenAIAgentsPlugin]`` with default
    ``ModelActivityParameters`` and ``mcp_servers`` when the extra is installed, else none
    (and ``mcp_servers`` is refused: nothing could serve them)."""
    if not AVAILABLE:
        if mcp_servers:
            raise TypeError("mcp_servers= needs the agents extra: pip install 'kontra-sdk[agents]'")
        return []
    from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

    return [
        OpenAIAgentsPlugin(
            model_params=ModelActivityParameters(), mcp_servers=dict(mcp_servers or {}) or None
        )
    ]


def payload_converter_class() -> type[PayloadConverter]:
    """The payload converter a seller's client needs: ``OpenAIPayloadConverter`` when the
    extra is installed, else temporalio's pydantic one.

    The plugin goes on the worker (``plugins=``), which leaves the client -- and so its data
    converter -- as the worker made it. The model activity's input (``ActivityModelInput``)
    does not pass temporalio's strict pydantic decoding: OpenAI's request types drift from
    what the SDK sends, and ``OpenAIPayloadConverter`` (pydantic, ``exclude_unset``, a
    lenient fallback when validation fails) is the converter the integration is built on."""
    if not AVAILABLE:
        return PydanticPayloadConverter
    from temporalio.openai_agents import OpenAIPayloadConverter

    return OpenAIPayloadConverter
