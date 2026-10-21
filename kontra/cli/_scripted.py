"""The model ``kontra graph`` gives an agent workflow's mock runs (decisions I27): no network,
no key, no tokens spent.

An agent workflow (one whose worker has Temporal's ``OpenAIAgentsPlugin`` in its ``plugins=``,
the seller's own dependency) runs its loop as workflow code and every model call as the
agents plugin's model activity, whose model comes from the plugin's ``model_provider``. The mock runs give the plugin this one. Its first answer
calls every function tool the agent was given, once each, in one response; once their
outputs are in its input, it answers with a final output drawn from the agent's output type
(:func:`kontra._sample.from_schema`), or a short text for a text agent. That is the shape a
real model's turns take -- ``examples/chef-agents`` runs model, ``lookup_pantry``, model --
and every tool the agent has is scheduled in some run. Handoffs are not taken.

Imported only for a worker whose plugins include the agents plugin: it imports ``agents``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable, Mapping
from datetime import timedelta
from typing import Any, cast

from agents import (
    AgentOutputSchemaBase,
    FunctionTool,
    Handoff,
    Model,
    ModelProvider,
    ModelResponse,
    ModelSettings,
    ModelTracing,
    Tool,
    TResponseInputItem,
    Usage,
)
from agents.items import TResponseOutputItem, TResponseStreamEvent
from openai.types.responses import (
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
)
from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

from kontra._sample import Unsampleable, from_schema

__all__ = ["TEXT", "scripted_plugin"]

#: A text agent's final output.
TEXT = "a mock answer (kontra graph)"
#: The model activity's start-to-close timeout: the plugin's own default for its own model.
_MODEL_TIMEOUT = timedelta(seconds=60)


def _answered(given: str | list[TResponseInputItem]) -> bool:
    """Whether the model's input holds a tool's output: it called its tools already."""
    if isinstance(given, str):
        return False
    items = cast("list[object]", given)
    return any(
        isinstance(item, Mapping)
        and cast("Mapping[str, object]", item).get("type") == "function_call_output"
        for item in items
    )


def _sampled(schema: Mapping[str, Any], run: int, seed: int) -> Any:
    try:
        return from_schema(schema, run, seed)
    except Unsampleable:
        return {}


class _ScriptedModel(Model):
    def __init__(self, run: Callable[[], int], seed: int) -> None:
        self._run, self._seed = run, seed

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> ModelResponse:
        run = self._run()
        functions = [t for t in tools if isinstance(t, FunctionTool)]
        output: list[TResponseOutputItem]
        if functions and not _answered(input):
            output = [
                ResponseFunctionToolCall(
                    arguments=json.dumps(_sampled(t.params_json_schema, run, self._seed)),
                    call_id=f"call_{k}",
                    name=t.name,
                    type="function_call",
                    id=f"fc_{k}",
                    status="completed",
                )
                for k, t in enumerate(functions)
            ]
        else:
            text = TEXT
            if output_schema is not None and not output_schema.is_plain_text():
                text = json.dumps(_sampled(output_schema.json_schema(), run, self._seed))
            output = [
                ResponseOutputMessage(
                    id="msg_0",
                    content=[ResponseOutputText(text=text, annotations=[], type="output_text")],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ]
        return ModelResponse(output=output, usage=Usage(), response_id=None)

    def stream_response(
        self,
        system_instructions: str | None,
        input: str | list[TResponseInputItem],
        model_settings: ModelSettings,
        tools: list[Tool],
        output_schema: AgentOutputSchemaBase | None,
        handoffs: list[Handoff],
        tracing: ModelTracing,
        **kwargs: Any,
    ) -> AsyncIterator[TResponseStreamEvent]:
        raise NotImplementedError("the mock runs' model does not stream")


class _ScriptedProvider(ModelProvider):
    def __init__(self, model: Model) -> None:
        self._model = model

    def get_model(self, model_name: str | None) -> Model:
        return self._model


def scripted_plugin(run: Callable[[], int], seed: int) -> OpenAIAgentsPlugin:
    """Temporal's agents plugin with the scripted model; ``run()`` is the mock run in
    progress (its index picks the mode its outputs are drawn in)."""
    return OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(start_to_close_timeout=_MODEL_TIMEOUT),
        model_provider=_ScriptedProvider(_ScriptedModel(run, seed)),
    )
