"""Binds the code to the offering; all worker tuning lives here (D11), and this is the one file
that imports `kontra` (decisions I29). `serve()` binds offering.yaml and quote.py beside this
file to `ChefFlow`, and registers every `@activity.defn` function of this project. Every
keyword is a `temporalio.worker.Worker` argument, passed through verbatim -- `plugins=`
included: Temporal's `OpenAIAgentsPlugin`, which runs the agent's model and tool calls as
activities, goes on the worker's client ahead of Kontra's plugin, so its payload converter is
the one the client uses (and Kontra's codec encrypts under it). Its integration,
`temporalio-openai-agents`, is this project's dependency, not kontra-sdk's.

The model endpoint is this worker's, set the Agents SDK's own way -- its default client --
so any OpenAI-compatible server answers the agent: Ollama, vLLM, OpenRouter, Mistral. The
model's name is the SDK's `OPENAI_DEFAULT_MODEL`, from this worker's environment too.
`model_provider=OpenAIProvider()` is what makes the plugin's model activity use that client:
without it, the plugin builds its own `AsyncOpenAI()` from the environment when the worker is
built, and the default client set here is never asked."""

import os
from datetime import timedelta

from agents import (
    OpenAIProvider,
    set_default_openai_api,
    set_default_openai_client,
    set_tracing_disabled,
)
from openai import AsyncOpenAI
from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
from workflows import ChefFlow

from kontra import serve

set_default_openai_client(
    AsyncOpenAI(
        base_url=os.environ.get("LLM_BASE_URL", "http://localhost:11434/v1"),  # Ollama's
        api_key=os.environ.get("LLM_API_KEY", "unused"),  # Ollama and vLLM take any
        max_retries=0,  # Temporal retries the model activity
    ),
    use_for_tracing=False,
)
set_default_openai_api("chat_completions")  # what OpenAI-compatible servers speak
set_tracing_disabled(True)  # the SDK's traces would go to api.openai.com

serve(
    ChefFlow,
    max_concurrent_activities=10,
    graceful_shutdown_timeout=timedelta(seconds=30),
    # model_provider=OpenAIProvider(): the model activity then dials the default client set
    # above; the plugin's own default builds AsyncOpenAI() from the environment instead.
    plugins=[
        OpenAIAgentsPlugin(model_params=ModelActivityParameters(), model_provider=OpenAIProvider())
    ],
)
