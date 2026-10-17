"""Binds the code to the offering; all worker tuning lives here (D11). `serve()` binds
offering.yaml and quote.py beside this file to `ChefFlow` and registers every activity the
workflow imports; the workflow imports `agents`, so the worker runs Temporal's OpenAI Agents
plugin too (decisions I25). Every keyword is a `temporalio.worker.Worker` argument, passed
through verbatim.

The model endpoint is this worker's, set the Agents SDK's own way -- its default client --
so any OpenAI-compatible server answers the agent: Ollama, vLLM, OpenRouter, Mistral. The
model's name is the SDK's `OPENAI_DEFAULT_MODEL`, from this worker's environment too."""

import os
from datetime import timedelta

from agents import set_default_openai_api, set_default_openai_client, set_tracing_disabled
from openai import AsyncOpenAI
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

serve(ChefFlow, max_concurrent_activities=10, graceful_shutdown_timeout=timedelta(seconds=30))
