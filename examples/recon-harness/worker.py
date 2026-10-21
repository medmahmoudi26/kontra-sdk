"""Binds the code to the offering; all worker tuning lives here (D11), and this is the one file
that imports `kontra` (decisions I29). The entry is a bound handler, `ReconAgent.sweep`: the
agent's `run` returns nothing, so Kontra runs one turn of it per hire through its own wrapper
workflow, `kontra.turn.v1` (one hire is one turn). `serve()` binds offering.yaml and quote.py
beside this file to it and registers every `@activity.defn` function of this project (the
model, `think`); every keyword is a `temporalio.worker.Worker` argument, passed through
verbatim -- `workflows=` for the subagent, `plugins=` for the harness.

`AgentHarnessPlugin` registers the tools' activities and the subagent activity, and puts the
harness's pydantic payload converter on the worker's Temporal client, ahead of Kontra's plugin,
which comes last: Kontra's codec is a payload codec, so it encrypts every payload under the
harness's converter. The harness is this project's dependency, pinned exactly
(`temporal-agent-harness==0.6.0`), not kontra-sdk's.

Which model answers is this worker's environment: no `LLM_BASE_URL`, the scripted model;
with one, that endpoint, for `LLM_MODEL` (activities.py, `think`)."""

from datetime import timedelta

from activities import TOOLS
from temporal_agent_harness.plugin import AgentHarnessPlugin
from workflows import CertChecker, ReconAgent

from kontra import serve

serve(
    ReconAgent.sweep,
    workflows=[CertChecker],
    plugins=[AgentHarnessPlugin(tools=TOOLS)],
    max_concurrent_activities=10,
    graceful_shutdown_timeout=timedelta(seconds=30),
)
