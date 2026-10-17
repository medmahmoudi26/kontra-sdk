# chef-agents: an agent as workflow code

A seller offering, `acme/chef-agents@0.1.0`: give it `{"craving": "something quick with eggs"}`,
get back `{"recipe": "..."}`, one dish from what is in the pantry, for a flat 0.05 USD. The
cook is an agent from OpenAI's [Agents SDK](https://openai.github.io/openai-agents-python/),
run durably through Temporal's integration: the agent loop is workflow code, and every model
call and every tool call is an activity, retried and recorded like any other.

| File | Owner of |
|---|---|
| `offering.yaml` | name, version, limits, display text, and `uses_hosted_model: true` |
| `models.py` | `CookInput` (`craving`, 1 to 500 characters) and `Dish` (`recipe`); the published contract is derived from them |
| `workflows.py` | `ChefFlow`: an `Agent` whose tool is the `lookup_pantry` activity (`workflow.activity_as_tool`), run with `Runner.run` |
| `activities.py` | `lookup_pantry`, the tool: what is on a shelf |
| `quote.py` | the price: 0.05 USD a dish, and a refusal for a blank craving |
| `worker.py` | `serve(ChefFlow, ...)`, and the model endpoint the agent's model calls dial |
| `input.json` | a sample input, for `kontra quote` and `kontra hire` |
| `contract.yaml` | the derived contract document, kept as a golden file (seam ③) |

## Nothing to switch on

`workflows.py` imports `agents`; that is the whole opt-in. `serve()` sees the import and gives
the worker what such a workflow needs: Temporal's `OpenAIAgentsPlugin` (the model activity, the
tool activities, the sandbox settings) and the plugin's payload converter. A workflow that does
not import `agents` gets neither, however the worker is installed. There is no keyword for it:
the workflow does what it wants, and a worker keyword never changes what workflow code may do
(`docs/decisions.md` I25). You need the extra that carries Temporal's integration:

```sh
pip install 'kontra-sdk[agents]'
```

## The model is the worker's

Which server answers the agent, and which model, is worker config, never workflow code: the
agent in `workflows.py` names no model. `worker.py` sets the Agents SDK's own default client
(`set_default_openai_client`) and API (`set_default_openai_api("chat_completions")`) before
`serve(ChefFlow)`, and the plugin's model activity, on this worker, dials it. Any OpenAI-compatible server works:

| Server | `LLM_BASE_URL` | `LLM_API_KEY` | `OPENAI_DEFAULT_MODEL` |
|---|---|---|---|
| Ollama (the default) | `http://localhost:11434/v1` | anything | `qwen2.5` |
| vLLM | `http://localhost:8000/v1` | anything | the model it serves |
| OpenRouter | `https://openrouter.ai/api/v1` | your key | `mistralai/mistral-small-3.2-24b-instruct` |
| Mistral | `https://api.mistral.ai/v1` | your key | `mistral-small-latest` |

`OPENAI_DEFAULT_MODEL` is the Agents SDK's variable for the model an agent that names none
runs; set it in the worker's environment. Tracing is off in `worker.py`: the SDK would send its
traces to api.openai.com. With Ollama on the same machine, the buyer's craving never leaves it.

`uses_hosted_model: true` tells the buyer that their input, decrypted on your worker, goes to
a model provider: whichever one you point the worker at. Model calls are activity payloads,
encrypted in Temporal like the rest of the job.

## Run it

`kontra dev` runs a local Kontra in Docker with play money and logs you in to it, as `acme`;
this directory then runs unchanged, and `kontra serve` publishes the offering itself.

```sh
ollama pull qwen2.5
kontra dev up --handle acme      # KONTRA_HOME=~/.kontra-dev keeps it apart from a real login
OPENAI_DEFAULT_MODEL=qwen2.5 kontra serve
# then, in another terminal:
kontra quote acme/chef-agents -f input.json            # 0.05 USD
kontra hire acme/chef-agents -f input.json --max-price 0.05 --wait --accept
```

To sell it for real, sign in (`kontra login`), replace `acme` in `offering.yaml` with your
handle, then `kontra publish` and `kontra serve`, as in `../hello/README.md`.

## Tested

`tests/examples/test_chef_agents.py` runs this directory's `worker.py` as `acme` against
`kontra.testing`'s marketplace and a Temporal dev server, with `LLM_BASE_URL` pointing at an
OpenAI-compatible stand-in on 127.0.0.1: a hire is answered by the stand-in, which is asked
twice (the tool call, then the dish, after the `lookup_pantry` activity ran), for the model
the worker named, and nothing in the worker tries to reach any other host. It needs no
`OPENAI_API_KEY`. The answer of a real model is not tested: point `LLM_BASE_URL` at one.

`contract.yaml` is the derived contract, a golden file (see `../enrich/README.md`).
