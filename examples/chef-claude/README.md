# chef-claude: an agent that is a process

A seller offering, `acme/chef-claude@0.1.0`: give it `{"craving": "something quick with eggs"}`,
get back `{"recipe": "..."}`, one dish from what is in the pantry, for a flat 0.25 USD. The
cook is Claude Code, the `claude` binary, run in print mode. No agent SDK: the agent's loop is
the binary's, and one activity drives it. This is the shape for any agent that is a process
rather than workflow code.

| File | Owner of |
|---|---|
| `offering.yaml` | name, version, limits, display text, and `uses_hosted_model: true` |
| `models.py` | `CookInput` (`craving`, 1 to 500 characters) and `Dish` (`recipe`); the published contract is derived from them |
| `workflows.py` | `ChefFlow`: one `execute_activity`, with a `heartbeat_timeout` and a `RetryPolicy` |
| `activities.py` | `cook`: spawns `claude -p ... --output-format stream-json`, heartbeats per line, returns on its `result` |
| `quote.py` | the price: 0.25 USD a dish, and a refusal for a blank craving |
| `worker.py` | `serve(ChefFlow, ...)`, and how many `claude` processes run at once |
| `input.json` | a sample input, for `kontra quote` and `kontra hire` |
| `contract.yaml` | the derived contract document, kept as a golden file (seam ③) |

## How it survives a crash

`cook` starts `claude` with `asyncio.create_subprocess_exec` and reads its `stream-json` events
one line at a time. Each line is a heartbeat (the buyer sees it as progress), and the
`result` event is the activity's return value. The workflow runs `cook` with a 20-second
`heartbeat_timeout`: kill the worker and Temporal notices within it, then retries `cook` on
whichever worker polls next. While `claude` is quiet (a long model turn) `cook` heartbeats
every 5 seconds anyway; quiet for 5 minutes, it is stuck, and the attempt fails and is
retried.

The binary keeps its own session on disk. Attempt 1 names it: `--session-id` is
`uuid5(NAMESPACE_URL, <workflow id>)`, the same for every attempt of a job and different for
every job. A retry passes `--resume` with that id instead, so `claude` carries on from its last
turn rather than starting over. Whether this machine holds the session is `claude`'s to say:
it refuses `--resume` of a session it never saw and `--session-id` of one it has, and `cook`
then runs the other once. So a retry that lands on another machine of a fleet, or on one whose
home did not survive a restart, starts the job's session there; an attempt 1 that finds the
session already there (the workflow was reset: same workflow id) resumes it.

On Linux `claude` dies with its worker, even a `kill -9`ed one (`PR_SET_PDEATHSIG`): it would
otherwise go on spending tokens, and writing the session that the retry is resuming.

## The buyer writes part of the prompt

The craving goes into `claude`'s prompt, so the binary gets nothing that touches your
machine: `--tools WebSearch` leaves it one tool (no Bash, no Read, no Write), `--allowedTools`
lets it use that one without asking, `--strict-mcp-config` keeps your own MCP servers out,
`--safe-mode` leaves out your own Claude Code setup (`CLAUDE.md` files, hooks, plugins,
skills: a hook would see the buyer's words), and `--max-turns` bounds it. Widen any of these
only as far as you would let a stranger's words reach.

The prompt goes to `claude` on stdin, not on its command line, which every local user can read
(`/proc/<pid>/cmdline`). It runs in `~/.kontra-chef-claude`, made `0700`, and `cook` refuses
that directory if it is not yours or others may write it: `claude` runs the hooks of a
`.claude/settings.json` it finds where it runs, even in print mode.

`claude` keeps each job's session, the buyer's craving in it, under `~/.claude/projects/`
(the directory named after `~/.kontra-chef-claude`): prune it as you would any buyer data.
Run the worker as a user that holds nothing else of yours, and give `claude` its credentials
there: logged in, or `ANTHROPIC_API_KEY` in the worker's environment.

`uses_hosted_model: true` tells the buyer that their input, decrypted on your worker, goes to
Anthropic.

## Run it

`claude` must be on the worker's PATH (else `/usr/local/bin/claude` is used). With `kontra dev`
(a local Kontra in Docker, play money, logged in as `acme`), this directory runs unchanged:

```sh
kontra dev up --handle acme      # KONTRA_HOME=~/.kontra-dev keeps it apart from a real login
kontra serve
# then, in another terminal:
kontra hire acme/chef-claude -f input.json --max-price 0.25 --wait --accept
```

Stop `kontra serve` with `kill -9` while the job runs and start it again: the job completes,
and the second `claude` is a `--resume` of the first one's session.

## Tested

`tests/examples/test_chef_claude.py` runs this directory's `worker.py` as `acme` against
`kontra.testing`'s marketplace and a Temporal dev server, with a stand-in `claude` first on
PATH that records its arguments and stdin: a hire starts `--session-id <uuid5 of the job's
workflow id>`; the worker is killed (SIGKILL) while it runs, its `claude` goes with it, a new
worker is started, and the retry runs `--resume` with the same id and delivers. It also runs
`cook` alone, attempt by attempt: a retry on a machine without the session starts it, an
attempt 1 that finds it resumes it, the craving reaches only stdin, a kitchen others may
write is refused, and a silent `claude` is stopped. A real `claude` is not run there.

`contract.yaml` is the derived contract, a golden file (see `../enrich/README.md`).
