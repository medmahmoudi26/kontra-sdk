# legacy-actor: an old kontra actor as an offering

`kontra-actors/python/probe` was an Actor with two Methods (`fetch`, `head`), run by Sessions over
Batches of units, scheduled on fleets. This directory is its `head` Method as a v2 offering, to
show what each old concept becomes. The other frozen actors can follow the same path when someone
needs them.

| v1 (actor) | v2 (offering) |
|---|---|
| `@actor.method(takes=Target, emits=Head)` | the workflow signature `ProbeInput -> ProbeOutput` (`models.py`); the contract is derived from it |
| a Batch of units | a list in the input |
| `dataset.push(record)` | a list in the output, or an `Artifact` when it is large |
| `@actor.load` | module-level state in `activities.py`, for what every job on the worker shares (this probe needs none: one HTTP client per batch) |
| a unit that fails | that target's `error` in its `Head`; a target never fails the job |
| `global_state` counters | gone: accounting, not output; a workflow can count for itself |
| Session checkpoints, resume | Temporal's history and retries |
| progress reports | `activity.heartbeat({"percent", "message"})`, for the whole job, forwarded to the buyer |
| `actor.json` targets (container / machine sizes) | the seller's own hardware; tuning is `Worker(...)` kwargs in `worker.py` |
| fleets | each batch probes 16 targets at once, and a worker runs `max_concurrent_activities` batches; run more `kontra serve` processes for more, and `max_task_queue_activities_per_second` caps them together |
| datasets, reports | not core in v2; a buyer gets the output, an Artifact for files |
| pricing: none | `quote.py`: a fixed price per job, from the input |

A target is a `url` or a `host` (probed as `http://<host>`), exactly one of the two. The JSON
Schema subset nexgen accepts cannot say "exactly one of", so `models.py` says it and the
worker's handler checks the model: a target with neither or both gets a rejected quote.

**The targets are the buyer's.** Each one has an overall deadline, redirects are followed with
HEAD (no body is ever read), and whatever a target does becomes the `error` of its `Head`. The
old body's blocking `urllib` could not keep those promises (see `activities.py`), so the port
uses httpx's async client instead. Targets are probed from your machine, though, and a
`Head` tells the buyer the status and `Server` header of whatever answered: run the worker
where they cannot reach anything private (cloud metadata, your LAN), or filter addresses
before probing.

Run it like any offering:

```sh
kontra serve        # from this directory, with a profile from `kontra login` or `kontra dev`
```

`contract.yaml` is the derived contract, a golden file sealed with the example key (see
`../enrich/README.md`).
