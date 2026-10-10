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
| `@actor.load` | module-level state in `activities.py` |
| `global_state` counters | gone: accounting, not output; a workflow can count for itself |
| Session checkpoints, resume | Temporal's history and retries |
| `actor.json` targets (container / machine sizes) | the seller's own hardware; tuning is `Worker(...)` kwargs in `worker.py` |
| fleets | run more `kontra serve` processes; `max_task_queue_activities_per_second` caps them together |
| datasets, reports | not core in v2; a buyer gets the output, an Artifact for files |
| pricing: none | `quote.py`: a fixed price per job, from the input |

Run it like any offering:

```sh
kontra serve        # from this directory, with a profile from `kontra login` or `kontra dev`
```
