# enrich: the PRD's running example

A seller offering, `acme/enrich@1.2.0`: resolve a URL list to records (status, title, domain),
delivered as an encrypted JSONL `Artifact` plus a small inline summary.

| File | Owner of |
|---|---|
| `offering.yaml` | name, version, limits, display text — declarative, no code names |
| `models.py` | the In/Out types; the published contract is derived from them |
| `workflows.py`, `activities.py` | plain Temporal; nothing from kontra |
| `quote.py` | the price, from the input |
| `worker.py` | the binding of workflow + quote to the offering, and all worker tuning |
| `contract.yaml` | the derived contract document, kept as a golden file (seam ③) |

```sh
kontra serve                                   # sell it
kontra quote acme/enrich -f in.json            # price it
kontra hire acme/enrich -f in.json --max-price 1
```

Change `acme` to your own handle before publishing.
