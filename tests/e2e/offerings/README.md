# Offerings for tests/e2e

Small seller projects, laid out like `kontra init` makes them (§6.1), that the end-to-end tests
serve with `kontra serve`. Each test copies one into a temporary directory and writes its
`offering.yaml` there (party, limits), so the files here carry no `offering.yaml` and are never
served in place. As in every seller project, `workflows.py` and `activities.py` are vanilla
Temporal and import nothing from `kontra` (decisions I29); `models.py` may use
`kontra.types.Artifact`, `quote.py` takes `kontra.serve.Quote`, and `worker.py` is the one file
that imports the rest.

| Project | For | What it does |
|---|---|---|
| `vault` | acceptance 8 | an `Artifact` in, the same bytes out as a new `Artifact`, and an inline note large enough to be claim-checked |
| `pulse` | acceptance 13 | heartbeats `{percent, message}` N times; or runs an activity that never heartbeats |
| `bulk` | acceptances 14, 15 | writes a file of N pseudo-random bytes and returns it as an `Artifact` (or the gate's refusal) |
