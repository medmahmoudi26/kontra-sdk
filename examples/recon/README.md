# recon: composing two offerings inside one buyer workflow

`recon.py` hires `acme/probe` (`../legacy-actor`) and `acme/enrich` (`../enrich`) at the same
time from a workflow in the buyer's own namespace, for `https://<domain>/` and
`https://www.<domain>/`. If the probe does not deliver (it fails, its quote is refused, or the
recon is cancelled), the enrichment is cancelled while it still runs: its hold refunds at once
and the seller's workflow is cancelled through Nexus (acceptance criterion 11). An enrichment
that already delivered is a delivery like any other, for the buyer to accept or reject.

```sh
python main.py example.com      # with a profile from `kontra login` or `kontra dev`
```

`main.py` runs a worker for the `Recon` workflow with `kontra.hire.worker` (a plain Temporal
worker whose client carries your keys, plus the hire activities) and prints the result. The
enrichment's records come back as an artifact reference (`enrich.records`); download it with
your key, e.g. `kontra.artifacts.download`.

Composition gives you only the final results (no progress stream); use `kontra.hire.Kontra` in a
script when you want `job.progress()`.
