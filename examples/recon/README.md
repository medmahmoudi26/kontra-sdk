# recon: composing two offerings inside one buyer workflow

`recon.py` hires `acme/probe` and `acme/enrich` at the same time from a workflow in the buyer's
own namespace. If the probe fails, the enrichment is cancelled and its hold refunds — the buyer
never pays for half a recon (acceptance criterion 11).

```sh
python main.py example.com
```

Composition gives you only the final results (no progress stream); use `kontra.hire.Kontra` in a
script when you want `job.progress()`.
