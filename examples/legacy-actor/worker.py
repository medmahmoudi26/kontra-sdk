"""Binds the code to the offering, and the one file that imports `kontra` (decisions I29):
`serve()` binds offering.yaml and quote.py beside this file to `ProbeFlow`, and registers
every `@activity.defn` function of this project (here `head_batch`)."""

from workflows import ProbeFlow

from kontra import serve

serve(ProbeFlow, max_concurrent_activities=8)
