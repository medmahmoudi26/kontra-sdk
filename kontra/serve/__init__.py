"""The seller side (PRD §6): bind plain Temporal code to an offering and serve it.

```python
from kontra.serve import Worker, offering, Quote

worker = Worker(
    offerings=[offering("offering.yaml", workflow=EnrichFlow, quote=quote)],
    activities=[fetch_batch],
    max_concurrent_activities=40,          # any temporalio.worker.Worker keyword
)
worker.run()
```

Workflows and activities import nothing from here (D1); only ``worker.py`` and
``quote.py`` do.
"""

from kontra.contract.quote import Quote
from kontra.serve.handler import OfferingService
from kontra.serve.hold import HOLD_HEADER, HoldRefused, HoldVerifier
from kontra.serve.interceptors import JOB_HEADER, KontraInterceptor
from kontra.serve.jobs import JobDirectory, JobEntry, SellerKeyCache
from kontra.serve.offering import BindingError, BoundOffering, offering
from kontra.serve.progress import ProgressForwarder
from kontra.serve.worker import ServeError, Worker, seller_data_converter

__all__ = [
    "HOLD_HEADER",
    "JOB_HEADER",
    "BindingError",
    "BoundOffering",
    "HoldRefused",
    "HoldVerifier",
    "JobDirectory",
    "JobEntry",
    "KontraInterceptor",
    "OfferingService",
    "ProgressForwarder",
    "Quote",
    "SellerKeyCache",
    "ServeError",
    "Worker",
    "offering",
    "seller_data_converter",
]
