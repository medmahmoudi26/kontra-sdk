"""The seller side (PRD §6): bind plain Temporal code to an offering and serve it.

```python
from kontra import serve

serve(EnrichFlow, max_concurrent_activities=40)   # the whole worker.py (kontra.serve._serve)
```

or, with the lists written out:

```python
from kontra.serve import Worker, offering, Quote

worker = Worker(
    offerings=[offering("offering.yaml", workflow=EnrichFlow, quote=quote)],
    activities=[fetch_batch],
    max_concurrent_activities=40,          # any temporalio.worker.Worker keyword
)
worker.run()
```

Workflows and activities import nothing from here: they reach Temporal through the façade
(``from kontra import workflow, activity``; D1, decisions I20). Only ``worker.py`` and
``quote.py`` import ``kontra.serve``.

This package is callable: ``kontra.serve(...)`` is :func:`serve`. Python binds the attribute
``kontra.serve`` to this package when it is imported -- ``from kontra import serve`` imports
it, so that name is the package, while ``from kontra.serve import serve`` is the function --
and both serve the same.
"""

import sys
import types
from typing import Unpack

from temporalio.worker import WorkerConfig

from kontra import _agents
from kontra.contract.quote import Quote
from kontra.serve._serve import serve, serve_from
from kontra.serve.handler import OfferingService
from kontra.serve.hold import HOLD_HEADER, HoldRefused, HoldVerifier
from kontra.serve.interceptors import JOB_HEADER, KontraInterceptor
from kontra.serve.jobs import JobDirectory, JobEntry, SellerKeyCache
from kontra.serve.offering import BindingError, BoundOffering, QuoteFunction, offering
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
    "serve",
]


class _CallablePackage(types.ModuleType):
    """``kontra.serve`` itself, callable as :func:`serve`."""

    def __call__(
        self,
        *workflows: type,
        quote: QuoteFunction | None = None,
        mcp_servers: _agents.MCPServers | None = None,
        **temporal_worker_options: Unpack[WorkerConfig],
    ) -> Worker:
        return serve_from(
            sys._getframe(1).f_globals,
            workflows,
            quote=quote,
            mcp_servers=mcp_servers,
            options=temporal_worker_options,
        )


sys.modules[__name__].__class__ = _CallablePackage
