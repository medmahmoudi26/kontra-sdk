"""The seller side (PRD §6): bind vanilla Temporal code to an offering and serve it.

```python
from kontra import serve

serve(EnrichFlow, max_concurrent_activities=40)   # the whole worker.py (kontra.serve._serve)
serve(ReconAgent.sweep, plugins=[AgentHarnessPlugin()])   # or one handler of an agent
```

or, with the lists written out:

```python
from kontra.serve import Worker, offering, Quote

worker = Worker(
    offerings=[offering("offering.yaml", entry=EnrichFlow, quote=quote)],
    activities=[fetch_batch],
    max_concurrent_activities=40,          # any temporalio.worker.Worker keyword
)
worker.run()
```

Workflows and activities import nothing from here, nor from ``kontra`` at all: they are
vanilla ``temporalio`` (decisions I29), and run on any Temporal worker (all but
:class:`~kontra.types.Artifact` I/O, which needs the runtime the plugin installs).
``worker.py`` is the one file that imports ``kontra`` (``quote.py`` takes :class:`Quote` from
here, and ``models.py`` may use :mod:`kontra.types`). A seller with a worker main of their own puts
:class:`~kontra.plugin.KontraPlugin` on their Temporal client instead.

This package is callable: ``kontra.serve(...)`` is :func:`serve`. Python binds the attribute
``kontra.serve`` to this package when it is imported -- ``from kontra import serve`` imports
it, so that name is the package, while ``from kontra.serve import serve`` is the function --
and both serve the same.
"""

import sys
import types
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Unpack

from temporalio.worker import WorkerConfig

from kontra.contract.quote import Quote
from kontra.serve._serve import serve, serve_from
from kontra.serve.errors import ServeError
from kontra.serve.handler import OfferingService
from kontra.serve.hold import HOLD_HEADER, HoldRefused, HoldVerifier
from kontra.serve.interceptors import JOB_HEADER, KontraInterceptor
from kontra.serve.jobs import JobDirectory, JobEntry, SellerKeyCache
from kontra.serve.offering import BindingError, BoundOffering, QuoteFunction, offering
from kontra.serve.progress import ProgressForwarder
from kontra.serve.worker import Worker, seller_data_converter

if TYPE_CHECKING:
    from kontra.plugin import KontraPlugin

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
    "KontraPlugin",
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


def __getattr__(name: str) -> Any:
    if name == "KontraPlugin":  # lazily: kontra.plugin imports this package
        from kontra.plugin import KontraPlugin

        return KontraPlugin
    raise AttributeError(f"module 'kontra.serve' has no attribute {name!r}")


class _CallablePackage(types.ModuleType):
    """``kontra.serve`` itself, callable as :func:`serve`."""

    def __call__(
        self,
        entry: type | Callable[..., Any],
        /,
        *,
        quote: QuoteFunction | None = None,
        engine: str | None = None,
        **worker_kwargs: Unpack[WorkerConfig],
    ) -> Worker:
        return serve_from(
            sys._getframe(1).f_globals, entry, quote=quote, options=worker_kwargs, engine=engine
        )


sys.modules[__name__].__class__ = _CallablePackage
