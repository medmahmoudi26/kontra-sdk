"""The buyer side (PRD §7): three doors, one semantics.

```python
from kontra.hire import Kontra

k = Kontra()                                         # reads ~/.kontra/profile
q = await k.quote("acme/enrich", urls=urls)          # validated, encrypted, relayed
job = await q.hire(max_price=20)                     # locks the hold, starts execute
result = await job.result()                          # decrypted Record
await job.accept()                                   # optional; auto after the review window
```

Inside a buyer's own workflow (run it on :func:`worker`)::

    records = await service("acme/enrich", max_price=20).execute(urls=urls)

The third door is ``kontra mcp`` (decisions I5), built on :class:`Kontra`.

The sandbox-safe parts (``service``, ``Record``, the errors) are imported here eagerly; the
client and the worker (HTTP, Temporal client, codec) are imported on first use, so a
workflow module importing ``kontra.hire`` stays light.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from kontra.hire._errors import (
    HireRefused,
    InsufficientFunds,
    InvalidDelivery,
    InvalidInput,
    JobFailed,
    JobKeyMissing,
    KontraError,
    PriceAboveMax,
    QuoteRejected,
    SellerBusy,
)
from kontra.hire._record import Record
from kontra.hire._service import ServiceClient, service

if TYPE_CHECKING:
    from kontra.hire._client import Job, Kontra, Progress, QuoteResult, TemporalUnavailable
    from kontra.hire._worker import HireActivities, HireWorker, worker

__all__ = [
    "HireActivities",
    "HireRefused",
    "HireWorker",
    "InsufficientFunds",
    "InvalidDelivery",
    "InvalidInput",
    "Job",
    "JobFailed",
    "JobKeyMissing",
    "Kontra",
    "KontraError",
    "PriceAboveMax",
    "Progress",
    "QuoteRejected",
    "QuoteResult",
    "Record",
    "SellerBusy",
    "ServiceClient",
    "TemporalUnavailable",
    "service",
    "worker",
]

_LAZY = {
    "Job": "kontra.hire._client",
    "Kontra": "kontra.hire._client",
    "Progress": "kontra.hire._client",
    "QuoteResult": "kontra.hire._client",
    "TemporalUnavailable": "kontra.hire._client",
    "HireActivities": "kontra.hire._worker",
    "HireWorker": "kontra.hire._worker",
    "worker": "kontra.hire._worker",
}


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module), name)
