"""The public dev gate and its ``kontra dev up`` host process (decisions I34).

Two gate packages, one per environment (CLAUDE.md, seam ②): the **prod** gate lives in the
private ``kontra-cloud`` repository and is never imported here; the **dev** gate is
:class:`~kontra.devgate.gate.DevGate` in this one, the same class :mod:`kontra.testing` serves
under its old name ``FakeGate`` (OD2). :class:`~kontra.devgate.stack.DevStack` runs that gate
under uvicorn on the running event loop with a pinned, checksum-verified Temporal CLI dev server
as its child, loopback only; ``python -m kontra.devgate`` is the host process ``kontra dev up``
spawns.

Imports are lazy (as in :mod:`kontra.testing`), so a path that only touches state (``down``,
``status``, ``logs``) never imports FastAPI or uvicorn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from kontra.devgate.gate import DevGate, platform_defaults
    from kontra.devgate.stack import DevStack, StackConfig
    from kontra.devgate.state import Ports

__all__ = [
    "DevGate",
    "DevStack",
    "Ports",
    "StackConfig",
    "platform_defaults",
]

_LAZY = {
    "DevGate": "kontra.devgate.gate",
    "platform_defaults": "kontra.devgate.gate",
    "DevStack": "kontra.devgate.stack",
    "StackConfig": "kontra.devgate.stack",
    "Ports": "kontra.devgate.state",
}


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(module), name)
