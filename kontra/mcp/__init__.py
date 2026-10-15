"""``kontra mcp``: hire from an agent (PRD §7.3).

The third door to the buyer side, next to a script and a workflow, with the same semantics:
eight MCP tools -- ``search``, ``describe``, ``quote``, ``hire``, ``status``, ``result``,
``accept``, ``reject`` -- and, for a seller, ``workers`` (its own versions' Workers view,
decisions I21), built on :class:`kontra.hire.Kontra` with the local profile. It
runs on the agent's machine, over stdio, because only that machine holds the key that
decrypts the results (decisions I5).

```python
from kontra.mcp import server
server().run("stdio")                  # what `kontra mcp` does (main())
```

Needs the ``mcp`` extra (``pip install 'kontra-sdk[mcp]'``).
"""

try:
    from kontra.mcp._server import INLINE_LIMIT, KontraTools, main, server
except ModuleNotFoundError as err:  # the optional extra is missing
    if err.name is not None and err.name.split(".")[0] in ("mcp", "mcp_types"):
        raise ModuleNotFoundError(
            "kontra mcp needs the MCP SDK: pip install 'kontra-sdk[mcp]'", name=err.name
        ) from err
    raise

__all__ = ["INLINE_LIMIT", "KontraTools", "main", "server"]
