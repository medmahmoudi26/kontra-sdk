"""Kontra: a marketplace of durable, quoted jobs.

Sellers write plain Temporal workflows and bind them in ``worker.py`` with
:mod:`kontra.serve`. Buyers hire them with :mod:`kontra.hire` from a script, from inside
their own workflow, or from an agent through ``kontra mcp``.
"""

__version__ = "0.1.0"
