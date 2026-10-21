"""The graph an example's README documents: its one ``mermaid`` block, which the README-golden
tests (``test_cli.py``, and ``tests/examples/test_recon_harness.py`` for a run's graph) hold
to what is drawn."""

from __future__ import annotations

import re
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
#: A README's Mermaid block, fences excluded.
MERMAID = re.compile(r"^```mermaid\n(.*?)^```$", re.MULTILINE | re.DOTALL)


def readme_graph(example: str) -> str:
    """The graph ``examples/<example>/README.md`` documents."""
    [block] = MERMAID.findall((EXAMPLES / example / "README.md").read_text(encoding="utf-8"))
    return block
