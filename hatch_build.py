"""Put the spec files and ``kontra dev up --observability``'s stack inside a wheel's package.

In a checkout they live outside it (``spec/`` and the ``observability/`` and
``docs/dashboards/`` files the observability compose file mounts: one copy each); the sdist
carries them already inside (``kontra/_spec``, ``kontra/_dev/``), so that it holds the package,
the README and the licence and nothing else. A wheel built from either tree gets the same files.
The stack's files keep their places relative to ``observability/compose.yaml``, which names them
by relative path. The e2e ``compose.yaml`` (the prod gate image) is repository tooling and is not
shipped (decisions I34).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

#: checkout path -> path inside the package
OUTSIDE = {
    "spec": "kontra/_spec",
    "observability": "kontra/_dev/observability",
    "docs/dashboards": "kontra/_dev/docs/dashboards",
}


class CheckoutFiles(BuildHookInterface):  # type: ignore[type-arg]
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        root = Path(self.root)
        for source, target in OUTSIDE.items():
            if (root / source).exists():
                build_data["force_include"][str(root / source)] = target
            elif not (root / target).exists():
                raise FileNotFoundError(f"neither {source} nor {target} is in {root}")
