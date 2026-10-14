"""Put the spec files and the ``kontra dev`` stack inside the package of a wheel.

In a checkout they live outside it (``spec/``, ``compose.yaml``: one copy each); the sdist
carries them already inside (``kontra/_spec``, ``kontra/_dev/compose.yaml``), so that it
holds the package, the README and the licence and nothing else. A wheel built from either
tree gets the same files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

#: checkout path -> path inside the package
OUTSIDE = {"spec": "kontra/_spec", "compose.yaml": "kontra/_dev/compose.yaml"}


class CheckoutFiles(BuildHookInterface):  # type: ignore[type-arg]
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        root = Path(self.root)
        for source, target in OUTSIDE.items():
            if (root / source).exists():
                build_data["force_include"][str(root / source)] = target
            elif not (root / target).exists():
                raise FileNotFoundError(f"neither {source} nor {target} is in {root}")
