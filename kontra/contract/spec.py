"""The versioned spec files under ``spec/`` (the x-kontra schema and the reserved
``Kontra*`` definitions), found wherever the package is installed.

They live at the repository root because they are the public, language-neutral contract
(a Go seller reads them too). An editable install finds them next to the package; a wheel
carries them as ``kontra/_spec`` (``pyproject.toml`` force-includes them). Both copies are
the same bytes, so the hash a wheel computes equals the hash a checkout computes.
"""

from __future__ import annotations

import copy
import json
from functools import cache
from pathlib import Path
from typing import Any

__all__ = [
    "KONTRA_TYPES_FILE",
    "RESERVED_PREFIX",
    "X_KONTRA_FILE",
    "X_KONTRA_V1_FILE",
    "find_spec_dir",
    "kontra_types",
    "reserved_def",
    "reserved_names",
    "spec_dir",
    "x_kontra_schema",
]

X_KONTRA_FILE = "x-kontra.v2.json"
#: The spec every document published before 0.2.0 was sealed under (it seals the seller's
#: ``public_key``). Read-only: such a document is read back for the jobs hired under it,
#: never published, quoted or hired (``validate_document(..., read_only=True)``).
X_KONTRA_V1_FILE = "x-kontra.v1.json"
KONTRA_TYPES_FILE = "kontra-types.v1.json"
#: Definitions whose name starts with this belong to the spec, never to a seller.
RESERVED_PREFIX = "Kontra"

_PACKAGE_DIR = Path(__file__).resolve().parent.parent  # .../kontra


def find_spec_dir(package_dir: Path = _PACKAGE_DIR) -> Path:
    """The first of ``<package>/_spec`` (wheel) and ``<package>/../spec`` (checkout)
    that holds both spec files."""
    candidates = [package_dir / "_spec", package_dir.parent / "spec"]
    for cand in candidates:
        if (cand / X_KONTRA_FILE).is_file() and (cand / KONTRA_TYPES_FILE).is_file():
            return cand
    tried = ", ".join(str(c) for c in candidates)
    raise FileNotFoundError(
        f"kontra spec files not found (tried {tried}); the installation is incomplete"
    )


@cache
def spec_dir() -> Path:
    return find_spec_dir()


@cache
def _load(name: str) -> dict[str, Any]:
    with (spec_dir() / name).open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


def x_kontra_schema(version: int = 2) -> dict[str, Any]:
    """``spec/x-kontra.v2.json``, or ``v1`` to read an old document back (a fresh copy;
    callers may mutate it)."""
    if version not in (1, 2):
        raise ValueError(f"no x-kontra spec v{version}")
    return copy.deepcopy(_load(X_KONTRA_FILE if version == 2 else X_KONTRA_V1_FILE))


def kontra_types() -> dict[str, Any]:
    """``spec/kontra-types.v1.json`` (a fresh copy)."""
    return copy.deepcopy(_load(KONTRA_TYPES_FILE))


def reserved_names() -> frozenset[str]:
    """``{"KontraArtifact", "KontraQuote"}``."""
    return frozenset(_load(KONTRA_TYPES_FILE)["$defs"])


def reserved_def(name: str) -> dict[str, Any]:
    """The canonical body of one reserved definition (a fresh copy)."""
    defs: dict[str, Any] = _load(KONTRA_TYPES_FILE)["$defs"]
    if name not in defs:
        raise KeyError(f"{name!r} is not a reserved Kontra definition")
    body: dict[str, Any] = copy.deepcopy(defs[name])
    return body
