"""``kontra init``: the §6.1 project layout, rendered from ``kontra/templates/offering``.

Templates end in ``.tmpl`` (so no tool mistakes them for package code) and use exactly
three placeholders: ``{{party}}``, ``{{name}}`` (validated handles) and ``{{Class}}`` (the
CamelCase of the name, the prefix of the model and workflow classes). Nothing else is
substituted, and a placeholder left over after rendering is a bug that stops the scaffold
before anything is written.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path, PurePosixPath

from kontra.contract.names import validate_handle

__all__ = ["TEMPLATE", "class_prefix", "render", "scaffold"]

TEMPLATE = "offering"
_SUFFIX = ".tmpl"


def class_prefix(name: str) -> str:
    """``word-count`` -> ``WordCount``. Seller types may not start with ``Kontra``
    (decisions I2: those names are reserved for the spec's definitions), so such a name
    gets an ``Offering`` prefix."""
    prefix = "".join(part[:1].upper() + part[1:] for part in name.split("-"))
    return f"Offering{prefix}" if prefix.startswith("Kontra") else prefix


def _walk(node: Traversable, at: PurePosixPath) -> Iterator[tuple[PurePosixPath, str]]:
    for child in sorted(node.iterdir(), key=lambda t: t.name):
        if child.is_dir():
            yield from _walk(child, at / child.name)
        elif child.name.endswith(_SUFFIX):
            yield at / child.name[: -len(_SUFFIX)], child.read_text(encoding="utf-8")


def render(party: str, name: str) -> dict[PurePosixPath, str]:
    """Every file of the scaffold, by relative path."""
    validate_handle(party, what="party")
    validate_handle(name, what="offering name", allow_reserved=True)
    values = {"{{party}}": party, "{{name}}": name, "{{Class}}": class_prefix(name)}
    root = resources.files("kontra") / "templates" / TEMPLATE
    out: dict[PurePosixPath, str] = {}
    for rel, text in _walk(root, PurePosixPath()):
        for key, value in values.items():
            text = text.replace(key, value)
        if "{{" in text:
            raise RuntimeError(f"template {rel} has an unknown placeholder")  # a bug here
        out[rel] = text
    if not out:
        raise RuntimeError("the offering templates are missing from the kontra package")
    return out


def scaffold(directory: Path, party: str, name: str) -> list[Path]:
    """Write the scaffold into ``directory``, which must not exist or be empty. Files are
    created exclusively, so nothing that appeared meanwhile is overwritten."""
    files = render(party, name)
    if directory.exists() and (not directory.is_dir() or any(directory.iterdir())):
        raise FileExistsError(f"{directory} already exists and is not an empty directory")
    written: list[Path] = []
    for rel, text in files.items():
        path = directory.joinpath(*rel.parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        written.append(path)
    return written
