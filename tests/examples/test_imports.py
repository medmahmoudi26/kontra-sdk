"""Acceptance 3 (D1): seller ``workflows.py`` and ``activities.py`` import nothing from
kontra -- not by an import statement, not dynamically, and not when they are loaded.
``models.py`` may use ``kontra.types`` (``Artifact``), and nothing else from kontra.

The acceptance text is ``grep -r "import kontra" examples/*/workflows.py
examples/*/activities.py`` returning nothing; that grep runs here verbatim, next to checks
it would miss (``from kontra.x import y``, ``importlib.import_module("kontra")``)."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from .support import EXAMPLES, SELLERS

SELLER_CODE = sorted(p for n in ("workflows.py", "activities.py") for p in EXAMPLES.glob(f"*/{n}"))
MODELS = sorted(EXAMPLES.glob("*/models.py"))
#: Calls that import (or run) code named by a string.
_DYNAMIC = frozenset(
    {"__import__", "import_module", "find_spec", "run_module", "exec", "eval", "compile"}
)
_IMPORT_LINE = re.compile(r"^\s*(?:from\s+kontra\b|import\s+(?:.*[\s,])?kontra\b)", re.MULTILINE)


def _rel(path: Path) -> str:
    return str(path.relative_to(EXAMPLES.parent))


def _is_kontra(module: str | None) -> bool:
    return module is not None and (module == "kontra" or module.startswith("kontra."))


def _docstrings(tree: ast.AST) -> set[int]:
    """ids of the string constants that are docstrings (prose, not code)."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                found.add(id(body[0].value))
    return found


def kontra_imports(source: str, *, allowed: frozenset[str] = frozenset()) -> list[str]:
    """Every way ``source`` reaches kontra, except modules in ``allowed``: import
    statements, dynamic imports and code run from strings that name kontra, and import-ish
    calls whose target is not a literal (which no check could clear)."""
    tree = ast.parse(source)
    prose = _docstrings(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [
                f"import {a.name}"
                for a in node.names
                if _is_kontra(a.name) and a.name not in allowed
            ]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and _is_kontra(node.module) and node.module not in allowed:
                found.append(f"from {node.module} import ...")
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name in _DYNAMIC:
                first = node.args[0] if node.args else None
                if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                    found.append(f"{name}(<not a literal>) at line {node.lineno}")
                elif "kontra" in first.value:
                    found.append(f"{name}({first.value!r}) at line {node.lineno}")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in prose
            and _is_kontra(node.value.strip())
        ):
            found.append(f"the module name {node.value!r} at line {node.lineno}")
    return found


# --------------------------------------------------------------------------- the checks


def test_every_seller_is_checked() -> None:
    assert {p.parent.name for p in SELLER_CODE} >= set(SELLERS)
    assert len(SELLER_CODE) == 2 * len({p.parent for p in SELLER_CODE})


def test_the_acceptance_grep_prints_nothing() -> None:
    """``grep -r "import kontra" examples/*/workflows.py examples/*/activities.py``."""
    result = subprocess.run(
        ["grep", "-r", "import kontra", *map(str, SELLER_CODE)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode, result.stdout) == (1, "")  # 1: no line matched


@pytest.mark.parametrize("path", SELLER_CODE, ids=_rel)
def test_seller_code_imports_nothing_from_kontra(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    assert _IMPORT_LINE.findall(source) == []
    assert kontra_imports(source) == []


@pytest.mark.parametrize("path", MODELS, ids=_rel)
def test_models_use_only_kontra_types(path: Path) -> None:
    assert (
        kontra_imports(path.read_text(encoding="utf-8"), allowed=frozenset({"kontra.types"})) == []
    )


@pytest.mark.parametrize("name", SELLERS)
def test_loading_seller_code_loads_no_kontra_but_the_artifact_type(name: str) -> None:
    """Importing ``workflows`` and ``activities`` (and what they import) loads no kontra
    module beyond ``kontra.types``, which models.py may use for ``Artifact``. This also
    catches an import a module makes while it loads, whatever it is spelled as."""
    probe = (
        "import json, sys\n"
        "import workflows, activities\n"
        "print(json.dumps(sorted(m for m in sys.modules if m == 'kontra' or m.startswith('kontra.'))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=EXAMPLES / name,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert set(json.loads(result.stdout)) <= {"kontra", "kontra.types"}


# --------------------------------------------------------------------------- the checker's teeth


@pytest.mark.parametrize(
    "source",
    [
        "import kontra",
        "import os, kontra.serve as s",
        "from kontra.serve import Quote",
        "from kontra import hire",
        "import importlib\nm = importlib.import_module('kontra.hire')",
        "m = __import__('kontra')",
        "exec('import kontra')",
        "import importlib\nname = 'kon' + 'tra'\nimportlib.import_module(name)",
        "MOD = 'kontra.serve'",
    ],
)
def test_the_checker_finds_kontra(source: str) -> None:
    assert kontra_imports(source) != []


@pytest.mark.parametrize(
    "source",
    [
        '"""Nothing from kontra is imported here; import kontra elsewhere."""',
        "import temporalio\nfrom models import Artifact",
        "def f():\n    '''kontra.serve does that for you'''\n",
        "import kontra_tools  # another package",
    ],
)
def test_the_checker_passes_plain_code(source: str) -> None:
    assert kontra_imports(source) == []
