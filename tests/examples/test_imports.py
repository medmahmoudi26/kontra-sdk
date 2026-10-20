"""Acceptance 3 (D1): seller ``workflows.py`` and ``activities.py`` reach Temporal only through
the kontra façade -- ``from kontra import workflow, activity, RetryPolicy, ...`` -- and never
import ``temporalio`` or ``nexusrpc`` themselves: not by an import statement, not
dynamically, and not by naming the module in a string. The façade is Temporal's own objects
re-exported, so replacing ``from kontra import workflow`` with ``from temporalio import
workflow`` still runs on any Temporal; what it must never bring in is the serving stack
(gate client, codec, auth), which loading the code is checked for.

No seller module imports Temporal (the publish gate refuses it anywhere in an offering);
``models.py`` may use ``kontra.types`` (``Artifact``), and nothing else from kontra. The
scaffold ``kontra init`` writes is held to the same rules.

The acceptance text is ``grep -r "import kontra" examples/*/workflows.py
examples/*/activities.py`` returning nothing; that grep runs here verbatim: the façade is
imported with ``from kontra import ...``, never ``import kontra``."""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath

import pytest

from kontra.cli._scaffold import render, scaffold

from .support import EXAMPLES, SELLERS

SELLER_CODE = sorted(p for n in ("workflows.py", "activities.py") for p in EXAMPLES.glob(f"*/{n}"))
SELLER_MODULES = sorted(p for name in SELLERS for p in (EXAMPLES / name).glob("*.py"))
MODELS = sorted(EXAMPLES.glob("*/models.py"))
SCAFFOLD = {str(rel): text for rel, text in render("acme", "word-count").items()}
SCAFFOLD_CODE = ("workflows.py", "activities.py")
SCAFFOLD_MODULES = sorted(rel for rel in SCAFFOLD if PurePosixPath(rel).suffix == ".py")

#: Temporal, reached by seller code only through the façade.
TEMPORAL = frozenset({"temporalio", "nexusrpc"})
#: The kontra modules seller code may import from: the façade and the ``Artifact`` type.
FACADE = frozenset({"kontra", "kontra.workflow", "kontra.activity", "kontra.types"})
#: What ``from kontra import ...`` may take in seller code (``Worker`` and ``serve`` are
#: worker.py's, and load the serving stack).
FACADE_NAMES = frozenset({"workflow", "activity", "RetryPolicy", "ApplicationError", "Page"})
#: What may load with the façade, measured: ``kontra/__init__`` loads ``workflow``,
#: ``activity`` and ``_parallel``; ``kontra.workflow`` loads ``_agents``; ``_parallel`` imports
#: ``kontra.contract.names``, which runs the ``kontra.contract`` package (whose ``quote``
#: uses ``kontra.ledger``'s money, and whose derivation ``kontra.types``).
LOADED_WITH_THE_FACADE = frozenset(
    {"kontra", "kontra.workflow", "kontra.activity", "kontra._parallel", "kontra._agents"}
    | {"kontra.types", "kontra.ledger", "kontra.contract"}
)
#: The serving stack: what worker.py loads, never workflow or activity code.
SERVING = ("kontra.serve", "kontra.gate_client", "kontra.codec", "kontra.auth", "kontra.hire")
#: Calls that import (or run) code named by a string.
_DYNAMIC = frozenset(
    {"__import__", "import_module", "find_spec", "run_module", "exec", "eval", "compile"}
)
_IMPORT_LINE = re.compile(
    r"^\s*(?:from\s+(?:temporalio|nexusrpc)\b|import\s+(?:.*[\s,])?(?:temporalio|nexusrpc|kontra)\b)",
    re.MULTILINE,
)


def _rel(path: Path) -> str:
    return str(path.relative_to(EXAMPLES.parent))


def _under(module: str | None, roots: frozenset[str]) -> bool:
    return module is not None and module.split(".", 1)[0] in roots


def _docstrings(tree: ast.AST) -> set[int]:
    """ids of the string constants that are docstrings (prose, not code)."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                found.add(id(body[0].value))
    return found


def imports_of(
    source: str,
    roots: frozenset[str],
    *,
    allowed: frozenset[str] = frozenset(),
    names: frozenset[str] | None = None,
) -> list[str]:
    """Every way ``source`` reaches a package in ``roots``, except ``from`` imports of
    modules in ``allowed`` (taking only ``names`` from a package, when given): import
    statements, dynamic imports and code run from strings that name one, and import-ish
    calls whose target is not a literal (which no check could clear)."""
    tree = ast.parse(source)
    prose = _docstrings(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found += [f"import {a.name}" for a in node.names if _under(a.name, roots)]
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and _under(node.module, roots):
                if node.module not in allowed:
                    found.append(f"from {node.module} import ...")
                elif names is not None and "." not in (node.module or ""):
                    found += [
                        f"from {node.module} import {a.name}"
                        for a in node.names
                        if a.name not in names
                    ]
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name in _DYNAMIC:
                first = node.args[0] if node.args else None
                if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                    found.append(f"{name}(<not a literal>) at line {node.lineno}")
                elif any(root in first.value for root in roots):
                    found.append(f"{name}({first.value!r}) at line {node.lineno}")
        elif (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in prose
            and _under(node.value.strip(), roots)
            and re.fullmatch(r"[\w.]+", node.value.strip())
        ):
            found.append(f"the module name {node.value!r} at line {node.lineno}")
    return found


def temporal_imports(source: str) -> list[str]:
    return imports_of(source, TEMPORAL)


def kontra_imports(source: str) -> list[str]:
    """What seller code reaches in kontra beyond the façade."""
    return imports_of(source, frozenset({"kontra"}), allowed=FACADE, names=FACADE_NAMES)


def _loaded_kontra(cwd: Path) -> set[str]:
    """The kontra modules loaded by importing ``workflows`` and ``activities`` in ``cwd``."""
    probe = (
        "import json, sys\n"
        "import workflows, activities\n"
        "print(json.dumps(sorted(m for m in sys.modules if m == 'kontra' or m.startswith('kontra.'))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return set(json.loads(result.stdout))


def _within_the_facade(loaded: set[str]) -> None:
    assert {m for m in loaded if m.startswith(SERVING)} == set()
    assert {
        m
        for m in loaded
        if m not in LOADED_WITH_THE_FACADE and not m.startswith("kontra.contract.")
    } == set()


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
def test_seller_code_imports_temporal_only_through_the_facade(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    assert _IMPORT_LINE.findall(source) == []
    assert temporal_imports(source) == []
    assert kontra_imports(source) == []


@pytest.mark.parametrize("path", SELLER_MODULES, ids=_rel)
def test_no_seller_module_imports_temporal(path: Path) -> None:
    """worker.py, quote.py and models.py too: the publish gate refuses Temporal anywhere."""
    assert temporal_imports(path.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize("path", MODELS, ids=_rel)
def test_models_use_only_kontra_types(path: Path) -> None:
    found = imports_of(
        path.read_text(encoding="utf-8"),
        frozenset({"kontra"}),
        allowed=frozenset({"kontra.types"}),
    )
    assert found == []


@pytest.mark.parametrize("name", SELLERS)
def test_loading_seller_code_loads_the_facade_not_the_serving_stack(name: str) -> None:
    """Importing ``workflows`` and ``activities`` (and what they import) loads kontra's
    façade and what it needs, never the serving stack. This also catches an import a module
    makes while it loads, whatever it is spelled as."""
    _within_the_facade(_loaded_kontra(EXAMPLES / name))


@pytest.mark.parametrize("name", SCAFFOLD_CODE)
def test_the_scaffold_imports_temporal_only_through_the_facade(name: str) -> None:
    source = SCAFFOLD[name]
    assert "import kontra" not in source
    assert _IMPORT_LINE.findall(source) == []
    assert temporal_imports(source) == []
    assert kontra_imports(source) == []


@pytest.mark.parametrize("name", SCAFFOLD_MODULES)
def test_no_scaffold_module_imports_temporal(name: str) -> None:
    assert temporal_imports(SCAFFOLD[name]) == []


def test_loading_the_scaffold_loads_the_facade_not_the_serving_stack(tmp_path: Path) -> None:
    scaffold(tmp_path / "word-count", "acme", "word-count")
    _within_the_facade(_loaded_kontra(tmp_path / "word-count"))


# --------------------------------------------------------------------------- the checker's teeth


@pytest.mark.parametrize(
    "source",
    [
        "import temporalio",
        "from temporalio import workflow",
        "from temporalio.common import RetryPolicy",
        "import os, nexusrpc as n",
        "import importlib\nm = importlib.import_module('temporalio.workflow')",
        "m = __import__('nexusrpc')",
        "exec('import temporalio')",
        "import importlib\nname = 'tempor' + 'alio'\nimportlib.import_module(name)",
        "MOD = 'temporalio.client'",
    ],
)
def test_the_checker_finds_temporal(source: str) -> None:
    assert temporal_imports(source) != []


@pytest.mark.parametrize(
    "source",
    [
        "import kontra",
        "import kontra.workflow",
        "import os, kontra.serve as s",
        "from kontra.serve import Quote",
        "from kontra import serve",
        "from kontra import Worker, workflow",
        "from kontra import hire",
        "from kontra._parallel import Page",
        "import importlib\nm = importlib.import_module('kontra.hire')",
        "m = __import__('kontra')",
        "exec('import kontra')",
        "import importlib\nname = 'kon' + 'tra'\nimportlib.import_module(name)",
        "MOD = 'kontra.serve'",
    ],
)
def test_the_checker_finds_kontra_beyond_the_facade(source: str) -> None:
    assert kontra_imports(source) != []


@pytest.mark.parametrize(
    "source",
    [
        '"""Temporal through the façade; with `from temporalio import workflow` it runs."""',
        "from kontra import RetryPolicy, workflow",
        "from kontra import ApplicationError, Page, activity",
        "from kontra.workflow import execute_activities_in_parallel",
        "from kontra.activity import heartbeat",
        "from kontra.types import Artifact\nfrom models import Batch",
        "def f():\n    '''temporalio.workflow does that for you'''\n",
        "import temporalio_tools  # another package",
        "import kontra_tools  # another package",
        "MESSAGE = 'temporalio is behind the façade'",
    ],
)
def test_the_checker_passes_facade_code(source: str) -> None:
    assert temporal_imports(source) == []
    assert kontra_imports(source) == []
