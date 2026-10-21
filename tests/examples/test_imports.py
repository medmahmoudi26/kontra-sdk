"""Acceptance 3 (decisions I29): a seller's ``workflows.py`` and ``activities.py`` are vanilla
Temporal. They import ``temporalio`` themselves and nothing from ``kontra``: not by an import
statement, not dynamically, and not by naming the module in a string. So they run on any
Temporal worker unmodified, which is checked here by deleting ``worker.py`` and running the
workflow on a plain ``temporalio.worker.Worker`` with ``kontra`` blocked from importing.

``worker.py`` is the one file of an offering that imports ``kontra`` (it binds the code to the
offering with ``serve``). Besides it, ``quote.py`` takes ``Quote`` from ``kontra.serve`` and
nothing else, and ``models.py`` may use ``kontra.types`` (``Artifact``) and nothing else.
Loading the workflows and activities therefore loads at most ``kontra`` and ``kontra.types``,
and no ``kontra`` module at all for a seller whose models have no ``Artifact``. The scaffold
``kontra init`` writes is held to the same rules.

The acceptance text is ``grep -rE '^\\s*(from|import)\\s+kontra\\b' examples/*/workflows.py
examples/*/activities.py`` printing nothing; that grep runs here verbatim."""

from __future__ import annotations

import ast
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

import pytest

from kontra.cli._scaffold import render, scaffold
from kontra.testing.devserver import DevServer

from .support import EXAMPLES, SELLERS

SELLER_CODE = sorted(
    p
    for n in ("workflows.py", "activities.py")
    for p in EXAMPLES.glob(f"*/{n}")
    if (p.parent / "worker.py").is_file()  # a seller's: buyer code hires through kontra.hire
)
SELLER_MODULES = sorted(p for name in SELLERS for p in (EXAMPLES / name).glob("*.py"))
MODELS = sorted(EXAMPLES.glob("*/models.py"))
SCAFFOLD = {str(rel): text for rel, text in render("acme", "word-count").items()}
SCAFFOLD_CODE = ("workflows.py", "activities.py")
#: The scaffold's offering modules (its tests are the seller's tests, and import anything).
SCAFFOLD_MODULES = sorted(
    rel
    for rel in SCAFFOLD
    if PurePosixPath(rel).suffix == ".py" and PurePosixPath(rel).parts[0] != "tests"
)

KONTRA = frozenset({"kontra"})
#: What each file of an offering may take from ``kontra``: module -> the names a ``from``
#: import of it may take (None: any). A file not named here takes nothing, except
#: ``worker.py``, which may import anything.
MAY_IMPORT: Mapping[str, Mapping[str, frozenset[str] | None]] = {
    "models.py": {"kontra.types": None},
    "quote.py": {"kontra.serve": frozenset({"Quote"})},
}
WORKER = "worker.py"
#: The most that loading a seller's workflows and activities may load of ``kontra``:
#: ``kontra.types`` for ``Artifact`` in its models, and the package around it (whose
#: ``__init__`` loads nothing).
LOADED_WITH_TYPES = frozenset({"kontra", "kontra.types"})
#: The acceptance grep (CLAUDE.md, docs/acceptance.md): any import of kontra, at any indent.
ACCEPTANCE_GREP = r"^\s*(from|import)\s+kontra\b"
#: Calls that import (or run) code named by a string.
_DYNAMIC = frozenset(
    {"__import__", "import_module", "find_spec", "run_module", "exec", "eval", "compile"}
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
    allowed: Mapping[str, frozenset[str] | None] | None = None,
) -> list[str]:
    """Every way ``source`` reaches a package in ``roots``, except ``from`` imports of a module
    in ``allowed`` taking only the names it allows (any, for None): import statements,
    dynamic imports and code run from strings that name one, and import-ish calls whose
    target is not a literal (which no check could clear)."""
    allowed = allowed or {}
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
                    continue
                names = allowed[node.module]
                if names is not None:
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


def kontra_imports(source: str, filename: str = "workflows.py") -> list[str]:
    """What ``source``, as the file ``filename`` of an offering, reaches in kontra beyond what
    that file may (see :data:`MAY_IMPORT`)."""
    return imports_of(source, KONTRA, allowed=MAY_IMPORT.get(filename))


def imports_temporal(source: str) -> bool:
    """``source`` imports ``temporalio`` itself (an import statement)."""
    return imports_of(source, frozenset({"temporalio"})) != []


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


# --------------------------------------------------------------------------- the examples


def test_every_seller_is_checked() -> None:
    assert {p.parent.name for p in SELLER_CODE} >= set(SELLERS)
    assert len(SELLER_CODE) == 2 * len({p.parent for p in SELLER_CODE})


def test_the_acceptance_grep_prints_nothing() -> None:
    """``grep -rE '^\\s*(from|import)\\s+kontra\\b' examples/*/workflows.py
    examples/*/activities.py``."""
    result = subprocess.run(
        ["grep", "-rE", ACCEPTANCE_GREP, *map(str, SELLER_CODE)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert (result.returncode, result.stdout) == (1, "")  # 1: no line matched


def test_the_acceptance_grep_finds_an_import() -> None:
    """Its teeth: every spelling of a kontra import at the start of a line, at any indent."""
    lines = [
        "from kontra import workflow",
        "import kontra",
        "    from kontra.types import Artifact",
        "import kontra.serve as s",
    ]
    result = subprocess.run(
        ["grep", "-cE", ACCEPTANCE_GREP],
        input="\n".join([*lines, "import kontra_tools", "# from kontra import x? no"]) + "\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.stdout.strip() == str(len(lines))


@pytest.mark.parametrize("path", SELLER_CODE, ids=_rel)
def test_seller_code_is_vanilla_temporal(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    assert kontra_imports(source) == []
    assert imports_temporal(source)  # Temporal, imported directly


@pytest.mark.parametrize("path", SELLER_MODULES, ids=_rel)
def test_every_seller_module_imports_only_what_its_file_may(path: Path) -> None:
    """worker.py imports anything; quote.py only ``kontra.serve.Quote``; models.py only
    ``kontra.types``; every other module nothing from kontra."""
    if path.name == WORKER:
        return
    assert kontra_imports(path.read_text(encoding="utf-8"), path.name) == []


@pytest.mark.parametrize("path", MODELS, ids=_rel)
def test_models_use_only_kontra_types(path: Path) -> None:
    found = imports_of(path.read_text(encoding="utf-8"), KONTRA, allowed={"kontra.types": None})
    assert found == []


@pytest.mark.parametrize("name", SELLERS)
def test_loading_seller_code_loads_at_most_kontra_types(name: str) -> None:
    """Importing ``workflows`` and ``activities`` (and what they import) loads no kontra
    module but ``kontra.types`` and its package, and only when the models use it. This also
    catches an import a module makes while it loads, whatever it is spelled as."""
    models = (EXAMPLES / name / "models.py").read_text(encoding="utf-8")
    uses_types = imports_of(models, KONTRA) != []
    assert _loaded_kontra(EXAMPLES / name) == (LOADED_WITH_TYPES if uses_types else set())


# --------------------------------------------------------------------------- the buyer's agent

#: examples/buyer-harness-agent: buyer code, so it hires through ``kontra.hire``. Its agent's
#: module takes nothing from kontra; the glue only the workflow door (``service``) and, for
#: its quote activity's type, ``Kontra``. ``main.py`` runs it, and imports anything.
BUYER = EXAMPLES / "buyer-harness-agent"
BUYER_MAY_IMPORT: Mapping[str, Mapping[str, frozenset[str] | None]] = {
    "workflows.py": {},
    "tools.py": {"kontra.hire": frozenset({"service", "Kontra"})},
}
#: What loading the agent and its glue loads of kontra: the sandbox-safe half of kontra.hire
#: (no client, no HTTP, no codec), as a workflow that hires needs.
LOADED_BY_THE_BUYER_AGENT = frozenset(
    {
        "kontra",
        "kontra.hire",
        "kontra.hire._errors",
        "kontra.hire._record",
        "kontra.hire._service",
        "kontra.types",
    }
)


def test_the_buyer_agent_is_not_seller_code() -> None:
    assert not (BUYER / WORKER).exists()
    assert BUYER not in {p.parent for p in SELLER_CODE}
    assert {p.name for p in BUYER.glob("*.py")} == {*BUYER_MAY_IMPORT, "main.py"}


@pytest.mark.parametrize("name", sorted(BUYER_MAY_IMPORT))
def test_the_buyer_agent_imports_temporal_and_hires_through_kontra_hire(name: str) -> None:
    source = (BUYER / name).read_text(encoding="utf-8")
    assert imports_temporal(source)
    assert imports_of(source, KONTRA, allowed=BUYER_MAY_IMPORT[name]) == []


def test_loading_the_buyer_agent_loads_only_the_workflow_door_of_kontra_hire() -> None:
    probe = (
        "import json, sys\n"
        "import workflows, tools\n"
        "print(json.dumps(sorted(m for m in sys.modules if m == 'kontra' or m.startswith('kontra.'))))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=BUYER,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert set(json.loads(result.stdout)) == LOADED_BY_THE_BUYER_AGENT


# --------------------------------------------------------------------------- the scaffold


@pytest.mark.parametrize("name", SCAFFOLD_CODE)
def test_the_scaffold_is_vanilla_temporal(name: str) -> None:
    source = SCAFFOLD[name]
    assert re.search(ACCEPTANCE_GREP, source, re.MULTILINE) is None
    assert kontra_imports(source) == []
    assert imports_temporal(source)


@pytest.mark.parametrize("name", SCAFFOLD_MODULES)
def test_every_scaffold_module_imports_only_what_its_file_may(name: str) -> None:
    if name == WORKER:
        return
    assert kontra_imports(SCAFFOLD[name], name) == []


def test_loading_the_scaffold_loads_no_kontra(tmp_path: Path) -> None:
    scaffold(tmp_path / "word-count", "acme", "word-count")
    assert _loaded_kontra(tmp_path / "word-count") == set()


# --------------------------------------------------------------------------- any Temporal

#: examples/hello without worker.py and quote.py, on a plain Temporal worker, with every
#: ``kontra`` import refused: Temporal's own client, worker and pydantic converter.
_ANY_TEMPORAL = r"""
import asyncio, importlib.abc, json, os, sys, uuid


class NoKontra(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "kontra" or name.startswith("kontra."):
            raise ImportError(f"{name}: kontra is not installed here")
        return None


sys.meta_path.insert(0, NoKontra())

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from activities import greet
from models import HelloInput
from workflows import HelloFlow


async def main():
    client = await Client.connect(sys.argv[1], data_converter=pydantic_data_converter)
    queue = f"any-temporal-{uuid.uuid4().hex}"
    async with Worker(client, task_queue=queue, workflows=[HelloFlow], activities=[greet]):
        out = await client.execute_workflow(
            HelloFlow.run, HelloInput(name="Ada"), id=queue, task_queue=queue
        )
    loaded = sorted(m for m in sys.modules if m == "kontra" or m.startswith("kontra."))
    print(json.dumps({"result": out.model_dump(mode="json"), "kontra": loaded}))


asyncio.run(main())
# Leave without finalizing: with temporalio 1.34 the Rust core's threads can crash it
# (SIGSEGV) after a clean run, as kontra.cli.main says; what was printed is flushed first.
sys.stdout.flush()
os._exit(0)
"""


@pytest.mark.temporal
def test_without_worker_py_the_code_runs_on_any_temporal(
    dev_server: DevServer, tmp_path: Path
) -> None:
    """The acceptance line "delete ``worker.py`` and the seller's code runs on any Temporal,
    unmodified": examples/hello copied without ``worker.py`` and ``quote.py`` (the two files
    that import kontra), run on a plain ``temporalio.worker.Worker`` against the dev server
    in a process where importing ``kontra`` fails."""
    project = tmp_path / "hello"
    shutil.copytree(
        EXAMPLES / "hello",
        project,
        ignore=shutil.ignore_patterns(WORKER, "quote.py", "__pycache__"),
    )
    assert not (project / WORKER).exists()
    result = subprocess.run(
        [sys.executable, "-c", _ANY_TEMPORAL, dev_server.grpc_address],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"result": {"greeting": "Hi Ada"}, "kontra": []}


@pytest.mark.temporal
def test_the_any_temporal_check_has_teeth(dev_server: DevServer, tmp_path: Path) -> None:
    """The same run refuses a seller module that imports kontra: the blocker is real."""
    project = tmp_path / "hello"
    shutil.copytree(EXAMPLES / "hello", project, ignore=shutil.ignore_patterns(WORKER))
    activities = project / "activities.py"
    activities.write_text("import kontra\n" + activities.read_text(encoding="utf-8"))
    result = subprocess.run(
        [sys.executable, "-c", _ANY_TEMPORAL, dev_server.grpc_address],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode != 0
    assert "kontra: kontra is not installed here" in result.stderr


# --------------------------------------------------------------------------- the checker's teeth


@pytest.mark.parametrize(
    "source",
    [
        "import kontra",
        "import kontra.workflow",
        "import os, kontra.serve as s",
        "from kontra import workflow",
        "from kontra import RetryPolicy, ApplicationError",
        "from kontra.workflow import x",
        "from kontra.types import Artifact",
        "from kontra.serve import Quote",
        "from kontra import serve",
        "from kontra import Worker",
        "from kontra import hire",
        "from kontra.contrib.parallel import execute_activities_in_parallel",
        "import importlib\nm = importlib.import_module('kontra.hire')",
        "m = __import__('kontra')",
        "exec('import kontra')",
        "import importlib\nname = 'kon' + 'tra'\nimportlib.import_module(name)",
        "MOD = 'kontra.serve'",
    ],
)
def test_the_checker_finds_any_kontra_in_seller_code(source: str) -> None:
    assert kontra_imports(source) != []


@pytest.mark.parametrize(
    "source",
    [
        "from temporalio import workflow",
        "from temporalio import activity, workflow",
        "from temporalio.common import RetryPolicy",
        "from temporalio.exceptions import ApplicationError",
        "import temporalio",
        "from temporalio.openai_agents.workflow import activity_as_tool",
        '"""Runs on any Temporal; worker.py binds it with `from kontra import serve`."""',
        "def f():\n    '''kontra.serve does that for you'''\n",
        "import kontra_tools  # another package",
        "from models import Batch",
        "MESSAGE = 'kontra is what worker.py imports'",
    ],
)
def test_the_checker_passes_vanilla_temporal(source: str) -> None:
    assert kontra_imports(source) == []


@pytest.mark.parametrize(
    ("filename", "source", "ok"),
    [
        ("models.py", "from kontra.types import Artifact", True),
        ("models.py", "from kontra.serve import Quote", False),
        ("models.py", "from kontra import types", False),
        ("quote.py", "from kontra.serve import Quote", True),
        ("quote.py", "from kontra.serve import Quote, Worker", False),
        ("quote.py", "from kontra import serve", False),
        ("quote.py", "import kontra.serve", False),
        ("activities.py", "from kontra.types import Artifact", False),
        ("helpers.py", "from kontra.serve import Quote", False),
    ],
)
def test_the_checker_holds_each_file_to_its_own_rule(filename: str, source: str, ok: bool) -> None:
    assert (kontra_imports(source, filename) == []) is ok
