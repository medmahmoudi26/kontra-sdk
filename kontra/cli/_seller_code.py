"""What ``kontra publish`` refuses in seller code (kontra-sdk#7, kontra-cloud#4).

Seller code stays behind the façade: ``seller -> kontra -> temporalio``. A module of the
offering's project that imports ``temporalio`` or ``nexusrpc`` (``temporalio.openai_agents``
and ``temporalio.contrib.openai_agents`` included; OpenAI's own ``agents`` stays allowed),
or that calls ``continue_as_new`` (the only continue-as-new is ``kontra.batch.v1``'s), is
refused, naming the file and line.

This runs here because it is the one step that has the code: the gate receives only the
contract document, and the seller's modules never leave the seller's machine. It reads the
source; nothing is imported or run. A ``kontra dev`` profile skips it.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

__all__ = ["Finding", "check_project"]

#: Modules seller code may not import: Temporal and Nexus stay behind ``kontra``.
DENIED = ("temporalio", "nexusrpc")
#: Calls that import code named by a string.
_DYNAMIC = frozenset({"__import__", "import_module", "find_spec", "run_module"})
#: Directories that are not the offering's code.
_SKIP_DIRS = frozenset(
    {"tests", "test", "venv", "node_modules", "__pycache__", "build", "dist", "site-packages"}
)


@dataclass(frozen=True, slots=True)
class Finding:
    path: Path
    line: int
    message: str

    def render(self, root: Path) -> str:
        """``file:line: message``, the file relative to the project."""
        return f"{self.path.relative_to(Path(root).resolve())}:{self.line}: {self.message}"


def _denied(module: str) -> bool:
    return any(module == d or module.startswith(d + ".") for d in DENIED)


def _modules(root: Path) -> Iterator[Path]:
    for path in sorted(root.rglob("*.py")):
        parts = path.relative_to(root).parts[:-1]
        if any(p.startswith(".") or p in _SKIP_DIRS for p in parts):
            continue
        yield path


def check_source(path: Path, source: str) -> list[Finding]:
    """The findings in one module's source."""
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        return [Finding(path, e.lineno or 0, f"cannot be parsed: {e.msg}")]
    found: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _denied(alias.name):
                    found.append(
                        Finding(path, node.lineno, f"imports {alias.name}: import from kontra")
                    )
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module is not None and _denied(node.module):
                found.append(
                    Finding(path, node.lineno, f"imports from {node.module}: import from kontra")
                )
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
            if name == "continue_as_new":
                found.append(
                    Finding(
                        path,
                        node.lineno,
                        "calls continue_as_new: use workflow.execute_activities_in_parallel, "
                        "which continues as new for you",
                    )
                )
            elif name in _DYNAMIC and node.args:
                first = node.args[0]
                if (
                    isinstance(first, ast.Constant)
                    and isinstance(first.value, str)
                    and _denied(first.value)
                ):
                    found.append(Finding(path, node.lineno, f"imports {first.value} by name"))
    return found


def check_project(root: str | Path) -> list[Finding]:
    """Every finding in the project at ``root`` (its tests and virtualenvs excluded), in
    file then line order."""
    base = Path(root).resolve()
    found: list[Finding] = []
    for path in _modules(base):
        found += check_source(path, path.read_text(encoding="utf-8", errors="replace"))
    return sorted(found, key=lambda f: (str(f.path), f.line))
