"""Hold the documentation to the code it describes (run by `make lint`, so CI runs it).

The README, docs/, the examples' READMEs and the README `kontra init` writes are the first
thing a seller or buyer copies from, so each promise they make is checked against the
package:

* every ```python block compiles as a module, so it runs when pasted into a file (a
  top-level ``await`` or ``async with`` does not);
* every ``from kontra... import name`` in those blocks names a module and attribute that
  exist;
* every ``kontra ...`` command, in a shell block or inline, names real subcommands and real
  options of the ``kontra`` CLI;
* every ``make <target>`` names a target of the Makefile;
* every ``KONTRA_*`` variable they mention is read somewhere (the package, compose.yaml,
  the Makefile, the Temporal server, the workflows);
* every ``make <target>`` a workflow runs exists, and every repository path a workflow
  builds from or reads (``context``, ``file``, ``go-version-file``, ...) is there.

It prints one line per problem and exits 1 if there is any.
"""

from __future__ import annotations

import ast
import importlib
import re
import shlex
import sys
from collections.abc import Iterator
from pathlib import Path

import click
import yaml

ROOT = Path(__file__).resolve().parents[2]
DOCS = [
    ROOT / "README.md",
    ROOT / "CONTRIBUTING.md",
    ROOT / "SECURITY.md",
    *sorted((ROOT / "docs").glob("*.md")),
    ROOT / "spec" / "README.md",
    ROOT / "temporal" / "authorizer" / "README.md",
    *sorted((ROOT / "examples").glob("*/README.md")),
    # What `kontra init` writes next to a seller's code: read before anything else here.
    ROOT / "kontra" / "templates" / "offering" / "README.md.tmpl",
]
# Where a KONTRA_* variable can be read: the package, the stack, the build, the server.
READERS = [
    *(ROOT / "kontra").rglob("*.py"),
    *(ROOT / "kontra" / "templates").rglob("*.tmpl"),
    ROOT / "compose.yaml",
    ROOT / "Makefile",
    *(ROOT / "temporal" / "authorizer").rglob("*.go"),
    *(ROOT / ".github" / "workflows").glob("*.yml"),
]

_FENCE = re.compile(r"^```([a-z]*)\n(.*?)^```", re.M | re.S)
_INLINE = re.compile(r"`([^`\n]+)`")
_ENV_ASSIGN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*=\S*")
_PLACEHOLDER = re.compile(r"<[^<>]*>")
_KONTRA_VAR = re.compile(r"\bKONTRA_[A-Z0-9_]*[A-Z0-9]\b")
_MAKE_TARGET = re.compile(r"^([a-z][a-z0-9-]*):(?!=)", re.M)
# Step inputs and variables that name a file or directory of this repository.
_PATH_KEYS = {"context", "file", "go-version-file", "cache-dependency-path", "GITLEAKS_CONFIG"}


def _blocks(text: str) -> Iterator[tuple[str, str, int]]:
    """(language, body, line of the opening fence) of each fenced block."""
    for m in _FENCE.finditer(text):
        yield m.group(1), m.group(2), text.count("\n", 0, m.start()) + 1


def _prose(text: str) -> str:
    """The text with fenced blocks blanked out (line numbers kept)."""
    return _FENCE.sub(lambda m: "\n" * m.group(0).count("\n"), text)


def _commands(line: str) -> Iterator[list[str]]:
    """The argv of each command on a documented shell line: placeholders (``<job id>``)
    become one word, optional brackets and ``a | b`` alternatives are dropped, a trailing
    comment is cut, ``&&`` separates commands, and leading ``VAR=value`` is skipped."""
    line = _PLACEHOLDER.sub("PLACEHOLDER", line).replace("[", " ").replace("]", " ")
    try:
        words = shlex.split(line, comments=True)
    except ValueError:
        return
    command: list[str] = []
    for word in [*words, "&&"]:
        if word in ("&&", ";"):
            while command and _ENV_ASSIGN.fullmatch(command[0]):
                command.pop(0)
            if command:
                yield command
            command = []
        elif word != "|":
            command.append(word)


def _check_kontra(argv: list[str], cli: click.Group) -> str | None:
    """None if ``argv`` (``kontra ...``) names real commands and options, else why not.
    A bare ``kontra`` is the command (or the import name), not a call to check."""
    if len(argv) == 1:
        return None
    ctx_chain: list[click.Command] = [cli]
    command: click.Command = cli
    rest = argv[1:]
    while rest and isinstance(command, click.Group) and rest[0] in command.commands:
        command = command.commands[rest[0]]
        ctx_chain.append(command)
        rest = rest[1:]
    if isinstance(command, click.Group) and (
        not command.invoke_without_command or (rest and not rest[0].startswith("-"))
    ):
        return f"`{' '.join(argv)}`: no such command"
    known = {"--help", "-h"}
    for cmd in ctx_chain:
        for param in cmd.params:
            if isinstance(param, click.Option):
                known.update(param.opts)
                known.update(param.secondary_opts)
    for word in rest:
        if word.startswith("-") and word != "-":
            name = word.split("=", 1)[0]
            if name not in known:
                return f"`{' '.join(argv)}`: no option {name} there"
    return None


def _check_python(body: str, where: str) -> Iterator[str]:
    try:
        tree = ast.parse(body)
        compile(body, where, "exec")
    except SyntaxError as e:
        yield f"{where}: does not run as a file: {e.msg} (line {e.lineno} of the block)"
        return
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            if node.module.split(".")[0] != "kontra":
                continue
            try:
                module = importlib.import_module(node.module)
            except ImportError as e:
                yield f"{where}: cannot import {node.module}: {e}"
                continue
            for alias in node.names:
                if alias.name != "*" and not hasattr(module, alias.name):
                    yield f"{where}: {node.module} has no {alias.name}"
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "kontra":
                    try:
                        importlib.import_module(alias.name)
                    except ImportError as e:
                        yield f"{where}: cannot import {alias.name}: {e}"


def _workflow_steps(path: Path) -> Iterator[tuple[str, dict[str, object]]]:
    """(``job/step``, step) for every step of a workflow file."""
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for job_id, job in (doc.get("jobs") or {}).items():
        for i, step in enumerate(job.get("steps") or [], start=1):
            yield f"{job_id}/{step.get('name') or step.get('uses') or i}", step


def main() -> int:
    from kontra.cli import cli

    targets = set(_MAKE_TARGET.findall((ROOT / "Makefile").read_text(encoding="utf-8")))
    readers = "\n".join(p.read_text(encoding="utf-8") for p in READERS if p.is_file())
    problems: list[str] = []

    def shell_line(line: str, where: str) -> None:
        for argv in _commands(line):
            if argv[0] == "kontra":
                why = _check_kontra(argv, cli)
                if why is not None:
                    problems.append(f"{where}: {why}")
            elif argv[0] == "make":
                problems.extend(
                    f"{where}: `{' '.join(argv)}`: the Makefile has no target {t}"
                    for t in argv[1:]
                    if not t.startswith("-") and "=" not in t and t not in targets
                )

    for doc in DOCS:
        text = doc.read_text(encoding="utf-8")
        rel = doc.relative_to(ROOT)
        for lang, body, start in _blocks(text):
            if lang == "python":
                problems += _check_python(body, f"{rel}:{start}")
            elif lang in ("sh", "bash", "console", "shell"):
                for offset, line in enumerate(body.splitlines(), start=1):
                    shell_line(line, f"{rel}:{start + offset}")
        prose = _prose(text)
        for m in _INLINE.finditer(prose):
            span = m.group(1).strip()
            words = span.split()
            while words and _ENV_ASSIGN.fullmatch(words[0]):
                words.pop(0)
            if words and words[0] in ("kontra", "make"):
                shell_line(span, f"{rel}:{prose.count(chr(10), 0, m.start()) + 1}")
        for var in sorted(set(_KONTRA_VAR.findall(text))):
            if var not in readers:
                problems.append(f"{rel}: {var} is read nowhere in the package, stack or build")

    for workflow in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        rel = workflow.relative_to(ROOT)
        for where, step in _workflow_steps(workflow):
            run = step.get("run")
            if isinstance(run, str):
                for line in run.splitlines():
                    if line.strip().startswith("make "):
                        shell_line(line.strip(), f"{rel}: {where}")
            given: dict[str, object] = {}
            for section in ("with", "env"):
                block = step.get(section)
                if isinstance(block, dict):
                    given.update({str(k): v for k, v in block.items()})  # pyright: ignore[reportUnknownVariableType]
            problems.extend(
                f"{rel}: {where}: {key} {value} does not exist"
                for key, value in given.items()
                if key in _PATH_KEYS
                and isinstance(value, str)
                and "${{" not in value
                and not (ROOT / value).exists()
            )

    for p in problems:
        print(p)
    if problems:
        print(f"{len(problems)} problem(s) in the docs", file=sys.stderr)
        return 1
    print(f"docs ok: {len(DOCS)} files and the workflows checked against the package")
    return 0


if __name__ == "__main__":
    sys.exit(main())
