"""``kontra publish`` refuses seller code that reaches past the façade (kontra-sdk#7,
kontra-cloud#4): a ``temporalio`` or ``nexusrpc`` import, and any ``continue_as_new``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from kontra.cli._seller_code import check_project, check_source

from .support import Cli

_P = Path("seller/workflows.py")


def _messages(source: str) -> list[tuple[int, str]]:
    return [(f.line, f.message) for f in check_source(_P, source)]


@pytest.mark.parametrize(
    "source",
    [
        "import temporalio\n",
        "from temporalio import workflow\n",
        "import temporalio.workflow as w\n",
        "from temporalio.common import RetryPolicy\n",
        "import nexusrpc\n",
        "from nexusrpc.handler import service_handler\n",
        "from temporalio.openai_agents import OpenAIAgentsPlugin\n",
        "from temporalio.contrib.openai_agents import OpenAIAgentsPlugin\n",
        "import importlib\nimportlib.import_module('temporalio.workflow')\n",
        "__import__('nexusrpc')\n",
    ],
)
def test_temporal_and_nexus_imports_are_refused(source: str) -> None:
    [(line, message)] = _messages(source)
    assert line == source.count("\n")
    assert "import" in message


@pytest.mark.parametrize(
    "source",
    [
        "workflow.continue_as_new(args=[1])\n",
        "from kontra import workflow as w\nw.continue_as_new()\n",
        "continue_as_new()\n",
    ],
)
def test_continue_as_new_is_refused_wherever_it_comes_from(source: str) -> None:
    [(line, message)] = _messages(source)
    assert line == source.count("\n")
    assert message.startswith("calls continue_as_new: use workflow.execute_activities_in_parallel")


@pytest.mark.parametrize(
    "source",
    [
        "from kontra import workflow, activity, RetryPolicy, ApplicationError, serve\n",
        "import kontra\n",
        "from agents import Agent, Runner  # OpenAI's SDK: the seller's own dependency\n",
        "import temporalio_helpers  # another package, only named alike\n",
        "x = 'temporalio'  # a string is not an import\n",
        "from .temporalio import thing  # a relative import is the project's own\n",
    ],
)
def test_the_facade_and_other_packages_are_allowed(source: str) -> None:
    assert _messages(source) == []


def test_a_project_is_checked_except_its_tests_and_environments(tmp_path: Path) -> None:
    (tmp_path / "workflows.py").write_text("from kontra import workflow\n")
    (tmp_path / "activities.py").write_text("x = 1\nfrom temporalio import activity\n")
    (tmp_path / "lib").mkdir()
    (tmp_path / "lib" / "flows.py").write_text("workflow.continue_as_new()\n")
    for skipped in ("tests", ".venv", "venv", "build", "__pycache__"):
        (tmp_path / skipped).mkdir()
        (tmp_path / skipped / "x.py").write_text("import temporalio\n")
    (tmp_path / "broken.py").write_text("def (:\n")
    rendered = [f.render(tmp_path) for f in check_project(tmp_path)]
    assert rendered == [
        "activities.py:2: imports from temporalio: import from kontra",
        "broken.py:1: cannot be parsed: invalid syntax",
        "lib/flows.py:1: calls continue_as_new: use workflow.execute_activities_in_parallel, "
        "which continues as new for you",
    ]


class _Profile:
    party = "acme"

    def __init__(self, *, dev: bool) -> None:
        self.dev = dev


class _Gate:
    published: list[Any] = []

    async def publish(self, document: Any) -> Any:
        self.published.append(document)
        raise AssertionError("refused code must never reach the gate")


class _Session:
    def __init__(self, *, dev: bool) -> None:
        self.profile = _Profile(dev=dev)
        self.gate = _Gate()

    async def __aenter__(self) -> _Session:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def resolve(self) -> _Profile:
        return self.profile


def test_publish_refuses_outside_dev_and_names_file_and_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import kontra.auth

    cli = Cli(tmp_path / "h", cwd=tmp_path)
    cli("init", "shop", "--party", "acme")
    project = tmp_path / "shop"
    with (project / "activities.py").open("a") as f:
        f.write("\nfrom temporalio import activity as raw\n")
    lines = (project / "activities.py").read_text().count("\n")
    monkeypatch.setattr(kontra.auth.Session, "from_env", lambda: _Session(dev=False))
    result = cli("publish", "--dir", str(project))
    assert result.exit_code == 1, result.output
    assert "seller code must stay behind the kontra façade" in result.stderr
    assert f"activities.py:{lines}: imports from temporalio: import from kontra" in result.stderr
    assert _Gate.published == []


def test_a_scaffolded_project_passes(tmp_path: Path) -> None:
    """`kontra init` then `kontra publish` must work: the template is behind the façade."""
    Cli(tmp_path / "h", cwd=tmp_path)("init", "fresh", "--party", "acme")
    assert [f.render(tmp_path / "fresh") for f in check_project(tmp_path / "fresh")] == []
