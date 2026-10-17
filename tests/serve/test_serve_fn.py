"""``serve(Flow)``: what it binds (offering.yaml and quote.py beside worker.py, the first
workflow), what it registers (every imported ``kontra.activity.defn``, the agents plugin when
the workflows imported ``agents``), when it runs (``__main__`` only), how the CLI finds it,
and the callable ``kontra.serve`` package."""

from __future__ import annotations

import importlib.util
import inspect
import runpy
import sys
import textwrap
import types
import uuid
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

import kontra
from kontra import _agents
from kontra.cli import cli as kontra_cli
from kontra.cli._project import load_worker
from kontra.serve import BindingError, Worker
from kontra.serve.worker import TEMPORAL_KWARGS, workflow_runner
from kontra.testing.devserver import DevServer

AGENTS = importlib.util.find_spec("temporalio.openai_agents") is not None

_FILES = {
    "models.py": """
        from pydantic import BaseModel

        class Inp(BaseModel):
            text: str

        class Out(BaseModel):
            text: str
        """,
    "activities.py": """
        from kontra import activity

        @activity.defn
        async def shout(text: str) -> str:
            return text.upper()

        class Acts:
            @activity.defn
            async def whisper(self, text: str) -> str:
                return text.lower()
        """,
    "workflows.py": """
        from datetime import timedelta

        from kontra import workflow

        with workflow.unsafe.imports_passed_through():
            from activities import shout
            from models import Inp, Out

        @workflow.defn
        class Child:
            @workflow.run
            async def run(self, text: str) -> str:
                return text

        @workflow.defn
        class Flow:
            @workflow.run
            async def run(self, inp: Inp) -> Out:
                text = await workflow.execute_activity(
                    shout, inp.text, start_to_close_timeout=timedelta(seconds=30)
                )
                return Out(text=text)
        """,
    "quote.py": """
        from models import Inp

        from kontra.serve import Quote

        async def quote(inp: Inp) -> Quote:
            return Quote(price=0.01)
        """,
}


def _project(root: Path, worker: str, *, quote: bool = True, **more: str) -> Path:
    """An offering project in ``root`` whose worker.py is ``worker``."""
    root.mkdir(parents=True)
    files = {**_FILES, "worker.py": worker, **more}
    if not quote:
        del files["quote.py"]
    for name, text in files.items():
        (root / name).write_text(textwrap.dedent(text).lstrip())
    (root / "offering.yaml").write_text(
        "name: acme/serve-fn\n"
        f"version: 1.0.{uuid.uuid4().int % 10_000}\n"
        "description: serve() binds this\n"
        "pricing_hint: $0.01\n"
        "max_duration: 10m\n"
        "review_window: 1h\n"
        "max_output_bytes: 1MB\n"
        "artifact_retention: 1d\n"
    )
    return root


class Cli:
    """``kontra`` in-process, as one party with its own home and nothing from the shell."""

    def __init__(self, home: Path) -> None:
        self.home = home

    def __call__(self, *args: str) -> Result:
        env: dict[str, str | None] = {
            k: None for k in ("KONTRA_GATE", "KONTRA_API_KEY", "KONTRA_NEXGEN", "KONTRA_DEBUG")
        }
        env["KONTRA_HOME"] = str(self.home)
        return CliRunner().invoke(kontra_cli, list(args), env=env)


ONE_LINE = """
    from kontra import serve
    from workflows import Flow

    serve(Flow)
    """


@pytest.fixture
def no_run(monkeypatch: pytest.MonkeyPatch) -> list[Worker]:
    """``Worker.run`` records the worker instead of connecting anywhere."""
    ran: list[Worker] = []
    monkeypatch.setattr(Worker, "run", lambda self, **_: ran.append(self))  # pyright: ignore
    return ran


def _names(fns: list[Any]) -> list[str]:
    return [getattr(f, "__name__", repr(f)) for f in fns]


# --------------------------------------------------------------------------- binding


def test_serve_binds_offering_yaml_and_quote_py_beside_worker_py(
    tmp_path: Path, no_run: list[Worker]
) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    with load_worker(root) as worker:
        [bound] = worker.offerings
        assert bound.source == root / "offering.yaml"
        assert bound.workflow is sys.modules["workflows"].Flow
        assert bound.quote is sys.modules["quote"].quote
        assert sys.modules["quote"].__file__ == str(root / "quote.py")
        assert "workflows" not in worker.options  # no further workflow, none passed
        assert "plugins" not in worker.options
    assert no_run == []  # imported, not run


def test_the_first_workflow_is_bound_and_the_rest_registered(
    tmp_path: Path, no_run: list[Worker]
) -> None:
    root = _project(
        tmp_path / "p",
        """
        from datetime import timedelta

        from kontra import serve
        from workflows import Child, Flow

        serve(Flow, Child, max_concurrent_activities=7,
              graceful_shutdown_timeout=timedelta(seconds=3))
        """,
    )
    with load_worker(root) as worker:
        flows = sys.modules["workflows"]
        assert worker.offerings[0].workflow is flows.Flow
        assert worker.options["workflows"] == [flows.Child]
        assert worker.options["max_concurrent_activities"] == 7
        assert worker.options["graceful_shutdown_timeout"].total_seconds() == 3


def test_a_quote_passed_needs_no_quote_py(tmp_path: Path) -> None:
    root = _project(
        tmp_path / "p",
        """
        from kontra import serve
        from kontra.serve import Quote
        from models import Inp
        from workflows import Flow

        async def priced(inp: Inp) -> Quote:
            return Quote(price=1)

        serve(Flow, quote=priced)
        """,
        quote=False,
    )
    with load_worker(root) as worker:
        assert worker.offerings[0].quote is sys.modules["worker"].priced


def test_no_quote_py_and_no_quote_is_a_fix_it(tmp_path: Path) -> None:
    root = _project(tmp_path / "p", ONE_LINE, quote=False)
    with (
        pytest.raises(BindingError, match=r"quote\.py, which does not exist.*or pass quote="),
        load_worker(root),
    ):
        pass


def test_quote_py_without_a_quote_function(tmp_path: Path) -> None:
    root = _project(tmp_path / "p", ONE_LINE, **{"quote.py": "price = 1\n"})
    with pytest.raises(BindingError, match="defines no `quote`"), load_worker(root):
        pass


def test_serve_refuses_no_workflow_and_offerings(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match=r"serve\(MyFlow\)"):
        kontra.serve()
    root = _project(
        tmp_path / "p",
        "from kontra import serve\nfrom workflows import Flow\nserve(Flow, offerings=[])\n",
    )
    with pytest.raises(TypeError, match=r"Worker\(offerings="), load_worker(root):
        pass
    # Worker's own refusals reach the seller unchanged.
    root = _project(
        tmp_path / "q",
        "from kontra import serve\nfrom workflows import Flow\nserve(Flow, task_queue='x')\n",
    )
    with pytest.raises(TypeError, match="task_queue is not configurable"), load_worker(root):
        pass


# --------------------------------------------------------------------------- activities


def test_every_imported_activity_is_registered_but_methods(tmp_path: Path) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    with load_worker(root) as worker:
        assert _names(worker.options["activities"]) == ["shout"]
        assert worker.options["activities"][0] is sys.modules["activities"].shout


def test_activities_passed_are_added_after_the_imported_ones(tmp_path: Path) -> None:
    root = _project(
        tmp_path / "p",
        """
        from activities import Acts, shout
        from kontra import serve
        from workflows import Flow

        acts = Acts()
        serve(Flow, activities=[shout, acts.whisper])
        """,
    )
    with load_worker(root) as worker:
        given = worker.options["activities"]
        assert _names(given) == ["shout", "whisper"]  # shout once
        assert given[1] == sys.modules["worker"].acts.whisper


def test_another_projects_activities_are_not_registered(tmp_path: Path) -> None:
    """kontra.activity.defn remembers every activity of the process; serve() takes those
    whose module is the one imported now, so a project loaded earlier (here: still loaded,
    its same-named `activities` set aside) lends it nothing."""
    other = dict(_FILES)
    other["activities.py"] = _FILES["activities.py"].replace("shout", "bellow")
    other["workflows.py"] = _FILES["workflows.py"].replace("shout", "bellow")
    first = _project(tmp_path / "one", ONE_LINE, **other)
    second = _project(tmp_path / "two", ONE_LINE)
    with load_worker(first) as one:
        assert _names(one.options["activities"]) == ["bellow"]
        with load_worker(second) as two:
            assert _names(two.options["activities"]) == ["shout"]


# --------------------------------------------------------------------------- running


def test_python_worker_py_runs_it(tmp_path: Path, no_run: list[Worker]) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    names = {"models", "activities", "workflows", "quote", "worker"}
    sys.path.insert(0, str(root))
    try:
        scope = runpy.run_path(str(root / "worker.py"), run_name="__main__")
    finally:
        sys.path.remove(str(root))
        for name in names:
            sys.modules.pop(name, None)
    [ran] = no_run
    assert ran.offerings[0].workflow is scope["Flow"]


def test_kontra_serve_runs_the_worker_serve_built(tmp_path: Path, no_run: list[Worker]) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    result = Cli(tmp_path / "home")("serve", "--dir", str(root))
    assert result.exit_code == 0, result.output
    [ran] = no_run
    assert ran.offerings[0].source == root / "offering.yaml"


def test_kontra_contract_and_publish_find_it_without_starting_it(
    tmp_path: Path, no_run: list[Worker]
) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    cli = Cli(tmp_path / "home")
    contract = cli("contract", "--dir", str(root))
    assert contract.exit_code == 0, contract.output
    assert "acme/serve-fn" in contract.stdout
    publish = cli("publish", "--dir", str(root))  # no profile: refused after loading
    assert publish.exit_code == 1
    assert "worker.py" not in publish.stderr
    assert no_run == []


def test_the_explicit_worker_still_loads_and_both_forms_agree(tmp_path: Path) -> None:
    named = _project(
        tmp_path / "named",
        "from kontra import serve\nfrom workflows import Flow\nworker = serve(Flow)\n",
    )
    with load_worker(named) as worker:
        assert worker is sys.modules["worker"].worker
    explicit = _project(
        tmp_path / "explicit",
        """
        from activities import shout
        from quote import quote
        from workflows import Flow

        from kontra.serve import Worker, offering

        worker = Worker(offerings=[offering("offering.yaml", workflow=Flow, quote=quote)],
                        activities=[shout])
        """,
    )
    with load_worker(explicit) as worker:
        assert worker is sys.modules["worker"].worker


def test_two_workers_in_one_worker_py_are_refused(tmp_path: Path) -> None:
    cli = Cli(tmp_path / "home")
    twice = _project(
        tmp_path / "twice",
        "from kontra import serve\nfrom workflows import Flow\nserve(Flow)\nserve(Flow)\n",
    )
    with pytest.raises(RuntimeError, match=r"calls serve\(\) twice"), load_worker(twice):
        pass
    both = _project(
        tmp_path / "both",
        """
        from kontra import serve
        from quote import quote
        from workflows import Flow

        from kontra.serve import Worker, offering

        worker = Worker(offerings=[offering("offering.yaml", workflow=Flow, quote=quote)])
        serve(Flow)
        """,
    )
    result = cli("contract", "--dir", str(both))
    assert (result.exit_code, "keep one" in result.stderr) == (1, True)
    neither = _project(tmp_path / "neither", "x = 1\n")
    result = cli("contract", "--dir", str(neither))
    assert (result.exit_code, "must call `serve(YourFlow)`" in result.stderr) == (1, True)


# --------------------------------------------------------------------------- kontra.serve


_CALLABLE = """
import sys

{imports}

from workflows import Flow

worker = {call}(Flow)
assert worker.offerings[0].source.parent == __import__("pathlib").Path(__file__).parent
"""


@pytest.mark.parametrize(
    ("imports", "call"),
    [
        ("from kontra import serve", "serve"),  # the package, made callable
        ("from kontra.serve import serve", "serve"),  # the function
        ("import kontra", "kontra.serve"),
        ("import kontra.serve", "kontra.serve"),
    ],
)
def test_every_spelling_of_serve_calls_it_for_the_caller(
    tmp_path: Path, imports: str, call: str
) -> None:
    """``kontra.serve`` is a package; once imported, Python binds that name to it, so
    ``from kontra import serve`` is the package. It is callable, and resolves paths from its
    own caller as the function does."""
    root = _project(tmp_path / "p", _CALLABLE.format(imports=imports, call=call))
    with load_worker(root) as worker:
        assert worker.offerings[0].source == root / "offering.yaml"


def test_from_kontra_import_serve_before_and_after_the_package_is_imported() -> None:
    import subprocess

    probe = textwrap.dedent(
        """
        import sys
        assert "kontra.serve" not in sys.modules
        from kontra import serve as before
        import kontra.serve
        from kontra import serve as after
        import kontra
        assert callable(before) and callable(after) and callable(kontra.serve)
        assert kontra.serve.serve is kontra.serve.__dict__["serve"]
        print(type(before).__name__, type(after).__name__)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert out.split() == ["_CallablePackage", "_CallablePackage"]


# --------------------------------------------------------------------------- agents
#
# No keyword says "this workflow runs an agent" (decisions I25): a workflow that does imports
# OpenAI's ``agents``, and serve() reads that from the process after worker.py imported it.

#: ``workflows.py`` of a workflow that runs an agent: it imports OpenAI's Agents SDK.
_PASSED = " " * 12  # inside `with workflow.unsafe.imports_passed_through():`, dedented
AGENT_WORKFLOWS = _FILES["workflows.py"].replace(
    f"{_PASSED}from models import Inp, Out\n",
    f"{_PASSED}from models import Inp, Out\n{_PASSED}from agents import Agent, Runner\n",
)
assert "from agents import Agent, Runner" in AGENT_WORKFLOWS


@pytest.fixture
def agents_installed() -> None:
    if not AGENTS:
        pytest.skip("the [agents] extra is not installed")


@pytest.fixture
def no_openai_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nothing in the environment points at OpenAI: what a worker gets is what serve() made."""
    for name in ("OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_DEFAULT_MODEL"):
        monkeypatch.delenv(name, raising=False)


def test_agents_is_not_a_keyword(tmp_path: Path) -> None:
    """``serve(Flow, agents=True)`` is a ``TypeError`` like any keyword that is not
    ``temporalio.worker.Worker``'s; nothing that builds a worker or its converter takes it."""
    from kontra.serve._serve import serve_from
    from kontra.serve.worker import seller_data_converter

    for fn in (
        kontra.serve.serve,
        type(kontra.serve).__call__,
        serve_from,
        seller_data_converter,
        _agents.plugins,
        _agents.payload_converter_class,
    ):
        assert "agents" not in inspect.signature(fn).parameters, fn
    assert "agents" not in TEMPORAL_KWARGS
    root = _project(
        tmp_path / "p",
        "from kontra import serve\nfrom workflows import Flow\nserve(Flow, agents=True)\n",
    )
    with (
        pytest.raises(TypeError, match=r"unexpected keyword argument 'agents'"),
        load_worker(root),
    ):
        pass


def test_a_workflow_that_imports_no_agents_gets_no_plugin(
    tmp_path: Path,
    no_openai_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Installed is not imported: with the extra or without it, the worker of a workflow
    that does not import ``agents`` gets no plugin and temporalio's pydantic converter, and
    needs no OPENAI_API_KEY. ``mcp_servers`` is worker config the workflow never asks for:
    unused, and implying nothing."""
    from temporalio.contrib.pydantic import PydanticPayloadConverter

    root = _project(
        tmp_path / "p",
        "from kontra import serve\nfrom workflows import Flow\n"
        "serve(Flow, mcp_servers={'shodan': object})\n",
    )
    for installed in (True, False):
        monkeypatch.setattr(_agents, "AVAILABLE", installed)
        with load_worker(root) as worker:
            assert "plugins" not in worker.options
            converter = _agents.payload_converter_class(worker.options.get("plugins"))
            assert converter is PydanticPayloadConverter
    assert "agents" not in sys.modules  # importing kontra.workflow did not import it either


def _openai_agents_installed_in(site: Path) -> Path:
    """An installation of OpenAI's Agents SDK in ``site``, as pip makes one: the
    ``openai-agents`` distribution and its ``agents`` package, whose ``__init__.py`` this
    returns."""
    package = site / "agents"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    info = site / "openai_agents-0.0.1.dist-info"
    info.mkdir()
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: openai-agents\nVersion: 0.0.1\n")
    return package / "__init__.py"


def test_an_agents_import_without_the_extra_names_the_extra(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OpenAI's Agents SDK imported -- the ``openai-agents`` distribution's ``agents`` -- and
    not Temporal's integration: the worker could not run the workflow's agent, and says what
    installs it."""
    monkeypatch.setattr(_agents, "AVAILABLE", False)
    site = tmp_path / "site"
    theirs = types.ModuleType("agents")
    theirs.__file__ = str(_openai_agents_installed_in(site))
    monkeypatch.syspath_prepend(str(site))
    monkeypatch.setitem(sys.modules, "agents", theirs)  # what the workflow imported
    root = _project(tmp_path / "p", ONE_LINE)
    with (
        pytest.raises(TypeError, match=r"imports OpenAI's Agents SDK.*kontra-sdk\[agents\]"),
        load_worker(root),
    ):
        pass
    with pytest.raises(AttributeError, match=r"kontra-sdk\[agents\]"):
        _agents.facade("activity_as_tool")


def test_a_sellers_own_module_named_agents_runs_no_agent(
    tmp_path: Path, no_openai_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The signal is OpenAI's ``agents``, not the name: a seller's own ``agents.py`` that the
    workflow module imports is no Agents SDK, so, with the extra or without it, its worker
    gets no plugin and the pydantic converter (and no ``TypeError``)."""
    from temporalio.contrib.pydantic import PydanticPayloadConverter

    workflows = _FILES["workflows.py"].replace(
        f"{_PASSED}from models import Inp, Out\n",
        f"{_PASSED}from models import Inp, Out\n{_PASSED}from agents import GREETING\n",
    )
    root = _project(
        tmp_path / "p",
        ONE_LINE,
        **{"workflows.py": workflows, "agents.py": "GREETING = 'hi'\n"},
    )
    for installed in (True, False):
        monkeypatch.setattr(_agents, "AVAILABLE", installed)
        with load_worker(root) as worker:
            assert sys.modules["agents"].GREETING == "hi"  # the seller's own, imported
            assert "plugins" not in worker.options
            converter = _agents.payload_converter_class(worker.options.get("plugins"))
            assert converter is PydanticPayloadConverter


#: ``worker.py`` with the ``Worker`` written out rather than ``serve()``.
EXPLICIT = """
    from activities import shout
    from quote import quote
    from workflows import Flow

    from kontra.serve import Worker, offering

    worker = Worker(offerings=[offering("offering.yaml", workflow=Flow, quote=quote)],
                    activities=[shout])
    """


def test_an_explicit_worker_of_an_agent_workflow_gets_the_plugin_too(
    tmp_path: Path, agents_installed: None, no_openai_env: None
) -> None:
    """``worker = Worker(...)`` written out follows the same rule as serve(): its workflows
    imported ``agents``, so it gets the plugin, and its converter follows. The seller could
    not pass the plugin in a published offering: ``kontra publish`` refuses ``temporalio``
    imports. Without the import, no plugin."""
    from temporalio.openai_agents import OpenAIAgentsPlugin, OpenAIPayloadConverter

    root = _project(tmp_path / "p", EXPLICIT, **{"workflows.py": AGENT_WORKFLOWS})
    with load_worker(root) as worker:
        plugins = worker.options["plugins"]
    assert [type(p) for p in plugins] == [OpenAIAgentsPlugin]
    assert _agents.payload_converter_class(plugins) is OpenAIPayloadConverter
    sys.modules.pop("agents")  # a new worker process
    plain = _project(tmp_path / "q", EXPLICIT)
    with load_worker(plain) as worker:
        assert "plugins" not in worker.options


def test_the_facade_imports_agents_only_when_a_workflow_names_it() -> None:
    """``kontra.workflow`` is imported by every workflow; ``activity_as_tool`` and
    ``mcp_server`` load ``agents`` when first named, so the import stays the signal. A star
    import names neither, and ``dir()`` lists them without loading them."""
    import subprocess

    probe = textwrap.dedent(
        """
        import sys
        import kontra, kontra.serve, kontra.hire, kontra.testing
        from kontra import workflow
        from kontra.workflow import *
        print("agents" in sys.modules)
        if workflow._agents.AVAILABLE:
            assert {"activity_as_tool", "mcp_server"} <= set(dir(workflow))
            print("agents" in sys.modules)
            from temporalio.openai_agents import workflow as theirs
            assert workflow.activity_as_tool is theirs.activity_as_tool
            assert workflow.mcp_server is theirs.temporal_mcp_server
            print("agents" in sys.modules)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    ).stdout
    assert out.split() == (["False", "False", "True"] if AGENTS else ["False"])


def test_an_agents_import_installs_the_plugin_with_the_mcp_servers(
    tmp_path: Path, agents_installed: None, no_openai_env: None
) -> None:
    """The worker of a workflow that imports ``agents`` gets the plugin -- the model activity,
    the MCP servers' activities, the seller's own plugins after it -- and the converter its
    activities decode with. It needs no OPENAI_API_KEY to be built: the model endpoint is
    read at the first model call."""
    from temporalio import activity as temporal_activity
    from temporalio.openai_agents import OpenAIAgentsPlugin, OpenAIPayloadConverter

    root = _project(
        tmp_path / "p",
        """
        from kontra import serve
        from theirs import plugin
        from workflows import Flow

        def shodan():
            raise AssertionError("made on the worker, when a workflow lists its tools")

        serve(Flow, mcp_servers={"shodan": shodan}, plugins=[plugin])
        """,
        **{
            "workflows.py": AGENT_WORKFLOWS,
            "theirs.py": """
                from temporalio.plugin import SimplePlugin

                plugin = SimplePlugin("theirs")
                """,
        },
    )
    with load_worker(root) as worker:
        assert "agents" in sys.modules
        plugins = worker.options["plugins"]
        assert plugins[1:] == [sys.modules["theirs"].plugin]  # the seller's, after ours
    assert isinstance(plugins[0], OpenAIAgentsPlugin)
    activities = plugins[0].activities(None)  # type: ignore[misc]  # what it adds on a worker
    names = {temporal_activity._Definition.must_from_callable(a).name for a in activities}
    assert {"invoke_model_activity", "temporalio.mcp.shodan.call-tool"} <= names
    assert _agents.uses_agents(plugins)
    assert _agents.payload_converter_class(plugins) is OpenAIPayloadConverter


def test_the_sellers_own_agents_plugin_is_the_one(
    tmp_path: Path, agents_installed: None, no_openai_env: None
) -> None:
    """An ``OpenAIAgentsPlugin`` the seller passes in ``plugins=`` (Temporal's own config, its
    model timeouts say) is the worker's: serve() adds none of its own, which would register
    the same activities twice. ``mcp_servers=`` then belongs in that plugin."""
    from temporalio.openai_agents import OpenAIAgentsPlugin

    theirs = """
        from datetime import timedelta

        from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin

        plugin = OpenAIAgentsPlugin(
            model_params=ModelActivityParameters(start_to_close_timeout=timedelta(minutes=5))
        )
        """
    root = _project(
        tmp_path / "p",
        "from kontra import serve\nfrom theirs import plugin\nfrom workflows import Flow\n"
        "serve(Flow, plugins=[plugin])\n",
        **{"workflows.py": AGENT_WORKFLOWS, "theirs.py": theirs},
    )
    with load_worker(root) as worker:
        [plugin] = worker.options["plugins"]
        assert plugin is sys.modules["theirs"].plugin
    assert isinstance(plugin, OpenAIAgentsPlugin)
    both = _project(
        tmp_path / "q",
        "from kontra import serve\nfrom theirs import plugin\nfrom workflows import Flow\n"
        "serve(Flow, plugins=[plugin], mcp_servers={'shodan': object})\n",
        **{"workflows.py": AGENT_WORKFLOWS, "theirs.py": theirs},
    )
    with pytest.raises(TypeError, match="give that plugin the MCP servers"), load_worker(both):
        pass


def test_the_model_endpoint_is_the_agents_sdks_default_client(
    agents_installed: None, no_openai_env: None
) -> None:
    """``set_default_openai_client`` and ``set_default_openai_api``, the Agents SDK's own, in
    worker.py decide what the model activity dials. The API is read when the plugin is built
    (so worker.py sets it before serve()); the client at the model call, so one set after
    serve() built the plugin counts too. (The plugin's default provider
    would build ``AsyncOpenAI()`` from the environment when the worker is built, and never
    see either; with no key in the environment it would refuse to build at all.)"""
    import agents
    from agents import OpenAIChatCompletionsModel
    from agents.models import _openai_shared
    from openai import AsyncOpenAI

    saved = (
        _openai_shared.get_default_openai_client(),
        _openai_shared.get_use_responses_by_default(),
    )
    try:
        agents.set_default_openai_api("chat_completions")
        [plugin] = _agents.plugins()  # `agents` is imported: the workflow runs an agent
        endpoint = AsyncOpenAI(base_url="http://127.0.0.1:9/v1", api_key="local")
        agents.set_default_openai_client(endpoint, use_for_tracing=False)
        invoke = plugin.activities(None)[0]  # type: ignore[misc]  # invoke_model_activity
        model = invoke.__self__._model_provider.get_model(None)
        assert isinstance(model, OpenAIChatCompletionsModel)
        assert model._client is endpoint
    finally:
        _openai_shared._default_openai_client = saved[0]
        _openai_shared.set_use_responses_by_default(saved[1])


def test_plugins_is_a_temporal_worker_keyword() -> None:
    """serve() puts the agents plugin on the worker through temporalio's own ``plugins=``,
    which ``Worker`` passes verbatim: the client Kontra connects stays Kontra's."""
    assert "plugins" in TEMPORAL_KWARGS


@pytest.mark.temporal
async def test_an_agent_workflow_with_an_activity_tool_runs_on_the_plugin(
    dev_server: DevServer, agents_installed: None
) -> None:
    """The plugin on the worker (as serve() puts it), Kontra's sandbox and the payload
    converter :func:`kontra._agents.payload_converter_class` names for it: the agent loop
    runs in the workflow, the model call and ``workflow.activity_as_tool`` as activities.
    With temporalio's strict ``PydanticPayloadConverter`` instead, the model activity fails
    to decode its input (``ActivityModelInput``, 71 validation errors) and retries forever.
    The model is Temporal's test model, so nothing leaves the machine."""
    import temporalio.worker
    from temporalio.client import Client
    from temporalio.converter import DataConverter
    from temporalio.openai_agents import ModelActivityParameters, OpenAIAgentsPlugin
    from temporalio.openai_agents.testing import (
        ResponseBuilders,
        TestModel,
        TestModelProvider,
    )

    from .agentflows import Triage, lookup

    model = TestModel.returning_responses(
        [
            ResponseBuilders.tool_call('{"host": "a.example"}', "lookup"),
            ResponseBuilders.output_message("a.example is up"),
        ]
    )
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(), model_provider=TestModelProvider(model)
    )
    base = dev_server.client.config()
    base["data_converter"] = DataConverter(
        payload_converter_class=_agents.payload_converter_class([plugin])
    )
    client = Client(**base)
    queue = f"agents-{uuid.uuid4().hex[:8]}"
    async with temporalio.worker.Worker(
        client,
        task_queue=queue,
        workflows=[Triage],
        activities=[lookup],
        plugins=[plugin],
        workflow_runner=workflow_runner(),  # Kontra's sandbox: kontra passed through
    ):
        out = await client.execute_workflow(
            Triage.run, "a.example", id=f"triage-{uuid.uuid4().hex[:8]}", task_queue=queue
        )
    assert out == "a.example is up"
