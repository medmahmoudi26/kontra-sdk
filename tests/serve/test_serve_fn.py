"""``serve(entry)``: what it binds (offering.yaml and quote.py beside worker.py, the entry),
what it registers (every ``@activity.defn`` function of the project's modules, found by
scanning them; ``workflows=`` and ``plugins=`` passed through as temporalio's keywords), when
it runs (``__main__`` only), how the CLI finds it, and the callable ``kontra.serve``
package. Seller code here is vanilla temporalio (decisions I29)."""

from __future__ import annotations

import importlib.util
import inspect
import runpy
import sys
import textwrap
import uuid
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

import kontra
from kontra.cli import cli as kontra_cli
from kontra.cli._project import load_worker
from kontra.serve import BindingError, Worker
from kontra.serve._serve import project_activities
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
        from temporalio import activity

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

        from temporalio import workflow

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


def test_the_entry_is_bound_and_further_workflows_registered(
    tmp_path: Path, no_run: list[Worker]
) -> None:
    """Every keyword is temporalio's, verbatim: further workflows go in ``workflows=``."""
    root = _project(
        tmp_path / "p",
        """
        from datetime import timedelta

        from kontra import serve
        from workflows import Child, Flow

        serve(Flow, workflows=[Child], max_concurrent_activities=7,
              graceful_shutdown_timeout=timedelta(seconds=3))
        """,
    )
    with load_worker(root) as worker:
        flows = sys.modules["workflows"]
        assert worker.offerings[0].workflow is flows.Flow
        assert worker.options["workflows"] == [flows.Child]
        assert worker.options["max_concurrent_activities"] == 7
        assert worker.options["graceful_shutdown_timeout"].total_seconds() == 3


def test_serve_picks_the_engine_from_the_entry_and_an_override_must_agree(
    tmp_path: Path, no_run: list[Worker]
) -> None:
    """Seam ⑥ (decisions I32): ``serve(entry)`` derives the engine from the entry's type --
    temporalio code runs on ``temporal`` -- and ``engine=`` may only say the same."""
    root = _project(tmp_path / "p", ONE_LINE)
    with load_worker(root) as worker:
        assert worker.offerings[0].engine == "temporal"
    root = _project(
        tmp_path / "q",
        """
        from kontra import serve
        from workflows import Flow

        serve(Flow, engine="temporal")
        """,
    )
    with load_worker(root) as worker:
        assert worker.offerings[0].engine == "temporal"
    root = _project(
        tmp_path / "r",
        """
        from kontra import serve
        from workflows import Flow

        serve(Flow, engine="golem")
        """,
    )
    with pytest.raises(ValueError, match="engine"), load_worker(root):
        pass


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


def test_serve_refuses_no_entry_two_entries_and_offerings(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match="entry"):
        kontra.serve()  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="positional"):
        kontra.serve(object, object)  # type: ignore[call-arg]  # serve(Flow, workflows=[Child])
    root = _project(
        tmp_path / "x",
        "from kontra import serve\nfrom workflows import Flow\nserve(Flow.run)\n",
    )
    with (
        pytest.raises(BindingError, match=r"@workflow\.defn class.*@agent\.accepts"),
        load_worker(root),
    ):
        pass
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


def test_every_activity_of_the_project_is_registered_but_methods(tmp_path: Path) -> None:
    root = _project(tmp_path / "p", ONE_LINE)
    with load_worker(root) as worker:
        assert _names(worker.options["activities"]) == ["shout"]
        assert worker.options["activities"][0] is sys.modules["activities"].shout


def test_activities_passed_are_added_after_the_found_ones(tmp_path: Path) -> None:
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
    """The scan reads the modules loaded from under worker.py's own directory: a project
    loaded earlier (here: still loaded, its same-named `activities` set aside) lends it
    nothing, and neither does any module of the test process outside the project."""
    other = dict(_FILES)
    other["activities.py"] = _FILES["activities.py"].replace("shout", "bellow")
    other["workflows.py"] = _FILES["workflows.py"].replace("shout", "bellow")
    first = _project(tmp_path / "one", ONE_LINE, **other)
    second = _project(tmp_path / "two", ONE_LINE)
    with load_worker(first) as one:
        assert _names(one.options["activities"]) == ["bellow"]
        with load_worker(second) as two:
            assert _names(two.options["activities"]) == ["shout"]


def _load(path: Path, name: str) -> None:
    """Import ``path`` as module ``name`` (as a test run of the project would load it)."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)


_ACTIVITY = """
from temporalio import activity

@activity.defn(name="{name}")
async def {name}(text: str) -> str:
    return text
"""


def test_the_scan_skips_tests_environments_and_reexports(tmp_path: Path) -> None:
    """Activities in the project's modules, once each (a function a module imports from
    another is found in its own), in import then definition order; none from the project's
    tests, a virtual environment, build output or a dot directory -- all loaded here, as a
    test run of the project would load them -- and none from outside the project."""
    root = tmp_path / "p"
    files = {
        "acts_one.py": _ACTIVITY.format(name="one"),
        "acts_two.py": "from acts_one import one  # a re-export\n"
        + _ACTIVITY.format(name="two")
        + _ACTIVITY.format(name="three"),
        "tests/test_it.py": _ACTIVITY.format(name="in_tests"),
        ".venv/lib/dep.py": _ACTIVITY.format(name="in_venv"),
        "build/lib/copy.py": _ACTIVITY.format(name="in_build"),
        ".hidden/x.py": _ACTIVITY.format(name="in_dot_dir"),
    }
    names: list[str] = []
    sys.path.insert(0, str(root))
    try:
        for rel, text in files.items():
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
            names.append(path.stem)
            _load(path, path.stem)
        importlib.import_module("tests.serve.flows")  # activities outside the project
        assert _names(project_activities(root.resolve())) == ["one", "two", "three"]
    finally:
        sys.path.remove(str(root))
        for name in names:
            sys.modules.pop(name, None)


def test_two_activities_of_one_name_are_refused_naming_both(tmp_path: Path) -> None:
    """Temporal registers one activity per name: two in the project are refused when
    worker.py is imported, naming both modules -- never the last one silently winning."""
    root = _project(
        tmp_path / "p",
        "import twin\nfrom kontra import serve\nfrom workflows import Flow\nserve(Flow)\n",
        **{
            "twin.py": """
                from temporalio import activity

                @activity.defn(name="shout")
                async def shout_again(text: str) -> str:
                    return text
                """,
        },
    )
    with (
        pytest.raises(
            BindingError,
            match=r"'shout' is defined twice in the project: twin\.shout_again and activities\.shout",
        ),
        load_worker(root),
    ):
        pass


_FACTORY = """
    from temporalio import activity

    def make(name: str):
        @activity.defn(name=name)
        async def fetch(url: str) -> str:
            return name + url

        return fetch
    """


def _activity_names(fns: list[Any]) -> list[str]:
    return [getattr(f, "__temporal_activity_definition").name for f in fns]


def test_activities_a_factory_of_the_project_makes_are_registered(tmp_path: Path) -> None:
    """An activity a function makes (``fetch_a = make("fetch_a")``) has the factory's module
    as its own, and that module never binds it: the scan used to pass it over, the worker
    started, and the first job failed on an activity not registered. It is found where a
    project module binds it, after those defined at module level, once however often it is
    bound; two made with one name are refused, naming both."""
    worker_py = "import made\nfrom kontra import serve\nfrom workflows import Flow\nserve(Flow)\n"
    made = """
        from factory import make

        fetch_a = make("fetch_a")
        fetch_b = make("fetch_b")
        again = fetch_a
        """
    root = _project(tmp_path / "p", worker_py, **{"factory.py": _FACTORY, "made.py": made})
    with load_worker(root) as worker:
        found = worker.options["activities"]
        assert _activity_names(found) == ["shout", "fetch_a", "fetch_b"]
        assert found[1] is sys.modules["made"].fetch_a
    twice = made + 'fetch_c = make("fetch_a")\n'
    root = _project(tmp_path / "q", worker_py, **{"factory.py": _FACTORY, "made.py": twice})
    with (
        pytest.raises(
            BindingError,
            match=r"'fetch_a' is defined twice in the project: made\.fetch_a \(made by "
            r"factory\.make\.<locals>\.fetch\) and made\.fetch_c",
        ),
        load_worker(root),
    ):
        pass


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


# --------------------------------------------------------------------------- plugins
#
# A seller's own Temporal plugins pass through as temporalio's ``plugins=`` keyword (decisions
# I29): Kontra sniffs no import. Temporal's OpenAI Agents integration is the seller's
# dependency and their plugin; on the client, before Kontra's, its converter is the one the
# worker gets (``test_plugin.py`` checks the order against a running worker).


def test_plugins_is_a_temporal_worker_keyword_passed_verbatim(tmp_path: Path) -> None:
    from temporalio.plugin import SimplePlugin

    assert "plugins" in TEMPORAL_KWARGS
    assert "mcp_servers" not in inspect.signature(kontra.serve.serve).parameters
    root = _project(
        tmp_path / "p",
        """
        from kontra import serve
        from theirs import plugin
        from workflows import Flow

        serve(Flow, plugins=[plugin])
        """,
        **{
            "theirs.py": "from temporalio.plugin import SimplePlugin\n\nplugin = SimplePlugin('theirs')\n"
        },
    )
    with load_worker(root) as worker:
        [plugin] = worker.options["plugins"]
        assert plugin is sys.modules["theirs"].plugin
        assert isinstance(plugin, SimplePlugin)


def test_importing_agents_installs_nothing(tmp_path: Path) -> None:
    """A workflow that imports OpenAI's ``agents`` gets no plugin it did not ask for: the
    integration is the seller's, in ``plugins=``."""
    workflows = _FILES["workflows.py"].replace(
        "            from models import Inp, Out\n",
        "            from models import Inp, Out\n            import agents  # noqa: F401\n",
    )
    root = _project(tmp_path / "p", ONE_LINE, **{"workflows.py": workflows})
    with load_worker(root) as worker:
        assert "plugins" not in worker.options


AGENTS = importlib.util.find_spec("temporalio.openai_agents") is not None


@pytest.mark.temporal
@pytest.mark.skipif(not AGENTS, reason="temporalio-openai-agents (the dev group's) is missing")
async def test_an_agent_workflow_with_an_activity_tool_runs_on_the_plugin(
    dev_server: DevServer,
) -> None:
    """``OpenAIAgentsPlugin`` on the client (where ``serve()`` routes a client plugin), Kontra's
    sandbox, and no converter picked by hand: the plugin's own ``OpenAIPayloadConverter`` is
    the client's, the agent loop runs in the workflow, the model call and
    ``activity_as_tool`` as activities. With temporalio's strict ``PydanticPayloadConverter``
    instead, the model activity fails to decode its input (``ActivityModelInput``, 71
    validation errors) and retries forever. The model is Temporal's test model, so nothing
    leaves the machine."""
    import temporalio.worker
    from temporalio.client import Client
    from temporalio.openai_agents import (
        ModelActivityParameters,
        OpenAIAgentsPlugin,
        OpenAIPayloadConverter,
    )
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
    base["plugins"] = [plugin]
    client = Client(**base)
    assert client.data_converter.payload_converter_class is OpenAIPayloadConverter
    queue = f"agents-{uuid.uuid4().hex[:8]}"
    async with temporalio.worker.Worker(
        client,
        task_queue=queue,
        workflows=[Triage],
        activities=[lookup],
        workflow_runner=workflow_runner(),  # Kontra's sandbox: kontra passed through
    ):
        out = await client.execute_workflow(
            Triage.run, "a.example", id=f"triage-{uuid.uuid4().hex[:8]}", task_queue=queue
        )
    assert out == "a.example is up"
