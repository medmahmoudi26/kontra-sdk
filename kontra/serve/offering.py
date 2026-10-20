"""``offering(...)``: bind an entry and a quote function to an ``offering.yaml`` (PRD §6.5).

The yaml owns the facts (name, version, limits); the code owns the binding (D11). This
module checks the binding *when worker.py is imported*, so ``kontra serve`` and
``kontra publish`` both fail on the first line that is wrong, with a fix-it, instead of on
the first job. The entry is one of two things (decisions I29):

* a workflow class: a ``@workflow.defn`` class in an importable module whose ``run`` takes
  exactly one pydantic model and returns one (a Nexus input and output are objects);
* a bound handler: one ``@agent.accepts`` method of an agent written with Temporal's agent
  harness (``serve(ReconAgent.sweep)``), whose class is found from the function itself and
  checked by construct-and-compare; its input and output models are read from its own type
  hints (an ``Annotated[...]`` parameter the harness injects is not the input). The job runs
  one turn of the agent (:mod:`kontra.serve.turn`). Kontra never imports the harness: it
  reads what the decorators stamp, and only to cross-check.

Then, for either:

* ``quote`` is ``async def quote(inp: <that same input model>) -> Quote``;
* the contract derives (types-first) or validates (schema-first, ``contract=``) -- the
  document is built once with a throwaway key, so a type nexgen cannot express is refused
  here, not at publish time. Derivation is :mod:`kontra.contract`'s, from the two models;
  this module only introspects.

The engine the offering runs on (seam ⑥, decisions I32) is derived from the entry's type
(:func:`engine_for`): everything ``kontra.serve`` binds is ``temporalio`` code, so it is
``temporal``. An explicit ``engine=`` must agree; it is published beside the contract
(``PublishRequest.engine``), never in it, so no contract hash depends on it.

Paths are relative to the module that calls :func:`offering` (``worker.py``), so the worker
runs from any directory.
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal, cast, get_origin, get_type_hints

from pydantic import BaseModel
from temporalio import workflow as temporal_workflow

from kontra.contract import (
    ContractError,
    InputValidator,
    OfferingSpec,
    Quote,
    contract_hash,
    derive_document,
    from_yaml,
    load_offering,
    merge_document,
)
from kontra.contract.names import OfferingId
from kontra.gate_client.models import DEFAULT_ENGINE, ENGINE_NAMES, EngineName
from kontra.serve.turn import AGENT_ACCEPTS, AGENT_HANDLERS, AGENT_INIT_DATA, TurnJob, TurnWorkflow

__all__ = [
    "BindingError",
    "BoundOffering",
    "EntryKind",
    "QuoteFunction",
    "engine_for",
    "offering",
]

#: ``async def quote(inp: Model) -> Quote``.
QuoteFunction = Callable[[Any], Awaitable[Quote]]
#: What an offering is bound to: a workflow class, or one handler of an agent.
EntryKind = Literal["workflow", "handler"]


class BindingError(TypeError):
    """``offering(...)`` was given an entry or quote function of the wrong shape."""


#: The engine each kind of entry runs on: a ``@workflow.defn`` class and an agent's
#: ``@agent.accepts`` handler are both temporalio code (decisions I29).
_ENGINES: Mapping[EntryKind, EngineName] = {"workflow": "temporal", "handler": "temporal"}


def engine_for(kind: EntryKind, engine: str | None = None) -> EngineName:
    """The engine an entry of ``kind`` runs on (seam ⑥, decisions I32): derived from the
    entry's type, as :func:`serve` picks the adapter. ``engine``: an explicit choice, which
    must be an engine's name and agree with the entry (a ``ValueError`` otherwise: the code
    decides what it runs on, a keyword cannot move it)."""
    derived = _ENGINES[kind]
    if engine is None:
        return derived
    if engine not in ENGINE_NAMES:
        raise ValueError(
            f"engine={engine!r} is not an engine; the engines are {list(ENGINE_NAMES)}"
        )
    if engine != derived:
        raise ValueError(
            f"kontra.serve binds temporalio code, which runs on the {derived!r} engine; got "
            f"engine={engine!r}"
        )
    return derived


def _caller_dir(depth: int) -> Path:
    """The directory of the module ``depth`` frames up (``worker.py``), else the cwd (a REPL)."""
    frame = sys._getframe(depth + 1)
    filename = frame.f_globals.get("__file__")
    return Path(filename).resolve().parent if isinstance(filename, str) else Path.cwd()


def _resolve(path: str | Path, base: Path) -> Path:
    p = Path(path).expanduser()
    return p if p.is_absolute() else base / p


def _model(tp: object) -> type[BaseModel] | None:
    return tp if isinstance(tp, type) and issubclass(tp, BaseModel) else None


def _workflow_types(
    cls: object,
) -> tuple[Callable[..., Awaitable[Any]], type[BaseModel], type[BaseModel]]:
    """``(run_fn, input model, output model)`` of a workflow class, or a fix-it."""
    name = getattr(cls, "__name__", repr(cls))
    defn = temporal_workflow._Definition.from_class(cls) if isinstance(cls, type) else None
    if defn is None:
        raise BindingError(
            f"workflow={name} is not a Temporal workflow class; decorate the class with "
            "@workflow.defn and its entry point with @workflow.run"
        )
    if defn.name is None:
        raise BindingError(
            f"workflow={name} is a dynamic workflow; an offering's workflow has a fixed name "
            "and one typed input"
        )
    if getattr(cls, "__module__", "__main__") == "__main__":
        raise BindingError(
            f"workflow={name} is defined in __main__; move it to an importable module "
            "(workflows.py) -- Temporal's sandbox re-imports workflows by module name"
        )
    run_fn = defn.run_fn
    params = list(inspect.signature(run_fn).parameters.values())[1:]  # drop self
    if len(params) != 1 or params[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise BindingError(
            f"{name}.run must take exactly one argument, a pydantic model "
            f"(async def run(self, inp: {name}Input) -> {name}Output); it takes {len(params)}"
        )
    try:
        hints = get_type_hints(run_fn)
    except Exception as exc:  # a forward reference that does not resolve
        raise BindingError(f"cannot read the type hints of {name}.run: {exc}") from exc
    inp = _model(hints.get(params[0].name))
    out = _model(hints.get("return"))
    if inp is None:
        raise BindingError(
            f"{name}.run({params[0].name}) must be annotated with a pydantic model, e.g. "
            f"class {name}Input(BaseModel): ...; the contract is derived from it"
        )
    if out is None:
        raise BindingError(
            f"{name}.run must declare a pydantic model as its return type "
            f"(-> {name}Output); a Nexus output is an object"
        )
    return run_fn, inp, out


@dataclass(frozen=True)
class _Entry:
    """What :func:`_entry` read from an entry."""

    kind: EntryKind
    #: The workflow the job runs: the bound workflow, or Kontra's turn wrapper.
    workflow: type
    run_fn: Callable[..., Awaitable[Any]]
    input_type: type[BaseModel]
    output_type: type[BaseModel]
    agent: type | None = None
    agent_type: str | None = None
    handler: str | None = None


def _owner(fn: Callable[..., Any]) -> type:
    """The class an ``@agent.accepts`` function was defined in, found from the function's
    module and qualified name and accepted only if that class's attribute *is* the function
    (construct-and-compare: nothing is taken from a name alone)."""
    name = getattr(fn, "__name__", repr(fn))
    qualname = str(getattr(fn, "__qualname__", ""))
    module_name = str(getattr(fn, "__module__", ""))
    parts = qualname.split(".")
    if "<locals>" in parts:
        raise BindingError(
            f"{qualname} is defined inside a function; move the agent to an importable module "
            "(agents.py) -- Temporal's sandbox re-imports workflows by module name"
        )
    if len(parts) != 2:
        raise BindingError(
            f"serve() takes an agent's handler as `serve(Agent.{name})`, a method of a "
            f"module-level class; {qualname!r} is not one"
        )
    if module_name == "__main__":
        raise BindingError(
            f"{qualname} is defined in __main__; move the agent to an importable module "
            "(agents.py) -- Temporal's sandbox re-imports workflows by module name"
        )
    module = sys.modules.get(module_name)
    owner = getattr(module, parts[0], None) if module is not None else None
    if not isinstance(owner, type) or getattr(owner, fn.__name__, None) is not fn:
        raise BindingError(
            f"{qualname} cannot be found again as {module_name}.{qualname}; pass the handler "
            "of a module-level agent class, as `serve(Agent.handler)`"
        )
    heirs = _inheritors(owner, fn)
    if heirs:
        # `Heir.handler is Owner.handler`: a function cannot say which class it was read
        # through, so the class it names is not necessarily the one the seller meant.
        names = ", ".join(f"{h.__module__}.{h.__qualname__}" for h in heirs)
        raise BindingError(
            f"{qualname} is inherited unchanged by {names}: `serve({heirs[0].__name__}."
            f"{fn.__name__})` and `serve({qualname})` are the same function, and Kontra "
            f"cannot tell which agent you mean. Override {fn.__name__} in each agent class "
            "that sells it (decorated with @agent.accepts; it may just call super()), so "
            "every agent has a handler of its own"
        )
    return owner


def _inheritors(owner: type, fn: Callable[..., Any]) -> list[type]:
    """The workflow classes below ``owner`` (an ``@agent.defn`` of their own) that expose
    ``fn`` itself, not an override of it."""
    found: list[type] = []
    seen: set[type] = set()
    pending = list(owner.__subclasses__())
    while pending:
        cls = pending.pop(0)
        if cls in seen:
            continue
        seen.add(cls)
        pending.extend(cls.__subclasses__())
        if "__temporal_workflow_definition" in vars(cls) and getattr(cls, fn.__name__, None) is fn:
            found.append(cls)
    return found


def _handler_types(fn: Callable[..., Any]) -> _Entry:
    """A bound handler's agent and models, or a fix-it."""
    cls = _owner(fn)
    label = f"{cls.__name__}.{fn.__name__}"
    defn = temporal_workflow._Definition.from_class(cls)
    if defn is None or defn.name is None:
        raise BindingError(
            f"{cls.__name__} is not an agent workflow with a fixed name; decorate it with "
            "@agent.defn (temporal-agent-harness). A handler an agent inherits from a base "
            f"class names the base: override {fn.__name__} in the agent class"
        )
    if not inspect.iscoroutinefunction(fn):
        raise BindingError(f"{label} must be an async handler")
    try:
        hints = get_type_hints(fn, include_extras=True)
    except Exception as exc:
        raise BindingError(f"cannot read the type hints of {label}: {exc}") from exc
    params = [
        p
        for p in list(inspect.signature(fn).parameters.values())[1:]  # drop self
        if get_origin(hints.get(p.name)) is not Annotated  # injected by the harness
    ]
    if len(params) != 1 or params[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise BindingError(
            f"{label} must take exactly one message, a pydantic model (async def "
            f"{fn.__name__}(self, req: Request) -> Reply); it takes {len(params)}"
        )
    inp = _model(hints.get(params[0].name))
    out = _model(hints.get("return"))
    if inp is None or out is None:
        raise BindingError(
            f"{label} must take a pydantic model and return one; the contract is derived from them"
        )
    handlers = getattr(cls, AGENT_HANDLERS, None)
    if isinstance(handlers, Mapping):
        declared = cast(Mapping[str, Any], handlers).get(fn.__name__)
        if declared is None:
            raise BindingError(f"{cls.__name__} declares no handler named {fn.__name__!r}")
        if (
            getattr(declared, "input_type", inp) is not inp
            or getattr(declared, "output_type", out) is not out
        ):
            raise BindingError(
                f"{label}: the agent declares other models for this handler than its type hints do"
            )
    init = getattr(cls, AGENT_INIT_DATA, None)
    if getattr(init, "required", False) is True:
        raise BindingError(
            f"{cls.__name__} requires init data (`data: Model` in its @agent.init), which a "
            "Kontra job cannot pass: it is not in the contract. Make it optional "
            "(`data: Model | None = None`)"
        )
    return _Entry(
        kind="handler",
        workflow=TurnWorkflow,
        run_fn=TurnWorkflow.run,
        input_type=inp,
        output_type=out,
        agent=cls,
        agent_type=defn.name,
        handler=fn.__name__,
    )


def _entry(entry: object) -> _Entry:
    """The binding of a workflow class or of a bound handler, or a fix-it."""
    if isinstance(entry, type):
        if getattr(entry, AGENT_HANDLERS, None) is not None:
            raise BindingError(
                f"{entry.__name__} is an agent: pass the handler that serves the offering, "
                f"`serve({entry.__name__}.<handler>)` -- a job is one turn of one handler"
            )
        run_fn, inp, out = _workflow_types(entry)
        return _Entry("workflow", entry, run_fn, inp, out)
    if inspect.isfunction(entry) and getattr(entry, AGENT_ACCEPTS, False) is True:
        return _handler_types(entry)
    raise BindingError(
        f"serve() takes a @workflow.defn class (serve(MyFlow)) or an agent's @agent.accepts "
        f"handler (serve(MyAgent.handler)), got {entry!r}"
    )


def _check_quote(fn: object, inp: type[BaseModel]) -> QuoteFunction:
    name = getattr(fn, "__name__", repr(fn))
    fix = f"async def {name if name.isidentifier() else 'quote'}(inp: {inp.__name__}) -> Quote"
    if not callable(fn):
        raise BindingError(f"quote={fn!r} is not a function; write {fix}")
    if not inspect.iscoroutinefunction(fn):
        raise BindingError(f"quote={name} must be async: {fix}")
    params = list(inspect.signature(fn).parameters.values())
    if len(params) != 1 or params[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise BindingError(f"quote={name} must take exactly one argument: {fix}")
    try:
        hints = get_type_hints(fn)
    except Exception as exc:
        raise BindingError(f"cannot read the type hints of quote={name}: {exc}") from exc
    if hints.get(params[0].name) is not inp:
        raise BindingError(
            f"quote={name} must take the workflow's input model {inp.__name__}, the same "
            f"class the workflow receives: {fix}"
        )
    ret = hints.get("return")
    if ret is not None and ret is not Quote:
        raise BindingError(f"quote={name} must return a kontra.serve.Quote: {fix}")
    return cast(QuoteFunction, fn)


class BoundOffering:
    """One offering version bound to its code: what ``Worker`` serves and ``kontra publish``
    publishes. Build it with :func:`offering`."""

    def __init__(
        self,
        *,
        spec: OfferingSpec,
        source: Path,
        workflow: type,
        run_fn: Callable[..., Awaitable[Any]],
        input_type: type[BaseModel],
        output_type: type[BaseModel],
        quote: QuoteFunction,
        contract_path: Path | None = None,
        nexus_document: Mapping[str, Any] | None = None,
        kind: EntryKind = "workflow",
        agent: type | None = None,
        agent_type: str | None = None,
        handler: str | None = None,
        engine: EngineName = DEFAULT_ENGINE,
    ) -> None:
        if (kind == "handler") != bool(agent is not None and agent_type and handler):
            raise ValueError("a handler binding names its agent, the agent's type and handler")
        #: The engine the offering runs on (seam ⑥): published beside the contract, never in
        #: it, and fixed per version by the catalog (decisions I32).
        self.engine: EngineName = engine_for(kind, engine)
        self.spec = spec
        self.source = source
        #: The workflow the job runs (``job-<id>``): the bound workflow, or Kontra's turn
        #: wrapper (:class:`~kontra.serve.turn.TurnWorkflow`) for a handler.
        self.workflow = workflow
        self.run_fn = run_fn
        self.input_type = input_type
        self.output_type = output_type
        self.quote = quote
        self.contract_path = contract_path
        #: ``"workflow"`` or ``"handler"`` (an agent's ``@agent.accepts`` method).
        self.kind: EntryKind = kind
        #: A handler's agent class, its Temporal workflow type, and the handler's name.
        self.agent = agent
        self.agent_type = agent_type
        self.handler = handler
        self._nexus_document = dict(nexus_document) if nexus_document is not None else None
        self._document: dict[str, Any] | None = None
        self._validator: InputValidator | None = None

    @property
    def offering_id(self) -> OfferingId:
        return self.spec.offering_id

    @property
    def streams(self) -> tuple[str, ...]:
        """The topics of the job's stream a buyer may read (``offering.yaml`` ``streams:``,
        sorted): catalog data, published beside the contract and never in its hash
        (decisions I31). A bound handler's are mirrored from its agent's stream."""
        return self.spec.streams

    @property
    def entry_workflow(self) -> type:
        """The seller's workflow class a worker serving this offering registers: the bound
        workflow, or a handler's agent. :class:`~kontra.plugin.KontraPlugin` claims the
        worker whose ``workflows`` hold it."""
        if self.agent is not None:
            return self.agent
        return self.workflow

    def job_input(self, inp: BaseModel) -> Any:
        """What the job's workflow is started with: the validated input itself, or, for a
        handler, the turn wrapper's :class:`~kontra.serve.turn.TurnJob`, whose payload is the
        input's JSON keyed by alias -- what the harness validates the message against, and
        what temporalio's pydantic converter writes for a workflow entry."""
        if self.agent_type is not None and self.handler is not None:
            payload = inp.model_dump(mode="json", by_alias=True)
            return TurnJob(self.agent_type, self.handler, payload)
        return inp

    @property
    def key(self) -> str:
        """``acme/enrich@1.2.0``."""
        return self.offering_id.key

    @property
    def schema_first(self) -> bool:
        return self._nexus_document is not None

    def document(self) -> dict[str, Any]:
        """The sealed contract document for this binding: derived from the workflow's
        types, or the hand-written document merged with offering.yaml.

        No key goes in. Spec v2 seals the party (the first component of ``name``) and
        nothing else, so a seller may add or revoke a recipient without republishing --
        which would be a new version, a new endpoint and a new task queue (decisions I7)."""
        if self._document is None:
            self._document = self.build_document()
        return self._document

    def build_document(self) -> dict[str, Any]:
        """:meth:`document`, built afresh and not remembered."""
        if self._nexus_document is not None:
            return merge_document(self.spec, self._nexus_document)
        return derive_document(self.spec, self.input_type, self.output_type)

    def contract_hash(self) -> str:
        """The hash the catalog, this worker and every hold token must agree on (§11.2)."""
        return contract_hash(self.document())

    def validator(self) -> InputValidator:
        """Validates ``execute``/``quote`` inputs against the served contract."""
        if self._validator is None:
            self._validator = InputValidator(self.document())
        return self._validator

    def __repr__(self) -> str:
        if self.agent is not None:
            entry = f"entry={self.agent.__name__}.{self.handler}"
        else:
            entry = f"workflow={self.workflow.__name__}"
        return f"offering({str(self.source)!r}, {entry}, key={self.key!r})"


def offering(
    path: str | Path,
    *,
    quote: QuoteFunction,
    entry: type | Callable[..., Any] | None = None,
    workflow: type | None = None,
    contract: str | Path | None = None,
    engine: str | None = None,
) -> BoundOffering:
    """Bind ``entry`` and ``quote`` to the offering in ``path`` (``offering.yaml``).

    ``entry`` is a ``@workflow.defn`` class or an agent's ``@agent.accepts`` handler
    (``Agent.handler``); ``workflow=`` is the same keyword for a workflow class, as worker.py
    files written before handlers spell it. ``path`` and ``contract`` are relative to the
    calling module's directory. ``contract`` is a hand-written Nexus document (YAML or JSON)
    for schema-first sellers (§11.1); without it the contract is derived from the entry's
    input and output models. ``engine``: what the offering runs on, derived from the entry
    when omitted (:func:`engine_for`; an explicit one must agree).
    """
    if (entry is None) == (workflow is None):
        raise TypeError("offering() binds one entry: pass entry= (or workflow=), not both")
    base = _caller_dir(1)
    source = _resolve(path, base)
    spec = load_offering(source)
    bound_entry = _entry(entry if entry is not None else workflow)
    chosen = engine_for(bound_entry.kind, engine)
    inp, out = bound_entry.input_type, bound_entry.output_type
    fn = _check_quote(quote, inp)
    nexus_doc: dict[str, Any] | None = None
    contract_path: Path | None = None
    if contract is not None:
        contract_path = _resolve(contract, base)
        try:
            text = contract_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ContractError("$", f"cannot read the contract {contract_path}: {exc}") from None
        nexus_doc = from_yaml(text, source=contract_path.name)
    bound = BoundOffering(
        spec=spec,
        source=source,
        workflow=bound_entry.workflow,
        run_fn=bound_entry.run_fn,
        input_type=inp,
        output_type=out,
        quote=fn,
        contract_path=contract_path,
        nexus_document=nexus_doc,
        kind=bound_entry.kind,
        agent=bound_entry.agent,
        agent_type=bound_entry.agent_type,
        handler=bound_entry.handler,
        engine=chosen,
    )
    # Fail at import, not at the first job: derive (or validate) the document once here.
    bound.build_document()
    return bound
