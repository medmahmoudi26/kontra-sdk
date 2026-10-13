"""``offering(...)``: bind a workflow and a quote function to an ``offering.yaml`` (PRD §6.5).

The yaml owns the facts (name, version, limits); the code owns the binding (D11). This
module checks the binding *when worker.py is imported*, so ``kontra serve`` and
``kontra publish`` both fail on the first line that is wrong, with a fix-it, instead of on
the first job:

* the workflow is a ``@workflow.defn`` class in an importable module whose ``run`` takes
  exactly one pydantic model and returns one (a Nexus input and output are objects);
* ``quote`` is ``async def quote(inp: <that same model>) -> Quote``;
* the contract derives (types-first) or validates (schema-first, ``contract=``) -- the
  document is built once with a throwaway key, so a type nexgen cannot express is refused
  here, not at publish time.

Paths are relative to the module that calls :func:`offering` (``worker.py``), so the worker
runs from any directory.
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, cast, get_type_hints

from pydantic import BaseModel
from temporalio import workflow as temporal_workflow

from kontra.codec.age import generate_identity
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

__all__ = ["BindingError", "BoundOffering", "QuoteFunction", "offering"]

#: ``async def quote(inp: Model) -> Quote``.
QuoteFunction = Callable[[Any], Awaitable[Quote]]


class BindingError(TypeError):
    """``offering(...)`` was given a workflow or quote function of the wrong shape."""


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
    ) -> None:
        self.spec = spec
        self.source = source
        self.workflow = workflow
        self.run_fn = run_fn
        self.input_type = input_type
        self.output_type = output_type
        self.quote = quote
        self.contract_path = contract_path
        self._nexus_document = dict(nexus_document) if nexus_document is not None else None
        self._documents: dict[str, dict[str, Any]] = {}
        self._validators: dict[str, InputValidator] = {}

    @property
    def offering_id(self) -> OfferingId:
        return self.spec.offering_id

    @property
    def key(self) -> str:
        """``acme/enrich@1.2.0``."""
        return self.offering_id.key

    @property
    def schema_first(self) -> bool:
        return self._nexus_document is not None

    def document(self, public_key: str) -> dict[str, Any]:
        """The sealed contract document for this binding and the seller's key: derived from
        the workflow's types, or the hand-written document merged with offering.yaml."""
        doc = self._documents.get(public_key)
        if doc is None:
            doc = self._documents[public_key] = self.build_document(public_key)
        return doc

    def build_document(self, public_key: str) -> dict[str, Any]:
        """:meth:`document`, built afresh and not remembered."""
        if self._nexus_document is not None:
            return merge_document(self.spec, self._nexus_document, public_key)
        return derive_document(self.spec, self.input_type, self.output_type, public_key)

    def contract_hash(self, public_key: str) -> str:
        """The hash the catalog, this worker and every hold token must agree on (§11.2)."""
        return contract_hash(self.document(public_key))

    def validator(self, public_key: str) -> InputValidator:
        """Validates ``execute``/``quote`` inputs against the served contract."""
        found = self._validators.get(public_key)
        if found is None:
            found = InputValidator(self.document(public_key))
            self._validators[public_key] = found
        return found

    def __repr__(self) -> str:
        return (
            f"offering({str(self.source)!r}, workflow={self.workflow.__name__}, key={self.key!r})"
        )


def offering(
    path: str | Path,
    *,
    workflow: type,
    quote: QuoteFunction,
    contract: str | Path | None = None,
) -> BoundOffering:
    """Bind ``workflow`` and ``quote`` to the offering in ``path`` (``offering.yaml``).

    ``path`` and ``contract`` are relative to the calling module's directory. ``contract`` is
    a hand-written Nexus document (YAML or JSON) for schema-first sellers (§11.1); without it
    the contract is derived from ``workflow``'s ``run`` signature.
    """
    base = _caller_dir(1)
    source = _resolve(path, base)
    spec = load_offering(source)
    run_fn, inp, out = _workflow_types(workflow)
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
        workflow=workflow,
        run_fn=run_fn,
        input_type=inp,
        output_type=out,
        quote=fn,
        contract_path=contract_path,
        nexus_document=nexus_doc,
    )
    # Fail at import, not at the first job: derive (or validate) once with a throwaway key.
    # The real document is built with the seller's key when the worker starts.
    bound.build_document(generate_identity()[1])
    return bound
