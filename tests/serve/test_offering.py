"""``offering(...)``: the binding is checked when worker.py is imported, with a fix-it."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel
from temporalio import workflow

from kontra.codec.age import generate_identity
from kontra.contract import ContractError, OfferingError, nexgen_view, to_yaml
from kontra.serve import BindingError, Quote, offering

from . import seller
from .flows import UpperFlow
from .models import Order, Shout
from .support import write_offering

HERE = Path(__file__).parent


def test_the_path_is_relative_to_the_calling_module(tmp_path: Path) -> None:
    cwd = Path.cwd()
    os.chdir(tmp_path)  # kontra serve may run from anywhere
    try:
        bound = seller.bind_relative()
    finally:
        os.chdir(cwd)
    assert bound.source == HERE / "offering.yaml"
    assert bound.key == "acme/upper@1.0.0"
    assert (bound.input_type, bound.output_type) == (Order, Shout)
    assert bound.workflow is UpperFlow
    assert bound.offering_id.task_queue == "acme/upper@1.0.0"
    assert not bound.schema_first


def test_a_missing_or_wrong_offering_yaml_is_refused(tmp_path: Path) -> None:
    with pytest.raises(OfferingError):
        offering(tmp_path / "nope.yaml", workflow=UpperFlow, quote=seller.quote)
    bad = tmp_path / "offering.yaml"
    bad.write_text(
        "name: acme/upper\nversion: 1.0.0\ndescription: x\nmax_duration: 1h\nworkflow: x\n"
    )
    with pytest.raises(OfferingError, match="never names code"):
        offering(bad, workflow=UpperFlow, quote=seller.quote)


def test_the_document_and_hash_follow_the_key(tmp_path: Path) -> None:
    bound = seller.bind(write_offering(tmp_path))
    a, b = generate_identity()[1], generate_identity()[1]
    assert bound.document(a) is bound.document(a)
    assert bound.document(a)["x-kontra"]["public_key"] == a
    assert bound.contract_hash(a) == bound.document(a)["x-kontra"]["contract_hash"]
    assert bound.contract_hash(a) != bound.contract_hash(b)
    v = bound.validator(a)
    v.validate({"text": "x"})
    with pytest.raises(ContractError):
        v.validate({"text": "x", "extra": 1})


# --------------------------------------------------------------------------- the workflow


class In(BaseModel):
    x: int


class Out(BaseModel):
    y: int


async def quote_in(inp: In) -> Quote:
    return Quote(price=1)


class NotAWorkflow:
    async def run(self, inp: In) -> Out:
        return Out(y=inp.x)


@workflow.defn(sandboxed=False)
class TwoArgs:
    @workflow.run
    async def run(self, a: In, b: In) -> Out:
        return Out(y=a.x)


@workflow.defn(sandboxed=False)
class ScalarIn:
    @workflow.run
    async def run(self, a: int) -> Out:
        return Out(y=a)


@workflow.defn(sandboxed=False)
class ScalarOut:
    @workflow.run
    async def run(self, a: In) -> int:
        return a.x


@workflow.defn(sandboxed=False)
class Fine:
    @workflow.run
    async def run(self, a: In) -> Out:
        return Out(y=a.x)


@pytest.mark.parametrize(
    ("wf", "message"),
    [
        (NotAWorkflow, "decorate the class with @workflow.defn"),
        (TwoArgs, "must take exactly one argument"),
        (ScalarIn, "must be annotated with a pydantic model"),
        (ScalarOut, "must declare a pydantic model as its return type"),
    ],
)
def test_a_workflow_of_the_wrong_shape_is_refused(tmp_path: Path, wf: Any, message: str) -> None:
    with pytest.raises(BindingError, match=message):
        offering(write_offering(tmp_path), workflow=wf, quote=quote_in)


@workflow.defn(sandboxed=False)
class Script:
    @workflow.run
    async def run(self, a: In) -> Out:
        return Out(y=a.x)


def test_a_workflow_in_main_is_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Script, "__module__", "__main__")  # as if defined in `python worker.py`
    with pytest.raises(BindingError, match="move it to an importable module"):
        offering(write_offering(tmp_path), workflow=Script, quote=quote_in)


# --------------------------------------------------------------------------- the quote


def quote_sync(inp: In) -> Quote:
    return Quote(price=1)


async def quote_other(inp: Out) -> Quote:
    return Quote(price=1)


async def quote_two(inp: In, extra: int) -> Quote:
    return Quote(price=1)


async def quote_bad_return(inp: In) -> int:
    return 1


@pytest.mark.parametrize(
    ("fn", "message"),
    [
        (quote_sync, "must be async"),
        (quote_other, "must take the workflow's input model In"),
        (quote_two, "must take exactly one argument"),
        (quote_bad_return, "must return a kontra.serve.Quote"),
        ("quote", "is not a function"),
    ],
)
def test_a_quote_of_the_wrong_shape_is_refused(tmp_path: Path, fn: Any, message: str) -> None:
    with pytest.raises(BindingError, match=message):
        offering(write_offering(tmp_path), workflow=Fine, quote=fn)


# --------------------------------------------------------------------------- the contract


class Loose(BaseModel):
    anything: Any


@workflow.defn(sandboxed=False)
class LooseFlow:
    @workflow.run
    async def run(self, a: Loose) -> Out:
        return Out(y=1)


async def quote_loose(inp: Loose) -> Quote:
    return Quote(price=1)


def test_a_type_the_contract_cannot_express_fails_at_import(tmp_path: Path) -> None:
    with pytest.raises(ContractError, match="anything"):
        offering(write_offering(tmp_path), workflow=LooseFlow, quote=quote_loose)


def test_schema_first(tmp_path: Path) -> None:
    """A hand-written Nexus document (contract=) is merged with offering.yaml; the same
    contract written by hand hashes like the derived one, and an edit moves the hash."""
    path = write_offering(tmp_path)
    derived = seller.bind(path)
    pk = generate_identity()[1]
    hand = tmp_path / "contract.yaml"
    hand.write_text(to_yaml(nexgen_view(derived.document(pk))))
    bound = offering(path, workflow=UpperFlow, quote=seller.quote, contract=hand)
    assert bound.schema_first
    assert bound.contract_path == hand
    assert bound.contract_hash(pk) == derived.contract_hash(pk)
    doc = nexgen_view(derived.document(pk))
    doc["$defs"]["Order"]["properties"]["text"]["maxLength"] = 10
    hand.write_text(to_yaml(doc))
    edited = offering(path, workflow=UpperFlow, quote=seller.quote, contract=hand)
    assert edited.contract_hash(pk) != derived.contract_hash(pk)
    with pytest.raises(ContractError):
        edited.validator(pk).validate({"text": "x" * 11})
    with pytest.raises(ContractError, match="cannot read the contract"):
        offering(path, workflow=UpperFlow, quote=seller.quote, contract="missing.yaml")
