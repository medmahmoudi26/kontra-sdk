"""End to end (decisions I38, kontra-sdk#63): what a buyer receives is what the published
contract describes, for a workflow entry and for a bound handler alike.

``tests/serve/serialized_sellers.py`` sells one output with every way a pydantic model
serializes otherwise than it validates -- a computed field, an excluded one, a serialization
alias, a type-changing serializer, defaults of each kind -- once as a workflow and once as an
agent's handler. A real hire of each, through the fake gate and a Temporal dev server,
delivers the model's ``model_dump(mode="json", by_alias=True)``: it validates against the
catalog's contract (what ``kontra.hire`` checks every delivery against), reads back through
``kontra.hire``'s decoding, and parses as the type a buyer generates from that contract.
Before I38 the contract was the validation schema: the computed ``area`` was a member it
refused, ``secret`` one it required, and the handler failed every job (its reply lacked the
excluded field). A workflow entry on a client whose payload converter leaves unset fields
out (OpenAI Agents' does) delivers them all the same: the job's result is the model's
serialization, not the converter's options'."""

from __future__ import annotations

import textwrap
import uuid
from pathlib import Path
from typing import Any, Literal

import pytest
from pydantic import BaseModel, ConfigDict

from kontra.cli._project import load_worker
from kontra.contract import InputValidator, validate_document
from kontra.hire._record import wrap  # pyright: ignore[reportPrivateUsage]
from kontra.testing import Marketplace
from kontra.testing.devserver import DevServer

from .serialized_sellers import Order, make_delivery
from .support import MARKER, Buyer, write_offering

pytest.importorskip("temporal_agent_harness", reason="the dev group's temporal-agent-harness")

_WORKFLOW = """
from kontra import serve
from tests.serve.serialized_sellers import DeliverFlow, quote

serve(DeliverFlow, quote=quote)
"""
_UNSET = """
from kontra import serve
from tests.serve.serialized_sellers import DeliverFlow, quote, unset_json

serve(DeliverFlow, quote=quote, plugins=[unset_json])
"""
_HANDLER = """
from temporal_agent_harness.plugin import AgentHarnessPlugin

from kontra import serve
from tests.serve.serialized_sellers import DeliverAgent, quote

serve(DeliverAgent.deliver, quote=quote, plugins=[AgentHarnessPlugin()])
"""


def _project(root: Path, worker: str) -> Path:
    path = write_offering(root)
    path.rename(root / "offering.yaml")
    (root / "worker.py").write_text(textwrap.dedent(worker).lstrip())
    return root


class _Pulled(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PulledLeaf(_Pulled):
    v: int


class PulledDog(_Pulled):
    kind: Literal["dog"]
    good: bool


class PulledCat(_Pulled):
    kind: Literal["cat"]
    lives: int


class PulledDelivery(_Pulled):
    """The output type a buyer generates from the contract (``kontra pull``): its members,
    all of them required, and their JSON types -- nothing of the seller's model."""

    w: int
    h: int
    area: int
    out_label: str
    at: int
    n: int
    tags: list[str]
    note: str | None
    leaf: PulledLeaf
    pet: PulledCat | PulledDog
    color: Literal["red"]


def test_both_entry_kinds_publish_one_contract(tmp_path: Path) -> None:
    """The output a workflow returns and the one a handler replies with are the same
    model, so the two offerings describe the same delivery."""
    schemas: list[dict[str, Any]] = []
    for name, worker in (("workflow", _WORKFLOW), ("handler", _HANDLER)):
        with load_worker(_project(tmp_path / name, worker)) as project:
            [bound] = project.offerings
            schemas.append(validate_document(bound.document()).output_schema)
    assert schemas[0] == schemas[1]
    output = schemas[0]
    assert sorted(output["required"]) == sorted(PulledDelivery.model_fields)
    assert "secret" not in output["properties"]


@pytest.mark.temporal
@pytest.mark.parametrize(
    "worker", [_WORKFLOW, _UNSET, _HANDLER], ids=["workflow", "exclude_unset", "handler"]
)
async def test_a_delivered_result_is_what_the_published_contract_describes(
    dev_server: DevServer, tmp_path: Path, worker: str
) -> None:
    value = {"w": 2, "h": 3, "label": MARKER, "at": "2026-10-20T12:00:00Z"}
    with load_worker(_project(tmp_path / "p", worker)) as project:
        [bound] = project.offerings
        async with Marketplace(dev_server=dev_server) as mp:
            acme = await mp.party("acme")
            bob = await mp.party(f"bob-{uuid.uuid4().hex[:6]}", balance="100.00")
            async with mp.serve(project, acme):
                entry = await bob.gate.get_offering(bound.key)
                buyer = Buyer(mp, bob)
                hired = await buyer.hire(entry, value)
                handle = await buyer.start(hired, value)
                out = await handle.result()

    info = validate_document(entry.document)  # the catalog's, as a buyer reads it
    InputValidator(info).validate_output(out)
    expected = make_delivery(Order.model_validate(value))
    assert out == expected.model_dump(mode="json", by_alias=True)
    assert (out["area"], out["out_label"], out["at"], out["n"]) == (6, MARKER, 1792497600, 3)
    assert "secret" not in out
    assert wrap(out, info.output_schema).to_dict() == out
    assert PulledDelivery.model_validate(out).leaf == PulledLeaf(v=0)
