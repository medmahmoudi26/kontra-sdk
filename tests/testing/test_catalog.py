"""The catalog on the fake gate (§11.4, decisions I1/I3): publish validates and needs a
fresh session of the owner, the gate assigns an opaque endpoint name once per version and
creates the endpoint through the control plane, and the catalog is the only map from an
endpoint back to its offering."""

from __future__ import annotations

from collections.abc import Awaitable
from datetime import timedelta

import pytest

from kontra._control import TemporalError
from kontra.contract import seal
from kontra.contract.names import is_endpoint_name
from kontra.escrow import ObservedStatus
from kontra.gate_client import GateError

from .conftest import World
from .support import json_payload, offering_doc, ok_quote, wire

#: A buyer key for a direct `GateClient.hire` call. Real hires make one per job
#: (`Profile.new_pending_job_identity`); a test that drives the route itself only needs a
#: well-formed recipient (decisions I7).
_A_BUYER_KEY = "age1e0q4lyexsefmh6hurumvgl3cmmsfnz4zyfhkd7lfrjcpqc2e250qs6j4el"


async def test_publish_assigns_an_opaque_endpoint_and_creates_it(world: World) -> None:
    acme = await world.party("acme")
    entry = await world.publish(acme)
    assert entry.offering == "acme/enrich@1.0.0"
    assert (entry.service, entry.task_queue) == ("acme.enrich", "acme/enrich@1.0.0")
    assert is_endpoint_name(entry.endpoint)
    assert "acme" not in entry.endpoint
    assert "enrich" not in entry.endpoint
    created = world.control.endpoints[entry.endpoint]
    assert (created.namespace, created.task_queue) == ("acme", "acme/enrich@1.0.0")
    assert entry.card["name"] == "acme/enrich"
    assert entry.card["supportedInterfaces"][0]["url"].endswith("/v1/catalog/acme/enrich/1.0.0")
    assert entry.recipients == [acme.profile.public_key]


async def test_republishing_a_version_keeps_its_endpoint(world: World) -> None:
    acme = await world.party("acme")
    first = await world.publish(acme)
    same = await world.publish(acme)
    assert same.endpoint == first.endpoint
    changed = await world.publish(acme, description="Upper-cases text, quickly")
    assert changed.endpoint == first.endpoint
    assert changed.contract_hash != first.contract_hash
    newer = await world.publish(acme, version="1.1.0")
    assert newer.endpoint != first.endpoint
    assert len(world.control.endpoints) == 2


async def test_a_version_with_open_jobs_keeps_its_contract(world: World) -> None:
    acme, buyer = await world.party("acme"), await world.party()
    entry = await world.publish(acme)
    world.control.responder = lambda p, h: ok_quote(100)
    q = await buyer.gate.quote(entry.name, wire(json_payload({"text": "x"})))
    assert q.quote_id is not None
    job = await buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)
    with pytest.raises(GateError) as e:
        await world.publish(acme, description="changed under a paying buyer")
    assert (e.value.status, e.value.code) == (409, "version_in_use")
    assert e.value.detail == {"open_jobs": 1, "contract_hash": entry.contract_hash}
    await world.publish(acme)  # the same contract again is fine
    world.control.observe_as("acme", f"job-{job.job_id}", ObservedStatus.FAILED)
    await world.gate.sweep()
    await world.publish(acme, description="changed once nobody is waiting")


async def test_publish_refusals(world: World) -> None:
    acme, mallory = await world.party("acme"), await world.party("mallory")
    doc = offering_doc("acme")
    with pytest.raises(GateError) as e:
        await mallory.gate.publish(doc)
    assert (e.value.status, e.value.code) == (403, "not_owner")
    edited = dict(doc, **{"x-kontra": dict(doc["x-kontra"], description="tampered")})
    with pytest.raises(GateError) as e:
        await acme.gate.publish(edited)
    assert (e.value.status, e.value.code) == (422, "invalid_contract")
    assert e.value.detail == {"path": "x-kontra.contract_hash"}
    outside = seal({**doc, "$defs": {**doc["$defs"], "EnrichInput": {"$ref": "https://evil/x"}}})
    with pytest.raises(GateError) as e:
        await acme.gate.publish(outside)
    assert e.value.code == "invalid_contract"
    world.gate.advance(timedelta(hours=1))  # the sign-in no longer counts as a confirmation
    await acme.gate.me()  # a new access token, on the gate's clock
    raw = world.bearer(await acme.access_token())  # no confirm hook: the gate's own answer
    with pytest.raises(GateError) as e:
        await raw.publish(doc)
    assert (e.value.status, e.value.code) == (403, "confirmation_required")
    assert (await acme.gate.publish(doc)).offering == "acme/enrich@1.0.0"  # a dev login confirms


async def test_a_temporal_outage_leaves_the_version_unpublished_until_retried(world: World) -> None:
    acme = await world.party("acme")
    world.control.fail_endpoints = TemporalError("frontend down")
    with pytest.raises(GateError) as e:
        await world.publish(acme)
    assert (e.value.status, e.value.code) == (503, "endpoint_unavailable")
    with pytest.raises(GateError):
        await world.anon().get_offering("acme/enrich")  # invisible until the endpoint exists
    world.control.fail_endpoints = None
    entry = await world.publish(acme)
    assert (await world.anon().get_offering("acme/enrich")).endpoint == entry.endpoint


async def test_an_endpoint_with_another_target_is_a_conflict(world: World) -> None:
    from kontra._control import EndpointConflict

    acme = await world.party("acme")
    world.control.fail_endpoints = EndpointConflict("taken")
    with pytest.raises(GateError) as e:
        await world.publish(acme)
    assert (e.value.status, e.value.code) == (409, "endpoint_conflict")


async def test_lookups_resolve_partial_versions_to_the_latest(world: World) -> None:
    acme = await world.party("acme")
    for v in ("1.0.0", "1.2.0", "1.10.3", "2.0.0"):
        await world.publish(acme, version=v)
    anon = world.anon()
    assert (await anon.get_offering("acme/enrich")).version == "2.0.0"
    assert (await anon.get_offering("acme/enrich@1")).version == "1.10.3"
    assert (await anon.get_offering("acme/enrich@1.2")).version == "1.2.0"
    assert (await anon.get_offering("acme/enrich@1.0.0")).version == "1.0.0"
    assert (await anon.card("acme/enrich@1"))["version"] == "1.10.3"
    for missing in ("acme/enrich@3", "acme/other", "acme/enrich@1.3"):
        with pytest.raises(GateError) as e:
            await anon.get_offering(missing)
        assert e.value.code == "offering_not_found"


async def test_a_retired_version_is_answered_at_its_exact_version_only(world: World) -> None:
    """As the gate's catalog.lookup: the exact version a job names is a historical fact, so
    it is answered retired too (decisions I22); nothing points a buyer at it again."""
    acme, buyer = await world.party("acme"), await world.party()
    await world.publish(acme)
    newer = await world.publish(acme, version="1.1.0")
    world.control.responder = lambda p, h: ok_quote(100)
    q = await buyer.gate.quote("acme/enrich@1.1.0", wire(json_payload({"text": "x"})))
    assert q.quote_id is not None
    world.gate.offerings["acme/enrich@1.1.0"].retired = True

    anon = world.anon()
    entry = await anon.get_offering("acme/enrich@1.1.0")
    assert (entry.retired, entry.contract_hash, entry.document) == (
        True,
        newer.contract_hash,
        newer.document,
    )
    assert (await anon.card("acme/enrich@1.1.0"))["version"] == "1.1.0"
    assert (await anon.get_offering("acme/enrich")).offering == "acme/enrich@1.0.0"
    assert (await anon.get_offering("acme/enrich@1")).offering == "acme/enrich@1.0.0"
    assert not (await anon.get_offering("acme/enrich@1.0.0")).retired
    assert [i.offering for i in (await anon.search("enrich")).items] == ["acme/enrich@1.0.0"]

    async def refusal(call: Awaitable[object]) -> tuple[int, str]:
        with pytest.raises(GateError) as e:
            await call
        return e.value.status, e.value.code

    assert await refusal(anon.get_offering("acme/enrich@1.1")) == (404, "offering_not_found")
    assert await refusal(anon.by_endpoint(newer.endpoint)) == (404, "endpoint_not_found")
    quote = buyer.gate.quote("acme/enrich@1.1.0", wire(json_payload({"text": "x"})))
    assert await refusal(quote) == (404, "offering_not_found")
    hire = buyer.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)
    assert await refusal(hire) == (409, "offering_unavailable")


async def test_a_retired_version_is_never_rewritten(world: World) -> None:
    """Its document is the only copy of the contract its jobs were hired under."""
    acme = await world.party("acme")
    entry = await world.publish(acme)
    world.gate.offerings[entry.offering].retired = True
    with pytest.raises(GateError) as e:
        await world.publish(acme, description="Upper-cases text, quickly")
    assert (e.value.status, e.value.code) == (409, "version_retired")
    assert e.value.detail == {"contract_hash": entry.contract_hash}
    again = await world.publish(acme)  # the same contract: nothing is rewritten
    assert (again.contract_hash, again.retired) == (entry.contract_hash, True)
    assert (await world.anon().get_offering(entry.offering)).contract_hash == entry.contract_hash


async def test_an_exact_version_never_confirmed_is_not_found(world: World) -> None:
    """A row whose endpoint was never created is an unfinished publish nobody hired."""
    acme = await world.party("acme")
    world.control.fail_endpoints = TemporalError("frontend down")
    with pytest.raises(GateError):
        await world.publish(acme)
    with pytest.raises(GateError) as e:
        await world.anon().get_offering("acme/enrich@1.0.0")
    assert (e.value.status, e.value.code) == (404, "offering_not_found")


@pytest.mark.parametrize(
    "path",
    [
        "/v1/catalog/acme/enrich/1.0",
        "/v1/catalog/acme/enrich/01",
        "/v1/catalog/acme/enrich/1.0.0.0",
        "/v1/catalog/acme/enrich@1/latest",
        "/v1/catalog/ACME/enrich/latest",
        "/v1/catalog/acme/enrich/latest%0A",
        "/v1/catalog/acme/enrich/" + "1" * 30,
    ],
)
async def test_catalog_paths_are_validated_segment_by_segment(world: World, path: str) -> None:
    import httpx

    acme = await world.party("acme")
    await world.publish(acme, version="1.0.0")
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        r = await http.get(path)
    if path.endswith("/1.0"):
        assert r.status_code == 200  # a partial version is a reference too
    else:
        assert (r.status_code, r.json()["code"]) == (404, "offering_not_found"), path


async def test_by_endpoint_is_an_exact_lookup(world: World) -> None:
    acme = await world.party("acme")
    entry = await world.publish(acme)
    anon = world.anon()
    assert (await anon.by_endpoint(entry.endpoint)).offering == entry.offering
    for other in ("k" + "0" * 32, entry.endpoint.upper(), entry.endpoint[:-1], "acme.enrich"):
        with pytest.raises(GateError) as e:
            await anon.by_endpoint(other)
        assert (e.value.status, e.value.code) == (404, "endpoint_not_found")


async def test_search_lists_latest_versions_with_reputation_and_pages(world: World) -> None:
    sellers = [await world.party(f"seller{i}") for i in range(3)]
    for s in sellers:
        await world.publish(s)
        await world.publish(s, version="1.1.0", tags=["text", "fast"])
    anon = world.anon()
    page = await anon.search(limit=2)
    assert [i.offering for i in page.items] == ["seller0/enrich@1.1.0", "seller1/enrich@1.1.0"]
    assert page.next_cursor is not None
    rest = await anon.search(cursor=page.next_cursor, limit=2)
    assert [i.offering for i in rest.items] == ["seller2/enrich@1.1.0"]
    assert rest.next_cursor is None
    assert next(i.offering for i in (await anon.search("FAST")).items) == "seller0/enrich@1.1.0"
    assert (await anon.search("nothing-like-this")).items == []
    assert page.items[0].seller.party == "seller0"
    assert page.items[0].seller.recipients == [sellers[0].profile.public_key]
    with pytest.raises(GateError) as e:
        await anon.search(cursor="garbage!")
    assert e.value.code == "invalid_cursor"


async def test_party_view_counts_reputation_without_self_hires(world: World) -> None:
    acme, buyer = await world.party("acme"), await world.party()
    entry = await world.publish(acme)
    world.control.responder = lambda p, h: ok_quote(100)

    async def job(who: object) -> str:
        session = buyer if who == "buyer" else acme
        q = await session.gate.quote(entry.name, wire(json_payload({"text": "x"})))
        assert q.quote_id is not None
        return (await session.gate.hire(q.quote_id, buyer_key=_A_BUYER_KEY)).job_id

    released, self_hire, failed = await job("buyer"), await job("acme"), await job("buyer")
    for j in (released, self_hire):
        world.control.observe_as("acme", f"job-{j}", ObservedStatus.COMPLETED, b"r")
    world.control.observe_as("acme", f"job-{failed}", ObservedStatus.FAILED)
    await world.gate.sweep()
    await buyer.gate.accept(released)
    await acme.gate.accept(self_hire)
    view = await world.anon().party("acme")
    assert (view.released, view.refunded, view.disputed) == (1, 1, 0)
    with pytest.raises(GateError) as e:
        await world.anon().party("nobody")
    assert e.value.code == "party_not_found"
