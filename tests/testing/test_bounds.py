"""The wire models' ceilings (seam ②) as a gate meets them, over HTTP: a body built to make
validation expensive is one error, and the largest legitimate error -- a contract error
naming a path as long as the document allows -- still reaches the client as an error
body, not a 500."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from kontra.gate_client import GateError
from kontra.gate_client import models as m

from .conftest import World
from .support import offering_doc

MiB = 1024 * 1024


async def _post(world: World, path: str, token: str, body: bytes) -> httpx.Response:
    async with httpx.AsyncClient(transport=world.transport, base_url=world.gate.base_url) as http:
        return await http.post(
            path,
            content=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )


async def test_a_megabyte_of_bad_etags_is_one_error(world: World) -> None:
    alice = await world.party()
    body = {"upload_id": "u", "etags": [1] * (MiB // 2), "ciphertext_sha256": "0" * 64}
    resp = await _post(
        world,
        f"/v1/artifacts/{'0' * 32}/complete",
        await alice.access_token(),
        json.dumps(body).encode(),
    )
    assert resp.status_code == 422
    err = m.ErrorBody.model_validate_json(resp.content)
    assert err.code == "invalid_request"
    assert err.detail is not None
    assert [(e["loc"], e["type"]) for e in err.detail["errors"]] == [
        (["body", "etags"], "too_long")
    ]
    # Under the cap, every item bad: still one error, not one per item.
    bad = {**body, "etags": ["\x01" + "e" * 90] * m.MAX_PARTS}
    resp = await _post(
        world,
        f"/v1/artifacts/{'0' * 32}/complete",
        await alice.access_token(),
        json.dumps(bad).encode(),
    )
    assert resp.status_code == 422
    err = m.ErrorBody.model_validate_json(resp.content)
    assert err.detail is not None
    assert [(e["loc"], e["type"]) for e in err.detail["errors"]] == [
        (["body", "etags"], "value_error")
    ]


async def test_megabytes_of_stray_keys_are_one_error(world: World) -> None:
    alice = await world.party()
    body: dict[str, Any] = {"offering": "acme/enrich", "input": {"b64": "AAAA"}}
    body.update((f"k{i}", 0) for i in range(400_000))
    resp = await _post(world, "/v1/quotes", await alice.access_token(), json.dumps(body).encode())
    assert resp.status_code == 422
    err = m.ErrorBody.model_validate_json(resp.content)
    assert err.detail is not None
    assert len(err.detail["errors"]) == 1
    assert err.message == "body: Value error, unknown field(s): 'k0', 'k1', 'k2' and 399997 more"


async def test_a_megabyte_long_stray_key_is_an_error_body_not_a_500(world: World) -> None:
    """The key would be in the error's location, and from there in the message, past what
    an error body holds: it is named by its first characters only."""
    alice = await world.party()
    body = {"offering": "acme/enrich", "input": {"b64": "AAAA"}, "x" * MiB: 0}
    resp = await _post(world, "/v1/quotes", await alice.access_token(), json.dumps(body).encode())
    assert resp.status_code == 422
    err = m.ErrorBody.model_validate_json(resp.content)
    assert err.message == f"body: Value error, unknown field(s): '{'x' * 40}'"


async def test_the_longest_contract_error_is_still_an_error_body(world: World) -> None:
    """A definition name as long as the 256 KiB canonical document allows: the error names
    its path, and the answer is still a well-formed 422 the client decodes."""
    alice = await world.party()
    assert alice.profile.public_key is not None
    doc = offering_doc(alice.profile.party, alice.profile.public_key)
    doc["$defs"]["d" * 200_000] = {"type": "object"}
    with pytest.raises(GateError) as e:
        await alice.gate.publish(doc)
    assert (e.value.status, e.value.code) == (422, "invalid_contract")
    assert len(e.value.message) > 200_000
    assert e.value.detail == {"path": "$defs." + "d" * 200_000}
