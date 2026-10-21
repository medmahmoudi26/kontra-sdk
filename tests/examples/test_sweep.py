"""``examples/sweep`` end to end: its own ``worker.py`` serving as ``acme``, hired by a buyer
over the local sites (:mod:`.sites`). Pages that miss headers, or do not answer, are scored;
a sweep of pages that send every header is all clear. The report is downloaded and read,
and the job's own history, read back with :func:`kontra.graph.parse`, is the path through
the README's graph that the job took: one fan-out group, then the per-URL checks -- one
activity node, in one parallel group, since fewer URLs than the workflow lets open start in
one workflow task -- then ``score`` or ``all_clear``. The checks are activities of the job's
own workflow (vanilla Temporal, no child workflow)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from kontra import graph
from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m
from kontra.types import Artifact

from .sites import Sites
from .support import START, Market

pytestmark = [pytest.mark.temporal, pytest.mark.asyncio(loop_scope="package")]

OFFERING = "acme/sweep"
CHECK = "check_headers"
ALL = [
    "strict-transport-security",
    "content-security-policy",
    "x-content-type-options",
    "x-frame-options",
    "referrer-policy",
]
#: The test sites' robots.txt, as the report keeps it: each path once, comments cut.
DISALLOW = ["/admin/", "/backup.zip", "/"]
CONTACT = ["mailto:security@example.test", "https://example.test/report"]


def _origin(url: str) -> str:
    return url.rstrip("/")


async def _job_graph(market: Market, job_id: str) -> graph.Graph:
    """The job's run, as the seller's namespace holds it (it starts no child)."""
    client = market.seller_client()
    workflow_id = job_workflow_id(job_id)
    history = await client.get_workflow_handle(workflow_id).fetch_history()
    return graph.parse(history.to_json_dict(), workflow_id=workflow_id)


def _edges(g: graph.Graph) -> set[tuple[str, str]]:
    return {(e["from"], e["to"]) for e in g["edges"]}


def _path(branch: str) -> set[tuple[str, str]]:
    """The README graph's edges a job that took ``branch`` walks."""
    return {
        ("start", "plan"),
        ("plan", "fetch_robots"),
        ("plan", "fetch_security_txt"),
        ("fetch_robots", CHECK),
        ("fetch_security_txt", CHECK),
        (CHECK, branch),
        (branch, "write_report"),
        ("write_report", "end"),
    }


async def _report(artifact: Artifact, tmp_path: Path) -> dict[str, Any]:
    assert artifact.content_type == "application/json"
    path = await artifact.download(tmp_path / "report.json")  # verifies size and sha256
    return json.loads(path.read_text())


async def test_sweep_end_to_end(market: Market, tmp_path: Path) -> None:
    sites: Sites = market.sites
    buyer = await market.buyer()
    refused = sites.refused()
    urls = [
        sites.secure("/hardened"),  # every header: not flagged
        sites.plain("/hardened"),
        sites.secure("/partial"),  # no HSTS on HTTPS: B
        sites.plain("/partial"),  # frame-ancestors stands in for X-Frame-Options; no HSTS needed
        sites.secure("/page/1"),  # nothing: F
        sites.plain("/page/2"),  # nothing, and HSTS does not count on HTTP: F all the same
        sites.redirect("/hardened", name="secure"),  # the final response is what is checked
        sites.secure("/hardened#top"),  # the first URL again
        refused,
    ]
    q = await buyer.k.quote(OFFERING, urls=urls)
    assert (q.ok, q.offering, q.price, q.currency) == (
        True,
        "acme/sweep@0.1.0",
        Decimal("0.02"),  # 9 x 0.002, half up
        "USD",
    )
    job = await q.hire(max_price="0.02")
    result = await job.result(timeout=120)

    assert result.to_dict()["summary"] == {
        "grade": "F",
        "urls": 8,  # the repeated URL once
        "flagged": 3,
        "unreachable": 1,
        "sites": 3,
        "without_security_txt": 2,  # plain HTTP has none; the refused site answered nothing
    }
    report = await _report(result.report, tmp_path)
    assert report["summary"] == result.to_dict()["summary"]
    assert (
        report["pages"]
        == [  # worst first
            {"url": sites.secure("/page/1"), "grade": "F", "missing": ALL},
            {"url": sites.plain("/page/2"), "grade": "F", "missing": ALL[1:]},
            {"url": sites.secure("/partial"), "grade": "B", "missing": ALL[:1]},
        ]
    )
    assert report["unreachable"] == [{"url": refused, "error": "ConnectError"}]
    assert list(report["advice"]) == ALL
    assert report["advice"]["x-content-type-options"] == "nosniff"
    secure, plain = _origin(sites.secure()), _origin(sites.plain())
    assert report["robots"] == [
        {"site": secure, "status": 200, "disallow": DISALLOW, "error": None},
        {"site": plain, "status": 200, "disallow": DISALLOW, "error": None},
        {"site": _origin(refused), "status": None, "disallow": [], "error": "ConnectError"},
    ]
    assert [(s["site"], s["status"], s["contact"]) for s in report["security_txt"]] == [
        (secure, 200, CONTACT),
        (plain, 404, []),  # RFC 9116: served over HTTPS
        (_origin(refused), None, []),
    ]
    assert report["security_txt"][0]["expires"] == "2031-01-01T00:00:00.000Z"

    g = await _job_graph(market, job.job_id)
    assert _edges(g) == _path("score")
    groups = {n["id"]: n["group"] for n in g["nodes"]}
    assert groups["fetch_robots"] == groups["fetch_security_txt"]  # one parallel group
    assert not [n for n in g["nodes"] if n["kind"] == "child"]
    [check] = [n for n in g["nodes"] if n["name"] == CHECK]
    assert (check["id"], check["kind"], check["label"]) == (CHECK, "activity", CHECK)

    view = await job.accept()
    assert (view.state, view.escrow) == (m.JobState.COMPLETED, m.Escrow.RELEASED)
    bal = await buyer.balance()
    assert (bal.available_minor, bal.held_minor) == (START - 2, 0)


async def test_a_sweep_that_finds_nothing_is_all_clear(market: Market, tmp_path: Path) -> None:
    sites: Sites = market.sites
    buyer = await market.buyer()
    urls = [sites.secure("/hardened"), sites.plain("/hardened"), sites.plain("/partial")]
    job = await buyer.k.hire(OFFERING, max_price="0.01", urls=urls)
    result = await job.result(timeout=120)
    assert result.to_dict()["summary"] == {
        "grade": "A",
        "urls": 3,
        "flagged": 0,
        "unreachable": 0,
        "sites": 2,
        "without_security_txt": 1,
    }
    report = await _report(result.report, tmp_path)
    assert (report["pages"], report["unreachable"], report["advice"]) == ([], [], {})
    assert [r["disallow"] for r in report["robots"]] == [DISALLOW, DISALLOW]
    assert _edges(await _job_graph(market, job.job_id)) == _path("all_clear")
