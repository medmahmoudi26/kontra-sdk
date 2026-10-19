"""``examples/sweep``'s payload bound (its README's "Limits"): every value crosses Temporal as
one payload, 2 MB at most on a Temporal without kontra's codec, so what the job carries is
bounded in bytes as the payload converter writes them -- the URLs by the contract (RFC 3986:
ASCII), each kept line of a site's files by its JSON size, whatever the site sends (control
characters are six bytes each as JSON, a quote two, an emoji four). Run in the example's
directory, as ``python worker.py`` would import it."""

from __future__ import annotations

import json
import subprocess
import sys

from .support import EXAMPLES

_SCRIPT = r"""
import asyncio, json
from pydantic import ValidationError
from temporalio.contrib.pydantic import pydantic_data_converter
import activities as A
from models import HEADERS, Findings, Flagged, Robots, SecurityTxt, SweepInput, Tally

convert = pydantic_data_converter.payload_converter
def size(value):
    [payload] = convert.to_payloads([value])
    return len(payload.data)

def files(ch):
    line = ch * 200
    robots = "".join(f"Disallow: /{i}{line}\n" for i in range(12))
    security = "".join(f"Contact: {i}{line}\n" for i in range(7)) + f"Expires: {line}\n"
    return robots, security

# The largest job: 200 URLs of 2,048 characters, each on a site of its own.
urls = [f"https://{'a' * 2030}{i:03d}.test" for i in range(200)]
urls = [u + "/" + "p" * (2048 - len(u) - 1) for u in urls]
plan = asyncio.run(A.plan(SweepInput(urls=urls)))
sizes, kept = {"plan": size(plan)}, []
for ch in ("x", "\x01", '"', "\\", "\U0001F600", "�"):
    robots_txt, security_txt = files(ch)
    disallow = A._disallowed(robots_txt)
    contact, expires = A._contacts(security_txt)
    kept += [*disallow, *contact, expires]
    found = Findings(
        tally=Tally(
            checked=len(plan.urls),
            flagged=[Flagged(url=u, missing=list(HEADERS)) for u in plan.urls],
        ),
        robots=[Robots(site=s, status=200, disallow=disallow) for s in plan.sites],
        security_txt=[
            SecurityTxt(site=s, status=200, contact=contact, expires=expires) for s in plan.sites
        ],
    )
    sizes[f"findings {ch!r}"] = size(found)
    sizes[f"report {ch!r}"] = size(asyncio.run(A.score(found)))
refused = []
for url in ("https://example.test/ü", 'https://example.test/"', "https://example.test/{x}"):
    try:
        SweepInput(urls=[url])
    except ValidationError:
        refused.append(url)
print(json.dumps({"sizes": sizes, "kept": kept, "refused": refused}))
"""


def _probe() -> dict[str, object]:
    out = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        cwd=EXAMPLES / "sweep",
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )
    return json.loads(out.stdout)


def test_the_largest_payload_stays_under_the_readmes_bound_whatever_the_sites_send() -> None:
    found = _probe()
    sizes = found["sizes"]
    assert isinstance(sizes, dict)
    assert max(sizes.values()) < 1_700_000 < 2 * 1024 * 1024, sizes  # the README's 1.7 MB
    kept = found["kept"]
    assert isinstance(kept, list)
    assert all(0 < len(json.dumps(line, ensure_ascii=False).encode()) - 2 <= 100 for line in kept)
    # A URL outside RFC 3986's characters is refused by the contract: percent-encode it.
    refused = found["refused"]
    assert isinstance(refused, list)
    assert len(refused) == 3
