"""The example sellers fetch (enrich) and probe (legacy-actor) the buyer's URLs from the
seller's machine, inside the seller's network: neither may be made to reach a destination
that is not public -- the cloud metadata service, loopback, the seller's LAN.

Each activity runs as plain Temporal code (``ActivityEnvironment``) in its example's own
directory, as ``python worker.py`` would import it, with the default settings (nothing but
public addresses) or the one the tests use for their sites (127.0.0.1 allowed). A resolver
can be scripted per host name, to give one name a public and a private address, or to
answer differently the second time (DNS rebinding)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from typing import Any

import pytest

from .sites import Sites
from .support import ALLOW_LOCAL_SITES, EXAMPLES

_SCRIPT = """
import asyncio, json, socket, sys
from temporalio.testing import ActivityEnvironment
import activities, models

urls, answers = json.loads(sys.argv[1]), json.loads(sys.argv[2])
asked = {}
real = asyncio.base_events.BaseEventLoop.getaddrinfo

async def scripted(self, host, port, *args, **kwargs):
    if host not in answers:
        return await real(self, host, port, *args, **kwargs)
    n = asked[host] = asked.get(host, 0) + 1
    found = answers[host][min(n, len(answers[host])) - 1]
    return [
        (socket.AF_INET6 if ":" in a else socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, port))
        for a in found
    ]

asyncio.base_events.BaseEventLoop.getaddrinfo = scripted

async def main():
    return await ActivityEnvironment().run(ACTIVITY, BATCH)

out = asyncio.run(main())
print(json.dumps({"out": [o.model_dump(mode="json") for o in out], "asked": asked}))
"""
CASES = {
    "enrich": dict(
        activity="activities.fetch_batch",
        batch="models.Batch(urls=urls, done=0, total=len(urls))",
    ),
    "legacy-actor": dict(
        activity="activities.head_batch",
        batch="models.Batch(targets=[models.Target(url=u) for u in urls], done=0, total=len(urls))",
    ),
}
#: Destinations never reached by default, as address literals or a name that resolves there.
FORBIDDEN = [
    "http://169.254.169.254/latest/meta-data/",  # the cloud metadata service
    "http://[fd00:ec2::254]/latest/meta-data/",  # its IPv6 address
    "http://10.0.0.1/",
    "http://172.16.0.1/",
    "http://192.168.1.1/",
    "http://100.64.0.1/",  # carrier-grade NAT
    "http://224.0.0.1/",  # multicast
    "http://[ff02::1]/",
    "http://0.0.0.0:{port}/",
    "http://[::]:{port}/",
    "http://127.0.0.1:{port}/",
    "http://[::1]:{port}/",
    "http://[::ffff:127.0.0.1]:{port}/",  # IPv4-mapped IPv6
    "http://[::ffff:169.254.169.254]/",
    "http://[64:ff9b::a9fe:a9fe]/",  # NAT64 of the metadata address
    "http://[fe80::1]/",
    "http://2130706433:{port}/",  # 127.0.0.1 as one decimal number: resolved, then checked
    "http://metadata.test/",  # a name that resolves to the metadata address
    "http://mixed.test/",  # a name with a public and a private address
]
NAMES = {
    "metadata.test": [["169.254.169.254"]],
    "mixed.test": [["93.184.215.14", "10.1.2.3"]],
}


def _run(
    name: str, urls: list[str], sites: Sites, *, allow: bool, answers: dict[str, Any] | None = None
) -> dict[str, Any]:
    env = {k: v for k, v in os.environ.items() if k not in ALLOW_LOCAL_SITES}
    env["SSL_CERT_FILE"] = str(sites.ca_file)
    if allow:
        env.update(ALLOW_LOCAL_SITES)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            _SCRIPT.replace("ACTIVITY", CASES[name]["activity"]).replace(
                "BATCH", CASES[name]["batch"]
            ),
            json.dumps(urls),
            json.dumps(answers or {}),
        ],
        cwd=EXAMPLES / name,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _errors(ran: dict[str, Any]) -> list[str | None]:
    return [row["error"] for row in ran["out"]]


@pytest.mark.parametrize("name", sorted(CASES))
def test_nothing_but_public_addresses_by_default(name: str, sites: Sites) -> None:
    """Every non-public destination is refused before any connection, as that URL's error
    row; the batch goes on. The test sites themselves (127.0.0.1) are refused too."""
    urls = [u.format(port=sites.port()) for u in FORBIDDEN] + [sites.plain("/"), sites.secure("/")]
    start = time.monotonic()
    ran = _run(name, urls, sites, allow=False, answers=NAMES)
    assert _errors(ran) == ["ForbiddenAddress"] * len(urls)
    assert all(row["status"] is None for row in ran["out"])
    assert time.monotonic() - start < 30  # refused, not left to time out connecting


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_setting_allows_what_it_names_and_nothing_else(name: str, sites: Sites) -> None:
    """With 127.0.0.1 allowed (the tests' setting), the sites answer; another loopback
    address, and every other non-public destination, are still refused."""
    port = sites.port()
    urls = [
        sites.plain("/"),
        f"http://127.0.0.2:{port}/",
        f"http://[::1]:{port}/",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.1/",
    ]
    ran = _run(name, urls, sites, allow=True)
    assert _errors(ran) == [None] + ["ForbiddenAddress"] * 4
    assert ran["out"][0]["status"] == 200


@pytest.mark.parametrize("name", sorted(CASES))
def test_every_redirect_is_checked_again(name: str, sites: Sites) -> None:
    """An allowed site that redirects somewhere private: the redirect is refused before
    it is followed, as the URL itself would be. A redirect to an allowed place is followed."""
    port = sites.port()
    urls = [
        sites.redirect("http://169.254.169.254/latest/meta-data/"),
        sites.redirect(f"http://127.0.0.2:{port}/page/x"),
        sites.redirect("http://metadata.test/latest/meta-data/"),
        sites.redirect(sites.plain("/page/fine")),
        sites.redirect("/page/relative"),
    ]
    ran = _run(name, urls, sites, allow=True, answers=NAMES)
    assert _errors(ran) == ["ForbiddenAddress"] * 3 + [None, None]
    assert [row["status"] for row in ran["out"][3:]] == [200, 200]
    if name == "enrich":
        assert [row["title"] for row in ran["out"][3:]] == ["Page fine", "Page relative"]


@pytest.mark.parametrize("name", sorted(CASES))
def test_the_checked_address_is_the_one_connected_to(name: str, sites: Sites) -> None:
    """DNS rebinding: a name that answers an allowed address when it is checked and the
    metadata address on any later lookup. The name is resolved once, the connection goes to
    the address that was checked, and the request still carries the name (Host header)."""
    port = sites.port()
    answers = {"rebind.test": [["127.0.0.1"], ["169.254.169.254"]]}
    ran = _run(name, [f"http://rebind.test:{port}/host"], sites, allow=True, answers=answers)
    assert ran["asked"] == {"rebind.test": 1}
    [row] = ran["out"]
    assert (row["status"], row["error"]) == (200, None)
    if name == "enrich":
        assert row["title"] == f"Host rebind.test:{port}"


@pytest.mark.parametrize("name", sorted(CASES))
def test_tls_still_checks_the_name_not_the_address(name: str, sites: Sites) -> None:
    """Connecting to the checked address changes nothing in TLS: the server is sent the URL's
    name and its certificate must be for that name (the sites' is for localhost and
    127.0.0.1, not for rebind.test)."""
    secure = sites.port("secure")
    answers = {"localhost": [["127.0.0.1"]], "rebind.test": [["127.0.0.1"]]}
    urls = [f"https://localhost:{secure}/page/tls", f"https://rebind.test:{secure}/"]
    ran = _run(name, urls, sites, allow=True, answers=answers)
    ok, wrong_name = ran["out"]
    assert (ok["status"], ok["error"]) == (200, None)
    assert (wrong_name["status"], wrong_name["error"]) == (None, "ConnectError")
