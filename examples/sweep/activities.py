"""Temporal activities through the kontra façade (D1): `activity` is Temporal's, and its
`defn` also notes the activity for `serve()` to register. Each is a unit Temporal schedules,
times out and retries on its own:

* `plan`: the input normalised, each URL once, and the sites it is on;
* `fetch_robots` and `fetch_security_txt`: one file of every site, the two at once;
* `check_headers`: one URL's response headers (the workflow runs one per URL);
* `score` or `all_clear`: the report, from what was found;
* `write_report`: the report as an `Artifact`.

Every URL comes from the buyer, so none is trusted: whatever one does (refuse, hang, stream
forever, redirect in circles, answer garbage, fail to parse), it becomes that URL's or that
site's `error`. It never fails an activity: a failed activity is retried, then fails the
whole job, and the buyer is refunded for work that was done. A file is read up to
`READ_AT_MOST` bytes, as sent (never inflated), and a page's body is never read at all.

And the URLs are fetched from the seller's machine, inside the seller's network: a buyer
could name the cloud metadata service (169.254.169.254), the seller's router or a database
on its LAN, and read what answered in the report. So each URL's host is resolved here,
every address it resolves to must be public (see `_public`), and the request goes to the
address that was checked -- httpx never resolves the name again, so a DNS answer that
changes between the check and the connection (rebinding) cannot swap it. Each redirect is
checked the same way. A URL that fails the check gets the error `ForbiddenAddress`. (The
guard is examples/enrich's.)
"""

import asyncio
import ipaddress
import os
import socket
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlsplit, urlunsplit

import httpx
from models import (
    HEADERS,
    Artifact,
    Findings,
    Grade,
    Graded,
    Header,
    HeaderCheck,
    Plan,
    Report,
    Robots,
    SecurityTxt,
    SweepInput,
    SweepSummary,
)

from kontra import activity

T = TypeVar("T")

#: What one URL, or one site's file, may take in all. The client's timeout applies to each
#: network operation, so a server that sends a byte every few seconds would otherwise hold
#: the activity forever. The workflow sizes the activities' timeouts from it.
URL_DEADLINE_SECONDS = 30
#: Sites whose file is fetched at once, in `fetch_robots` and `fetch_security_txt`.
SITES_AT_ONCE = 16
#: The most of a robots.txt or a security.txt read: a site that streams forever must not
#: fill the worker's memory.
READ_AT_MOST = 64_000
#: What is kept of one site's files: every site's go into the report's payloads.
DISALLOW_KEPT = 10
CONTACTS_KEPT = 5
LINE_KEPT = 100
#: Redirects followed per URL, all within its deadline.
MAX_REDIRECTS = 10
#: Files are asked for, and read, as sent: a few kB of gzip can inflate to gigabytes.
_HEADERS = {"Accept-Encoding": "identity"}
#: Each URL gets a connection of its own: a pooled one, opened to the same address for
#: another host name, would carry this request under that host's TLS identity.
_LIMITS = httpx.Limits(max_keepalive_connections=0)
#: IPv6 addresses that carry an IPv4 one for a NAT64 gateway to reach.
_NAT64 = ipaddress.ip_network("64:ff9b::/96")
_DEFAULT_PORTS = {"http": 80, "https": 443}
#: For each header a page misses, a value to start from.
ADVICE: dict[Header, str] = {
    "strict-transport-security": "max-age=31536000; includeSubDomains",
    "content-security-policy": "default-src 'self'; frame-ancestors 'self'",
    "x-content-type-options": "nosniff",
    "x-frame-options": "SAMEORIGIN",
    "referrer-policy": "strict-origin-when-cross-origin",
}


def _networks(setting: str) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """The comma-separated networks of `setting` (`127.0.0.1/32,10.1.0.0/16`). A malformed
    one stops the worker as it starts: a guard must not run unlike what its setting says."""
    return tuple(
        ipaddress.ip_network(n.strip(), strict=False) for n in setting.split(",") if n.strip()
    )


#: Non-public networks a URL may reach anyway: none unless `SWEEP_ALLOWED_PRIVATE_NETWORKS`
#: names some (the README says when to). The tests set it to reach their sites on 127.0.0.1.
ALLOWED_PRIVATE_NETWORKS = _networks(os.environ.get("SWEEP_ALLOWED_PRIVATE_NETWORKS", ""))


class ForbiddenAddress(Exception):
    """The URL's host is, or resolves to, an address that is not on the public internet."""


def _public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Whether `ip` is a public unicast address: not loopback, private (10/8, 172.16/12,
    192.168/16, fc00::/7), link-local (169.254/16 -- the cloud metadata service -- and
    fe80::/10), carrier-grade NAT (100.64/10), multicast, unspecified or reserved. An IPv6
    address that carries an IPv4 one (mapped, NAT64) is judged by that IPv4 address; one
    that tunnels to one (6to4, Teredo) or is site-local is never public."""
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.ipv4_mapped is not None:
            return _public(ip.ipv4_mapped)
        if ip in _NAT64:
            return _public(ipaddress.IPv4Address(int(ip) & 0xFFFF_FFFF))
        if ip.sixtofour is not None or ip.teredo is not None or ip.is_site_local:
            return False
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


async def _checked_address(url: httpx.URL) -> str:
    """The address to connect to for `url`: its host resolved (an address literal too), and
    every address it resolves to checked -- one bad answer refuses the URL."""
    port = _DEFAULT_PORTS.get(url.scheme)
    if port is None:
        raise httpx.UnsupportedProtocol(f"only http and https URLs are fetched, not {url.scheme!r}")
    host = url.raw_host.decode("ascii")  # IDNA-encoded; an IPv6 literal without brackets
    try:
        found = await asyncio.get_running_loop().getaddrinfo(
            host, url.port or port, type=socket.SOCK_STREAM
        )
    except socket.gaierror as e:  # a name that does not resolve: httpx's error for it
        raise httpx.ConnectError(f"{host} does not resolve: {e}") from e
    addresses = [ipaddress.ip_address(info[4][0]) for info in found]
    for ip in addresses:
        if not (_public(ip) or any(ip in net for net in ALLOWED_PRIVATE_NETWORKS)):
            raise ForbiddenAddress(f"{host} is at {ip}, not a public address")
    if not addresses:
        raise ForbiddenAddress(f"{host} resolves to no address")
    return str(addresses[0])


def _pinned(http: httpx.AsyncClient, url: httpx.URL, address: str) -> httpx.Request:
    """A GET of `url` sent to `address`, the address that was checked. The Host header and
    TLS (the server name it sends, the certificate it accepts) are still the URL's host."""
    return http.build_request(
        "GET",
        url.copy_with(host=address),
        headers={"Host": url.netloc.decode("ascii")},
        extensions={"sni_hostname": url.raw_host.decode("ascii")},
    )


async def _read_head(r: httpx.Response, most: int) -> bytes:
    """At most the first `most` bytes of the body, as sent."""
    head = bytearray()
    if most > 0:
        async for chunk in r.aiter_raw():
            head += chunk
            if len(head) >= most:
                break
    return bytes(head[:most])


async def _get(http: httpx.AsyncClient, url: str, *, read: int) -> tuple[httpx.Response, bytes]:
    """The final response for `url`, and the first `read` bytes of its body. Redirects are
    followed here and not by httpx, which reads each redirect's whole body into memory
    before it moves on, and each one's destination is checked before it is fetched, like
    the URL itself. No body is read beyond what is asked for: the connection is closed."""
    target = httpx.URL(url)
    for _ in range(MAX_REDIRECTS + 1):
        r = await http.send(_pinned(http, target, await _checked_address(target)), stream=True)
        try:
            if not r.has_redirect_location:
                return r, await _read_head(r, read)
            target = target.join(r.headers["Location"])
        finally:
            await r.aclose()  # without reading the rest
    raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects")


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=10, headers=_HEADERS, limits=_LIMITS)


# --------------------------------------------------------------------------- plan


def _normalised(url: str) -> tuple[str, str | None]:
    """`url` as it is fetched -- scheme and host lowercased; a default port, credentials and
    the fragment left out; an empty path `/` -- and its site, `scheme://host[:port]`. A URL
    that does not parse stays as given, with no site: checking it says why."""
    try:
        parts = urlsplit(url)
        host, port = parts.hostname, parts.port
    except ValueError:  # an unclosed IPv6 bracket, a port that is no number or out of range
        return url, None
    scheme = parts.scheme.lower()
    if not host or scheme not in _DEFAULT_PORTS:
        return url, None
    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc += f":{port}"
    return urlunsplit((scheme, netloc, parts.path or "/", parts.query, "")), f"{scheme}://{netloc}"


@activity.defn
async def plan(inp: SweepInput) -> Plan:
    found = [_normalised(url) for url in inp.urls]
    urls = list(dict.fromkeys(url for url, _ in found))
    sites = list(dict.fromkeys(site for _, site in found if site is not None))
    return Plan(urls=urls, sites=sites)


# --------------------------------------------------------------------------- the sites' files


async def _file(http: httpx.AsyncClient, site: str, path: str) -> tuple[int, str]:
    """The status of `site` + `path` and the head of its body, as text."""
    async with asyncio.timeout(URL_DEADLINE_SECONDS):
        r, head = await _get(http, site + path, read=READ_AT_MOST)
    return r.status_code, head.decode("utf-8", errors="replace")


async def _each_site(
    sites: list[str], read: Callable[[httpx.AsyncClient, str], Awaitable[T]], what: str
) -> list[T]:
    """`read` of every site, `SITES_AT_ONCE` at a time, in the order of `sites`; a
    heartbeat as each one is done (progress for the buyer, and a sign of life)."""
    room = asyncio.Semaphore(SITES_AT_ONCE)
    done = 0
    async with _client() as http:

        async def one(site: str) -> T:
            nonlocal done
            async with room:
                found = await read(http, site)
            done += 1
            activity.heartbeat({"message": f"{what}: {done} of {len(sites)} sites"})
            return found

        return list(await asyncio.gather(*(one(site) for site in sites)))


def _disallowed(text: str) -> list[str]:
    """The paths of a robots.txt's `Disallow:` lines, whatever user agent they are for (where
    a site asks crawlers not to look is worth a look), each once, the first few."""
    paths: list[str] = []
    for line in text.splitlines():
        field, sep, value = line.partition("#")[0].partition(":")
        path = value.strip()[:LINE_KEPT]
        if sep and field.strip().lower() == "disallow" and path and path not in paths:
            paths.append(path)
            if len(paths) == DISALLOW_KEPT:
                break
    return paths


async def _robots(http: httpx.AsyncClient, site: str) -> Robots:
    try:
        status, text = await _file(http, site, "/robots.txt")
    except Exception as e:  # this site's failure, recorded; never the activity's
        return Robots(site=site, error=type(e).__name__)
    return Robots(site=site, status=status, disallow=_disallowed(text) if status == 200 else [])


@activity.defn
async def fetch_robots(sites: list[str]) -> list[Robots]:
    return await _each_site(sites, _robots, "robots.txt")


def _contacts(text: str) -> tuple[list[str], str | None]:
    """A security.txt's `Contact:` values (the first few) and its `Expires:`."""
    contact: list[str] = []
    expires: str | None = None
    for line in text.splitlines():
        field, sep, value = line.partition(":")
        name, value = field.strip().lower(), value.strip()[:LINE_KEPT]
        if not sep or not value:
            continue
        if name == "contact" and value not in contact and len(contact) < CONTACTS_KEPT:
            contact.append(value)
        elif name == "expires" and expires is None:
            expires = value
    return contact, expires


async def _security_txt(http: httpx.AsyncClient, site: str) -> SecurityTxt:
    try:
        status, text = await _file(http, site, "/.well-known/security.txt")
    except Exception as e:  # this site's failure, recorded; never the activity's
        return SecurityTxt(site=site, error=type(e).__name__)
    if status != 200:
        return SecurityTxt(site=site, status=status)
    contact, expires = _contacts(text)
    return SecurityTxt(site=site, status=status, contact=contact, expires=expires)


@activity.defn
async def fetch_security_txt(sites: list[str]) -> list[SecurityTxt]:
    return await _each_site(sites, _security_txt, "security.txt")


# --------------------------------------------------------------------------- one page


def _hsts(value: str) -> bool:
    """Whether a Strict-Transport-Security value sets a `max-age` above 0 (0 turns it off)."""
    for directive in value.split(";"):
        name, _, age = directive.partition("=")
        if name.strip().lower() == "max-age":
            age = age.strip().strip('"')
            # Read as digits, never as an int: a header of 5,000 digits is a ValueError.
            return age.isascii() and age.isdigit() and age.strip("0") != ""
    return False


def _missing(r: httpx.Response) -> list[Header]:
    """The headers `r` lacks, or sends with a value that does not do the job.
    Strict-Transport-Security counts on HTTPS only (a browser ignores it on plain HTTP), and
    a policy's `frame-ancestors` does what X-Frame-Options does."""
    h = r.headers
    policy = {
        d.split(None, 1)[0].lower()
        for d in h.get("content-security-policy", "").split(";")
        if d.strip()
    }
    has: dict[Header, bool] = {
        "strict-transport-security": r.url.scheme != "https"
        or _hsts(h.get("strict-transport-security", "")),
        "content-security-policy": bool(policy),
        "x-content-type-options": h.get("x-content-type-options", "").strip().lower() == "nosniff",
        "x-frame-options": h.get("x-frame-options", "").strip().lower() in ("deny", "sameorigin")
        or "frame-ancestors" in policy,
        "referrer-policy": h.get("referrer-policy", "").strip().lower() not in ("", "unsafe-url"),
    }
    return [name for name in HEADERS if not has[name]]


@activity.defn
async def check_headers(url: str) -> HeaderCheck:
    try:
        async with _client() as http, asyncio.timeout(URL_DEADLINE_SECONDS):
            r, _ = await _get(http, url, read=0)
    except Exception as e:  # this URL's failure, recorded; never the activity's
        return HeaderCheck(url=url, error=type(e).__name__)
    return HeaderCheck(url=url, status=r.status_code, missing=_missing(r))


# --------------------------------------------------------------------------- the report


def _grade(missing: int) -> Grade:
    grades: tuple[Grade, ...] = ("A", "B", "C", "D", "F")
    return grades[min(missing, len(grades) - 1)]


def _summary(found: Findings, grade: Grade | None) -> SweepSummary:
    return SweepSummary(
        grade=grade,
        urls=found.tally.checked,
        flagged=len(found.tally.flagged),
        unreachable=len(found.tally.unreachable),
        sites=len(found.robots),
        without_security_txt=sum(1 for s in found.security_txt if not s.contact),
    )


@activity.defn
async def score(found: Findings) -> Report:
    """The report of a sweep that found something: each flagged page graded, worst first,
    with what to send for each header; the pages that did not answer."""
    pages = sorted(
        (
            Graded(url=f.url, grade=_grade(len(f.missing)), missing=f.missing)
            for f in found.tally.flagged
        ),
        key=lambda g: (-len(g.missing), g.url),
    )
    grade: Grade | None = pages[0].grade if pages else "A"
    if len(found.tally.unreachable) == found.tally.checked:
        grade = None  # no page answered: there is nothing to grade
    missed = {h for page in pages for h in page.missing}
    return Report(
        summary=_summary(found, grade),
        pages=pages,
        unreachable=sorted(found.tally.unreachable, key=lambda u: u.url),
        advice={h: ADVICE[h] for h in HEADERS if h in missed},
        robots=found.robots,
        security_txt=found.security_txt,
    )


@activity.defn
async def all_clear(found: Findings) -> Report:
    """The report of a sweep where every page answered with every header: graded A, and
    the sites' files, which are worth reading anyway."""
    return Report(
        summary=_summary(found, "A"), robots=found.robots, security_txt=found.security_txt
    )


@activity.defn
async def write_report(report: Report) -> Artifact:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "report.json"
        path.write_text(report.model_dump_json(indent=2) + "\n", encoding="utf-8")
        return await Artifact.from_path(path, content_type="application/json")
