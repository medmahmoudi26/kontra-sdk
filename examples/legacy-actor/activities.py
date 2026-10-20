"""The old `head` method, as a Temporal activity. `actor.load` (per-worker state) becomes
module state; the fleet-wide `global_state` counters are gone — they were accounting, not
output, and a Temporal workflow can count for itself.

A target comes from the buyer, so none is trusted: whatever one does (refuse, stall, answer
garbage, redirect in circles, fail to parse as a URL), it becomes that target's `error`. It
never fails the batch: a failed batch is retried, then fails the whole job, and the buyer is
refunded for work that was done.

The old body was blocking `urllib` on a thread per target, and ported as it was it let one
target hold the worker. urllib's timeout applies to each socket operation, so a server that
sends a header byte every few seconds kept its thread as long as it liked (16 of them kept
every buyer's job on the worker waiting); and urllib turns a redirected HEAD into a GET and
reads the whole body of each redirect after that. Here HEAD stays HEAD through redirects
(a HEAD answer has no body), and an overall deadline per target cancels whatever the target
is in the middle of.

The targets are probed from the seller's machine, inside the seller's network: a buyer could
name the cloud metadata service (169.254.169.254) or any host on the seller's LAN and learn
from the `Head` what answers there. So each target's host is resolved here, every address
it resolves to must be public (see `_public`), and the request goes to the address that was
checked -- httpx never resolves the name again, so a DNS answer that changes between the
check and the connection (rebinding) cannot swap it. Redirects are followed here, each
destination checked the same way. A target that fails the check gets the error
`ForbiddenAddress`.
"""

import asyncio
import ipaddress
import os
import socket

import httpx
from models import Batch, Head, Target
from temporalio import activity

#: Targets probed at once per batch (what a fleet of the old actor's containers gave it).
#: Each one ends within its deadline, so a batch heartbeats at least that often.
CONCURRENCY = 16
#: What one target may take in all: resolving, connecting, TLS, redirects and the headers.
TARGET_DEADLINE_SECONDS = 30
#: Redirects followed per target, all within its deadline (httpx's own default).
MAX_REDIRECTS = 20
#: The Server header is the target's text: keep a sane prefix of it.
_SERVER_MAX = 256
#: Each target gets a connection of its own: a pooled one, opened to the same address for
#: another host name, would carry this request under that host's TLS identity.
_LIMITS = httpx.Limits(max_keepalive_connections=0)
#: IPv6 addresses that carry an IPv4 one for a NAT64 gateway to reach.
_NAT64 = ipaddress.ip_network("64:ff9b::/96")
_DEFAULT_PORTS = {"http": 80, "https": 443}


def _networks(setting: str) -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    """The comma-separated networks of `setting` (`127.0.0.1/32,10.1.0.0/16`). A malformed
    one stops the worker as it starts: a guard must not run unlike what its setting says."""
    return tuple(
        ipaddress.ip_network(n.strip(), strict=False) for n in setting.split(",") if n.strip()
    )


#: Non-public networks a target may reach anyway: none unless `PROBE_ALLOWED_PRIVATE_NETWORKS`
#: names some (the README says when to). The tests set it to reach their sites on 127.0.0.1.
ALLOWED_PRIVATE_NETWORKS = _networks(os.environ.get("PROBE_ALLOWED_PRIVATE_NETWORKS", ""))


class ForbiddenAddress(Exception):
    """The target's host is, or resolves to, an address that is not on the public internet."""


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
    every address it resolves to checked -- one bad answer refuses the target."""
    port = _DEFAULT_PORTS.get(url.scheme)
    if port is None:
        raise httpx.UnsupportedProtocol(f"only http and https URLs are probed, not {url.scheme!r}")
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
    """A HEAD of `url` sent to `address`, the address that was checked. The Host header and
    TLS (the server name it sends, the certificate it accepts) are still the URL's host."""
    return http.build_request(
        "HEAD",
        url.copy_with(host=address),
        headers={"Host": url.netloc.decode("ascii")},
        extensions={"sni_hostname": url.raw_host.decode("ascii")},
    )


async def _probe(http: httpx.AsyncClient, url: str) -> httpx.Response:
    """The final answer to a HEAD of `url`, redirects followed (with HEAD), each
    destination checked before it is asked."""
    target = httpx.URL(url)
    for _ in range(MAX_REDIRECTS + 1):
        r = await http.send(_pinned(http, target, await _checked_address(target)))
        if not r.has_redirect_location:
            return r
        target = target.join(r.headers["Location"])
    raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects")


async def _head(http: httpx.AsyncClient, target: Target) -> Head:
    url = target.address()
    try:
        async with asyncio.timeout(TARGET_DEADLINE_SECONDS):
            r = await _probe(http, url)
    except Exception as e:  # this target's failure, recorded; never the batch's
        return Head(url=url, error=type(e).__name__)
    return Head(url=url, status=r.status_code, server=r.headers.get("Server", "")[:_SERVER_MAX])


@activity.defn
async def head_batch(batch: Batch) -> list[Head]:
    slots = asyncio.Semaphore(CONCURRENCY)
    done = batch.done

    async def one(http: httpx.AsyncClient, target: Target) -> Head:
        nonlocal done
        async with slots:
            head = await _head(http, target)
        done += 1
        activity.heartbeat(
            {
                "percent": round(100 * done / batch.total, 1),
                "message": f"{done}/{batch.total} targets",
            }
        )
        return head

    async with httpx.AsyncClient(timeout=15, limits=_LIMITS) as http:
        return list(await asyncio.gather(*(one(http, t) for t in batch.targets)))
