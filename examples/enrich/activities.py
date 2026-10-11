"""Plain Temporal activities. Nothing from kontra is imported here (D1): progress is the
heartbeat you would write for Temporal anyway, and the file upload goes through the
`Artifact` type that lives in models.py.

Every URL comes from the buyer, so none is trusted: whatever one URL does (refuse, hang,
stream forever, answer garbage, fail to parse), it becomes the `error` of that URL's record.
It never fails the batch: a failed batch is retried, then fails the whole job, and the
buyer is refunded for work that was done.

The page is the buyer's too, and parsing it runs on the worker's event loop, where no
deadline can interrupt it: every step over its bytes is linear, so no page can stall the
worker (and every other job on it). Nothing a server sends is held in memory beyond the
head of the final page: redirects are followed here, without reading their bodies, and the
page is read as sent, never inflated.
"""

import asyncio
import codecs
import html
import json
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from models import Artifact, Batch, Record
from temporalio import activity

#: The title is found in two linear searches. The one-regex way,
#: `<title[^>]*>(.*?)</title>`, backtracks: on 200 kB of `<title>` repeated it spends over a
#: minute of CPU before it gives up.
_TITLE_OPEN = re.compile(r"<title[^<>]*>", re.IGNORECASE)
_TITLE_CLOSE = re.compile(r"</title>", re.IGNORECASE)
#: The character sets of the web (the WHATWG Encoding Standard's, by Python codec name). A
#: page declares its own charset, and Python has codecs that are no charset of any page:
#: decoding 200 kB as `punycode` takes seconds of CPU. Anything else is read as UTF-8.
_WEB_CHARSETS = frozenset(
    codecs.lookup(name).name
    for name in (
        "utf-8", "utf-16", "utf-16-le", "utf-16-be", "ascii", "latin-1", "cp866",
        "iso8859-2", "iso8859-3", "iso8859-4", "iso8859-5", "iso8859-6", "iso8859-7",
        "iso8859-8", "iso8859-10", "iso8859-13", "iso8859-14", "iso8859-15", "iso8859-16",
        "koi8-r", "koi8-u", "mac-roman", "mac-cyrillic", "cp874", "cp1250", "cp1251",
        "cp1252", "cp1253", "cp1254", "cp1255", "cp1256", "cp1257", "cp1258", "gbk",
        "gb18030", "big5", "euc-jp", "iso2022-jp", "shift-jis", "euc-kr",
    )
)  # fmt: skip
#: Only the head of a page is read: the title is near the top, and a URL that streams
#: forever must not fill the worker's memory.
READ_AT_MOST = 200_000
#: What one URL may take in all. The client's timeout applies to each network operation,
#: so a server that sends a byte every few seconds would otherwise hold the batch forever.
#: The workflow sizes the batch's start-to-close timeout from it.
URL_DEADLINE_SECONDS = 30
#: Redirects followed per URL, all within its deadline.
MAX_REDIRECTS = 10
#: Pages are asked for, and read, as sent: a few kB of gzip can inflate to gigabytes, and
#: httpx inflates a whole chunk (through every layer of `Content-Encoding: gzip, gzip`)
#: before anyone can stop it. A server that compresses anyway yields no title.
_HEADERS = {"Accept-Encoding": "identity"}


def _domain(url: str) -> str:
    try:
        return urlsplit(url).hostname or ""
    except ValueError:  # e.g. an unclosed IPv6 bracket
        return ""


def _charset(declared: str | None) -> str:
    """The codec to read a page with: its declared charset if it is a web one, else UTF-8."""
    try:
        name = codecs.lookup(declared or "utf-8").name
    except (LookupError, ValueError):
        return "utf-8"
    return name if name in _WEB_CHARSETS else "utf-8"


def _title(text: str) -> str | None:
    """The text of the page's first `<title>`, whitespace collapsed, at most 300 chars."""
    opened = _TITLE_OPEN.search(text)
    if opened is None:
        return None
    closed = _TITLE_CLOSE.search(text, opened.end())
    if closed is None:
        return None
    return html.unescape(" ".join(text[opened.end() : closed.start()].split()))[:300]


async def _read_head(r: httpx.Response) -> bytes:
    """At most the first READ_AT_MOST bytes of the body, as sent."""
    head = bytearray()
    async for chunk in r.aiter_raw():
        head += chunk
        if len(head) >= READ_AT_MOST:
            break
    return bytes(head[:READ_AT_MOST])


async def _get(http: httpx.AsyncClient, url: str) -> tuple[httpx.Response, bytes]:
    """The final response for `url` and the head of its page. Redirects are followed here
    and not by httpx, which reads each redirect's whole body into memory before it moves
    on: a redirect with an endless body would fill the worker's memory."""
    request = http.build_request("GET", url)
    for _ in range(MAX_REDIRECTS + 1):
        r = await http.send(request, stream=True)
        try:
            if r.next_request is None:
                return r, await _read_head(r)
            request = r.next_request
        finally:
            await r.aclose()  # without reading the rest
    raise httpx.TooManyRedirects(f"more than {MAX_REDIRECTS} redirects", request=request)


async def _fetch(http: httpx.AsyncClient, url: str) -> Record:
    domain = _domain(url)
    try:
        async with asyncio.timeout(URL_DEADLINE_SECONDS):
            r, head = await _get(http, url)
        text = head.decode(_charset(r.encoding), errors="replace")
    except Exception as e:  # this URL's failure, recorded; never the batch's
        return Record(url=url, domain=domain, error=type(e).__name__)
    return Record(url=url, domain=domain, status=r.status_code, title=_title(text))


@activity.defn
async def fetch_batch(batch: Batch) -> list[Record]:
    records: list[Record] = []
    async with httpx.AsyncClient(timeout=10, headers=_HEADERS) as http:
        for url in batch.urls:
            records.append(await _fetch(http, url))
            n = batch.done + len(records)
            activity.heartbeat(
                {"percent": round(100 * n / batch.total, 1), "message": f"{n} records"}
            )
    return records


@activity.defn
async def write_records(records: list[Record]) -> Artifact:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "records.jsonl"
        with path.open("w") as f:
            for rec in records:
                f.write(json.dumps(rec.model_dump(mode="json")) + "\n")
        return await Artifact.from_path(path, content_type="application/x-ndjson")
