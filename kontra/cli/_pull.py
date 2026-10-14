"""``kontra pull <offering> --lang ...``: a typed client in any nexgen language (§11.1).

The catalog entry is fetched, its document checked with :mod:`kontra.contract` exactly as a
buyer's SDK checks it (§11.3: "clients verify before generating stubs") -- the catalog's
rules, the hash it seals, and the entry agreeing with it -- and only then is its nexgen view
(the document without ``x-kontra``, decisions I2) written and handed to nexgen.
"""

from __future__ import annotations

import keyword
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click

from kontra.cli._account import gate_url
from kontra.cli._common import emit_json, fail, printable, run

if TYPE_CHECKING:
    from kontra.contract import ContractInfo
    from kontra.gate_client import models as m

__all__ = ["DOCUMENT_NAME", "pull", "verify_entry"]

#: The nexgen view is written into the output directory under this name.
DOCUMENT_NAME = "contract.nexusrpc.yaml"
_LANGS = {
    "go": "go",
    "python": "python",
    "typescript": "typescript",
    "ts": "typescript",
    "java": "java",
}
_GO_KEYWORDS = frozenset(
    {
        "break", "case", "chan", "const", "continue", "default", "defer", "else",
        "fallthrough", "for", "func", "go", "goto", "if", "import", "interface", "map",
        "package", "range", "return", "select", "struct", "switch", "type", "var",
    }
)  # fmt: skip


_JAVA_PACKAGE = re.compile(r"[a-z_][a-z0-9_]{0,63}(?:\.[a-z_][a-z0-9_]{0,63}){0,15}", re.ASCII)
_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1"})


def _catalog_url(url: str) -> str:
    """The contract decides the generated types: fetch it over TLS, or from this machine."""
    import httpx

    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL:
        fail(f"{url!r} is not a URL")
    if parsed.scheme == "https" or (parsed.scheme == "http" and parsed.host in _LOOPBACK):
        return url
    fail(f"refusing to fetch a contract from {url}: use https:// (plain http only to this machine)")


def verify_entry(ref: str, entry: m.CatalogEntry) -> ContractInfo:
    """The entry's contract, or a refusal: the document must pass the catalog's rules and
    reproduce its sealed hash, and the entry must describe that document and the version
    asked for. A mismatch means the catalog (or something between) is not to be trusted
    with code generation."""
    from kontra.contract import validate_document
    from kontra.contract.names import OfferingRef

    info = validate_document(entry.document)  # ContractError names the path at fault
    wanted = OfferingRef.parse(ref)
    problems: list[str] = []
    if info.contract_hash != entry.contract_hash:
        problems.append(
            f"the document hashes to {info.contract_hash}, the entry says {entry.contract_hash}"
        )
    if info.offering.key != entry.offering:
        problems.append(f"the document is {info.offering.key}, the entry says {entry.offering}")
    if info.offering.full_name != wanted.full or not wanted.matches(info.offering.version):
        problems.append(f"asked for {ref}, got {info.offering.key}")
    if info.facts.party != entry.party:
        problems.append(f"the document is {info.facts.party}'s, the entry says {entry.party}")
    if info.offering.service_name != entry.service:
        problems.append(f"the entry's service {entry.service!r} is not the document's")
    if problems:
        fail(f"refusing the catalog's {entry.offering}: " + "; ".join(problems))
    return info


def _check_output(out: Path) -> None:
    """``out`` must be new, empty, or a client an earlier ``pull`` wrote (it has
    :data:`DOCUMENT_NAME`). nexgen overwrites whatever shares a name with what it generates
    (``models.py``, ``__init__.py``, ``index.ts``, ...), and the default output is named after
    the offering -- exactly the directory ``kontra init <name>`` made for a seller's own
    project, whose ``models.py`` would be replaced by generated code."""
    if out.exists() and not out.is_dir():
        fail(f"{out} exists and is not a directory")
    if out.is_dir() and not (out / DOCUMENT_NAME).is_file() and any(out.iterdir()):
        fail(
            f"{out} already has files that `kontra pull` did not write (there is no "
            f"{DOCUMENT_NAME}), and nexgen would overwrite any of the same name; "
            "pass -o with a new or empty directory"
        )


def _default_output(name: str, lang: str, package: str | None) -> Path:
    """A directory whose name is a valid package name in ``lang`` (Go and Java take the
    package from it)."""
    if lang == "java":
        assert package is not None
        return Path(package.rsplit(".", 1)[-1])
    if lang == "go":
        base = name.replace("-", "")
        return Path(base + "client" if base in _GO_KEYWORDS else base)
    if lang == "python":
        base = name.replace("-", "_")
        return Path(base + "_client" if keyword.iskeyword(base) else base)
    return Path(name)


@click.command()
@click.argument("offering")
@click.option(
    "--lang",
    required=True,
    type=click.Choice(sorted(_LANGS)),
    help="The client's language (ts = typescript).",
)
@click.option(
    "-o",
    "--output",
    type=click.Path(file_okay=False, path_type=Path),
    help="The directory to generate into (default: named after the offering).",
)
@click.option(
    "--package",
    metavar="P",
    help="Java only, required: the base package; its last segment names the directory.",
)
@click.option(
    "--gate",
    metavar="URL",
    help="The gate (default: $KONTRA_GATE, else the profile's, else https://api.kontra.run).",
)
@click.option("--json", "as_json", is_flag=True, help="Machine-readable output.")
def pull(
    offering: str,
    lang: str,
    output: Path | None,
    package: str | None,
    gate: str | None,
    as_json: bool,
) -> None:
    """Generate a typed client for OFFERING from its verified catalog contract."""
    from kontra.cli._nexgen import find_nexgen
    from kontra.contract import nexgen_view, to_yaml
    from kontra.contract.names import OfferingRef
    from kontra.gate_client import GateClient
    from kontra.profile import kontra_home

    language = _LANGS[lang]
    if language == "java" and not package:
        fail("--lang java needs --package (e.g. com.example.enrich)")
    if language != "java" and package:
        fail("--package is for --lang java only")
    if package is not None and _JAVA_PACKAGE.fullmatch(package) is None:
        fail(f"--package {package!r} is not a Java package name (e.g. com.example.enrich)")
    OfferingRef.parse(offering)  # a malformed reference fails before any request
    url = _catalog_url(gate_url(gate))

    async def fetch() -> m.CatalogEntry:
        async with GateClient(url) as client:  # the catalog is public: no credentials sent
            return await client.get_offering(offering)

    entry = run(fetch)
    info = verify_entry(offering, entry)
    out = output if output is not None else _default_output(info.offering.name, language, package)
    out = out.expanduser().resolve()
    if language == "java" and package is not None and out.name != package.rsplit(".", 1)[-1]:
        fail(
            f"for --package {package} the output directory must be named {package.rsplit('.', 1)[-1]!r}"
        )
    _check_output(out)
    nexgen = find_nexgen(kontra_home())
    out.mkdir(parents=True, exist_ok=True)
    document = out / DOCUMENT_NAME
    document.write_text(to_yaml(nexgen_view(entry.document)), encoding="utf-8")
    nexgen.generate(language, document, out, package=package)
    summary: dict[str, Any] = {
        "offering": entry.offering,
        "contract_hash": entry.contract_hash,
        "endpoint": entry.endpoint,
        "service": entry.service,
        "language": language,
        "output": str(out),
        "document": str(document),
    }
    if as_json:
        emit_json(summary)
        return
    click.echo(f"pulled {entry.offering} ({entry.contract_hash}, verified)")
    click.echo(f"  {language} client in {out}")
    click.echo(f"  endpoint {printable(entry.endpoint)}, service {entry.service}")
