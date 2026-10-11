"""The CLI without a marketplace: the scaffold, loading worker.py, the contract command,
input files, the one-line errors, the default handle, and the nexgen plumbing."""

from __future__ import annotations

import json
import os
import re
import stat
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from kontra import __version__
from kontra.cli._common import default_handle, describe, read_input
from kontra.cli._nexgen import SHA256, release_target
from kontra.cli._project import load_worker
from kontra.cli._pull import _default_output, verify_entry
from kontra.cli._scaffold import class_prefix, render
from kontra.codec.age import generate_identity
from kontra.contract import (
    ContractError,
    from_yaml,
    load_offering,
    seal,
    validate_document,
)
from kontra.gate_client import GateError
from kontra.gate_client import models as m

from .support import Cli

REPO = Path(__file__).resolve().parents[2]
ENRICH = REPO / "examples" / "enrich"
PUBLIC_KEY = generate_identity()[1]


@pytest.fixture
def cli(tmp_path: Path) -> Cli:
    return Cli(tmp_path / "home")


# --------------------------------------------------------------------------- init


def test_init_scaffolds_the_project_layout(
    cli: Cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = cli("init", "word-count", "--party", "acme")
    assert result.exit_code == 0, result.output
    root = tmp_path / "word-count"
    files = sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file())
    assert files == [
        "README.md",
        "activities.py",
        "input.json",
        "models.py",
        "offering.yaml",
        "pyproject.toml",
        "quote.py",
        "tests/test_offering.py",
        "worker.py",
        "workflows.py",
    ]
    spec = load_offering(root / "offering.yaml")
    assert (spec.offering_id.key, spec.max_duration.total_seconds()) == (
        "acme/word-count@0.1.0",
        600,
    )
    # Acceptance 1 and 3: no Kontra in workflow or activity code, no code names in the yaml.
    for name in ("workflows.py", "activities.py"):
        text = (root / name).read_text()
        assert not re.search(r"^\s*(import|from)\s+kontra\b", text, re.MULTILINE), name
    offering_text = (root / "offering.yaml").read_text()
    for symbol in ("WordCountFlow", "count_words", "quote(", "WordCountInput"):
        assert symbol not in offering_text.split("#", 1)[0]
    assert json.loads((root / "input.json").read_text())["text"]
    assert "{{" not in "".join((root / f).read_text() for f in files)


def test_init_takes_the_party_from_the_profile_else_the_user(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = Cli(tmp_path / "nobody")("init", "shop")
    assert result.exit_code == 0, result.output
    spec = load_offering(tmp_path / "shop" / "offering.yaml")
    assert spec.offering_id.party == default_handle()


def test_init_refuses_bad_names_and_existing_directories(
    cli: Cli, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    for bad in ("Shop", "../evil", "a/b", "shop\n", "x-", "x" * 33):
        result = cli("init", bad, "--party", "acme")
        assert result.exit_code == 1, (bad, result.output)
        assert result.stderr.startswith("error: "), result.stderr
    assert cli("init", "shop", "--party", "Not A Handle").exit_code == 1
    assert cli("init", "shop", "--party", "kontra").exit_code == 1  # reserved
    (tmp_path / "taken").mkdir()
    (tmp_path / "taken" / "keep.txt").write_text("mine")
    result = cli("init", "taken", "--party", "acme")
    assert result.exit_code == 1
    assert "already exists" in result.stderr
    assert sorted(p.name for p in (tmp_path / "taken").iterdir()) == ["keep.txt"]
    assert list(tmp_path.iterdir()) == [tmp_path / "taken"]


def test_class_prefixes() -> None:
    assert class_prefix("word-count") == "WordCount"
    assert class_prefix("x2-y") == "X2Y"
    assert class_prefix("kontra-thing") == "OfferingKontraThing"  # Kontra* is reserved (I2)
    files = render("acme", "kontra-thing")
    assert (
        "class OfferingKontraThingFlow" in files[next(p for p in files if p.name == "workflows.py")]
    )


# --------------------------------------------------------------------------- contract


def test_contract_prints_the_sealed_document(cli: Cli) -> None:
    result = cli("contract", "--dir", str(ENRICH), "--public-key", PUBLIC_KEY)
    assert result.exit_code == 0, result.output
    doc = from_yaml(result.stdout)
    info = validate_document(doc)  # the catalog's rules and the sealed hash
    assert info.offering.key == "acme/enrich@1.2.0"
    assert info.facts.public_key == PUBLIC_KEY

    view = cli("contract", "--dir", str(ENRICH), "--public-key", PUBLIC_KEY, "--nexgen")
    assert view.exit_code == 0, view.output
    nexgen = yaml.safe_load(view.stdout)
    assert "x-kontra" not in nexgen
    assert nexgen["services"] == doc["services"]

    as_json = cli("contract", "--dir", str(ENRICH), "--public-key", PUBLIC_KEY, "--json")
    assert json.loads(as_json.stdout) == doc


def test_contract_writes_a_file_and_uses_the_profile_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from kontra.profile import Profile

    home = tmp_path / "home"
    _, public = Profile.ensure_identity(home)  # a key file, no profile: still the party's key
    out = tmp_path / "contract.yaml"
    result = Cli(home)("contract", "--dir", str(ENRICH), "-o", str(out))
    assert result.exit_code == 0, result.output
    assert validate_document(from_yaml(out.read_text())).facts.public_key == public
    assert result.stdout == ""


def test_contract_without_any_key_says_what_to_do(cli: Cli) -> None:
    result = cli("contract", "--dir", str(ENRICH))
    assert result.exit_code == 1
    assert "kontra login" in result.stderr
    bad = cli("contract", "--dir", str(ENRICH), "--public-key", "age1nope")
    assert (bad.exit_code, "not an age recipient" in bad.stderr) == (1, True)


def test_contract_names_the_offending_path(cli: Cli, tmp_path: Path) -> None:
    project = tmp_path / "broken"
    project.mkdir()
    for name in ("models.py", "workflows.py", "activities.py", "quote.py", "worker.py"):
        (project / name).write_text((ENRICH / name).read_text())
    (project / "offering.yaml").write_text(
        (ENRICH / "offering.yaml").read_text().replace("max_duration: 6h", "max_duration: 6 hours")
    )
    result = cli("contract", "--dir", str(project), "--public-key", PUBLIC_KEY)
    assert result.exit_code == 1
    assert result.stderr.startswith("error: ")
    assert "offering.yaml: max_duration:" in result.stderr
    assert len(result.stderr.strip().splitlines()) == 1


# --------------------------------------------------------------------------- worker.py


def test_load_worker_finds_the_module_level_worker_and_cleans_up(tmp_path: Path) -> None:
    before = set(sys.modules)
    with load_worker(ENRICH) as worker:
        assert [b.key for b in worker.offerings] == ["acme/enrich@1.2.0"]
        assert sys.modules["workflows"].__file__ == str(ENRICH / "workflows.py")
    assert "worker" not in sys.modules
    assert "workflows" not in sys.modules
    assert str(ENRICH) not in sys.path
    assert set(sys.modules) - before <= {m for m in set(sys.modules) if m.startswith("kontra")}


def test_load_worker_keeps_two_projects_apart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second project's models.py is not the first one's (both are `models`)."""
    monkeypatch.chdir(tmp_path)
    Cli(tmp_path / "h")("init", "one", "--party", "acme")
    Cli(tmp_path / "h")("init", "two", "--party", "acme")
    with load_worker(tmp_path / "one") as first:
        assert first.offerings[0].key == "acme/one@0.1.0"
        with load_worker(tmp_path / "two") as second:
            assert second.offerings[0].key == "acme/two@0.1.0"
            assert sys.modules["models"].__file__ == str(tmp_path / "two" / "models.py")
        assert sys.modules["models"].__file__ == str(tmp_path / "one" / "models.py")


def test_load_worker_refusals(cli: Cli, tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    result = cli("publish", "--dir", str(empty))
    assert (result.exit_code, "no worker.py" in result.stderr) == (1, True)

    wrong = tmp_path / "wrong"
    wrong.mkdir()
    (wrong / "worker.py").write_text("worker = 42\n")
    result = cli("contract", "--dir", str(wrong), "--public-key", PUBLIC_KEY)
    assert (result.exit_code, "module-level `worker = Worker(...)`" in result.stderr) == (1, True)

    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "worker.py").write_text("")
    (shadow / "json.py").write_text("")
    result = cli("contract", "--dir", str(shadow), "--public-key", PUBLIC_KEY)
    assert (result.exit_code, "json" in result.stderr) == (1, True)
    assert sys.modules["json"].__file__ != str(shadow / "json.py")


# --------------------------------------------------------------------------- errors & input


def test_a_command_without_a_profile_prints_one_line(cli: Cli) -> None:
    for args in (("balance",), ("jobs",), ("publish", "--dir", str(ENRICH)), ("topup", "5")):
        result = cli(*args)
        assert result.exit_code == 1, (args, result.output)
        assert result.stderr.startswith("error: not logged in"), result.stderr
        assert "Traceback" not in result.output


def test_describe_known_failures() -> None:
    err = GateError(403, "stale_session", "this needs a recent login")
    line = describe(err)
    assert line is not None
    assert "stale_session" in line
    assert "kontra login" in line
    assert describe(ContractError("$defs.A.properties.x", "is wrong")) == (
        "contract: $defs.A.properties.x: is wrong"
    )
    assert describe(RuntimeError("a bug")) is None  # unexpected: the traceback is shown


def test_describe_temporal_refusing_or_absent() -> None:
    """temporalio's failures reach `kontra serve` raw: one line each, not a traceback."""
    from temporalio.service import RPCError, RPCStatusCode

    denied = RPCError("Request unauthorized.", RPCStatusCode.PERMISSION_DENIED, b"")
    assert describe(denied) == "Temporal answered PERMISSION_DENIED: Request unauthorized."
    absent = describe(RuntimeError("Failed client connect: tcp connect error 127.0.0.1:1"))
    assert absent is not None
    assert absent.startswith("cannot reach Temporal (Failed client connect: tcp connect")

    class Subclass(RuntimeError):
        pass

    # Only temporalio's own bare RuntimeError: anything else with that text is a bug.
    assert describe(Subclass("Failed client connect: no")) is None


def test_a_bug_still_shows_its_traceback(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    import kontra.cli._account as account

    def boom(_: str | None) -> str:
        raise RuntimeError("a bug")

    monkeypatch.setattr(account, "gate_url_or_fail", boom)
    result = cli("login", "--dev")
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)


def test_read_input(tmp_path: Path) -> None:
    from click.exceptions import ClickException

    good = tmp_path / "in.json"
    good.write_text('{"text": "hi"}')
    assert read_input(str(good)) == {"text": "hi"}
    for content, needle in (
        ("[1]", "JSON object"),
        ("{", "not JSON"),
        ("\xff", "not JSON"),
        ('{"x": NaN}', "NaN is not a JSON number"),
        ('{"x": 1, "x": 2}', "appears twice"),
        ('{"x":' + "[" * 100_000 + "]" * 100_000 + "}", "not JSON"),
    ):
        bad = tmp_path / "bad.json"
        bad.write_bytes(content.encode("latin-1"))
        with pytest.raises(ClickException, match=needle):
            read_input(str(bad))
    with pytest.raises(ClickException, match="cannot read"):
        read_input(str(tmp_path / "missing.json"))
    huge = tmp_path / "huge.json"
    huge.write_bytes(b'{"x": "' + b"a" * (5 * 1024 * 1024) + b'"}')
    with pytest.raises(ClickException, match="Artifact"):
        read_input(str(huge))


def test_login_needs_a_gate(cli: Cli) -> None:
    result = cli("login", "--dev")
    assert result.exit_code == 1
    assert "--gate" in result.stderr


def test_topup_refuses_nonsense_amounts(cli: Cli) -> None:
    for amount in ("0", "-5", "abc", "0.001", "NaN"):
        result = cli("topup", "--", amount)
        assert result.exit_code == 1, amount
        assert "amount" in result.stderr


def test_job_refuses_two_actions_and_bad_ids(cli: Cli) -> None:
    job_id = "0" * 32
    assert "pick one" in cli("job", job_id, "--accept", "--cancel").stderr
    assert "not a job id" in cli("job", "../x").stderr


@pytest.mark.parametrize(
    ("user", "expected"),
    [
        ("alice", "alice"),
        ("Alice.Smith", "alice-smith"),
        ("root", "root-dev"),
        ("123", "dev"),
        ("", "dev"),
        ("_svc_build__", "svc-build"),
        ("José", "jos"),
        ("a" * 40, "a" * 32),
    ],
)
def test_default_handle(user: str, expected: str) -> None:
    assert default_handle(user) == expected


# --------------------------------------------------------------------------- pull


def _entry(doc: dict[str, Any], **changes: Any) -> m.CatalogEntry:
    info = validate_document(doc)
    fields: dict[str, Any] = {
        "offering": info.offering.key,
        "party": info.offering.party,
        "name": info.offering.full_name,
        "version": str(info.offering.version),
        "description": info.facts.description,
        "contract_hash": info.contract_hash,
        "document": doc,
        "card": {},
        "endpoint": "k" + "0" * 32,
        "service": info.offering.service_name,
        "task_queue": info.offering.task_queue,
        "public_key": info.facts.public_key,
        "published_at": "2026-10-11T00:00:00Z",
    }
    fields.update(changes)
    return m.CatalogEntry.model_validate(fields)


def test_pull_verifies_the_catalog_entry() -> None:
    from click.exceptions import ClickException

    with load_worker(ENRICH) as worker:
        doc = worker.offerings[0].build_document(PUBLIC_KEY)
    assert verify_entry("acme/enrich@1", _entry(doc)).offering.key == "acme/enrich@1.2.0"

    tampered = json.loads(json.dumps(doc))
    tampered["$defs"]["EnrichInput"]["properties"]["urls"]["maxItems"] = 5
    with pytest.raises(ContractError, match="contract_hash"):
        verify_entry("acme/enrich", _entry(doc, document=tampered))
    resealed = seal(tampered)  # a consistent document the entry does not describe
    with pytest.raises(ClickException, match="hashes to"):
        verify_entry("acme/enrich", _entry(resealed, contract_hash=_entry(doc).contract_hash))
    with pytest.raises(ClickException, match="public key"):
        verify_entry("acme/enrich", _entry(doc, public_key=generate_identity()[1]))
    with pytest.raises(ClickException, match="asked for"):
        verify_entry("acme/enrich@2", _entry(doc))
    with pytest.raises(ClickException, match="service"):
        verify_entry("acme/enrich", _entry(doc, service="evil.enrich"))


def test_pull_output_directories_are_package_names() -> None:
    assert _default_output("word-count", "go", None) == Path("wordcount")
    assert _default_output("go", "go", None) == Path("goclient")
    assert _default_output("word-count", "python", None) == Path("word_count")
    assert _default_output("class", "python", None) == Path("class_client")
    assert _default_output("word-count", "typescript", None) == Path("word-count")
    assert _default_output("x", "java", "com.example.enrich") == Path("enrich")


def test_nexgen_targets_are_pinned() -> None:
    assert release_target("Linux", "x86_64") == "x86_64-unknown-linux-gnu"
    assert release_target("Linux", "aarch64") == "aarch64-unknown-linux-gnu"
    assert release_target("Darwin", "arm64") == "aarch64-apple-darwin"
    assert release_target("Windows", "AMD64") == "x86_64-pc-windows-msvc"
    assert release_target("Linux", "riscv64") is None
    assert release_target("Plan9", "x86_64") is None
    for target in ("x86_64-unknown-linux-gnu", "aarch64-apple-darwin", "x86_64-pc-windows-msvc"):
        assert re.fullmatch(r"[0-9a-f]{64}", SHA256[target])


def _not_on_path(name: str) -> str | None:
    return None


def test_pull_refuses_a_cached_binary_others_can_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from click.exceptions import ClickException

    from kontra.cli import _nexgen

    monkeypatch.delenv(_nexgen.NEXGEN_ENV, raising=False)
    monkeypatch.setattr(_nexgen.shutil, "which", _not_on_path)
    target = release_target()
    if target is None:
        pytest.skip("no nexgen release for this platform")
    binary = tmp_path / "bin" / f"nexgen-{_nexgen.VERSION}" / "nexgen"
    binary.parent.mkdir(parents=True)
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o777)
    with pytest.raises(ClickException, match="another user"):
        _nexgen.find_nexgen(tmp_path)
    binary.chmod(stat.S_IRWXU)
    found = _nexgen.find_nexgen(tmp_path)
    assert found.binary == binary.resolve()
    assert not found.container


def test_mcp_is_imported_lazily_and_refuses_without_a_profile(cli: Cli) -> None:
    result = cli("mcp")
    assert result.exit_code == 1
    assert "not logged in" in result.stderr
    assert os.environ.get("KONTRA_HOME") != str(cli.home)  # the runner restored the env


def test_other_parties_text_cannot_drive_the_terminal(capsys: pytest.CaptureFixture[str]) -> None:
    from kontra.cli._common import CliError, printable

    assert printable("ok\x1b[2J\x1b]0;pwned\x07 done\x9b") == "ok[2J]0;pwned done"
    assert printable("two\nlines\tand a tab") == "two\nlines\tand a tab"
    CliError("the seller says: \x1b[31mred\x1b[0m").show()
    assert capsys.readouterr().err == "error: the seller says: [31mred[0m\n"


def test_max_price_is_checked_before_anything_is_sent(cli: Cli, tmp_path: Path) -> None:
    source = tmp_path / "in.json"
    source.write_text("{}")
    for bad in ("abc", "-1", "inf", "NaN"):
        result = cli("hire", "acme/enrich", "-f", str(source), "--max-price", bad)
        assert result.exit_code == 2, (bad, result.output)  # a usage error, not a gate call
        assert "--max-price" in result.stderr


def test_pull_refuses_plain_http_and_odd_packages(cli: Cli) -> None:
    remote = cli("pull", "acme/enrich", "--lang", "go", "--gate", "http://gate.example.com")
    assert remote.exit_code == 1
    assert "use https://" in remote.stderr
    for package in ("--output=/etc", "Com.Example", "a..b", "a.b-c", "x" * 65):
        result = cli("pull", "acme/enrich", "--lang", "java", "--package", package)
        assert result.exit_code in (1, 2), package
        assert "package" in result.stderr.lower(), (package, result.stderr)


def test_the_console_entry_point_keeps_exit_codes(tmp_path: Path) -> None:
    """`main()` leaves through os._exit (see its docstring): the codes must survive it."""
    import subprocess

    env = {k: v for k, v in os.environ.items() if not k.startswith("KONTRA_")}
    env["KONTRA_HOME"] = str(tmp_path / "home")

    def kontra(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "kontra.cli", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )

    version = kontra("--version")
    assert (version.returncode, version.stdout.strip()) == (0, f"kontra, version {__version__}")
    missing = kontra("balance")
    assert missing.returncode == 1
    assert missing.stderr.startswith("error: not logged in")
    usage = kontra("job")
    assert usage.returncode == 2
    assert "Missing argument" in usage.stderr
