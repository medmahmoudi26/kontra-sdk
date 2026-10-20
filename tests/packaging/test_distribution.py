"""What ``pip install kontra-sdk`` gets (contract §3): a real ``uv build`` of this tree, then
``.github/scripts/check_dist.py`` on it -- the package, the README and the licence, no
``tests/``, ``temporal/``, ``docs/`` or ``spec/``, and nothing describing the callback issue
the Authorizer works around. The checker runs on the release workflow's build too; the
last tests here show it has teeth."""

from __future__ import annotations

import io
import shutil
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CHECK = ROOT / ".github" / "scripts" / "check_dist.py"


def _check(dist: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CHECK), str(dist)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


@pytest.fixture(scope="module")
def dist(tmp_path_factory: pytest.TempPathFactory) -> Path:
    uv = shutil.which("uv")
    if uv is None:  # `make sync` needs uv: a machine that ran it has one
        pytest.fail("uv is not on PATH; the distributions cannot be built")
    out = tmp_path_factory.mktemp("dist")
    done = subprocess.run(
        [uv, "build", "--out-dir", str(out), "--quiet", str(ROOT)],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    return out


def test_the_build_carries_the_package_readme_and_licence_only(dist: Path) -> None:
    result = _check(dist)
    assert result.returncode == 0, result.stdout + result.stderr


def test_the_wheel_has_what_kontra_dev_and_the_contract_read(dist: Path) -> None:
    [wheel] = dist.glob("*.whl")
    names = set(zipfile.ZipFile(wheel).namelist())
    for needed in (
        "kontra/__init__.py",
        "kontra/py.typed",
        # The dev gate and its host process (decisions I34): kontra dev up runs these.
        "kontra/devgate/__init__.py",
        "kontra/devgate/__main__.py",
        "kontra/devgate/gate.py",
        "kontra/devgate/stack.py",
        "kontra/devgate/temporal.py",
        "kontra/devgate/temporal_cli.py",
        "kontra/devgate/state.py",
        "kontra/devgate/store.py",
        # The observability stack `kontra dev up --observability` runs (the e2e compose.yaml is
        # not shipped); the files its relative paths mount beside it.
        "kontra/_dev/observability/compose.yaml",
        "kontra/_dev/observability/grafana/datasources.yaml",
        "kontra/_dev/observability/grafana/dashboards.yaml",
        "kontra/_dev/observability/otelcol.yaml",
        "kontra/_dev/observability/loki.yaml",
        "kontra/_dev/observability/tempo.yaml",
        "kontra/_dev/docs/dashboards/temporal-core-sdks-otel.json",
        "kontra/_dev/docs/dashboards/server-general.json",
        "kontra/_dev/docs/dashboards/LICENSE",
        "kontra/_spec/x-kontra.v2.json",
        "kontra/_spec/x-kontra.v1.json",  # read-only, for the jobs hired under it
        "kontra/_spec/kontra-types.v1.json",
        "kontra/_spec/offering-streams.v1.json",
        "kontra/templates/offering/worker.py.tmpl",
        "kontra/cli/_console.html",  # kontra dev's graph page, which the codec server serves
        "kontra/testing/fake_gate/__init__.py",
        # What the platform gate imports, never copies (decisions I24, I25).
        "kontra/_passkeys.py",
        "kontra/_jobkeys.py",
        "kontra/testing/passkey_contract.py",
        "kontra/testing/jobkey_contract.py",
    ):
        assert needed in names, needed
    # The e2e stack's compose.yaml (the prod gate image) is repository tooling, never shipped.
    assert "kontra/_dev/compose.yaml" not in names
    assert not [
        n
        for n in names
        if n.endswith("compose.yaml") and n != "kontra/_dev/observability/compose.yaml"
    ]
    [sdist] = dist.glob("*.tar.gz")
    with tarfile.open(sdist) as tar:
        top = {name.split("/", 2)[1] for name in tar.getnames() if "/" in name}
    assert top == {"kontra", "README.md", "LICENSE", "pyproject.toml", "PKG-INFO", ".gitignore", "hatch_build.py"}  # fmt: skip


def test_the_observability_compose_is_the_only_one_shipped_and_names_no_gate(dist: Path) -> None:
    """The shipped observability compose has exactly its six services and nothing of the prod
    gate: not a back door to it (decisions I34, §7)."""
    import yaml

    [wheel] = dist.glob("*.whl")
    with zipfile.ZipFile(wheel) as z:
        raw = z.read("kontra/_dev/observability/compose.yaml")
        names = z.namelist()
    doc = yaml.safe_load(raw)
    assert set(doc["services"]) == {
        "prometheus",
        "prometheus-agent",
        "grafana",
        "otelcol",
        "loki",
        "tempo",
    }
    text = raw.decode()
    for forbidden in (
        "kontra-gate",
        "kontra-temporal",
        "KONTRA_GATE_IMAGE",
        "KONTRA_TEMPORAL_IMAGE",
    ):
        assert forbidden not in text, forbidden
    assert "gate" not in doc["services"]
    # It runs as itself only (-p/-f): nothing in it can bring another file, image or profile in.
    assert doc["name"] == "kontra-dev-observability"
    assert not {"include", "extends"} & set(doc)
    for name, svc in doc["services"].items():
        assert not {"profiles", "env_file", "extends", "build"} & set(svc), name
        # Third-party images by digest: no Temporal image, no image of ours, no variable.
        image = svc["image"]
        assert "@sha256:" in image, name
        assert not image.startswith(("temporalio/", "ghcr.io/", "${")), name
    # Its old Prometheus file left with compose.yaml's observability profile.
    assert "kontra/_dev/observability/prometheus.yml" not in names


def test_the_metadata_is_what_pypi_shows(dist: Path) -> None:
    [wheel] = dist.glob("*.whl")
    with zipfile.ZipFile(wheel) as z:
        [meta_name] = [n for n in z.namelist() if n.endswith(".dist-info/METADATA")]
        meta = z.read(meta_name).decode()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    assert "Name: kontra-sdk\n" in meta
    assert f"Version: {project['version']}\n" in meta
    assert "Version: 0.2.4\n" in meta
    assert "Requires-Python: >=3.11\n" in meta
    assert "License-Expression: Apache-2.0\n" in meta
    assert "Project-URL: Homepage, https://kontra.run\n" in meta
    assert "Description-Content-Type: text/markdown\n" in meta
    assert "Summary: Sell and hire durable, quoted jobs" in meta
    assert "pip install kontra-sdk" in meta  # the README is the long description
    assert "Classifier: Typing :: Typed\n" in meta
    # Never a bare `kontra` dependency: on PyPI that is someone else's package.
    requires = [line for line in meta.splitlines() if line.startswith("Requires-Dist:")]
    assert not [r for r in requires if r.split()[1].split("[")[0].split("<")[0] == "kontra"]
    # The [dev] extra is exactly FastAPI and uvicorn, and [testing] includes it (decisions I34).
    assert "Provides-Extra: dev\n" in meta
    extras = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"][
        "optional-dependencies"
    ]
    dev = {r.split(">")[0].split("<")[0].split("=")[0].strip() for r in extras["dev"]}
    assert dev == {"fastapi", "uvicorn"}
    testing = {
        r.split(">")[0].split("<")[0].split("=")[0].split("[")[0].strip() for r in extras["testing"]
    }
    assert dev <= testing, f"dev {dev} must be a subset of testing {testing}"


# --------------------------------------------------------------------------- the checker's teeth


def _fake_dist(tmp: Path, sdist: dict[str, str], wheel: dict[str, str]) -> Path:
    tmp.mkdir(parents=True)
    with tarfile.open(tmp / "kontra_sdk-0.1.0.tar.gz", "w:gz") as tar:
        for name, text in sdist.items():
            data = text.encode()
            info = tarfile.TarInfo(f"kontra_sdk-0.1.0/{name}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    with zipfile.ZipFile(tmp / "kontra_sdk-0.1.0-py3-none-any.whl", "w") as z:
        for name, text in wheel.items():
            z.writestr(name, text)
    return tmp


CLEAN_SDIST = {
    "kontra/__init__.py": "",
    "README.md": "# Kontra",
    "LICENSE": "Apache",
    "PKG-INFO": "",
}
CLEAN_WHEEL = {"kontra/__init__.py": "", "kontra_sdk-0.1.0.dist-info/METADATA": "Name: kontra-sdk"}


def test_a_clean_fake_passes(tmp_path: Path) -> None:
    result = _check(_fake_dist(tmp_path / "d", CLEAN_SDIST, CLEAN_WHEEL))
    assert result.returncode == 0, result.stdout


@pytest.mark.parametrize("top", ["tests", "temporal", "docs", "spec"])
def test_a_forbidden_directory_in_either_fails(tmp_path: Path, top: str) -> None:
    in_sdist = _check(_fake_dist(tmp_path / "s", {**CLEAN_SDIST, f"{top}/x.py": ""}, CLEAN_WHEEL))
    in_wheel = _check(_fake_dist(tmp_path / "w", CLEAN_SDIST, {**CLEAN_WHEEL, f"{top}/x.py": ""}))
    for result in (in_sdist, in_wheel):
        assert result.returncode == 1
        assert f"under {top}/" in result.stdout


@pytest.mark.parametrize(
    "text",
    [
        "A callback guard closes an upstream Temporal gap",
        "see temporal/authorizer/callbackguard",
        "it would let one seller forge another's Nexus completion",
        "completions can be FORGED by a handler",
        "only callbacks to temporal://system are allowed",
    ],
)
def test_a_mention_of_the_callback_issue_fails(tmp_path: Path, text: str) -> None:
    for where in ("sdist", "wheel"):
        sdist = {**CLEAN_SDIST, "README.md": text} if where == "sdist" else CLEAN_SDIST
        wheel = (
            {**CLEAN_WHEEL, "kontra_sdk-0.1.0.dist-info/METADATA": text}
            if where == "wheel"
            else CLEAN_WHEEL
        )
        result = _check(_fake_dist(tmp_path / where / text[:8].replace(" ", "_"), sdist, wheel))
        assert result.returncode == 1, (where, text)
        assert "describes the callback issue" in result.stdout


def test_a_file_named_after_it_fails(tmp_path: Path) -> None:
    wheel = {**CLEAN_WHEEL, "kontra/temporal_callback_forgery.py": ""}
    result = _check(_fake_dist(tmp_path / "d", CLEAN_SDIST, wheel))
    assert result.returncode == 1


def test_a_compose_file_other_than_observability_fails(tmp_path: Path) -> None:
    wheel = {**CLEAN_WHEEL, "kontra/_dev/compose.yaml": "services: {gate: {}}"}
    result = _check(_fake_dist(tmp_path / "d", CLEAN_SDIST, wheel))
    assert result.returncode == 1
    assert "compose file" in result.stdout


def test_the_observability_compose_is_allowed(tmp_path: Path) -> None:
    wheel = {**CLEAN_WHEEL, "kontra/_dev/observability/compose.yaml": "services: {prometheus: {}}"}
    result = _check(_fake_dist(tmp_path / "d", CLEAN_SDIST, wheel))
    assert result.returncode == 0, result.stdout


def test_a_shipped_yaml_naming_the_gate_fails(tmp_path: Path) -> None:
    wheel = {
        **CLEAN_WHEEL,
        "kontra/_dev/observability/compose.yaml": "image: ghcr.io/x/kontra-gate:dev\n",
    }
    result = _check(_fake_dist(tmp_path / "d", CLEAN_SDIST, wheel))
    assert result.returncode == 1
    assert "kontra-gate" in result.stdout
