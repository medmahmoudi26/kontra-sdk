"""What a release may carry to PyPI: ``python .github/scripts/check_dist.py DIST_DIR``.

``DIST_DIR`` holds exactly one sdist and one wheel (``uv build --out-dir DIST_DIR``). Both
must carry the package, the README and the licence (plus the metadata files a distribution
always has, and the wheel's build hook in the sdist) and nothing else: no ``tests/``,
``temporal/``, ``docs/``, ``spec/`` or ``examples/`` at their top. And no file in either --
names included -- may describe the upstream Temporal issue the Authorizer works around: it
is not disclosed, and PyPI is public and permanent.

Run by ``tests/packaging`` (so ``make check``) on a fresh build, and by the release
workflow on the build it publishes. Standard library only. Prints one line per problem and
exits 1 if there is any.
"""

from __future__ import annotations

import re
import sys
import tarfile
import zipfile
from pathlib import Path

#: Never at the top of a distribution.
FORBIDDEN_TOP = frozenset({"tests", "temporal", "docs", "spec", "examples", ".github", ".scratch"})
#: Everything the sdist may hold at its top, below ``kontra_sdk-<version>/``.
SDIST_TOP = frozenset(
    {"kontra", "README.md", "LICENSE", "pyproject.toml", "PKG-INFO", ".gitignore", "hatch_build.py"}
)
_WHEEL_INFO = re.compile(r"kontra_sdk-[^/]+\.dist-info", re.ASCII)
#: Phrases that describe the callback-forgery issue (case-insensitive, any whitespace).
MARKERS = tuple(
    re.compile(p, re.IGNORECASE)
    for p in (
        r"callback[\s_-]*guard",
        r"callback[\s_-]*forg",
        r"forg\w*(?:\W+\w+){0,5}?\W+completions?\b",
        r"\bcompletions?\W+(?:\w+\W+){0,5}?forg",
        r"temporal://system",
    )
)


def _members(path: Path) -> dict[str, bytes]:
    """Member name -> content, for the files of an sdist or a wheel."""
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as tar:
            out: dict[str, bytes] = {}
            for info in tar.getmembers():
                if info.isfile():
                    f = tar.extractfile(info)
                    out[info.name] = f.read() if f is not None else b""
            return out
    with zipfile.ZipFile(path) as wheel:
        return {name: wheel.read(name) for name in wheel.namelist() if not name.endswith("/")}


def _mentions(name: str, data: bytes) -> list[str]:
    text = name + "\n" + data.decode("utf-8", "replace")
    return [m.pattern for m in MARKERS if m.search(text)]


def problems(dist: Path) -> list[str]:
    sdists = sorted(dist.glob("*.tar.gz"))
    wheels = sorted(dist.glob("*.whl"))
    if len(sdists) != 1 or len(wheels) != 1:
        return [f"{dist}: expected one sdist and one wheel, found {len(sdists)} and {len(wheels)}"]
    found: list[str] = []
    for archive in (sdists[0], wheels[0]):
        members = _members(archive)
        if not members:
            found.append(f"{archive.name}: empty")
        for name, data in sorted(members.items()):
            parts = name.split("/")
            if archive is sdists[0]:
                parts = parts[1:]  # below kontra_sdk-<version>/
                if not parts or parts[0] not in SDIST_TOP:
                    found.append(f"{archive.name}: {name} is not the package, README or LICENSE")
            elif parts[0] != "kontra" and not _WHEEL_INFO.fullmatch(parts[0]):
                found.append(f"{archive.name}: {name} is outside the package")
            if parts and parts[0] in FORBIDDEN_TOP:
                found.append(f"{archive.name}: {name} is under {parts[0]}/")
            for marker in _mentions(name, data):
                found.append(f"{archive.name}: {name} describes the callback issue ({marker})")
        if not any(n.split("/")[-1] == "README.md" for n in members) and archive is sdists[0]:
            found.append(f"{archive.name}: no README.md")
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: check_dist.py DIST_DIR", file=sys.stderr)
        return 2
    found = problems(Path(argv[1]))
    for line in found:
        print(line)
    if not found:
        print(f"dist ok: {argv[1]} carries the package, the README and the licence only")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
