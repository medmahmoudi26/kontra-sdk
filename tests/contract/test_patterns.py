"""The pattern gate (kontra/contract/patterns.py): what a contract's ``pattern`` may be.

The cases come from running nexgen 0.2.5's own gate and Go's ``regexp`` on each pattern,
and from timing Python's ``re`` on adversarial input: every pattern the catalog accepts
must load in every generated client and must not backtrack exponentially in the seller's
handler, where a buyer chooses the input for free (a quote).
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from kontra.contract.formats import compile_pattern, pattern_problem

#: Accepted by the catalog, by nexgen for all four targets, and by Go's regexp.
PORTABLE = [
    r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$",
    r"^(ab)*$",
    r"^(a|bc)*$",
    r"^([a-z]+-)*$",
    r"^[a-z0-9][a-z0-9/._-]*$",
    r"\d+",
    r"^(\d+\.)*\d+$",
    r"^([a-z0-9]+\.)+[a-z]{2,}$",
    r"^(?:[a-z]+,)*[a-z]+$",
    r"^(a+)?$",
    r"^(ab|cd){1,3}$",
    r"^(a|b){3}$",
    r"^([ab])*$",
    r"\t\n\r\f\x41\.\/",
    r"[\-]",
    r"a\$",
    r"[a\]]",
    "a*?",
    "a{2}?",
    "(?:)",
    "^$",
    "",
    "a|",
    r"\d{1,3}(,\d{3})*",
    r"[^\S]",
    r"[\w-]",
    r"\W\D\s",
    "é",
    r"^https?://[^\s]+$",
    r"^[A-Za-z0-9.-]+(:[0-9]{1,5})?$",
    r"^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$",
    r"(\d{1,3}\.){3}\d{1,3}",
    r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
    r"(a{100}){10}",
    r"(?:[a-z0-9]|%[0-9a-f]{2})*",
    r"^[\w.+-]+@[\w-]+\.[\w.-]+$",
    r"^\+?[0-9]{7,15}$",
    r"[a-z]{0,1000}",
    r"^[0-9a-f]{64}$",
]


@pytest.mark.parametrize("pattern", PORTABLE)
def test_portable_patterns_are_accepted(pattern: str) -> None:
    assert pattern_problem(pattern) is None


@pytest.mark.parametrize(
    ("pattern", "fragment"),
    [
        # nexgen D7: ambiguous unbounded loops (exponential under backtracking)
        (r"^(a+)+$", "ambiguous repetition"),
        (r"^((a+))+$", "ambiguous repetition"),
        (r"^(?:a+)+$", "ambiguous repetition"),
        (r"^(a+){2,}$", "ambiguous repetition"),
        (r"^(a{2,5})*$", "ambiguous repetition"),
        (r"^(a?)*$", "ambiguous repetition"),
        (r"^(a*)*$", "ambiguous repetition"),
        (r"^(a|)*$", "ambiguous repetition"),
        (r"^(a+b?)+$", "ambiguous repetition"),
        (r"^(\w+\s?)*$", "ambiguous repetition"),
        (r"^(a|b)*$", "ambiguous repetition"),
        (r"(?:a|b)+", "ambiguous repetition"),
        (r"^(ab|cd)*$", "ambiguous repetition"),
        (r"^(a|b)+c$", "ambiguous repetition"),
        # beyond nexgen: loops one character can continue two ways (exponential in Python)
        (r"^(x[a-z]+)*$", "continue the loop in two ways"),
        (r"^(\w+\.?\w+)*$", "continue the loop in two ways"),
        (r"^(a|b|ab)+$", "continue the loop in two ways"),
        (r"^(a|aa)+$", "continue the loop in two ways"),
        (r"^(a+a)+$", "continue the loop in two ways"),
        (r"^(a+){3}$", "continue the loop in two ways"),
        # Go's regexp: a generated Go client would panic at import
        ("a{1001}", "more than 1000"),
        (r"(a{100}){11}", "product exceeds 1000"),
        (r"(a{2}){600}", "product exceeds 1000"),
        # not portable across the engines
        ("(?i)abc", "inline flag"),
        ("(?u)a", "inline flag"),
        ("(?L)a", "unknown group"),
        ("(?P<n>a)", "named group"),
        ("(?<n>a)", "named group"),
        ("(?=a)", "lookaround"),
        ("(?<!a)b", "lookaround"),
        ("(?#note)a", "comment"),
        ("(?>a)", "atomic"),
        (r"\bab", "word-boundary"),
        (r"\Aab", r"\A"),
        (r"a\Z", r"\Z"),
        (r"a\z", r"\z"),
        (r"\0", "octal"),
        (r"(a)\1", "backreference"),
        ("\\" + "u0041", r"\u"),
        (r"\x{41}", r"\x escape"),
        (r"\p{L}", "property"),
        (r"\v", r"\x0B"),
        (r"\_", "escapes '_'"),
        (r"\-", "escapes '-'"),
        ("[[:alpha:]]", "POSIX"),
        ("[a[]", "POSIX"),
        ("[]a]", "']' first"),
        ("[^]a]", "']' first"),
        ("[a&&b]", "set operator"),
        ("a}", "lone '}'"),
        ("a{", "lone '{'"),
        ("{", "lone '{'"),
        ("a]", "lone ']'"),
        (r"[\S.]", "negated class"),
        ("a**", "another repetition"),
        ("a{2}*", "another repetition"),
        ("a*+", "another repetition"),
        ("*a", "nothing to repeat"),
        ("^*", "nothing to repeat"),
        ("a{3,2}", "minimum exceeds"),
        ("[z-a]", "reversed"),
        (r"[a-\d]", "ends in a class"),
        ("(", "unclosed '('"),
        (")", "unbalanced ')'"),
        ("[a", "unclosed '['"),
        ("\\", "lone backslash"),
        ("(" * 70 + ")" * 70, "deeper than"),
        ("a" * 1001, "longer than"),
    ],
)
def test_refused_patterns_say_why(pattern: str, fragment: str) -> None:
    problem = pattern_problem(pattern)
    assert problem is not None, pattern
    assert fragment in problem, problem


#: Inputs that make a backtracking engine explore every way to split a run of
#: characters, followed by a character that makes the match fail at the end.
_ADVERSARIAL = ["a" * 5000 + "!", "1" * 5000 + "!", "xa" * 2500 + "!", "a." * 2500 + "!"]


@pytest.mark.parametrize("pattern", [p for p in PORTABLE if p.startswith("^")])
def test_accepted_anchored_patterns_run_in_linear_time(pattern: str) -> None:
    compiled = compile_pattern(pattern)
    for value in _ADVERSARIAL:
        started = time.perf_counter()
        compiled.search(value)
        assert time.perf_counter() - started < 0.5, (pattern, value[:10])


def test_the_refused_shapes_really_are_exponential_in_python() -> None:
    """Why the gate goes beyond nexgen: nexgen accepts this one, and Python's time grows
    tenfold with every few characters (seconds at 50, hours at 80) under a backtracking
    engine. Measured with Python's ``re`` on purpose: Kontra itself matches with RE2."""
    compiled = re.compile(r"^(x[a-z]+)*\Z")

    def seconds(units: int, runs: int) -> float:
        best = float("inf")
        for _ in range(runs):  # the fastest run: noise only ever adds time
            started = time.perf_counter()
            compiled.search("xa" * units + "!")
            best = min(best, time.perf_counter() - started)
        return best

    assert seconds(19, 2) > 50 * max(seconds(8, 5), 1e-5)


def test_patterns_match_in_linear_time() -> None:
    """Kontra matches with RE2, so even a pattern that backtracks exponentially in Python, and
    an unanchored one that is quadratic there, costs linear time on hostile input."""
    for pattern, text in [
        (r"^(x[a-z]+)*$", "xa" * 5_000 + "!"),
        (r"[a-z]+$", "a" * 200_000 + "!"),
    ]:
        started = time.perf_counter()
        assert compile_pattern(pattern).search(text) is None
        assert time.perf_counter() - started < 0.5


def test_analysis_is_bounded() -> None:
    """A seller cannot make the catalog spin: the worst shapes are refused quickly."""
    for pattern in [
        "(a?){1000}",
        "(?:" + "".join(f"\\x{i:02x}?" for i in range(32, 127)) + "){10}",
        "[a-z]{0,1000}[0-9]{0,1000}",
    ]:
        started = time.perf_counter()
        pattern_problem(pattern)
        assert time.perf_counter() - started < 2.0, pattern


@settings(max_examples=500, deadline=None)
@given(st.text(alphabet=st.sampled_from(list("ab()[]{}|*+?^$\\-.,:0123dws")), max_size=24))
def test_the_gate_never_crashes_and_accepted_patterns_compile(pattern: str) -> None:
    """Patterns come from strangers: refuse with a reason, never raise; and whatever is
    accepted compiles in Python with the semantics the validators use."""
    problem = pattern_problem(pattern)
    if problem is None:
        compile_pattern(pattern)


@pytest.mark.slow
def test_go_compiles_every_portable_pattern(tmp_path: Path) -> None:
    """nexgen emits Go patterns as package-level regexp.MustCompile: one that RE2 refuses
    panics the buyer's program at import. Needs a Go toolchain; skipped without one."""
    go = shutil.which("go")
    if go is None:
        pytest.skip("no Go toolchain")
    (tmp_path / "go.mod").write_text("module patterns\n\ngo 1.21\n", encoding="utf-8")
    (tmp_path / "patterns.json").write_text(json.dumps(PORTABLE), encoding="utf-8")
    (tmp_path / "main.go").write_text(
        """package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
)

func main() {
	var patterns []string
	data, _ := os.ReadFile("patterns.json")
	_ = json.Unmarshal(data, &patterns)
	failed := false
	for _, p := range patterns {
		if _, err := regexp.Compile(p); err != nil {
			fmt.Printf("%q: %v\\n", p, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}
""",
        encoding="utf-8",
    )
    proc = subprocess.run(
        [go, "run", "."], cwd=tmp_path, capture_output=True, text=True, timeout=300, check=False
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
