"""The ``pattern`` gate: which regular expressions a contract may carry.

A pattern in a contract runs in five engines: Python ``re`` (the seller's handler and the
buyer's SDK, through :class:`~kontra.contract.InputValidator`) and the regex engines of
nexgen's four targets (Go RE2, JavaScript ``u`` mode, Python, ``java.util.regex``). The
catalog accepts only what all of them read alike and run safely:

* **nexgen's own gate** (0.2.5, ``src/json_schema/pattern.rs``): no inline flags, named
  groups, lookaround, word boundaries, ``\\A``/``\\z``, POSIX or nested classes, set
  operators, lone ``{ } ]``, non-portable escapes, and its rule D7 against ambiguous
  unbounded loops (``^(a+)+$``, ``(a|b)*``). A pattern it refuses would make
  ``kontra pull`` fail for every buyer.
* **Go's repeat limits.** nexgen emits Go patterns as package-level
  ``regexp.MustCompile``, so ``a{1001}`` (or nested counts multiplying past 1000) would
  panic every buyer's Go program when it loads. nexgen does not check this.
* **Ambiguous loop bodies.** D7 still admits loops a backtracking engine explores in
  exponentially many ways -- ``^(x[a-z]+)*$`` takes seconds on 50 characters in Python,
  and ``(a|aa)+``, ``(\\w+\\.?\\w+)*`` likewise. Python runs these in the seller's handler
  on a buyer's input (a quote costs the buyer nothing), so every repetition must be
  *deterministic* in Glushkov's sense: at each point of the loop, one character selects
  at most one way to continue. Version numbers, slugs, URLs and base64 all are.

What remains is polynomial: an unanchored pattern is searched from every offset, so a
long string costs quadratic time in Python (``maxLength`` bounds it). Only a linear-time
engine (RE2) removes that.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["MAX_REPEAT", "PATTERN_MAX", "PatternProblem", "check_pattern"]

#: Longest pattern accepted, in characters.
PATTERN_MAX = 1000
#: Go's RE2 limit on a counted repetition, also applied to the product of nested counts.
MAX_REPEAT = 1000
_MAX_NESTING = 64
#: Work allowed for analysing one pattern: positions created (``x{1000}`` is 1000) and
#: pairs of positions compared. A seller cannot make the catalog spin on a document.
_MAX_POSITIONS = 20_000
_MAX_COMPARISONS = 100_000

#: Escapes ECMA-262 ``u`` mode allows outside a class (identity escapes) ...
_PORTABLE_ESCAPES = frozenset("^$\\.*+?()[]{}|/")
#: ... and inside a class, where ``-`` may be escaped too.
_PORTABLE_CLASS_ESCAPES = _PORTABLE_ESCAPES | {"-"}
_CONTROL_ESCAPES = {"t": "\t", "n": "\n", "r": "\r", "f": "\f"}
_HEX = frozenset("0123456789abcdefABCDEF")
_FLAG_CHARS = frozenset("imsuxU-")

Ranges = tuple[tuple[int, int], ...]


def _merge(ranges: list[tuple[int, int]]) -> Ranges:
    out: list[tuple[int, int]] = []
    for lo, hi in sorted(ranges):
        if out and lo <= out[-1][1] + 1:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return tuple(out)


@dataclass(frozen=True)
class _Chars:
    """A set of characters: ``ranges``, or everything but them when ``negated``."""

    negated: bool
    ranges: Ranges

    def overlaps(self, other: _Chars) -> bool:
        if self.negated and other.negated:
            return True  # two co-finite sets of a huge alphabet always meet
        if self.negated:
            return not _covered(other.ranges, self.ranges)
        if other.negated:
            return not _covered(self.ranges, other.ranges)
        return any(a <= d and c <= b for a, b in self.ranges for c, d in other.ranges)


def _covered(inner: Ranges, outer: Ranges) -> bool:
    """Every character of ``inner`` lies in ``outer`` (both merged)."""
    return all(any(c <= a and b <= d for c, d in outer) for a, b in inner)


def _one(ch: str) -> _Chars:
    return _Chars(False, ((ord(ch), ord(ch)),))


_DIGIT: Ranges = ((48, 57),)
_WORD: Ranges = ((48, 57), (65, 90), (95, 95), (97, 122))
_SPACE: Ranges = ((9, 13), (32, 32))  # \t \n \v \f \r and space: the ASCII set nexgen pins
_PERL: dict[str, _Chars] = {
    "d": _Chars(False, _DIGIT),
    "D": _Chars(True, _DIGIT),
    "w": _Chars(False, _WORD),
    "W": _Chars(True, _WORD),
    "s": _Chars(False, _SPACE),
    "S": _Chars(True, _SPACE),
}
_DOT = _Chars(True, ((10, 10),))  # nexgen emits . as [^\n]; Python's . is the same


class PatternProblem(ValueError):
    """Why a pattern is outside the portable subset."""


@dataclass
class _Node:
    kind: str  # empty char assert group concat alt repeat
    start: int
    end: int
    children: list[_Node] = field(default_factory=list["_Node"])
    chars: _Chars | None = None  # for kind == "char"
    lo: int = 0
    hi: int | None = None  # repeat upper bound; None is unbounded
    counted: bool = False  # written {n}, {n,} or {n,m}


# --------------------------------------------------------------------------- parsing


class _Parser:
    """A recursive-descent parser for the portable subset. Anything outside it is a
    :class:`PatternProblem` with the portable spelling, never a guess."""

    def __init__(self, pattern: str) -> None:
        self.p = pattern
        self.i = 0
        self.depth = 0

    def peek(self, offset: int = 0) -> str:
        j = self.i + offset
        return self.p[j] if j < len(self.p) else ""

    def parse(self) -> _Node:
        node = self.alternation()
        if self.i < len(self.p):  # only an unmatched ")" stops alternation() early
            raise PatternProblem(f"has an unbalanced ')' at position {self.i}")
        return node

    def alternation(self) -> _Node:
        start = self.i
        branches = [self.concat()]
        while self.peek() == "|":
            self.i += 1
            branches.append(self.concat())
        if len(branches) == 1:
            return branches[0]
        return _Node("alt", start, self.i, branches)

    def concat(self) -> _Node:
        start = self.i
        items: list[_Node] = []
        while self.i < len(self.p) and self.peek() not in "|)":
            items.append(self.quantified(self.atom()))
        if not items:
            return _Node("empty", start, start)
        if len(items) == 1:
            return items[0]
        return _Node("concat", start, self.i, items)

    def quantified(self, atom: _Node) -> _Node:
        bounds = self.quantifier()
        if bounds is None:
            return atom
        lo, hi, counted = bounds
        if atom.kind == "assert":
            raise PatternProblem(f"has a quantifier with nothing to repeat at position {atom.end}")
        if self.peek() == "?":
            self.i += 1  # lazy: the same language, and every engine has it
        node = _Node("repeat", atom.start, self.i, [atom], lo=lo, hi=hi, counted=counted)
        c = self.peek()
        if c in ("*", "+", "?") or (c == "{" and self._counted_at(self.i) is not None):
            raise PatternProblem(
                "applies a repetition operator directly to another repetition (a{2}*, a*+), "
                "which Go, JavaScript and Python reject; group or rewrite it"
            )
        return node

    def _counted_at(self, i: int) -> tuple[int, int | None, int] | None:
        """``{n}``, ``{n,}`` or ``{n,m}`` at ``i`` as (lo, hi, end), else None."""
        p = self.p
        j = k = i + 1
        while k < len(p) and "0" <= p[k] <= "9":
            k += 1
        if k == j or k - j > 9:
            return None
        lo = int(p[j:k])
        if k < len(p) and p[k] == "}":
            return lo, lo, k + 1
        if k >= len(p) or p[k] != ",":
            return None
        m = n = k + 1
        while n < len(p) and "0" <= p[n] <= "9":
            n += 1
        if n - m > 9 or n >= len(p) or p[n] != "}":
            return None
        return lo, (int(p[m:n]) if n > m else None), n + 1

    def quantifier(self) -> tuple[int, int | None, bool] | None:
        c = self.peek()
        if c in ("*", "+", "?"):
            self.i += 1
            return {"*": (0, None, False), "+": (1, None, False), "?": (0, 1, False)}[c]
        if c != "{":
            return None
        counted = self._counted_at(self.i)
        if counted is None:
            raise PatternProblem(
                f"has a lone '{{' at position {self.i} (not a {{n}}, {{n,}} or {{n,m}} "
                "quantifier), which JavaScript and Java reject; escape it as \\{"
            )
        lo, hi, end = counted
        if hi is not None and hi < lo:
            raise PatternProblem(
                f"has a repeat range {{{lo},{hi}}} whose minimum exceeds its maximum"
            )
        if max(lo, hi or 0) > MAX_REPEAT:
            raise PatternProblem(
                f"repeats more than {MAX_REPEAT} times, which Go's regexp refuses (a generated "
                "Go client would panic when it loads)"
            )
        self.i = end
        return lo, hi, True

    def atom(self) -> _Node:
        start = self.i
        c = self.peek()
        if c == "(":
            return self.group()
        if c == "[":
            return self.char_class()
        if c == "\\":
            chars = self.escape(in_class=False)
            return _Node("char", start, self.i, chars=chars)
        if c in ("*", "+", "?") or (c == "{" and self._counted_at(self.i) is not None):
            raise PatternProblem(f"has a quantifier with nothing to repeat at position {start}")
        if c in ("{", "}", "]"):
            raise PatternProblem(
                f"has a lone '{c}' outside a character class at position {start}, which "
                f"JavaScript's u mode rejects; escape it as \\{c}"
            )
        self.i += 1
        if c in ("^", "$"):
            return _Node("assert", start, self.i)
        return _Node("char", start, self.i, chars=_DOT if c == "." else _one(c))

    def group(self) -> _Node:
        start = self.i
        self.i += 1  # (
        if self.peek() == "?":
            nxt = self.peek(1)
            if nxt != ":":
                raise PatternProblem(self._group_problem(nxt))
            self.i += 2
        self.depth += 1
        if self.depth > _MAX_NESTING:
            raise PatternProblem(f"nests groups deeper than {_MAX_NESTING} levels")
        body = self.alternation()
        self.depth -= 1
        if self.peek() != ")":
            raise PatternProblem(f"has an unclosed '(' at position {start}")
        self.i += 1
        return _Node("group", start, self.i, [body])

    def _group_problem(self, nxt: str) -> str:
        if nxt in ("=", "!") or (nxt == "<" and self.peek(2) in ("=", "!")):
            return (
                "uses a lookaround ((?=, (?!, (?<=, (?<!), which RE2 (Go, nexgen) cannot "
                "compile; only the regular (no-backtracking) subset is supported"
            )
        if nxt in ("P", "<"):
            return (
                "uses a named group or backreference ((?P<..>, (?<..>, (?P=..)), which has "
                "no portable spelling; use a non-capturing group (?:...)"
            )
        if nxt == "#":
            return "uses a (?#...) comment, which RE2 and JavaScript reject"
        if nxt == ">":
            return "uses an atomic group (?>...), which RE2 and JavaScript reject"
        if nxt in _FLAG_CHARS:
            return (
                "uses an inline flag group ((?i), (?i:...)), which JavaScript cannot compile; "
                "spell the cases out (e.g. [Aa]) instead"
            )
        return f"uses an unknown group syntax (?{nxt}"

    def escape(self, *, in_class: bool) -> _Chars:
        """One escape: the characters it matches, or a refusal naming the portable form."""
        if self.i + 1 >= len(self.p):
            raise PatternProblem("ends with a lone backslash")
        c = self.p[self.i + 1]
        self.i += 2
        if c in _PERL:
            return _PERL[c]
        if c in _CONTROL_ESCAPES:
            return _one(_CONTROL_ESCAPES[c])
        if c == "x":
            digits = self.p[self.i : self.i + 2]
            if len(digits) == 2 and all(d in _HEX for d in digits):
                self.i += 2
                return _one(chr(int(digits, 16)))
            raise PatternProblem(
                "uses a \\x escape that is not exactly two hex digits (\\x{...} is a "
                "JavaScript SyntaxError and a Python error); write \\xHH or the character"
            )
        if c in (_PORTABLE_CLASS_ESCAPES if in_class else _PORTABLE_ESCAPES):
            return _one(c)
        raise PatternProblem(self._escape_problem(c))

    @staticmethod
    def _escape_problem(c: str) -> str:
        if c in ("b", "B"):
            return (
                "uses a word-boundary assertion (\\b, \\B), whose meaning beside non-ASCII "
                "text differs between engines; spell the delimiters explicitly"
            )
        if c in ("A", "z", "Z"):
            return (
                f"uses \\{c}, which is not portable (JavaScript has no \\A or \\z, RE2 no "
                "\\Z); use ^ and $ (every generated validator reads $ as end of input)"
            )
        if "0" <= c <= "9":
            return (
                "uses an octal escape or a backreference (\\0-\\9), which is not portable; "
                "use \\xHH for a control character"
            )
        if c in ("u", "U"):
            return (
                "uses a \\u/\\U escape, which Go's regexp cannot compile; write the character "
                "itself (or \\xHH for ASCII)"
            )
        if c in ("p", "P"):
            return (
                "uses a Unicode property escape (\\p{...}), which Python's re cannot compile; "
                "spell the set as an explicit class"
            )
        if c == "v":
            return "uses \\v, which Java reads as a class of vertical whitespace; use \\x0B"
        return (
            f"escapes {c!r}, which is not portable (JavaScript's u mode allows escaping only "
            "^ $ \\ . * + ? ( ) [ ] { } | / and, in a class, -); write it unescaped"
        )

    def char_class(self) -> _Node:
        start = self.i
        self.i += 1  # [
        negated = self.peek() == "^"
        if negated:
            self.i += 1
        if self.peek() == "]":
            raise PatternProblem(
                "places ']' first in a character class ([] or [^]...), which JavaScript's u "
                "mode rejects; escape it as \\]"
            )
        ranges: list[tuple[int, int]] = []
        perls: list[_Chars] = []
        members = 0
        prev: str | None = None  # the previous member, when it is one character
        while True:
            c = self.peek()
            if c == "":
                raise PatternProblem(f"has an unclosed '[' at position {start}")
            if c == "]":
                self.i += 1
                break
            if c == "[":
                raise PatternProblem(
                    "nests a class or uses a POSIX class ([[:alpha:]]) inside a character "
                    "class, which engines read differently; merge the members, and escape a "
                    "literal [ as \\["
                )
            pair = self.p[self.i : self.i + 2]
            if pair in ("&&", "--", "~~"):
                raise PatternProblem(
                    f"uses the class set operator {pair!r}, which JavaScript cannot compile and "
                    "the other engines read differently; spell the set explicitly"
                )
            if c == "-" and prev is not None and self.peek(1) not in ("]", ""):
                self.i += 1
                end = self._class_member()
                if not isinstance(end, str):
                    raise PatternProblem(
                        "has a character range that ends in a class (\\d, \\w, \\s)"
                    )
                if ord(end) < ord(prev):
                    raise PatternProblem(f"has a reversed character range {prev}-{end}")
                ranges[-1] = (ord(prev), ord(end))
                prev = None
                continue
            member = self._class_member()
            members += 1
            if isinstance(member, str):
                ranges.append((ord(member), ord(member)))
                prev = member
            else:
                perls.append(member)
                prev = None
        if len(perls) == 1 and members == 1:
            only = perls[0]  # [\S] is \S; [^\S] is \s
            chars = _Chars(only.negated != negated, only.ranges)
        else:
            if any(p.negated for p in perls):
                raise PatternProblem(
                    "uses a negated class (\\D, \\W, \\S) inside a multi-member character "
                    "class, an open-ended complement with no portable spelling; list the set"
                )
            for p in perls:
                ranges.extend(p.ranges)
            chars = _Chars(negated, _merge(ranges))
        return _Node("char", start, self.i, chars=chars)

    def _class_member(self) -> str | _Chars:
        """One class member: its character, or the set of a Perl class."""
        c = self.peek()
        if c != "\\":
            self.i += 1
            return c
        chars = self.escape(in_class=True)
        match chars.ranges:
            case ((lo, hi),) if lo == hi and not chars.negated:
                return chr(lo)
            case _:
                return chars


# --------------------------------------------------------------------------- D7


def _strip_groups(node: _Node) -> _Node:
    while node.kind == "group":
        node = node.children[0]
    return node


def _nullable(node: _Node) -> bool:
    if node.kind in ("empty", "assert"):
        return True
    if node.kind == "char":
        return False
    if node.kind == "repeat":
        return node.lo == 0 or _nullable(node.children[0])
    if node.kind == "alt":
        return any(_nullable(c) for c in node.children)
    return all(_nullable(c) for c in node.children)  # group, concat


def _reduces_to_inexact_repetition(node: _Node) -> bool:
    node = _strip_groups(node)
    if node.kind == "repeat":
        return not (node.counted and node.hi == node.lo)
    if node.kind == "alt":
        return any(_reduces_to_inexact_repetition(c) for c in node.children)
    if node.kind == "concat":
        kids = node.children
        return any(
            _reduces_to_inexact_repetition(c)
            and all(_nullable(o) for j, o in enumerate(kids) if j != i)
            for i, c in enumerate(kids)
        )
    return False


def _fixed_width(node: _Node) -> int | None:
    if node.kind in ("empty", "assert"):
        return 0
    if node.kind == "char":
        return 1
    if node.kind == "group":
        return _fixed_width(node.children[0])
    if node.kind == "concat":
        total = 0
        for c in node.children:
            w = _fixed_width(c)
            if w is None:
                return None
            total += w
        return total
    if node.kind == "alt":
        widths = {_fixed_width(c) for c in node.children}
        return widths.pop() if len(widths) == 1 else None
    if node.counted and node.hi == node.lo:
        w = _fixed_width(node.children[0])
        return None if w is None else w * node.lo
    return None


def _d7(node: _Node, pattern: str) -> str | None:
    """nexgen's rule D7 for one unbounded repetition: why its body is ambiguous."""
    body = _strip_groups(node.children[0])
    if _nullable(body):
        return "its body can match the empty string"
    if _reduces_to_inexact_repetition(body):
        return "it applies a quantifier to another open-ended quantifier"
    if body.kind == "alt":
        if len({pattern[c.start : c.end] for c in body.children}) < len(body.children):
            return "its body is an alternation with two identical branches"
        if (_fixed_width(body) or 0) > 0:
            return "its body alternates branches of one fixed width, which overflows Java's stack"
    return None


# --------------------------------------------------------------------------- determinism


@dataclass
class _Budget:
    positions: int = _MAX_POSITIONS
    comparisons: int = _MAX_COMPARISONS

    def spend(self, positions: int = 0, comparisons: int = 0) -> None:
        self.positions -= positions
        self.comparisons -= comparisons
        if self.positions < 0 or self.comparisons < 0:
            raise PatternProblem("is too complex to analyse; simplify its repetitions")


class _Glushkov:
    """Positions, first/last sets and follow sets of one expression (Glushkov's
    construction), with counted repetitions expanded into copies."""

    def __init__(self, budget: _Budget) -> None:
        self.budget = budget
        self.chars: list[_Chars] = []
        self.follow: list[set[int]] = []

    def _position(self, chars: _Chars) -> int:
        self.budget.spend(positions=1)
        self.chars.append(chars)
        self.follow.append(set())
        return len(self.chars) - 1

    def _seq(
        self, a: tuple[bool, set[int], set[int]], b: tuple[bool, set[int], set[int]]
    ) -> tuple[bool, set[int], set[int]]:
        for p in a[2]:
            self.follow[p] |= b[1]
        first = a[1] | (b[1] if a[0] else set())
        last = b[2] | (a[2] if b[0] else set())
        return a[0] and b[0], first, last

    def build(self, node: _Node) -> tuple[bool, set[int], set[int]]:
        """(nullable, first, last) of ``node``; fills ``follow``."""
        if node.kind in ("empty", "assert"):
            return True, set(), set()
        if node.kind == "char":
            assert node.chars is not None
            p = self._position(node.chars)
            return False, {p}, {p}
        if node.kind == "group":
            return self.build(node.children[0])
        if node.kind == "concat":
            out = self.build(node.children[0])
            for child in node.children[1:]:
                out = self._seq(out, self.build(child))
            return out
        if node.kind == "alt":
            nullable, first, last = False, set[int](), set[int]()
            for child in node.children:
                n, f, la = self.build(child)
                nullable, first, last = nullable or n, first | f, last | la
            return nullable, first, last
        return self._repeat(node)

    def _repeat(self, node: _Node) -> tuple[bool, set[int], set[int]]:
        child = node.children[0]
        parts: list[tuple[bool, set[int], set[int]]] = []
        if node.hi is None:
            parts += [self.build(child) for _ in range(max(node.lo - 1, 0))]
            n, f, la = self.build(child)
            for p in la:
                self.follow[p] |= f
            parts.append((n or node.lo == 0, f, la))
        else:
            parts += [self.build(child) for _ in range(node.lo)]
            optional: tuple[bool, set[int], set[int]] | None = None
            for _ in range(node.hi - node.lo):  # x{0,k} is (x (x (...)?)?)?
                copy = self.build(child)
                inner = copy if optional is None else self._seq(copy, optional)
                optional = (True, inner[1], inner[2])
            if optional is not None:
                parts.append(optional)
        if not parts:
            return True, set(), set()
        out = parts[0]
        for part in parts[1:]:
            out = self._seq(out, part)
        return out

    def ambiguous(self, first: set[int]) -> bool:
        """Whether two positions that can come next (first, or after some position)
        accept a common character. Bounded work: a seller cannot stall the catalog."""
        for group in (first, *self.follow):
            members = sorted(group)
            self.budget.spend(comparisons=len(members) * (len(members) - 1) // 2)
            for i, a in enumerate(members):
                for b in members[i + 1 :]:
                    if self.chars[a].overlaps(self.chars[b]):
                        return True
        return False


def _walk(node: _Node, pattern: str, product: int, budget: _Budget) -> None:
    if node.kind == "repeat":
        if node.counted:
            product *= node.hi if node.hi is not None else max(node.lo, 1)
            if product > MAX_REPEAT:
                raise PatternProblem(
                    f"nests counted repetitions whose product exceeds {MAX_REPEAT}, which Go's "
                    "regexp refuses (a generated Go client would panic when it loads)"
                )
        reason = _d7(node, pattern) if node.hi is None else None
        if reason is None and (node.hi is None or node.hi > 1):
            g = _Glushkov(budget)
            _, first, _ = g.build(node)
            if g.ambiguous(first):
                reason = (
                    "one character can continue the loop in two ways, as in (a|aa)+ or (x[a-z]+)*"
                )
        if reason is not None:
            raise PatternProblem(
                f"has an ambiguous repetition at position {node.start} ({reason}): linear under "
                "RE2, but exponential under the backtracking engines of Python, JavaScript and "
                "Java, so one crafted value would stall a validator; make each iteration start "
                "with a character that cannot continue the previous one (e.g. ^a+$ for "
                "^(a+)+$, ^[a-z]+(-[a-z]+)*$ for a hyphenated slug)"
            )
    for child in node.children:
        _walk(child, pattern, product, budget)


def check_pattern(pattern: str) -> None:
    """Raise :class:`PatternProblem` unless ``pattern`` is in the portable, safe subset."""
    if len(pattern) > PATTERN_MAX:
        raise PatternProblem(f"is longer than {PATTERN_MAX} characters")
    _walk(_Parser(pattern).parse(), pattern, 1, _Budget())
