"""The input and output types, and what the activities hand each other. The workflow's
signature is the contract: `kontra publish` derives the published JSON Schema from
`SweepInput` and `SweepOutput` (and what they use). The others are the activities' own, and
`Report` is the content of the report artifact.

Each value crosses Temporal as one payload (2 MB at most, on a Temporal without kontra's
codec; `kontra serve`'s codec carries one over 256 KiB in the claim-check bucket): the
URLs come back from `plan` in one, the flagged ones go to `score` in one, and the sites' files
into the report. So a job is at most 200 URLs of at most 2,048 characters, each written as
RFC 3986 writes a URL (ASCII; anything else percent-encoded), so one byte each as JSON, and
what is kept of each site's files is bounded in bytes (activities.py): the largest payload,
for 200 such URLs on as many sites, stays under 1.7 MB.

`Artifact` (`kontra.types`) is the one Kontra type a seller's models use (decisions I29): a
reference to an encrypted file in the claim-check bucket, never the bytes.
"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

__all__ = [
    "HEADERS",
    "Artifact",
    "Findings",
    "Flagged",
    "Grade",
    "Graded",
    "Header",
    "HeaderCheck",
    "Plan",
    "Report",
    "Robots",
    "SecurityTxt",
    "SweepInput",
    "SweepOutput",
    "SweepSummary",
    "Tally",
    "Unreachable",
]

#: A URL as RFC 3986 writes it: ASCII, anything else percent-encoded (`%C3%BC` for `ü`).
Url = Annotated[
    str, Field(pattern=r"^https?://[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+$", max_length=2048)
]

#: The response headers checked, as HTTP names them (lowercase: names are case-insensitive).
Header = Literal[
    "strict-transport-security",
    "content-security-policy",
    "x-content-type-options",
    "x-frame-options",
    "referrer-policy",
]
HEADERS: tuple[Header, ...] = (
    "strict-transport-security",
    "content-security-policy",
    "x-content-type-options",
    "x-frame-options",
    "referrer-policy",
)
#: A page's grade: A misses no header, B one, C two, D three, F four or more.
Grade = Literal["A", "B", "C", "D", "F"]


# --------------------------------------------------------------------------- the contract


class SweepInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    urls: list[Url] = Field(
        min_length=1,
        max_length=200,
        description="The pages to check. Each one's site (scheme, host, port) has its "
        "robots.txt and security.txt read once.",
    )


class SweepSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grade: Grade | None = Field(
        description="The worst grade of the pages that answered (A: no page misses a "
        "header); null when none answered."
    )
    urls: int = Field(description="Pages checked, each URL once.")
    flagged: int = Field(description="Pages that miss at least one header.")
    unreachable: int = Field(description="Pages that did not answer.")
    sites: int = Field(description="Sites whose robots.txt and security.txt were read.")
    without_security_txt: int = Field(
        description="Sites with no security.txt naming a contact (RFC 9116)."
    )


class SweepOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    report: Artifact = Field(
        description="The report (JSON): each flagged page's grade and missing headers, a "
        "value to start from for each, the pages that did not answer, and each site's "
        "robots.txt Disallow paths and security.txt contacts."
    )
    summary: SweepSummary


# --------------------------------------------------------------------------- the worker's own


class Plan(BaseModel):
    """The input, normalised: each URL once, as it is fetched, and the sites they are on."""

    # At least one: the input has one.
    urls: list[str] = Field(min_length=1)
    sites: list[str]


class Robots(BaseModel):
    """A site's robots.txt: where it asks crawlers not to go (for any user agent)."""

    site: str
    status: int | None = None
    disallow: list[str] = Field(default_factory=list[str])
    error: str | None = None


class SecurityTxt(BaseModel):
    """A site's /.well-known/security.txt (RFC 9116): who to tell about a vulnerability."""

    site: str
    status: int | None = None
    contact: list[str] = Field(default_factory=list[str])
    expires: str | None = None
    error: str | None = None


class HeaderCheck(BaseModel):
    """What `check_headers` found for one URL: its final response's missing headers, or
    why it did not answer."""

    url: str
    status: int | None = None
    missing: list[Header] = Field(default_factory=list[Header])
    error: str | None = None


class Flagged(BaseModel):
    url: str
    missing: list[Header]


class Unreachable(BaseModel):
    url: str
    error: str


class Tally(BaseModel):
    """What the workflow keeps of every `HeaderCheck` (`tally`): a page that has every header
    leaves only a count."""

    checked: int = 0
    flagged: list[Flagged] = Field(default_factory=list[Flagged])
    unreachable: list[Unreachable] = Field(default_factory=list[Unreachable])


class Findings(BaseModel):
    """Everything the job found: what `score` (or `all_clear`) makes the report from."""

    tally: Tally
    robots: list[Robots]
    security_txt: list[SecurityTxt]


class Graded(BaseModel):
    url: str
    grade: Grade
    missing: list[Header]


class Report(BaseModel):
    """The `report` artifact's content."""

    summary: SweepSummary
    #: The pages that miss a header, worst first.
    pages: list[Graded] = Field(default_factory=list[Graded])
    unreachable: list[Unreachable] = Field(default_factory=list[Unreachable])
    #: For each header some page misses: a value to start from.
    advice: dict[Header, str] = Field(default_factory=dict[Header, str])
    robots: list[Robots]
    security_txt: list[SecurityTxt]
