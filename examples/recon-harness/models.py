"""The types. `SweepRequest` and `SweepReport` are the handler's: `kontra publish` derives the
published contract from them, and the harness its own schema, which agree to the last member
(`tests/contract/test_harness.py`): both models forbid other keys and every output field is
required, with no default. A change to either needs a new `version`.

The rest are the tools' and the model's own types, never part of the contract. Plain
pydantic; the tools' module reads them as real classes (the harness reads a tool's types from
its module's globals), so neither module here uses `from __future__ import annotations`."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

# --------------------------------------------------------------------------- the contract


class SweepRequest(BaseModel):
    """The domains to sweep, in one turn of the agent. `want_approval` and `want_callback` ask
    for steps a person takes part in, which a hire has no one for: its quote refuses them."""

    model_config = ConfigDict(extra="forbid")

    domains: list[Annotated[str, Field(min_length=1, max_length=253)]] = Field(
        min_length=1, max_length=20, description="The domains to sweep."
    )
    want_approval: bool = Field(
        default=False, description="Post the findings once a person approves (not in a hire)."
    )
    want_callback: bool = Field(
        default=False, description="Ask a person to confirm the report (not in a hire)."
    )


class SweepReport(BaseModel):
    """What the sweep found, and what was done with it."""

    model_config = ConfigDict(extra="forbid")

    domains: int = Field(description="How many domains were swept.")
    targets: int = Field(description="How many hosts were probed.")
    expired: int = Field(description="Hosts whose certificate has expired.")
    weak: int = Field(description="Hosts whose certificate is weak.")
    posted: str | None = Field(description="Where the findings were posted, if they were.")
    confirmed: str | None = Field(description="What the person answered, if they were asked.")
    summary: str = Field(description="The model's summary of the sweep.")


# --------------------------------------------------------------------------- the tools'


class Targets(BaseModel):
    """What `plan` chose to probe."""

    hosts: list[str]

    def __str__(self) -> str:  # the tool's result, as the harness reports it (str(result))
        return f"{len(self.hosts)} targets"


class Probed(BaseModel):
    """What `probe` found."""

    probed: int
    open: int

    def __str__(self) -> str:
        return f"{self.probed}/{self.probed}"


class CertRequest(BaseModel):
    """The hosts whose certificates the subagent checks."""

    hosts: list[str]


class Cert(BaseModel):
    """One host's certificate."""

    host: str
    expired: bool
    weak: bool

    def __str__(self) -> str:
        return "expired" if self.expired else "weak" if self.weak else "ok"


class CertReport(BaseModel):
    """What the subagent found."""

    expired: int
    weak: int


# --------------------------------------------------------------------------- the model's


class Prompt(BaseModel):
    """What the model is asked at one step of the turn: the step, and what the turn learnt so
    far as short notes -- counts, never the buyer's domains."""

    step: str
    notes: list[str] = Field(default_factory=list[str])


class Thought(BaseModel):
    """The model's answer: its words, and what they cost."""

    model: str
    text: str
    input_tokens: int
    output_tokens: int
