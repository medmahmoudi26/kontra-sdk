"""What each tool answers, as pydantic models.

Every model is a tool's published ``outputSchema`` (MCP structured output): the agent's host
can validate the answer, and the model reads named fields instead of prose. Amounts are
decimal strings in the job's currency (never floats); times are UTC ISO 8601.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from kontra.gate_client import models as m

__all__ = [
    "ArtifactOut",
    "BalanceOut",
    "DescribeOut",
    "HireOut",
    "JobOut",
    "LimitsOut",
    "OfferingSummaryOut",
    "ProgressOut",
    "QuoteOut",
    "ResultOut",
    "SearchOut",
    "SellerOut",
    "StatusOut",
]


class _Out(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------- catalog


class SellerOut(_Out):
    party: str
    released: int = Field(description="Jobs this seller delivered and was paid for.")
    refunded: int = Field(description="Jobs refunded to their buyer (failed, late, canceled).")
    disputed: int = Field(description="Deliveries their buyer disputed.")


class OfferingSummaryOut(_Out):
    offering: str = Field(description="Exact reference (party/name@version).")
    name: str
    version: str
    description: str
    pricing_hint: str | None
    seller: SellerOut


class SearchOut(_Out):
    offerings: list[OfferingSummaryOut]
    next_cursor: str | None = Field(description="Pass as `cursor` to get more; null at the end.")


class LimitsOut(_Out):
    max_duration: str = Field(description="Longest a job may run before it is refunded (e.g. 6h).")
    max_duration_seconds: int
    review_window: str = Field(
        description="How long after delivery you may reject; silence then pays the seller."
    )
    review_window_seconds: int
    max_concurrent_jobs: int | None = Field(description="Jobs it runs at once; null: no limit.")
    max_output_bytes: int
    artifact_retention: str = Field(description="How long output files stay downloadable.")
    artifact_retention_seconds: int


class DescribeOut(_Out):
    offering: str = Field(description="Exact reference (party/name@version).")
    name: str
    version: str
    seller: str
    description: str
    pricing_hint: str | None
    tags: list[str]
    retired: bool = Field(
        description="True for an exact version that takes no new jobs (quote and hire refuse "
        "it); its jobs' results are still checked against this contract."
    )
    contract_hash: str
    limits: LimitsOut
    input_schema: dict[str, Any] = Field(
        description="JSON Schema (2020-12) the `input` of quote and hire must validate against."
    )
    output_schema: dict[str, Any] = Field(description="JSON Schema (2020-12) of the result.")
    input_takes_files: bool = Field(
        description="True when the input has file (KontraArtifact) fields, which this server "
        "cannot upload; such offerings are hired from the Python SDK (Kontra.upload)."
    )


# --------------------------------------------------------------------------- quote & hire


class QuoteOut(_Out):
    status: m.QuoteStatus = Field(description="ok, rejected (see reason) or busy (see eta).")
    offering: str = Field(description="The exact version that answered.")
    quote_id: str | None = Field(description="Pass to hire; only when status is ok.")
    price: str | None = Field(description="Decimal amount in `currency`.")
    currency: str | None
    max_duration_seconds: int | None = Field(description="Refunded if not delivered by then.")
    reason: str | None = Field(description="The seller's reason, when rejected or busy.")
    eta_seconds: int | None = Field(description="When busy: try again after this many seconds.")
    expires_at: datetime | None = Field(description="The quote must be hired before this.")


class BalanceOut(_Out):
    currency: str
    available: str = Field(description="Spendable now.")
    held: str = Field(description="Locked in escrow for open jobs.")


class HireOut(_Out):
    job_id: str
    offering: str
    price: str = Field(description="Locked in escrow now.")
    currency: str
    state: m.JobState | None
    escrow: m.Escrow | None
    deadline: datetime | None = Field(description="Refunded if not delivered by then.")
    balance: BalanceOut | None = Field(description="Your balance after the lock.")
    note: str


# --------------------------------------------------------------------------- jobs


class ProgressOut(_Out):
    summary: str = Field(description="One line: percent and message, or where the job is.")
    seq: int
    at: datetime
    source: m.ProgressSource = Field(
        description="heartbeat: the seller's own report; history: what the platform observed."
    )
    activity: str | None
    percent: float | None
    message: str | None
    details: Any = Field(
        default=None, description="The seller's heartbeat details (JSON), when small."
    )
    state: m.JobState | None
    activities_running: list[str]
    activities_completed: int | None


class JobOut(_Out):
    job_id: str
    offering: str
    seller: str
    state: m.JobState = Field(
        description="A2A task state: submitted, working, completed, failed or canceled."
    )
    escrow: m.Escrow = Field(description="held, released, refunded, disputed or resolved.")
    price: str
    currency: str
    created_at: datetime
    started_at: datetime | None
    deadline: datetime = Field(description="Refunded if not delivered by then.")
    delivered_at: datetime | None
    delivery_sha256: str | None = Field(description="Hash of the delivery as stored (ciphertext).")
    review_deadline: datetime | None = Field(
        description="After delivery: reject before this, or the payment is released."
    )
    reason: str | None = Field(description="Why it failed, was canceled or was disputed.")


class StatusOut(JobOut):
    progress: ProgressOut | None = Field(description="The latest progress event, if any.")
    last_heartbeat: ProgressOut | None = Field(
        description="The seller's latest own report, when the latest event is not one."
    )


class ArtifactOut(_Out):
    at: str = Field(description="Where the file sits in `output` (e.g. output.parts[1].file).")
    key: str
    size: int = Field(description="Bytes of the file (plaintext).")
    sha256: str = Field(description="Of the plaintext; verified on download.")
    content_type: str
    path: str | None = Field(description="The downloaded file, when download_dir was given.")


class ResultOut(_Out):
    job_id: str
    ready: bool = Field(description="False: not delivered yet; call again later.")
    state: m.JobState | None
    escrow: m.Escrow | None
    review_deadline: datetime | None = Field(
        description="Reject before this or the payment is released to the seller."
    )
    output: Any = Field(
        default=None,
        description="The decrypted output (files described, not inlined); null when not ready "
        "or too large (see output_file).",
    )
    output_bytes: int | None = Field(description="Size of the output as JSON.")
    output_file: str | None = Field(description="Where a large output was saved instead.")
    artifacts: list[ArtifactOut] = Field(
        description="The output's files, as many as fit here (see artifact_count)."
    )
    artifact_count: int = Field(description="How many file references the output holds.")
    note: str | None
