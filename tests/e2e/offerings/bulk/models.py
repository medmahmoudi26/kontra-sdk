"""A file of `size` pseudo-random bytes, returned as an artifact; or, when the gate refuses
the upload, the refusal."""

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

__all__ = ["Artifact", "BulkInput", "BulkOutput"]


class BulkInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    size: int = Field(ge=1, le=8 * 1024**3)
    seed: int = Field(ge=0, le=2**31)


class BulkOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    files: list[Artifact] = Field(max_length=1)  # empty when the upload was refused
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    refused: str = Field(max_length=4000)  # the refusal, when files is empty
