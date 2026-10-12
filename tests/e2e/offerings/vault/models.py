"""An artifact in, an artifact out, and an inline note large enough (over the codec's 256 KiB
threshold) to travel through the claim-check bucket."""

from pydantic import BaseModel, ConfigDict, Field

from kontra.types import Artifact

__all__ = ["Artifact", "VaultInput", "VaultOutput"]


class VaultInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: Artifact
    note: str = Field(max_length=4_000_000)


class VaultOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data: Artifact
    note: str = Field(max_length=4_000_000)
