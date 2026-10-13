"""How many heartbeats to send (tagged, so a test can find them), and how long to run an
activity that sends none."""

from pydantic import BaseModel, ConfigDict, Field


class PulseInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    beats: int = Field(ge=0, le=1000)
    tag: str = Field(max_length=200)
    quiet_seconds: int = Field(ge=0, le=600)


class PulseOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    beats: int = Field(ge=0)
    quiet_seconds: int = Field(ge=0)
