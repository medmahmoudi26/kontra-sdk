"""The types of the two project."""

from pydantic import BaseModel, ConfigDict


class TwoInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str


class TwoOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str
