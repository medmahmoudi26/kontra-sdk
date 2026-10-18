"""The types of the nested project."""

from pydantic import BaseModel, ConfigDict


class NestedInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # No min_length: run 0's batch over them gets no items, and schedules no activity.
    names: list[str]


class NestedOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seen: int
