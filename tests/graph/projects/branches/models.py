"""The types of the branches project."""

from pydantic import BaseModel, ConfigDict, Field


class BranchesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[str] = Field(min_length=1, max_length=1000)


class BranchesOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    checked: int
    shipped: bool


class Plan(BaseModel):
    # No min_length: run 0's batch gets no items and schedules no activity, and is still
    # the one node every other run's batch is.
    items: list[str]


class Verdict(BaseModel):
    flag: bool
