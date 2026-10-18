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
    # At least one item: a batch that schedules nothing in its first run is unnamed.
    items: list[str] = Field(min_length=1)


class Verdict(BaseModel):
    flag: bool
