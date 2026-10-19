"""The types of the stuck project."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StuckInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    then: Literal["done", "loop", "wait", "crash"]


class StuckOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    done: bool


class Odd(BaseModel):
    """No value of it can be sampled: a lookahead, and no examples."""

    model_config = ConfigDict(regex_engine="python-re")

    code: str = Field(pattern=r"^(?=x)y$")
