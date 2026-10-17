"""The input and output types. The workflow's signature is the contract: `kontra publish`
derives the published JSON Schema from these models, and a buyer's input is checked against
it before it is encrypted. A change here needs a new `version`."""

from pydantic import BaseModel, ConfigDict, Field


class CookInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    craving: str = Field(min_length=1, max_length=500, description="What you feel like eating.")


class Dish(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recipe: str = Field(description="One dish from what is in the pantry, and how to make it.")
