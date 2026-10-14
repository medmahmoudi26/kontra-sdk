"""The input and output types. The workflow's signature is the contract: `kontra publish`
derives the published JSON Schema from these models (`extra="forbid"` becomes
`additionalProperties: false`, `min_length` becomes `minLength`), and a buyer's input is
checked against it before it is encrypted. A change here needs a new `version`."""

from pydantic import BaseModel, ConfigDict, Field


class HelloInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100, description="Who to greet.")


class HelloOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    greeting: str = Field(description="Hi <name>.")
