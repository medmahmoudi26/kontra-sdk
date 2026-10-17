"""The pantry, as an activity the agent calls as a tool (`workflow.activity_as_tool` in
workflows.py): its name, docstring and argument become the tool's, and every call is an
activity -- retried, recorded, and run on this worker. Temporal through the kontra façade
(D1). A real offering reads its stock here."""

from kontra import activity

PANTRY: dict[str, list[str]] = {
    "fridge": ["eggs", "butter", "parmesan", "lemons"],
    "dry": ["spaghetti", "rice", "flour"],
    "spices": ["black pepper", "salt", "chili flakes"],
}


@activity.defn
async def lookup_pantry(shelf: str) -> list[str]:
    """What is on one shelf of the pantry: "fridge", "dry" or "spices"."""
    return PANTRY.get(shelf.strip().lower(), [])
