"""A workflow and an activity that log, for ``kontra serve --otlp``'s logs
(``tests/serve/test_logs.py``): plain Temporal, as a seller writes them. An importable
module, as Temporal's workflow sandbox requires."""

from __future__ import annotations

import logging
from datetime import timedelta

from temporalio import activity, workflow

#: A seller's own module logger, used inside an activity.
log = logging.getLogger(__name__)


@activity.defn
async def chatty(text: str) -> str:
    activity.logger.info("through activity.logger: %s", text)
    log.info("through a module logger: %s", text)
    try:
        raise ValueError(f"caught: {text}")
    except ValueError:
        log.exception("an exception, logged")
    return text


@workflow.defn
class Chatty:
    @workflow.run
    async def run(self, text: str) -> str:
        workflow.logger.info("through workflow.logger: %s", text)
        return await workflow.execute_activity(
            chatty, text, start_to_close_timeout=timedelta(seconds=30)
        )
