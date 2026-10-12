"""A minimal seller for the end-to-end tests, written against plain temporalio and the
gate's public contract (what ``kontra.serve`` will automate): a Nexus service with a sync
``quote`` and a workflow-run ``execute`` that verifies the hold token offline against the
gate's JWKS (D10, decisions I10) and starts ``job-<id>`` (decisions I6)."""

from __future__ import annotations

from collections.abc import AsyncGenerator, MutableMapping
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import jwt
import nexusrpc
import nexusrpc.handler
from nexusrpc.handler import StartOperationContext
from temporalio import nexus
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.nexus import WorkflowRunOperationContext
from temporalio.worker import Worker

from kontra.contract.names import job_workflow_id
from kontra.gate_client import models as m

from .seller_flows import EnrichJob, Order, shout

PRICE_PER_CHAR = 100


def seller_handler(
    entry: m.CatalogEntry,
    jwks: dict[str, Any],
    *,
    buyer_keys: MutableMapping[str, str] | None = None,
) -> Any:
    keys = jwt.PyJWKSet.from_dict(jwks)

    def verify(token: str) -> m.HoldClaims:
        kid = jwt.get_unverified_header(token)["kid"]
        key = next(k for k in keys.keys if k.key_id == kid)
        claims = m.HoldClaims.model_validate(
            jwt.decode(token, key.key, algorithms=["ES256"], audience="kontra:hold")
        )
        if claims.contract_hash != entry.contract_hash or claims.offering != entry.offering:
            raise nexusrpc.HandlerError(
                "hold token is for another contract", type=nexusrpc.HandlerErrorType.BAD_REQUEST
            )
        return claims

    class Handler:
        @nexusrpc.handler.sync_operation
        async def quote(self, ctx: StartOperationContext, order: Order) -> dict[str, Any]:
            if order.mode == "reject":
                return {"status": "rejected", "reason": "no thanks"}
            if order.mode == "boom":
                raise RuntimeError("quote exploded")
            return {
                "status": "ok",
                "amount_minor": PRICE_PER_CHAR * len(order.text),
                "currency": "USD",
                "max_duration_seconds": 3600,
            }

        @nexus.workflow_run_operation
        async def execute(
            self, ctx: WorkflowRunOperationContext, order: Order
        ) -> nexus.WorkflowHandle[dict[str, str]]:
            claims = verify(ctx.headers["kontra-hold"])
            if buyer_keys is not None:
                buyer_keys[claims.job_id] = claims.buyer_key
            try:
                return await ctx.start_workflow(
                    EnrichJob.run,
                    order,
                    id=job_workflow_id(claims.job_id),
                    execution_timeout=timedelta(seconds=claims.max_duration_seconds),
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                )
            except WorkflowAlreadyStartedError:  # one execution per hold, as kontra.serve
                raise nexusrpc.HandlerError(
                    f"job {claims.job_id} already ran",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                    retryable_override=False,
                ) from None

    return nexusrpc.handler.service_handler(name=entry.service)(Handler)()


@asynccontextmanager
async def serving(
    client: Client,
    entry: m.CatalogEntry,
    jwks: dict[str, Any],
    *,
    buyer_keys: MutableMapping[str, str] | None = None,
) -> AsyncGenerator[Worker]:
    """Serve ``entry`` from a worker on ``client`` (the seller's namespace)."""
    worker = Worker(
        client,
        task_queue=entry.task_queue,
        workflows=[EnrichJob],
        activities=[shout],
        nexus_service_handlers=[seller_handler(entry, jwks, buyer_keys=buyer_keys)],
    )
    async with worker:
        yield worker
