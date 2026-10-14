"""The buyer's own worker, for workflows that hire (PRD §7.2): :func:`worker`.

A buyer's workflow hires with :func:`kontra.hire.service`; what that needs from the worker
is three activities and a codec. ``kontra.hire.prepare`` resolves, validates, quotes and
locks the hold (and primes the seller-key cache the codec reads when it encrypts the Nexus
input); ``kontra.hire.cancel`` cancels a job at the gate so its hold refunds at once;
``kontra.hire.settle`` waits for the gate's verdict on a job whose ``execute`` ended without
a result, with the delivery if the gate counted one. The worker's client carries the buyer
codec: Nexus inputs are encrypted to each seller and to us, deliveries decrypt with our key,
everything else in our namespace is ours only.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

import temporalio.worker
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import ApplicationError

from kontra.codec.age import generate_identity as age_generate_identity
from kontra.gate_client import GateError
from kontra.gate_client import models as m
from kontra.hire._client import Kontra, _refusal  # pyright: ignore[reportPrivateUsage]
from kontra.hire._errors import InvalidDelivery
from kontra.hire._service import (
    CANCEL_ACTIVITY,
    PREPARE_ACTIVITY,
    SETTLE_ACTIVITY,
    Prepared,
    PrepareRequest,
    Settled,
)

__all__ = ["HireActivities", "HireWorker", "worker"]

#: Gate answers worth retrying: it (or the seller behind it) is down or overloaded.
_RETRY_STATUS = frozenset({408, 429, 500, 502, 503, 504})
#: ``prepare``'s heartbeat: the quote an attempt is about to lock.
_QUOTE_ID = "quote_id"


def _not_retryable(err: GateError) -> ApplicationError:
    return ApplicationError(
        err.message,
        {"status": err.status, "code": err.code},
        type="kontra.GateError",
        non_retryable=True,
    )


class HireActivities:
    """The activities a buyer's workflow needs, bound to one :class:`Kontra`."""

    def __init__(self, kontra: Kontra) -> None:
        self.kontra = kontra

    @activity.defn(name=PREPARE_ACTIVITY)
    async def prepare(self, req: PrepareRequest) -> Prepared:
        """Resolve, validate, quote and lock the hold; everything ``execute`` needs.

        Refusals the buyer must act on (rejected, busy, above ``max_price``, no funds, no
        such offering) are non-retryable hire errors; a gate or seller that is down is
        retried by the activity's retry policy."""
        k = self.kontra
        try:
            await self._release_earlier_hold()
            q = await k.quote(req.offering, req.input)
            # Resolved (and cached) by the quote: nothing after the lock can fail and make a
            # retry of this activity lock a second hold.
            info = k.contract(await k.offering(q.offering))
            if q.quote_id is not None:
                # Recorded before the lock: if this attempt dies after locking (a worker
                # crash, a timeout), the next attempt finds the hold it cannot use.
                activity.heartbeat({_QUOTE_ID: q.quote_id})
            hired = await k._lock(q, req.max_price)  # pyright: ignore[reportPrivateUsage]
        except GateError as err:
            if err.status in _RETRY_STATUS:
                raise
            raise _refusal(err, req.offering) or _not_retryable(err) from err
        return Prepared(
            job_id=hired.job_id,
            offering=hired.offering,
            hold_token=hired.hold_token,
            endpoint=hired.endpoint,
            service=hired.service,
            max_duration_seconds=hired.max_duration_seconds,
            seller_recipients=list(hired.seller_recipients),
            price=str(q.price),
            currency=hired.currency,
            output_schema=info.output_schema,
        )

    async def _release_earlier_hold(self) -> None:
        """A retry of ``prepare``: refund the hold an earlier attempt may have locked and
        never handed to the workflow (its hold token is lost with that attempt).

        The earlier attempt heartbeated its quote id before locking. Hiring that quote again
        with a cap below any price never locks anything: the gate answers ``quote_used``
        with the job it became, or refuses on price because nothing was locked. That job is
        cancelled here, so its hold refunds now -- left alone, it would refund only at its
        deadline, as a job the seller "never started"."""
        info = activity.info()
        earlier = info.heartbeat_details[0] if info.heartbeat_details else None
        if info.attempt <= 1 or not isinstance(earlier, dict):
            return
        quote_id = cast("dict[str, Any]", earlier).get(_QUOTE_ID)
        if not isinstance(quote_id, str):
            return
        gate = self.kontra.gate
        try:
            # A cap below any price never locks, so this never needs a usable job key;
            # the gate still requires one, so send a throwaway public half.
            _, throwaway = age_generate_identity()
            hired = await gate.hire(quote_id, buyer_key=throwaway, max_price_minor=-1)
        except GateError as err:
            job_id = (err.detail or {}).get("job_id") if err.code == "quote_used" else None
            if not isinstance(job_id, str):
                return  # never locked (refused on price), expired, or unknown
        else:  # a gate that ignored the cap: this call locked it, and nothing will use it
            job_id = hired.job_id
        activity.logger.warning(
            "job %s: locked by an earlier attempt of this hire and never used; cancelling it",
            job_id,
        )
        try:
            await gate.cancel(job_id)
        except GateError as err:
            if err.status != 409:  # 409: already closed
                raise

    @activity.defn(name=SETTLE_ACTIVITY)
    async def settle(self, job_id: str) -> Settled:
        """The gate's verdict on a job whose ``execute`` ended without a result: wait for
        the gate to close it; the decrypted delivery (checked against the contract) when
        the gate counted one."""
        k = self.kontra
        try:
            view = await k._closed_view(job_id, beat=activity.heartbeat)  # pyright: ignore[reportPrivateUsage]
            if view.state is not m.JobState.COMPLETED:
                return Settled(state=view.state.value)
            value = await k._open_delivery(await k.gate.result(job_id))  # pyright: ignore[reportPrivateUsage]
        except GateError as err:
            if err.status in _RETRY_STATUS:
                raise
            raise _not_retryable(err) from err
        await k._check_delivery(job_id, view.offering, value)  # pyright: ignore[reportPrivateUsage]
        if not isinstance(value, dict):  # the contract's output is an object
            raise InvalidDelivery(f"the delivery of job {job_id} is not an object", job_id=job_id)
        return Settled(state=view.state.value, delivery=cast("dict[str, Any]", value))

    @activity.defn(name=CANCEL_ACTIVITY)
    async def cancel(self, job_id: str) -> None:
        """Cancel the job at the gate: the hold refunds now. A job that is already closed
        (delivered, failed, canceled) has nothing left to refund."""
        try:
            await self.kontra.gate.cancel(job_id)
        except GateError as err:
            if err.status == 409:
                activity.logger.info("job %s is already closed: %s", job_id, err.message)
                return
            if err.status in _RETRY_STATUS:
                raise
            raise _not_retryable(err) from err

    @property
    def all(self) -> list[Callable[..., Any]]:
        return [self.prepare, self.cancel, self.settle]


class HireWorker(temporalio.worker.Worker):
    """A ``temporalio.worker.Worker``; it also closes the :class:`Kontra` that
    :func:`worker` made for it when it shuts down."""

    _owned: Kontra | None = None

    async def shutdown(self) -> None:
        try:
            await super().shutdown()
        finally:
            owned, self._owned = self._owned, None
            if owned is not None:
                await owned.aclose()


async def worker(
    client: Client | None = None,
    *,
    task_queue: str,
    workflows: Sequence[type] = (),
    activities: Sequence[Callable[..., Any]] = (),
    kontra: Kontra | None = None,
    **kwargs: Any,
) -> HireWorker:
    """A Temporal worker for the buyer's namespace that can run workflows which hire.

    The hire activities are registered next to ``activities``; ``client`` (default: the
    profile's, connected as the ``worker`` role) gets the buyer's data converter. ``kontra``
    is the buyer client the activities use (default: ``Kontra()`` -- ``KONTRA_API_KEY``, else
    the profile -- closed with the worker). Other keywords go to ``temporalio.worker.Worker`` unchanged.
    """
    owned = kontra is None
    k = kontra if kontra is not None else Kontra()
    try:
        temporal = k.adopt(client) if client is not None else await k.temporal()
        hire = HireActivities(k)
        w = HireWorker(
            temporal,
            task_queue=task_queue,
            workflows=workflows,
            activities=[*hire.all, *activities],
            **kwargs,
        )
    except BaseException:
        if owned:
            await k.aclose()
        raise
    if owned:
        w._owned = k
    return w
