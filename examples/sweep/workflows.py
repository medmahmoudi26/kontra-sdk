"""Temporal through the kontra façade (D1): `workflow` and `RetryPolicy` are Temporal's own
objects, so with `from temporalio import ...` this runs on any Temporal. Kontra starts it with
the id `job-<job id>` when a buyer hires the offering.

The job is the shapes `kontra graph` draws (the README has the graph):

1. `plan`: each URL once, and the sites they are on;
2. a fan-out: every site's robots.txt and security.txt, two different activities at once;
3. `execute_activities_in_parallel`: one `check_headers` per URL, at most 50 at a time,
   folded by `tally` as each completes, in one child workflow;
4. an `if` on what that found: `score` the pages, or `all_clear`;
5. `write_report`: the report, as an `Artifact`.
"""

import asyncio
from datetime import timedelta

from kontra import RetryPolicy, workflow

with workflow.unsafe.imports_passed_through():
    from activities import (
        SITES_AT_ONCE,
        URL_DEADLINE_SECONDS,
        all_clear,
        check_headers,
        fetch_robots,
        fetch_security_txt,
        plan,
        score,
        write_report,
    )
    from models import (
        Findings,
        Flagged,
        HeaderCheck,
        SweepInput,
        SweepOutput,
        Tally,
        Unreachable,
    )

QUICK = timedelta(seconds=30)
RETRY = RetryPolicy(maximum_attempts=3)


def tally(acc: Tally, check: HeaderCheck) -> Tally:
    """`reduce=`: one URL's check folded into the tally. It runs in the workflow, so it is
    pure, and module-level: it crosses into the batch by name."""
    acc.checked += 1
    if check.error is not None:
        acc.unreachable.append(Unreachable(url=check.url, error=check.error))
    elif check.missing:
        acc.flagged.append(Flagged(url=check.url, missing=check.missing))
    return acc


@workflow.defn
class SweepFlow:
    @workflow.run
    async def run(self, inp: SweepInput) -> SweepOutput:
        p = await workflow.execute_activity(plan, inp, start_to_close_timeout=QUICK)

        # Every site's two files, at once: one workflow task schedules both activities.
        # SITES_AT_ONCE sites at a time, each within its deadline.
        rounds = -(-len(p.sites) // SITES_AT_ONCE)
        per_site = {
            "start_to_close_timeout": timedelta(seconds=rounds * URL_DEADLINE_SECONDS + 60),
            "heartbeat_timeout": timedelta(minutes=1),
            "retry_policy": RETRY,
        }
        robots, security_txt = await asyncio.gather(
            workflow.execute_activity(fetch_robots, p.sites, **per_site),
            workflow.execute_activity(fetch_security_txt, p.sites, **per_site),
        )

        # One activity per URL, each retried on its own: a child workflow, kontra.batch.v1.
        found = await workflow.execute_activities_in_parallel(
            check_headers,
            items=p.urls,
            reduce=tally,
            init=Tally(),
            max_concurrent=50,
            start_to_close_timeout=timedelta(seconds=URL_DEADLINE_SECONDS + 30),
            retry_policy=RETRY,
        )

        findings = Findings(tally=found, robots=robots, security_txt=security_txt)
        if found.flagged or found.unreachable:
            report = await workflow.execute_activity(score, findings, start_to_close_timeout=QUICK)
        else:
            report = await workflow.execute_activity(
                all_clear, findings, start_to_close_timeout=QUICK
            )
        artifact = await workflow.execute_activity(
            write_report, report, start_to_close_timeout=timedelta(minutes=5)
        )
        return SweepOutput(report=artifact, summary=report.summary)
