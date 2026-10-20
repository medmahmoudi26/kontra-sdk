"""Vanilla Temporal: `workflow` and `RetryPolicy` are `temporalio`'s, and nothing here imports
`kontra`, so this runs on any Temporal worker as it is (decisions I29). Kontra starts it with
the id `job-<job id>` when a buyer hires the offering.

The job is the shapes `kontra graph` draws (the README has the graph):

1. `plan`: each URL once, and the sites they are on;
2. a fan-out: every site's robots.txt and security.txt, two different activities at once;
3. one `check_headers` per URL, at most `CHECKS_AT_ONCE` open at a time, each retried on its
   own, the results folded by `tally`;
4. an `if` on what that found: `score` the pages, or `all_clear`;
5. `write_report`: the report, as an `Artifact`.

Every URL's check is an activity of this workflow, so its input and result are in the job's
history: three events for the activity and at most three for the workflow task its result
wakes, and a URL (2,048 characters at most) each way. For the contract's 200 URLs that is at
most about 1,200 events and 1 MB, far inside what Temporal allows one history (51,200 events,
50 MB). Tens of thousands of items would not fit: such a job splits them across child
workflows, or continues as new.
"""

import asyncio
import functools
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

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
#: `check_headers` activities open at once (worker.py gives the worker as many slots).
CHECKS_AT_ONCE = 50


def tally(acc: Tally, check: HeaderCheck) -> Tally:
    """One URL's check folded into the tally: a page that sends every header leaves only a
    count. It runs in the workflow, so it does no I/O."""
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

        # One activity per URL, each retried on its own, never more than CHECKS_AT_ONCE open.
        slots = asyncio.Semaphore(CHECKS_AT_ONCE)

        async def check(url: str) -> HeaderCheck:
            async with slots:
                return await workflow.execute_activity(
                    check_headers,
                    url,
                    start_to_close_timeout=timedelta(seconds=URL_DEADLINE_SECONDS + 30),
                    retry_policy=RETRY,
                )

        checks = await asyncio.gather(*(check(url) for url in p.urls))
        found = functools.reduce(tally, checks, Tally())

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
