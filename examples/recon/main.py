"""Run the Recon workflow in your own namespace. `kontra.hire.worker` is a plain Temporal
worker whose client carries your keys (so Nexus inputs are encrypted to each seller and the
results decrypt for you) and which runs the hire activities.

    python main.py example.com
"""

import asyncio
import json
import re
import sys
import uuid

from recon import Recon

from kontra.hire import worker

#: A host name or address, with an optional port: what the recon builds its URLs from. It is
#: typed by a person, so it is checked whole before it reaches a URL or a workflow id.
_DOMAIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?(?::[0-9]{1,5})?")


def check_domain(domain: str) -> str:
    if not _DOMAIN.fullmatch(domain):
        raise ValueError(f"not a domain: {domain!r} (e.g. example.com)")
    return domain


async def main(domain: str) -> None:
    check_domain(domain)
    w = await worker(task_queue="recon", workflows=[Recon])
    async with w:
        result = await w.client.execute_workflow(
            Recon.run, domain, id=f"recon-{domain}-{uuid.uuid4().hex[:8]}", task_queue="recon"
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        domain = check_domain(sys.argv[1] if len(sys.argv) > 1 else "example.com")
    except ValueError as e:
        sys.exit(str(e))
    asyncio.run(main(domain))
