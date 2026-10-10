"""Run the Recon workflow in your own namespace. `kontra.hire.worker` is a plain Temporal
worker whose client carries your keys (so Nexus inputs are encrypted to each seller and the
results decrypt for you) and which runs the hire activities.

    python main.py example.com
"""

import asyncio
import json
import sys
import uuid

from recon import Recon

from kontra.hire import worker


async def main(domain: str) -> None:
    w = await worker(task_queue="recon", workflows=[Recon])
    async with w:
        result = await w.client.execute_workflow(
            Recon.run, domain, id=f"recon-{domain}-{uuid.uuid4().hex[:8]}", task_queue="recon"
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "example.com"))
