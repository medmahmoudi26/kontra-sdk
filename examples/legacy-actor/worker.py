from activities import head_batch
from quote import quote
from workflows import ProbeFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", workflow=ProbeFlow, quote=quote)],
    activities=[head_batch],
    max_concurrent_activities=8,
)

if __name__ == "__main__":
    worker.run()
