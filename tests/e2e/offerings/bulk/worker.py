from activities import produce
from quote import quote
from workflows import BulkFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", entry=BulkFlow, quote=quote)],
    activities=[produce],
)

if __name__ == "__main__":
    worker.run()
