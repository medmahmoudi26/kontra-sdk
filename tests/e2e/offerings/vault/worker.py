from activities import reseal
from quote import quote
from workflows import VaultFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", entry=VaultFlow, quote=quote)],
    activities=[reseal],
)

if __name__ == "__main__":
    worker.run()
