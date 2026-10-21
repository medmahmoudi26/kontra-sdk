from activities import pulse, quiet
from quote import quote
from workflows import PulseFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[offering("offering.yaml", entry=PulseFlow, quote=quote)],
    activities=[pulse, quiet],
)

if __name__ == "__main__":
    worker.run()
