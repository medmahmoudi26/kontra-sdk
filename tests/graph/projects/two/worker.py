from activities import act_a, act_b, unused
from quote import quote
from workflows import AFlow, BFlow

from kontra.serve import Worker, offering

worker = Worker(
    offerings=[
        offering("a.yaml", workflow=AFlow, quote=quote),
        offering("b.yaml", workflow=BFlow, quote=quote),
    ],
    activities=[act_a, act_b, unused],
)
