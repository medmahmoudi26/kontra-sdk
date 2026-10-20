from workflows import Inner, NestedFlow

from kontra import serve
from kontra.contrib.parallel import ParallelPlugin

serve(NestedFlow, workflows=[Inner], plugins=[ParallelPlugin()])
