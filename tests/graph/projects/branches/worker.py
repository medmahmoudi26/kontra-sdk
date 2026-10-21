from workflows import BranchesFlow

from kontra import serve
from kontra.contrib.parallel import ParallelPlugin

serve(BranchesFlow, plugins=[ParallelPlugin()])
