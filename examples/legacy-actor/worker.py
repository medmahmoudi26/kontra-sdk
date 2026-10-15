from workflows import ProbeFlow

from kontra import serve

serve(ProbeFlow, max_concurrent_activities=8)
