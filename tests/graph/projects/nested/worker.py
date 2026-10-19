from workflows import Inner, NestedFlow

from kontra import serve

serve(NestedFlow, Inner)
