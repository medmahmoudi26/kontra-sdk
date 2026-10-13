"""Settings every test shares."""

from __future__ import annotations

import os

# A developer's own key names a real party on the real gate (https://api.kontra.run, the
# default): no test may pick it up, in this process or in a subprocess it starts. Tests that
# use a key set their own.
os.environ.pop("KONTRA_API_KEY", None)
