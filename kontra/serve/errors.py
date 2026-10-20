"""Why a seller's worker refuses to start, or stopped."""

from __future__ import annotations

__all__ = ["ServeError"]


class ServeError(RuntimeError):
    """The worker refuses to start -- a contract that drifted from the catalog, an
    unpublished version, a key that does not match the profile, a temporalio outside the
    tested range, a plugin that undid what Kontra configured -- or stopped because its
    credentials were revoked."""
