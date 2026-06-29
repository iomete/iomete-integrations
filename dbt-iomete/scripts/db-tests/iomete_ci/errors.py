"""Error types shared across the provisioning package."""
from __future__ import annotations


class ProvisionError(RuntimeError):
    """A resource could not be ensured and tests would fail without it."""
