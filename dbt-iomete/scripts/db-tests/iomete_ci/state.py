"""Persisted record of what a provision run created."""
from __future__ import annotations

import json
import os


class ProvisionState:
    """The set of resources a provision run created, persisted incrementally.

    Written after every successful create so an interrupted run still leaves a
    file the teardown step can act on.
    """

    def __init__(self, path: str, domain: str):
        self.path = path
        self.data = {"domain": domain, "created": {}, "test_env": {}}
        self.flush()

    def set_created(self, **kwargs) -> None:
        self.data["created"].update({k: v for k, v in kwargs.items() if v is not None})
        self.flush()

    def set_test_env(self, **kwargs) -> None:
        self.data["test_env"].update({k: v for k, v in kwargs.items() if v is not None})
        self.flush()

    def flush(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as handle:
            json.dump(self.data, handle, indent=2)
        os.replace(tmp, self.path)
