from __future__ import annotations


class CooldownResource(Exception):
    """Raise from a user operation to mark the resource as cooling_down.

    cooldown_seconds: explicit cooldown duration (e.g. derived from a Retry-After
        header). If None, the framework's default cooldown table applies based on
        consecutive_cooldown count.
    reason: free-form string surfaced in logs and metrics.
    """

    def __init__(
        self, cooldown_seconds: float | None = None, reason: str | None = None
    ) -> None:
        super().__init__(reason or "resource cooldown")
        self.cooldown_seconds = cooldown_seconds
        self.reason = reason


class DisableResource(Exception):
    """Raise from a user operation to mark the resource as disabled."""

    def __init__(self, reason: str | None = None) -> None:
        super().__init__(reason or "resource disabled")
        self.reason = reason


class PoolExhausted(Exception):
    """Raised by the framework when the pool cannot satisfy a request.

    Covers four scenarios:
    - No eligible resource exists (all disabled, cooling down, or at capacity).
    - Max retry attempts exhausted.
    - Deadline exceeded.
    - With ``wait_for_cooldown=True``: the earliest cooldown expiry lands at or
      after the deadline, so waiting provably cannot help.
    """
