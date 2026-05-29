from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class Resource(Generic[T]):
    """A single pooled resource.

    `cooldown_until` and `last_acquired_at` are `time.monotonic()` readings, not epoch
    timestamps. They are only meaningful when compared to another `time.monotonic()`
    call in the same process — do not log, persist, or pass to `datetime.fromtimestamp`.
    """

    resource_id: str
    # repr=False: value is often a secret (API key, token); keep it out of reprs,
    # tracebacks, and logs.
    value: T = field(repr=False)

    max_in_flight: int | None = None  # None = unbounded concurrency
    status: str = "healthy"
    cooldown_until: float = 0.0
    last_acquired_at: float = 0.0
    consecutive_cooldown: int = 0


@dataclass
class Usage:
    """One in-flight use of a resource.

    `acquired_at` is a `time.monotonic()` reading, not an epoch timestamp — only
    meaningful relative to other `time.monotonic()` calls in this process.

    `task` holds a cancellable handle for the in-flight operation:
    - `asyncio.Task` when the operation returned a coroutine (framework wrapped it).
    - `asyncio.Future` when the operation directly returned a Future.
    - `None` when the operation returned a plain Awaitable with no `.cancel()`
      method. In that case `cancel_younger_usages` silently no-ops on this usage
      and it runs to natural completion -- cancellation is best-effort by design.
    """

    usage_id: str
    request_id: str
    resource_id: str
    acquired_at: float
    task: asyncio.Future | None = None
    status: str = "in_flight"  # in_flight | done | cancelled
