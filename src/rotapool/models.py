from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

T = TypeVar("T")

ResourceStatus = Literal["healthy", "cooling_down", "disabled"]
UsageStatus = Literal["in_flight", "done", "cancelled"]
RunOutcome = Literal["ok", "exhausted", "error", "cancelled"]

RESOURCE_STATUSES: tuple[ResourceStatus, ...] = (
    "healthy",
    "cooling_down",
    "disabled",
)
RUN_OUTCOMES: tuple[RunOutcome, ...] = ("ok", "exhausted", "error", "cancelled")


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
    status: ResourceStatus = "healthy"
    cooldown_until: float = 0.0
    last_acquired_at: float = 0.0
    consecutive_cooldown: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.resource_id, str) or not self.resource_id:
            raise ValueError("resource_id must be a non-empty string")
        # `not >= 1` instead of `< 1`: also rejects NaN, which would make the
        # capacity check (`current >= max_in_flight`) always false and the
        # resource silently unbounded.
        if self.max_in_flight is not None and not self.max_in_flight >= 1:  # noqa: S1940
            raise ValueError(
                f"max_in_flight must be >= 1 or None, got {self.max_in_flight}"
            )


@dataclass
class Usage:
    """One in-flight use of a resource.

    `acquired_at` is a `time.monotonic()` reading, not an epoch timestamp — only
    meaningful relative to other `time.monotonic()` calls in this process.

    `acquisition_order` is a per-pool monotonic sequence number used for exact
    older/younger comparisons when multiple usages share the same timestamp.

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
    acquisition_order: int
    task: asyncio.Future | None = None
    status: UsageStatus = "in_flight"


@dataclass(frozen=True)
class ResourceStats:
    """Gauges and monotonic counters for one pooled resource.

    Gauges are Prometheus-ready: numeric, with no process-monotonic timestamps.
    ``status`` is a closed enum -- use :meth:`status_one_hot` for a 0/1 series
    per state rather than encoding the enum as a number. ``max_in_flight`` of
    ``None`` (unlimited) maps to ``+Inf`` via :attr:`max_in_flight_gauge`.

    Counters only increase for the life of this membership. ``remove()`` drops
    the series; ``add()`` of the same id starts at 0.
    """

    resource_id: str
    status: ResourceStatus
    in_flight: int
    max_in_flight: int | None
    consecutive_cooldown: int
    cooldown_seconds_remaining: float
    acquires: int
    successes: int
    cooldowns: int
    disables: int
    sibling_cancels: int

    @property
    def max_in_flight_gauge(self) -> float:
        """``max_in_flight`` as a Prometheus gauge: ``+Inf`` when unlimited."""
        return math.inf if self.max_in_flight is None else float(self.max_in_flight)

    @property
    def eligible(self) -> bool:
        """Selectable right now: effectively healthy and under ``max_in_flight``."""
        return self.status == "healthy" and (
            self.max_in_flight is None or self.in_flight < self.max_in_flight
        )

    @property
    def saturated(self) -> bool:
        """Effectively healthy but at ``max_in_flight`` (unlimited is never saturated)."""
        return (
            self.status == "healthy"
            and self.max_in_flight is not None
            and self.in_flight >= self.max_in_flight
        )

    def status_one_hot(self) -> dict[ResourceStatus, int]:
        """0/1 gauge per status label -- the Prometheus enum-gauge convention."""
        return {status: int(status == self.status) for status in RESOURCE_STATUSES}

    @classmethod
    def __agent_notes__(cls) -> str:
        return """\
Do not construct ``ResourceStats``. Read ``pool.stats().resources[resource_id]``.

For Prometheus gauges use ``status_one_hot()`` (not a numeric enum) and
``max_in_flight_gauge`` (``+Inf`` when unbounded). ``eligible`` / ``saturated``
are derived. Counters are membership-scoped: ``remove()`` drops them;
re-``add()`` of the same id starts at 0. There is no ``last_acquired_at``.
"""


@dataclass(frozen=True)
class PoolStats:
    """Pool-wide gauges (current membership) and lifetime counters.

    Gauges are derived from the current resource set and go up and down with
    membership and health. ``by_status`` always contains every
    :data:`RESOURCE_STATUSES` key, including zeros, so a scraper can emit a
    stable label set.

    Pool-level counters survive ``remove()`` and must never be reconstructed by
    summing :attr:`resources` -- that would drop on membership change and break
    Prometheus ``rate()``. Resource-level counters live in each
    :class:`ResourceStats` and disappear with the resource.
    """

    resources: dict[str, ResourceStats]
    in_flight: int
    eligible: int
    saturated: int
    by_status: dict[ResourceStatus, int]
    attempts: int
    successes: int
    cooldowns: int
    disables: int
    sibling_cancels: int
    retries: int
    runs_ok: int
    runs_exhausted: int
    runs_error: int
    runs_cancelled: int

    @classmethod
    def __agent_notes__(cls) -> str:
        return """\
Do not construct ``PoolStats``. Call ``pool.stats()``.

Gauges (``in_flight``, ``eligible``, ``saturated``, ``by_status``) describe
current membership. Counters (``attempts``, ``successes``, ``cooldowns``,
``disables``, ``sibling_cancels``, ``retries``, ``runs_ok`` / ``runs_exhausted`` /
``runs_error`` / ``runs_cancelled``) are process-lifetime and survive
``remove()``. Never sum ``resources`` to rebuild them -- that drops on
membership change and breaks Prometheus ``rate()``.

``by_status`` always contains ``healthy``, ``cooling_down``, ``disabled``
(zeros included). There is no ``last_acquired_at``.
"""
