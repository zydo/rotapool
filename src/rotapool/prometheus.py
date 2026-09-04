"""Prometheus custom collector for :class:`rotapool.Pool`.

Requires the optional extra: ``pip install "rotapool[prometheus]"``.

Register **one** :class:`PoolCollector` per registry -- prometheus_client
reserves metric names globally, so a second collector with the same names
raises ``ValueError``. Distinguish pools with the ``pool`` label:

```python
from prometheus_client import REGISTRY
from rotapool.prometheus import PoolCollector

REGISTRY.register(PoolCollector(pool, pool_name="api_keys"))
# or several pools on the same collector:
REGISTRY.register(PoolCollector(pools={"api_keys": keys, "proxies": proxies}))
```

Metric names are frozen. Pool-level counters come from :meth:`Pool.stats`
lifetime totals, never from summing per-resource counters (that would drop
on ``remove()`` and break ``rate()``). Resource series are current membership
only. ``max_in_flight`` of ``None`` is exported as ``+Inf``. Status is a
one-hot gauge, not a numeric enum. ``last_acquired_at`` is not exported.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from .models import RESOURCE_STATUSES, RUN_OUTCOMES
from .pool import Pool

if TYPE_CHECKING:
    from prometheus_client.core import Metric
    from prometheus_client.registry import CollectorRegistry

try:
    from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "rotapool.prometheus requires the prometheus extra: "
        'pip install "rotapool[prometheus]"'
    ) from exc

# Frozen metric names. Changing these is a breaking change for dashboards.
RUNS_TOTAL = "rotapool_runs_total"
ATTEMPTS_TOTAL = "rotapool_attempts_total"
SUCCESSES_TOTAL = "rotapool_successes_total"
COOLDOWNS_TOTAL = "rotapool_cooldowns_total"
DISABLES_TOTAL = "rotapool_disables_total"
SIBLING_CANCELS_TOTAL = "rotapool_sibling_cancels_total"
RETRIES_TOTAL = "rotapool_retries_total"
IN_FLIGHT = "rotapool_in_flight"
ELIGIBLE = "rotapool_eligible"
SATURATED = "rotapool_saturated"
RESOURCES = "rotapool_resources"
RESOURCE_STATUS = "rotapool_resource_status"
RESOURCE_IN_FLIGHT = "rotapool_resource_in_flight"
RESOURCE_MAX_IN_FLIGHT = "rotapool_resource_max_in_flight"
RESOURCE_CONSECUTIVE_COOLDOWN = "rotapool_resource_consecutive_cooldown"
RESOURCE_COOLDOWN_REMAINING_SECONDS = "rotapool_resource_cooldown_remaining_seconds"
RESOURCE_ACQUIRES_TOTAL = "rotapool_resource_acquires_total"
RESOURCE_SUCCESSES_TOTAL = "rotapool_resource_successes_total"
RESOURCE_COOLDOWNS_TOTAL = "rotapool_resource_cooldowns_total"
RESOURCE_DISABLES_TOTAL = "rotapool_resource_disables_total"
RESOURCE_SIBLING_CANCELS_TOTAL = "rotapool_resource_sibling_cancels_total"

_RUN_COUNTER_ATTR: dict[str, str] = {
    "ok": "runs_ok",
    "exhausted": "runs_exhausted",
    "error": "runs_error",
    "cancelled": "runs_cancelled",
}


def _empty_families() -> list[Any]:
    """Metric families with names and labels but no samples.

    Used by both ``describe()`` (so registration does not scrape the pool)
    and ``collect()`` (which then fills samples).
    """
    return [
        CounterMetricFamily(
            RUNS_TOTAL,
            "Pool run() completions by outcome.",
            labels=["pool", "outcome"],
        ),
        CounterMetricFamily(
            ATTEMPTS_TOTAL,
            "Successful resource acquisitions (retry-loop attempts).",
            labels=["pool"],
        ),
        CounterMetricFamily(
            SUCCESSES_TOTAL,
            "Attempts that completed without a resource-health signal.",
            labels=["pool"],
        ),
        CounterMetricFamily(
            COOLDOWNS_TOTAL,
            "CooldownResource events, including escalations.",
            labels=["pool"],
        ),
        CounterMetricFamily(
            DISABLES_TOTAL,
            "Transitions to disabled (signal or admin).",
            labels=["pool"],
        ),
        CounterMetricFamily(
            SIBLING_CANCELS_TOTAL,
            "Younger in-flight usages cancelled by a sibling cooldown or disable.",
            labels=["pool"],
        ),
        CounterMetricFamily(
            RETRIES_TOTAL,
            "Transient retry signals (RetryOperation).",
            labels=["pool"],
        ),
        GaugeMetricFamily(
            IN_FLIGHT,
            "Current in-flight usages across current membership.",
            labels=["pool"],
        ),
        GaugeMetricFamily(
            ELIGIBLE,
            "Resources selectable right now (healthy and under max_in_flight).",
            labels=["pool"],
        ),
        GaugeMetricFamily(
            SATURATED,
            "Healthy resources at max_in_flight.",
            labels=["pool"],
        ),
        GaugeMetricFamily(
            RESOURCES,
            "Resource count by effective status.",
            labels=["pool", "status"],
        ),
        GaugeMetricFamily(
            RESOURCE_STATUS,
            "One-hot effective status (1 for the current state, 0 otherwise).",
            labels=["pool", "resource_id", "status"],
        ),
        GaugeMetricFamily(
            RESOURCE_IN_FLIGHT,
            "Current in-flight usages on this resource.",
            labels=["pool", "resource_id"],
        ),
        GaugeMetricFamily(
            RESOURCE_MAX_IN_FLIGHT,
            "Concurrency cap; +Inf when unlimited.",
            labels=["pool", "resource_id"],
        ),
        GaugeMetricFamily(
            RESOURCE_CONSECUTIVE_COOLDOWN,
            "Consecutive cooldown count (resets on success).",
            labels=["pool", "resource_id"],
        ),
        GaugeMetricFamily(
            RESOURCE_COOLDOWN_REMAINING_SECONDS,
            "Seconds until cooldown expiry; 0 when not cooling.",
            labels=["pool", "resource_id"],
        ),
        CounterMetricFamily(
            RESOURCE_ACQUIRES_TOTAL,
            "Acquisitions of this resource (membership-scoped).",
            labels=["pool", "resource_id"],
        ),
        CounterMetricFamily(
            RESOURCE_SUCCESSES_TOTAL,
            "Attempts on this resource that completed without a health signal.",
            labels=["pool", "resource_id"],
        ),
        CounterMetricFamily(
            RESOURCE_COOLDOWNS_TOTAL,
            "CooldownResource events on this resource, including escalations.",
            labels=["pool", "resource_id"],
        ),
        CounterMetricFamily(
            RESOURCE_DISABLES_TOTAL,
            "Transitions of this resource to disabled (signal or admin).",
            labels=["pool", "resource_id"],
        ),
        CounterMetricFamily(
            RESOURCE_SIBLING_CANCELS_TOTAL,
            "Younger usages on this resource cancelled by a sibling failure.",
            labels=["pool", "resource_id"],
        ),
    ]


class PoolCollector:
    """Prometheus custom collector over one or more :class:`Pool` instances.

    Parameters
    ----------
    pool:
        A single pool. Mutually exclusive with ``pools``.
    pool_name:
        Value of the ``pool`` label for ``pool``. Must be non-empty.
        Ignored when ``pools`` is given.
    pools:
        Dict of ``pool`` label -> pool. Use this (or :meth:`add`) to
        export several pools from one collector -- a second ``PoolCollector``
        on the same registry will collide on metric names.
    """

    def __init__(
        self,
        pool: Pool[Any] | None = None,
        *,
        pool_name: str = "default",
        pools: dict[str, Pool[Any]] | None = None,
    ) -> None:
        if pool is not None and pools is not None:
            raise TypeError("pass pool or pools, not both")
        self._pools: dict[str, Pool[Any]] = {}
        if pool is not None:
            self.add(pool, pool_name=pool_name)
        elif pools is not None:
            for name, p in pools.items():
                self.add(p, pool_name=name)

    def add(self, pool: Pool[Any], *, pool_name: str) -> PoolCollector:
        """Attach another pool. ``pool_name`` must be unique on this collector."""
        if not isinstance(pool_name, str) or not pool_name:
            raise ValueError("pool_name must be a non-empty string")
        current = dict(self._pools)
        if pool_name in current:
            raise ValueError(f"duplicate pool_name: {pool_name!r}")
        current[pool_name] = pool
        # Atomic swap so collect() iterating a previous dict is safe.
        self._pools = current
        return self

    def register(self, registry: CollectorRegistry | None = None) -> PoolCollector:
        """Register this collector on ``registry`` (default: global REGISTRY)."""
        if registry is None:
            from prometheus_client import REGISTRY as _REGISTRY

            registry = _REGISTRY
        registry.register(self)
        return self

    @classmethod
    def __agent_notes__(cls) -> str:
        return """\
### Use

Optional extra: ``pip install "rotapool[prometheus]"``. Import from
``rotapool.prometheus``, not ``rotapool``. Scrapes ``Pool.stats()`` -- do
not also scrape ``snapshot()`` into Prometheus.

```python
from prometheus_client import REGISTRY
from rotapool.prometheus import PoolCollector
REGISTRY.register(PoolCollector(pool, pool_name="api_keys"))
# several pools, one collector:
REGISTRY.register(PoolCollector(pools={"a": p1, "b": p2}))
```

``add()`` and ``register()`` return self for chaining. Frozen metric names
are module-level constants on ``rotapool.prometheus``.

### Don't

- Register a second ``PoolCollector`` on the same registry -- names collide.
  Put every pool on one collector (``pools=`` or ``add()``).
- Sum resource-level series to rebuild pool counters; pool-lifetime totals
  are already emitted from ``stats()``.
- Export ``CooldownResource.reason`` as a label (not in ``stats()``,
  unbounded cardinality).
"""

    def describe(self) -> Iterable[Metric]:
        return _empty_families()

    def collect(self) -> Iterable[Metric]:
        (
            runs,
            attempts,
            successes,
            cooldowns,
            disables,
            sibling_cancels,
            retries,
            in_flight,
            eligible,
            saturated,
            resources,
            resource_status,
            resource_in_flight,
            resource_max_in_flight,
            resource_consecutive_cooldown,
            resource_cooldown_remaining,
            resource_acquires,
            resource_successes,
            resource_cooldowns,
            resource_disables,
            resource_sibling_cancels,
        ) = _empty_families()

        for pool_name, pool in self._pools.items():
            stats = pool.stats()
            for outcome in RUN_OUTCOMES:
                runs.add_metric(
                    [pool_name, outcome],
                    getattr(stats, _RUN_COUNTER_ATTR[outcome]),
                )
            attempts.add_metric([pool_name], stats.attempts)
            successes.add_metric([pool_name], stats.successes)
            cooldowns.add_metric([pool_name], stats.cooldowns)
            disables.add_metric([pool_name], stats.disables)
            sibling_cancels.add_metric([pool_name], stats.sibling_cancels)
            retries.add_metric([pool_name], stats.retries)
            in_flight.add_metric([pool_name], stats.in_flight)
            eligible.add_metric([pool_name], stats.eligible)
            saturated.add_metric([pool_name], stats.saturated)
            for status in RESOURCE_STATUSES:
                resources.add_metric([pool_name, status], stats.by_status[status])
            for rs in stats.resources.values():
                rid = rs.resource_id
                one_hot = rs.status_one_hot()
                for status in RESOURCE_STATUSES:
                    resource_status.add_metric(
                        [pool_name, rid, status], one_hot[status]
                    )
                resource_in_flight.add_metric([pool_name, rid], rs.in_flight)
                resource_max_in_flight.add_metric(
                    [pool_name, rid], rs.max_in_flight_gauge
                )
                resource_consecutive_cooldown.add_metric(
                    [pool_name, rid], rs.consecutive_cooldown
                )
                resource_cooldown_remaining.add_metric(
                    [pool_name, rid], rs.cooldown_seconds_remaining
                )
                resource_acquires.add_metric([pool_name, rid], rs.acquires)
                resource_successes.add_metric([pool_name, rid], rs.successes)
                resource_cooldowns.add_metric([pool_name, rid], rs.cooldowns)
                resource_disables.add_metric([pool_name, rid], rs.disables)
                resource_sibling_cancels.add_metric(
                    [pool_name, rid], rs.sibling_cancels
                )

        yield runs
        yield attempts
        yield successes
        yield cooldowns
        yield disables
        yield sibling_cancels
        yield retries
        yield in_flight
        yield eligible
        yield saturated
        yield resources
        yield resource_status
        yield resource_in_flight
        yield resource_max_in_flight
        yield resource_consecutive_cooldown
        yield resource_cooldown_remaining
        yield resource_acquires
        yield resource_successes
        yield resource_cooldowns
        yield resource_disables
        yield resource_sibling_cancels
