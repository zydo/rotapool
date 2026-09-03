# API Reference

## `rotapool.Pool[T]`

```python
pool = Pool(
    resources: list[Resource[T]] | dict[str, Resource[T]],
    # resources:       A list of Resource objects, or a dict mapping resource_id -> Resource.
    #                  Duplicate resource_ids in list form raise ValueError, as does a
    #                  dict key that does not match its Resource's resource_id.

    max_attempts: int = 3,
    # max_attempts:    Total retry budget per run() call. Each attempt picks among
    #                  currently eligible resources -- one that triggered cooldown or
    #                  disable stays ineligible while that state lasts (a zero-second
    #                  cooldown can make it eligible again immediately). Effectively
    #                  capped at len(resources); raises PoolExhausted once spent.

    cooldown_table: tuple[float, ...] = (30.0, 120.0, 300.0, 600.0),
    # cooldown_table:  Escalation table indexed by consecutive_cooldown count.
    #                  1st cooldown -> cooldown_table[0], 2nd -> cooldown_table[1], etc.
    #                  Out-of-range values clamp to the last entry.
    #                  Entries must be finite and >= 0.

    strategy: Literal["round_robin", "primary_backup"] = "round_robin",
    # strategy:        Selection policy among eligible resources. "round_robin"
    #                  (default) balances by fewest in-flight, then oldest
    #                  last_acquired_at. "primary_backup" returns the first eligible
    #                  resource in list/dict order (ordering is the priority ranking).
    #                  Pool-level by design; not overridable per call. See "Selection".

    on_state_change: Callable[[str, ResourceStatus, ResourceStatus], None] | None = None,
    # on_state_change: Monitoring hook called as on_state_change(resource_id,
    #                  old_status, new_status) the moment a resource's health status
    #                  changes, so operators can log/alert without polling snapshot().
    #                  Fired for: CooldownResource (healthy -> cooling_down, and
    #                  cooling_down -> cooling_down for escalations/extensions -- every
    #                  cooldown event is delivered), DisableResource or admin disable()
    #                  (any -> disabled), admin enable() (any -> healthy), and lazy
    #                  cooldown expiry at selection time (cooling_down -> healthy).
    #                  NOT fired for add()/remove() (membership, not status), success
    #                  resets, or no-op admin calls. Called synchronously under the
    #                  pool lock: keep it fast, never block, never call the pool's
    #                  async methods (snapshot() and stats() are safe -- lock-free).
    #                  Exceptions are logged to the "rotapool" logger and swallowed.
    #                  Do not use it as a metrics bus.
)
```

```python
await pool.run(
    operation: Callable[[Resource[T]], Awaitable[R]],
    # operation:       Callable receiving the selected Resource and returning an
    #                  Awaitable. Raise CooldownResource or DisableResource to
    #                  signal resource health. Any other exception is treated as
    #                  "resource is fine" and propagates to the caller.
    #                  Accepted return types:
    #                    - coroutine          (typical async def)        -- cancellable
    #                    - asyncio.Future     (e.g. loop.create_future)  -- cancellable
    #                    - any Awaitable      (custom __await__)         -- best-effort
    #                  Returning a non-Awaitable raises TypeError at call time.

    *,                 # All following parameters are keyword-only.

    max_attempts: int | None = None,
    # max_attempts:    Per-call override of the pool's max_attempts. None = use pool default.

    deadline: float | None = None,
    # deadline:        Absolute time.monotonic() value that gates when each attempt may
    #                  start and caps the inter-attempt pause. Does not interrupt an
    #                  in-flight operation, so a single long call can overrun it. Raises
    #                  PoolExhausted when a new attempt would start past it. Non-finite
    #                  values (NaN, inf) raise ValueError -- comparisons against them
    #                  never fire, which would silently disable the deadline. None = none.

    retry_delay: float = 0.5,
    # retry_delay:     Base pause (seconds) between failed attempts. Must be >= 0
    #                  (negative and NaN raise ValueError before any attempt).
    #                  The actual pause is jittered to retry_delay * uniform(0.5, 1.5)
    #                  (mean stays retry_delay) so concurrent callers do not retry in
    #                  lockstep and stampede the next eligible resource.

    wait_for_cooldown: bool = False,
    # wait_for_cooldown: When no resource is eligible at the start of an attempt,
    #                  sleep until the earliest cooldown_until among cooling resources
    #                  and select again, instead of raising PoolExhausted immediately.
    #                  Never waits on disabled or saturated resources (no known wake-up
    #                  time); raises immediately when the earliest cooldown ends at or
    #                  after the deadline. The wake-up is jittered by an extra
    #                  retry_delay * uniform(0, 1), capped by the deadline, to avoid
    #                  waiter stampedes at the expiry instant. False = fail fast (default).

    request_id: str | None = None,
    # request_id:      Opaque string attached to every Usage created by this call.
    #                  Auto-generated UUID when None.
) -> R
```

```python
@pool.use(
    max_attempts: int | None = None,     # Per-call override; None = use pool default
    deadline: float | None = None,       # Absolute time.monotonic() deadline
    retry_delay: float = 0.5,            # Base pause between failed attempts (jittered +/-50%)
    wait_for_cooldown: bool = False,     # Wait out the earliest cooldown instead of failing fast
)
# Returns a decorator. The decorated function receives a Resource[T] as its
# first positional argument (injected by the wrapper), followed by caller args.
# Any callable returning an Awaitable is accepted (async def, sync function
# returning a coroutine / Future / awaitable). A callable that returns a
# non-Awaitable raises TypeError at call time.
```

```python
pool.snapshot() -> dict[str, dict[str, Any]]
# Returns a point-in-time summary of every resource. Thread-safe without the lock.
# A resource whose cooldown has expired is reported as "healthy" even though the
# stored status only flips on the next acquire. Operator / JSON view -- includes
# last_acquired_at (a time.monotonic() reading, not a scrape timestamp). For
# counters and Prometheus-ready gauges, use stats().
# Example return value:
# {
#     "key-1": {
#         "status": "healthy",                  # "healthy" | "cooling_down" | "disabled"
#         "in_flight": 2,                       # Current in-flight usage count
#         "max_in_flight": 4,                   # Concurrency cap (None = unlimited)
#         "consecutive_cooldown": 0,            # Escalation counter
#         "cooldown_seconds_remaining": 0.0,    # Seconds until cooldown expires (0 if healthy)
#         "last_acquired_at": 12345.67,         # time.monotonic() of last acquire
#     },
#     ...
# }
```

```python
pool.stats() -> PoolStats
# Lock-free metrics snapshot. Gauges describe current membership (expired
# cooldowns report as healthy). Pool-level counters are process-lifetime and
# survive remove(); per-resource counters are membership-scoped and start at 0
# on add(). Never sum resource counters to rebuild pool counters -- that drops
# on remove() and breaks Prometheus rate(). last_acquired_at is omitted on
# purpose: it is process-monotonic, not a scrape timestamp.
#
# PoolStats
#   resources: dict[str, ResourceStats]      # insertion order, current members
#   in_flight: int                           # sum of current in_flight
#   eligible: int                            # healthy and under max_in_flight
#   saturated: int                           # healthy and at max_in_flight
#   by_status: dict[status, int]             # always all three keys, zeros included
#   attempts, successes, cooldowns, disables, sibling_cancels: int  # lifetime
#   runs_ok, runs_exhausted, runs_error, runs_cancelled: int        # lifetime
#
# ResourceStats
#   status, in_flight, max_in_flight, consecutive_cooldown,
#   cooldown_seconds_remaining
#   acquires, successes, cooldowns, disables, sibling_cancels
#   eligible / saturated                     # derived bools
#   max_in_flight_gauge                      # +Inf when max_in_flight is None
#   status_one_hot()                         # {healthy: 0|1, cooling_down: ..., disabled: ...}
#
# PoolStats and ResourceStats are frozen dataclasses, exported from rotapool.
```

```python
await pool.add(
    resource_id: str,
    value: T,
    *,
    max_in_flight: int | None = None,
) -> Resource[T]
# Add a new healthy resource at runtime. The pool constructs the Resource using
# lifecycle defaults: status="healthy", cooldown_until=0.0, last_acquired_at=0.0,
# consecutive_cooldown=0. Duplicate resource_id raises ValueError. The new
# resource is appended to pool order, so under "primary_backup" it is lower
# priority than existing resources.

await pool.enable(resource_id: str) -> None
# Administratively return a resource to selection. Clears both the disabled state
# and any active cooldown, and resets consecutive_cooldown to 0 (a later failure
# escalates from the first cooldown_table slot). Idempotent on a healthy resource.
# Raises KeyError for an unknown resource_id.

await pool.disable(resource_id: str) -> None
# Administratively remove a resource from selection until enable(). In-flight
# usages are not cancelled (unlike an operation raising DisableResource) -- they
# run to natural completion. Idempotent on an already-disabled resource.
# Raises KeyError for an unknown resource_id.

await pool.remove(resource_id: str) -> None
# Drop a resource from the pool entirely -- the counterpart to add(). The
# resource disappears from selection, snapshot(), and stats().resources
# immediately, and the pool
# stops referencing its value (unlike disable(), which keeps it in memory --
# remove is for rotated/revoked secrets). In-flight usages finish naturally,
# exactly like admin disable(); a late CooldownResource from such a usage
# updates nothing, while a DisableResource still cancels younger siblings on
# it. Does not fire on_state_change (membership, not a status transition).
# Wakes wait_for_cooldown sleepers. Raises KeyError for an unknown resource_id.
# Re-adding the id later starts fresh, except that usages still draining from
# the removed resource count toward the re-added one's max_in_flight until
# they finish (conservative during the overlap).
```

## `rotapool.Resource[T]`

```python
resource = Resource(
    resource_id: str,
    # resource_id:          Unique identifier for this resource. Must be non-empty.

    value: T,
    # value:                The actual resource object (API key, proxy URL, etc.).

    max_in_flight: int | None = None,
    # max_in_flight:        Maximum concurrent usages. None = unlimited, 1 = exclusive.
    #                       Must be >= 1 or None.

    status: Literal["healthy", "cooling_down", "disabled"] = "healthy",
    # status:               Current health. Managed by the framework -- do not set
    #                       manually.

    cooldown_until: float = 0.0,
    # cooldown_until:       time.monotonic() deadline when status is "cooling_down".
    #                       Managed by the framework -- do not set manually.

    last_acquired_at: float = 0.0,
    # last_acquired_at:     time.monotonic() of most recent acquire. Affects selection
    #                       order (oldest first). Managed by the framework.

    consecutive_cooldown: int = 0,
    # consecutive_cooldown: Number of consecutive CooldownResource signals. Indexes into
    #                       the pool's cooldown_table. Resets to 0 on next success.
    #                       Managed by the framework -- do not set manually.
)
```

## Exceptions

| Exception          | Who raises it  | Meaning                                                                                                                     |
| ------------------ | -------------- | --------------------------------------------------------------------------------------------------------------------------- |
| `CooldownResource` | Your operation | Resource temporarily over capacity                                                                                          |
| `DisableResource`  | Your operation | Resource permanently bad (only `pool.enable()` brings it back)                                                              |
| `PoolExhausted`    | Framework      | No eligible resource, max attempts reached, deadline passed, or (with `wait_for_cooldown`) waiting cannot beat the deadline |

```python
raise CooldownResource(
    cooldown_seconds: float | None = None,
    # Explicit cooldown duration (e.g. from Retry-After header). Must be >= 0
    # (negative and NaN raise ValueError at construction).
    # None = use the pool's cooldown_table based on consecutive_cooldown count.

    reason: str | None = None,
    # Free-form string surfaced in the exception message and logs.
    # Not a metrics label -- stats() and PoolCollector do not export it.
)
```

```python
raise DisableResource(
    reason: str | None = None,
    # Free-form string surfaced in the exception message and logs.
    # Not a metrics label -- stats() and PoolCollector do not export it.
)
```

## `rotapool.prometheus.PoolCollector`

Optional extra -- not a core dependency: `pip install "rotapool[prometheus]"`.
Import from `rotapool.prometheus`, not `rotapool`. Register **one** collector
per Prometheus registry; a second collector with the same metric names raises
`ValueError`. Distinguish pools with the `pool` label.

```python
from prometheus_client import REGISTRY
from rotapool.prometheus import PoolCollector

# Single pool (pool_name defaults to "default"):
REGISTRY.register(PoolCollector(pool, pool_name="api_keys"))

# Several pools -- one collector, not two:
REGISTRY.register(PoolCollector(pools={"api_keys": keys, "proxies": proxies}))

# Or build it up:
PoolCollector().add(keys, pool_name="api_keys").add(proxies, pool_name="proxies").register()
```

```python
PoolCollector(
    pool: Pool | None = None,          # Single pool. Mutually exclusive with pools.
    *,
    pool_name: str = "default",        # Value of the "pool" label for `pool`. Non-empty.
                                       # Ignored when `pools` is given.
    pools: dict[str, Pool] | None = None,  # pool label -> Pool
)
collector.add(pool, *, pool_name: str) -> PoolCollector
# Attach another pool. Duplicate pool_name raises ValueError. Returns self.
collector.register(registry=None) -> PoolCollector
# Register on registry (default: prometheus_client.REGISTRY). Returns self.
```

Frozen metric names (changing them is a dashboard-breaking change):

| Metric                                         | Kind                        | Labels                                                         |
| ---------------------------------------------- | --------------------------- | -------------------------------------------------------------- |
| `rotapool_runs_total`                          | counter                     | `pool`, `outcome` (`ok` / `exhausted` / `error` / `cancelled`) |
| `rotapool_attempts_total`                      | counter                     | `pool`                                                         |
| `rotapool_successes_total`                     | counter                     | `pool`                                                         |
| `rotapool_cooldowns_total`                     | counter                     | `pool`                                                         |
| `rotapool_disables_total`                      | counter                     | `pool`                                                         |
| `rotapool_sibling_cancels_total`               | counter                     | `pool`                                                         |
| `rotapool_in_flight`                           | gauge                       | `pool`                                                         |
| `rotapool_eligible`                            | gauge                       | `pool`                                                         |
| `rotapool_saturated`                           | gauge                       | `pool`                                                         |
| `rotapool_resources`                           | gauge                       | `pool`, `status`                                               |
| `rotapool_resource_status`                     | gauge (0/1 one-hot)         | `pool`, `resource_id`, `status`                                |
| `rotapool_resource_in_flight`                  | gauge                       | `pool`, `resource_id`                                          |
| `rotapool_resource_max_in_flight`              | gauge (`+Inf` if unbounded) | `pool`, `resource_id`                                          |
| `rotapool_resource_consecutive_cooldown`       | gauge                       | `pool`, `resource_id`                                          |
| `rotapool_resource_cooldown_remaining_seconds` | gauge                       | `pool`, `resource_id`                                          |
| `rotapool_resource_acquires_total`             | counter                     | `pool`, `resource_id`                                          |
| `rotapool_resource_successes_total`            | counter                     | `pool`, `resource_id`                                          |
| `rotapool_resource_cooldowns_total`            | counter                     | `pool`, `resource_id`                                          |
| `rotapool_resource_disables_total`             | counter                     | `pool`, `resource_id`                                          |
| `rotapool_resource_sibling_cancels_total`      | counter                     | `pool`, `resource_id`                                          |

Pool-level counters come from `stats()` lifetime totals, never from summing
resource series. Resource series are current membership only. `last_acquired_at`
is not exported.

A runnable scrape is [`examples/prometheus_pool.py`](../examples/prometheus_pool.py).
