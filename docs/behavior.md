# Behavior Guide

## Selection

Among resources that are not disabled, not cooling down, and not at `max_in_flight`, the pool picks one according to its `strategy`:

- **`"round_robin"` (default)** -- fewest in-flight usages first, then oldest `last_acquired_at`. Best-effort fairness across resources; the pool cannot predict how long a usage will hold a slot, so this only balances by acquisition time.
- **`"primary_backup"`** -- return the first eligible resource in the original list/dict order. Later resources are only used when earlier ones are cooling down, disabled, or at capacity. Resource ordering is load-bearing under this strategy.

Selection and usage registration are atomic under one lock acquisition.

### Example: `primary_backup` for a paid-tier primary with a free-tier fallback

Send every request to the paid key first; only spill over to the free key when the primary is rate-limited (cooling down) or the paid quota is exhausted (disabled). Resource ordering in the list is the priority ranking -- the pool will never reach for `free-fallback` while `paid-primary` is still healthy and below capacity.

```python
pool = Pool(
    resources=[
        Resource(resource_id="paid-primary", value="sk-paid-..."),
        Resource(resource_id="free-fallback", value="sk-free-..."),
    ],
    strategy="primary_backup",
)
```

### Example: `primary_backup` with a capacity cap to fan out under bursts

Combine `max_in_flight` on the primary with `primary_backup` to get "use the primary up to N concurrent calls, then overflow to the next tier." Useful when the primary is fastest/cheapest but has a hard concurrency limit you do not want to breach.

```python
pool = Pool(
    resources=[
        Resource(resource_id="region-us",   value=us_client,   max_in_flight=8),
        Resource(resource_id="region-eu",   value=eu_client,   max_in_flight=8),
        Resource(resource_id="region-asia", value=asia_client),
    ],
    strategy="primary_backup",
)
# 1st-8th concurrent calls -> region-us
# 9th-16th             -> region-eu (us is at capacity)
# 17th+                -> region-asia (eu is at capacity)
```

## Cooldown Escalation

Each consecutive `CooldownResource` from the same resource escalates the cooldown:

| Consecutive count | Cooldown |
| ----------------- | -------- |
| 1st               | 30s      |
| 2nd               | 120s     |
| 3rd               | 300s     |
| 4th+              | 600s     |

You can override per event: `CooldownResource(cooldown_seconds=5)`, for example from a `Retry-After` header. The counter resets on the next success. An explicit cooldown is a floor, not a replacement: the new expiry is `max(current, now + seconds)`, so a short `Retry-After` never shortens a longer cooldown that is already running on that resource.

Custom tables are supported per pool:

```python
pool = Pool(
    resources=[...],
    cooldown_table=(10.0, 30.0, 60.0, 120.0),
)
```

## In-Flight Cancellation

When a resource receives a `CooldownResource` or `DisableResource` signal, the framework cancels younger in-flight usages on the same resource **if** `cancel_siblings=True` (the default). Older usages are left alone -- they may still succeed. This maximizes throughput while avoiding doomed requests. Pass `cancel_siblings=False` to let younger usages run to completion; rotapool then provides at-least-once execution only for the signalling usage's own retries, not for cancelled siblings.

rotapool provides **at-least-once** execution when sibling cancellation is on: a cancelled operation MAY already have produced side effects upstream. Operations that are not idempotent MUST set `cancel_siblings=False`.

Cancellation is best-effort: it works when the operation returns a coroutine (the framework wraps it in an `asyncio.Task`) or an `asyncio.Future` (cancelled directly). For plain awaitables with no `.cancel()` handle, cancellation silently no-ops for that usage and it runs to natural completion. Within a coroutine, the underlying I/O is only truly aborted if the operation uses cancellation-aware async libraries such as `httpx.AsyncClient` or `aiohttp`.

## Retry

`pool.run()` drives the retry loop. `@pool.use()` is a thin decorator shim over it. In fail-fast mode attempts are capped at `min(max_attempts, len(resources))`. With `wait_for_cooldown=True` the cap is `max_attempts` only, so a single-resource pool can wait out its own cooldown.

The pause between attempts is jittered: `retry_delay * uniform(0.5, 1.5)`, mean `retry_delay`. Without jitter, concurrent calls that hit the same cooldown would all wake at the same instant and stampede the next eligible resource.

By default, `run()` fails fast: when no resource is eligible at the start of an attempt, it raises `PoolExhausted` immediately -- even if a `deadline` would outlive the cooldowns. Pass `wait_for_cooldown=True` to instead sleep until the earliest `cooldown_until` among cooling resources and select again. Only a cooldown gives a known wake-up time, so this never waits on resources that are disabled or at `max_in_flight` -- if nothing is cooling, `PoolExhausted` raises as usual.

With a `deadline`, the wait only happens when the earliest cooldown ends before it; otherwise `PoolExhausted` raises immediately rather than sleeping out a wait that provably cannot help.

The wake-up is jittered too: each waiter sleeps an extra `retry_delay * uniform(0, 1)` past the expiry, capped by `deadline`, so concurrent waiters do not all fire at the recovered resource in the same instant. As with the retry pause, `retry_delay=0` disables the jitter.

Waiters also react to admin calls: `pool.add()` wakes them so they can acquire newly added capacity immediately, `pool.enable()` wakes them so they can acquire the now-eligible resource immediately, `pool.disable()` and `pool.remove()` wake them so they can re-evaluate and fail fast instead of sleeping out a cooldown that no longer matters.

## Admin Control

`pool.add(resource_id, value, max_in_flight=None)`, `pool.enable(resource_id)`, `pool.disable(resource_id)`, and `pool.remove(resource_id)` give operators write access to resource lifecycle state -- the counterpart to `snapshot()` / `stats()`:

- **`add()`** adds new capacity at runtime. You pass only `resource_id`, `value`, and optional `max_in_flight`; the pool constructs a fresh healthy `Resource` with no cooldown history. Duplicate `resource_id`s raise `ValueError`. Added resources append to pool order, so under `primary_backup` they are the lowest-priority fallback until earlier resources become unavailable.
- **`remove()`** drops a resource from the pool entirely: it disappears from selection, `snapshot()`, and `stats().resources`, and the pool stops referencing its `value` -- unlike `disable()`, which keeps the (often secret) value in memory. Pool-level `stats()` counters are unchanged. In-flight usages finish naturally, exactly like admin disable. Raises `KeyError` for an unknown `resource_id`; re-adding the same id later starts from fresh default state, except that usages still draining from the removed resource count toward the re-added one's `in_flight` (and `max_in_flight`) until they finish -- capacity stays conservative during the overlap.
- **`disable()`** removes a resource from selection until `enable()` is called. Unlike an operation raising `DisableResource`, in-flight usages are not cancelled -- admin disable is policy, not failure evidence, so running work, which may already have upstream side effects, finishes naturally.
- **`enable()`** returns a resource to selection: it clears both the disabled state and any active cooldown, and resets `consecutive_cooldown` to 0. Enable means "the operator says this resource is usable now", for example a rotated key, so if the operator is wrong, escalation restarts from the first `cooldown_table` slot rather than resuming where it left off.

All four are async because they take the pool lock. `enable()` / `disable()` are idempotent, and all raise `KeyError` for an unknown `resource_id`. Each wakes any `run(wait_for_cooldown=True)` sleepers so they re-evaluate immediately.

## Observability

There are three surfaces; they do different jobs.

`snapshot()` is the operator / JSON view: per resource, `status`, `in_flight`, `max_in_flight` (the cap that makes `in_flight` interpretable), `consecutive_cooldown`, `cooldown_seconds_remaining`, and `last_acquired_at`. It is lock-free and thread-safe, and reports an expired cooldown as `healthy` even though the stored status only flips on the next acquire. `last_acquired_at` is a `time.monotonic()` reading -- do not scrape it as a timestamp.

`stats()` is the metrics view. Gauges are Prometheus-ready: numeric, `by_status` always contains all three status keys (including zeros), `ResourceStats.status_one_hot()` is a 0/1 series per state, and `max_in_flight_gauge` is `+Inf` when unbounded. It does not include `last_acquired_at`. Counters only increase:

- **Pool-level** (`attempts`, `successes`, `cooldowns`, `disables`, `sibling_cancels`, and `runs_*` by outcome) are process-lifetime and survive `remove()`. Never reconstruct them by summing per-resource counters -- that would drop on membership change and break Prometheus `rate()`.
- **Per-resource** counters are membership-scoped: `remove()` drops the series; `add()` of the same id starts at 0.

`runs_ok` is a normal return, `runs_exhausted` is `PoolExhausted`, `runs_error` is any other exception from the operation (the resource is still marked healthy), and `runs_cancelled` is outer caller cancellation. Constructor / argument `ValueError` is not a run and is not counted. `cooldowns` counts every `CooldownResource` event, including escalations (`cooling_down -> cooling_down`). `disables` counts real transitions to disabled (signal or admin), not no-op admin calls.

Derived pool gauges: `eligible` is effectively healthy and under `max_in_flight`; `saturated` is effectively healthy and at `max_in_flight`; cooling or disabled resources are neither.

`prometheus_client` is not a core dependency. Install `rotapool[prometheus]` and register one `PoolCollector` per registry. Metric names are frozen (`rotapool_runs_total`, `rotapool_resource_status`, ... -- see `rotapool.prometheus`). The `pool` label distinguishes pools; a second collector on the same registry collides on names, so pass `pools={"api_keys": keys, "proxies": proxies}` instead.

```python
from prometheus_client import REGISTRY
from rotapool.prometheus import PoolCollector

REGISTRY.register(PoolCollector(pool, pool_name="api_keys"))
```

Without the extra, scrape `pool.stats()` into whatever client you already use -- the gauges are already numeric. A runnable scrape is [`examples/prometheus_pool.py`](../examples/prometheus_pool.py).

For push notifications, pass an `on_state_change` callback to the constructor:

```python
pool = Pool(resources, on_state_change=log_resource_event)

def log_resource_event(resource_id: str, old: str, new: str) -> None:
    logging.info("rotapool: %s %s -> %s", resource_id, old, new)
```

It is called at the moment a resource's health status changes:

- **Cooldown** -- `healthy -> cooling_down`, and `cooling_down -> cooling_down` when an escalation or extension lands while already cooling (the status alone cannot express magnitude, so every cooldown event is delivered; compare the two statuses if you only care about flips).
- **Disable** -- `any -> disabled`, from an operation raising `DisableResource` or admin `disable()`.
- **Enable** -- `any -> healthy`, including cooldown recovery.
- **Expiry** -- `cooling_down -> healthy`, fired lazily at selection time when an expired cooldown is observed.

It is not called for `add()` / `remove()` (membership, not a status transition), cooldown-state resets on success, or no-op admin calls such as enabling a healthy resource. The hook runs after the pool lock is released. A 3-parameter callable still works (deprecated); a 4-parameter callable receives a monotonic `seq`. Keep it fast, never call the pool's `async` methods (`snapshot()` and `stats()` are safe). An exception raised by the hook is logged to the `rotapool` logger and swallowed. Do not use it as a metrics bus -- incrementing counters from the hook is lossy (it does not fire on success, exhaustion, retries, or membership).

## Cancellation Discrimination

The framework distinguishes external cancellation, such as client disconnect or shutdown, from internal cancellation, such as resource failure, by checking `usage.status`. The cooldown/disable handler sets the status to `"cancelled"` under the pool lock before invoking `.cancel()` on the handle, so observing that status when `CancelledError` arrives means "we cancelled ourselves" -- except for the one-tick edge case described in the [cancellation gotcha](pitfalls-and-testing.md#gotcha-cancellation-only-hits-younger-siblings). Works on any Python 3.10+.
