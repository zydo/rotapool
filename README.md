# rotapool

Async resource pool with inline health feedback, automatic cooldown, and retry — for API keys, proxies, GPU workers, or anything that can rate-limit you or go down.

> **Designed for AI coding agents,** `rotapool` exposes machine-readable usage notes via [agent-readable](https://github.com/zydo/agent-readable). When generating code that uses `Pool`, tell your coding agent to run `pip install agent-readable` and then `python -m agent_readable rotapool.Pool` to learn about the operation contract, do/don't rules, anti-patterns, and failure modes. `agent-readable` is an optional dependency — `rotapool` works identically without it.

## Core idea

Most resource pools are passive — they hand out resources round-robin or at random, and rely on external health checks to detect and remove bad ones. `rotapool` closes that gap: every call through the pool is also a health probe. The pool learns from caller signals in real time and immediately adjusts which resources to offer — no external probers or manual updates needed.

Not every failure means the resource is bad — an HTTP 400 is your bug, but a 429 is the key's problem. You tell `rotapool` which is which by raising exceptions from inside your operation, and the pool reacts accordingly:

| Signal                              | Meaning                                 |
| ----------------------------------- | --------------------------------------- |
| normal return / any other exception | Resource is healthy                     |
| `CooldownResource`                  | Temporarily overloaded (e.g. 429)       |
| `DisableResource`                   | Permanently unusable (e.g. revoked key) |

`rotapool` handles the rest — picks the best resource, cools down bad ones, cancels doomed in-flight work, and retries automatically.

## Install

```bash
pip install rotapool
# or
uv add rotapool
```

Requires Python 3.10+. Zero runtime dependencies; `pip install "rotapool[agent]"` adds the optional [agent-readable](https://github.com/zydo/agent-readable) integration.

## Quick start

### Initialize the pool

```python
from rotapool import CooldownResource, DisableResource, Pool, Resource

# Define your resources (e.g. API keys)
pool = Pool(
    # A list or dict of Resource objects. Dict keys are used as resource IDs.
    resources=[
        Resource(
            resource_id="key-1",                 # Unique identifier (used in logs, metrics, snapshot)
            value="sk-aaa",                      # The actual resource value (generic type T)
            # max_in_flight=None,                # Max concurrent usages per resource (None = unlimited)
        ),
        Resource(resource_id="key-2", value="sk-bbb"),
        Resource(resource_id="key-3", value="sk-ccc"),
    ],
    max_attempts=3,                              # Total retry budget per run() call (capped at len(resources))
    cooldown_table=(30.0, 120.0, 300.0, 600.0),  # Escalation: 1st=30s, 2nd=120s, 3rd=300s, 4th+=600s
)
```

### Option 1: Use the decorator

```python
# Resource selection happens automatically per the pool's strategy (round_robin by default).
# All parameters are optional and forward to pool.run() on every call.
@pool.use(
    max_attempts=None,       # Override the pool's max_attempts for this decorated function
    deadline=None,           # Absolute time.monotonic() deadline; None = no deadline
    retry_delay=0.5,         # Base pause between failed attempts (jittered ±50%)
)
async def call_upstream(resource, url, payload):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {resource.value}"},
            json=payload,
        )

    if resp.status_code == 429:
        raise CooldownResource(
            cooldown_seconds=parse_retry_after(resp.headers.get("retry-after")),
            reason="rate limited",
        )

    if resp.status_code == 401:
        raise DisableResource(reason="invalid key")

    return resp.json()

# Call it — the framework picks the best key and retries on failure
result = await call_upstream("https://api.example.com/v1/chat", {"prompt": "hi"})
```

### Option 2: Direct `run()`

`@pool.use()` is a thin shim over `pool.run()`, but it only accepts the policy knobs that are safe to fix at decoration time (`max_attempts`, `deadline`, `retry_delay`). Anything that needs to vary **per call** must go through `run()` directly — most notably `request_id`, which is meant to correlate with caller-side context (e.g. an inbound HTTP request id) and would be wrong to bake into the decorator. Use `run()` directly when you want per-call overrides or when the call site can't be decorated:

```python
async def call_upstream(resource, url, payload):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {resource.value}"},
            json=payload,
        )

    if resp.status_code == 429:
        raise CooldownResource(reason="rate limited")
    if resp.status_code == 401:
        raise DisableResource(reason="invalid key")

    return resp.json()

# Operation receives the selected Resource as its first argument.
result = await pool.run(
    lambda resource: call_upstream(resource, "https://api.example.com/v1/chat", {"prompt": "hi"}),
    max_attempts=None,               # Override the pool's max_attempts for this call only
    deadline=time.monotonic() + 30,  # Gates the start of each attempt (not in-flight work); None = no deadline
    retry_delay=0.5,                 # Base pause between failed attempts (jittered ±50%)
    request_id="req-abc",            # Opaque string attached to every Usage; auto-UUID when None
)
```

## How it works

### Selection

Among resources that are not disabled, not cooling down, and not at `max_in_flight`, the pool picks one according to its `strategy`:

- **`"round_robin"` (default)** — fewest in-flight usages first, then oldest `last_acquired_at`. Best-effort fairness across resources; the pool can't predict how long a usage will hold a slot, so this only balances by acquisition time.
- **`"primary_backup"`** — return the first eligible resource in the original list/dict order. Later resources are only used when earlier ones are cooling down, disabled, or at capacity. Resource ordering is load-bearing under this strategy.

Selection and usage registration are atomic under one lock acquisition.

#### Example: `primary_backup` for a paid-tier primary with a free-tier fallback

Send every request to the paid key first; only spill over to the free key when the primary is rate-limited (cooling down) or the paid quota is exhausted (disabled). Resource ordering in the list is the priority ranking — the pool will never reach for `free-fallback` while `paid-primary` is still healthy and below capacity.

```python
pool = Pool(
    resources=[
        Resource(resource_id="paid-primary", value="sk-paid-..."),
        Resource(resource_id="free-fallback", value="sk-free-..."),
    ],
    strategy="primary_backup",
)
```

#### Example: `primary_backup` with a capacity cap to fan out under bursts

Combine `max_in_flight` on the primary with `primary_backup` to get "use the primary up to N concurrent calls, then overflow to the next tier." Useful when the primary is fastest/cheapest but has a hard concurrency limit you don't want to breach.

```python
pool = Pool(
    resources=[
        Resource(resource_id="region-us",   value=us_client,   max_in_flight=8),
        Resource(resource_id="region-eu",   value=eu_client,   max_in_flight=8),
        Resource(resource_id="region-asia", value=asia_client),
    ],
    strategy="primary_backup",
)
# 1st–8th concurrent calls -> region-us
# 9th–16th             -> region-eu (us is at capacity)
# 17th+                -> region-asia (eu is at capacity)
```

### Cooldown escalation

Each consecutive `CooldownResource` from the same resource escalates the cooldown:

| Consecutive count | Cooldown |
| ----------------- | -------- |
| 1st               | 30s      |
| 2nd               | 120s     |
| 3rd               | 300s     |
| 4th+              | 600s     |

You can override per-event: `CooldownResource(cooldown_seconds=5)` (e.g. from a `Retry-After` header). The counter resets on the next success.

Custom tables are supported per pool:

```python
pool = Pool(
    resources=[...],
    cooldown_table=(10.0, 30.0, 60.0, 120.0),
)
```

### In-flight cancellation (best-effort)

When a resource receives a `CooldownResource` or `DisableResource` signal, the framework cancels **younger** in-flight usages on the same resource. Older usages are left alone — they may still succeed. This maximises throughput while avoiding doomed requests.

Cancellation is **best-effort**: it works when the operation returns a coroutine (the framework wraps it in an `asyncio.Task`) or an `asyncio.Future` (cancelled directly). For plain awaitables with no `.cancel()` handle, cancellation silently no-ops for that usage and it runs to natural completion. Within a coroutine, the underlying I/O is only truly aborted if the operation uses cancellation-aware async libs (`httpx.AsyncClient`, `aiohttp`).

### Retry

`pool.run()` drives the retry loop. `@pool.use()` is a thin decorator shim over it. Attempts are capped at `min(max_attempts, len(resources))` — more retries than resources is pointless.

The pause between attempts is jittered: `retry_delay * uniform(0.5, 1.5)`, mean `retry_delay`. Without jitter, concurrent calls that hit the same cooldown would all wake at the same instant and stampede the next eligible resource.

### Cancellation discrimination

The framework distinguishes external cancellation (client disconnect, shutdown — re-raised) from internal cancellation (resource failure — swallowed and retried) by checking `usage.status`. The cooldown/disable handler sets the status to `"cancelled"` under the pool lock *before* invoking `.cancel()` on the handle, so observing that status when `CancelledError` arrives reliably means "we cancelled ourselves." Works on any Python 3.10+.

## API reference

### `rotapool.Pool[T]`

```python
pool = Pool(
    resources: list[Resource[T]] | dict[str, Resource[T]],
    # resources:       A list of Resource objects, or a dict mapping resource_id -> Resource.
    #                  Duplicate resource_ids in list form raise ValueError.

    max_attempts: int = 3,
    # max_attempts:    Total retry budget per run() call. Each attempt picks among
    #                  currently eligible resources — one that triggered cooldown or
    #                  disable stays ineligible while that state lasts (a zero-second
    #                  cooldown can make it eligible again immediately). Effectively
    #                  capped at len(resources); raises PoolExhausted once spent.

    cooldown_table: tuple[float, ...] = (30.0, 120.0, 300.0, 600.0),
    # cooldown_table:  Escalation table indexed by consecutive_cooldown count.
    #                  1st cooldown → cooldown_table[0], 2nd → cooldown_table[1], etc.
    #                  Out-of-range values clamp to the last entry.

    strategy: Literal["round_robin", "primary_backup"] = "round_robin",
    # strategy:        Selection policy among eligible resources. "round_robin"
    #                  (default) balances by fewest in-flight, then oldest
    #                  last_acquired_at. "primary_backup" returns the first eligible
    #                  resource in list/dict order (ordering is the priority ranking).
    #                  Pool-level by design; not overridable per call. See "Selection".
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
    #                  start and caps the inter-attempt pause. Does NOT interrupt an
    #                  in-flight operation, so a single long call can overrun it. Raises
    #                  PoolExhausted when a new attempt would start past it. None = none.

    retry_delay: float = 0.5,
    # retry_delay:     Base pause (seconds) between failed attempts. Must be >= 0.
    #                  The actual pause is jittered to retry_delay * uniform(0.5, 1.5)
    #                  (mean stays retry_delay) so concurrent callers don't retry in
    #                  lockstep and stampede the next eligible resource.

    request_id: str | None = None,
    # request_id:      Opaque string attached to every Usage created by this call.
    #                  Auto-generated UUID when None.
) -> R
```

```python
@pool.use(
    max_attempts: int | None = None,     # Per-call override; None = use pool default
    deadline: float | None = None,       # Absolute time.monotonic() deadline
    retry_delay: float = 0.5,            # Base pause between failed attempts (jittered ±50%)
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
# Example return value:
# {
#     "key-1": {
#         "status": "healthy",                  # "healthy" | "cooling_down" | "disabled"
#         "in_flight": 2,                       # Current in-flight usage count
#         "consecutive_cooldown": 0,            # Escalation counter
#         "cooldown_seconds_remaining": 0.0,    # Seconds until cooldown expires (0 if healthy)
#         "last_acquired_at": 12345.67,         # time.monotonic() of last acquire
#     },
#     ...
# }
```

### `rotapool.Resource[T]`

```python
resource = Resource(
    resource_id: str,
    # resource_id:          Unique identifier for this resource.

    value: T,
    # value:                The actual resource object (API key, proxy URL, etc.).

    max_in_flight: int | None = None,
    # max_in_flight:        Maximum concurrent usages. None = unlimited, 1 = exclusive.

    status: str = "healthy",
    # status:               Current health: "healthy", "cooling_down", or "disabled".
    #                       Managed by the framework — do not set manually.

    cooldown_until: float = 0.0,
    # cooldown_until:       time.monotonic() deadline when status is "cooling_down".
    #                       Managed by the framework — do not set manually.

    last_acquired_at: float = 0.0,
    # last_acquired_at:     time.monotonic() of most recent acquire. Affects selection
    #                       order (oldest first). Managed by the framework.

    consecutive_cooldown: int = 0,
    # consecutive_cooldown: Number of consecutive CooldownResource signals. Indexes into
    #                       the pool's cooldown_table. Resets to 0 on next success.
    #                       Managed by the framework — do not set manually.
)
```

### Exceptions

| Exception          | Who raises it  | Meaning                                                        |
| ------------------ | -------------- | -------------------------------------------------------------- |
| `CooldownResource` | Your operation | Resource temporarily over capacity                             |
| `DisableResource`  | Your operation | Resource permanently bad                                       |
| `PoolExhausted`    | Framework      | No eligible resource, max attempts reached, or deadline passed |

```python
raise CooldownResource(
    cooldown_seconds: float | None = None,
    # Explicit cooldown duration (e.g. from Retry-After header).
    # None = use the pool's cooldown_table based on consecutive_cooldown count.

    reason: str | None = None,
    # Free-form string surfaced in the exception message and logs.
)
```

```python
raise DisableResource(
    reason: str | None = None,
    # Free-form string surfaced in the exception message and logs.
)
```

## Resource types

`rotapool` is generic — `T` can be anything:

```python
# API keys (string bearer tokens)
Resource(resource_id="key-1", value="sk-...")

# HTTP proxies
Resource(resource_id="proxy-1", value="http://proxy:8080", max_in_flight=10)

# Browser sessions (exclusive)
Resource(resource_id="session-1", value=<webdriver>, max_in_flight=1)

# GPU workers
Resource(resource_id="gpu-0", value="cuda:0", max_in_flight=1)
```

## Operation shapes

`pool.run` and `@pool.use` accept any callable that returns an `Awaitable`. The framework picks the cancellation strategy at runtime based on what the callable returns:

```python
# 1. async def -- the typical case. Cancellation is full-strength: the
#    framework wraps the coroutine in a Task and cancels younger siblings
#    via task.cancel() on resource failure.
@pool.use()
async def call_async(resource, payload):
    async with httpx.AsyncClient() as client:
        return await client.post(url, json=payload,
                                 headers={"Authorization": f"Bearer {resource.value}"})

# 2. Sync function returning a coroutine -- previously rejected, now accepted.
#    Useful when you want to construct the coroutine yourself or thread args.
@pool.use()
def call_returning_coro(resource, payload):
    return some_async_helper(resource.value, payload)  # returns a coroutine

# 3. Sync function returning an asyncio.Future -- accepted and cancellable
#    via Future.cancel(). Useful for executor wrappers.
@pool.use()
def call_in_thread(resource, payload):
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, blocking_request, resource.value, payload)

# 4. Anything returning a plain Awaitable (custom __await__) is also accepted,
#    but with no cancel handle: younger sibling cancellation silently no-ops
#    for this usage and it runs to natural completion (best-effort).
```

A callable that returns a non-Awaitable (e.g. a plain `int`) raises `TypeError` at call time. The resource is marked healthy (your bug, not the resource's) and the error propagates to the caller.

## Pitfalls

### Anti-pattern: doing the real work outside `run()`

The pool only sees what happens **inside** the operation. Returning a client or handle from `run()` and using it afterwards means every later failure is invisible — the attempt is already recorded as success and the cooldown counter was reset.

```python
# WRONG — the actual API call is outside the pool's view.
client = await pool.run(lambda r: build_client(r.value))
response = await client.get("/things")  # invisible to pool
```

```python
# RIGHT — the call lives inside the operation, so 429s reach the pool.
async def fetch(resource):
    client = build_client(resource.value)
    try:
        return await client.get("/things")
    except RateLimited as e:
        raise CooldownResource(cooldown_seconds=e.retry_after)

response = await pool.run(fetch)
```

Return only plain values (bytes, dict, dataclass) from operations. For N backend calls, make N `run()` invocations.

### Don't

- **Don't raise `CooldownResource` for business errors** (404, validation failures). The next resource will return the same error and burn the retry budget for nothing — these belong in normal exceptions or return values.
- **Don't catch and swallow exceptions inside the operation.** The pool needs to see `CooldownResource` / `DisableResource` to update health; swallowing them turns rate limits into invisible successes.
- **Don't mutate `Resource` fields from outside the pool.** `status`, `cooldown_until`, `last_acquired_at`, and `consecutive_cooldown` are framework-owned lifecycle state.
- **Don't share one `Pool` across asyncio event loops.** The internal lock binds to the loop where it was first awaited; reusing the pool from a different loop is undefined behaviour.

### Gotcha: cancellation only hits younger siblings

When a resource raises `CooldownResource` or `DisableResource`, the framework cancels **younger** in-flight usages on that resource and retries them elsewhere. **Older** usages are left to run to completion — they may already have side effects upstream that you can't unwind.

`asyncio.CancelledError` from this sibling cancellation is swallowed by the framework and the affected usages retry on a fresh resource; only **outer caller cancellation** propagates back to the caller.

## Testing

```bash
# pip (>= 25.1 for --group)
pip install -e . --group dev
pytest

# uv
uv sync --all-extras
uv run pytest
```

## License

MIT
