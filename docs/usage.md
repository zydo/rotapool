# Usage Guide

## Core Idea

Most resource pools are passive -- they hand out resources round-robin or at random, and rely on external health checks to detect and remove bad ones. `rotapool` closes that gap: every call through the pool is also a health probe. The pool learns from caller signals in real time and immediately adjusts which resources to offer -- no external probers or manual updates needed.

Not every failure means the resource is bad -- an HTTP 400 is your bug, but a 429 is the key's problem. You tell `rotapool` which is which by raising exceptions from inside your operation, and the pool reacts accordingly:

| Signal                              | Meaning                                |
| ----------------------------------- | -------------------------------------- |
| normal return / any other exception | Resource is healthy                    |
| `CooldownResource`                  | Temporarily overloaded, e.g. 429       |
| `DisableResource`                   | Permanently unusable, e.g. revoked key |

`rotapool` handles the rest -- picks the best resource, cools down bad ones, cancels doomed in-flight work, and retries automatically.

## Initialize the Pool

```python
from rotapool import CooldownResource, DisableResource, Pool, Resource

# Define your resources (e.g. API keys)
pool = Pool(
    # A list of Resource objects, or a dict whose keys match each resource_id.
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

## Option 1: Use the Decorator

```python
# Resource selection happens automatically per the pool's strategy (round_robin by default).
# All parameters are optional and forward to pool.run() on every call.
@pool.use(
    max_attempts=None,         # Override the pool's max_attempts for this decorated function
    deadline=None,             # Absolute time.monotonic() deadline; None = no deadline
    retry_delay=0.5,           # Base pause between failed attempts (jittered +/-50%)
    wait_for_cooldown=False,   # Wait out the earliest cooldown instead of failing fast
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

# Call it -- the framework picks the best key and retries on failure.
result = await call_upstream("https://api.example.com/v1/chat", {"prompt": "hi"})
```

## Option 2: Direct `run()`

`@pool.use()` is a thin shim over `pool.run()`, but it only accepts the policy knobs that are safe to fix at decoration time (`max_attempts`, `deadline`, `retry_delay`, `wait_for_cooldown`). Anything that needs to vary per call must go through `run()` directly -- most notably `request_id`, which is meant to correlate with caller-side context, such as an inbound HTTP request id, and would be wrong to bake into the decorator.

Use `run()` directly when you want per-call overrides or when the call site cannot be decorated:

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
    max_attempts=None,              # Override the pool's max_attempts for this call only
    deadline=time.monotonic() + 30, # Gates the start of each attempt (not in-flight work); None = no deadline
    retry_delay=0.5,                # Base pause between failed attempts (jittered +/-50%)
    wait_for_cooldown=False,        # Wait out the earliest cooldown instead of failing fast
    request_id="req-abc",           # Opaque string attached to every Usage; auto-UUID when None
)
```

## Resource Types

`rotapool` is generic -- `T` can be anything:

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

## Operation Shapes

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

# 2. Sync function returning a coroutine -- accepted.
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

A callable that returns a non-Awaitable, such as a plain `int`, raises `TypeError` at call time. The resource is marked healthy because this is your bug, not the resource's, and the error propagates to the caller.
