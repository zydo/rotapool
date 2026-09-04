# Pitfalls and Testing

## Anti-pattern: Doing the Real Work Outside `run()`

The pool only sees what happens inside the operation. Returning a client or handle from `run()` and using it afterwards means every later failure is invisible -- the attempt is already recorded as success and the cooldown counter was reset.

```python
# WRONG -- the actual API call is outside the pool's view.
client = await pool.run(lambda r: build_client(r.value))
response = await client.get("/things")  # invisible to pool
```

```python
# RIGHT -- the call lives inside the operation, so 429s reach the pool.
async def fetch(resource):
    client = build_client(resource.value)
    try:
        return await client.get("/things")
    except RateLimited as e:
        raise CooldownResource(cooldown_seconds=e.retry_after)

response = await pool.run(fetch)
```

Return only plain values, such as bytes, dicts, or dataclasses, from operations. For N backend calls, make N `run()` invocations.

## Don't

- **Don't raise `CooldownResource` for business errors** such as 404 or validation failures. The next resource will return the same error and burn the retry budget for nothing -- these belong in normal exceptions or return values.
- **Don't catch and swallow exceptions inside the operation.** The pool needs to see `CooldownResource` / `DisableResource` to update health; swallowing them turns rate limits into invisible successes.
- **Don't mutate `Resource` fields from outside the pool.** `status`, `cooldown_until`, `last_acquired_at`, and `consecutive_cooldown` are framework-owned lifecycle state. For administrative control, use `await pool.add(id, value)` / `await pool.enable(id)` / `await pool.disable(id)` / `await pool.remove(id)` instead.
- **Don't share one `Pool` across asyncio event loops.** The internal lock binds to the loop where it was first awaited; reusing the pool from a different loop is undefined behavior.

## Gotcha: Cancellation Only Hits Younger Siblings

When a resource raises `CooldownResource` or `DisableResource`, the framework cancels younger in-flight usages on that resource and retries them elsewhere. Older usages are left to run to completion -- they may already have side effects upstream that you cannot unwind.

`asyncio.CancelledError` from this sibling cancellation is swallowed by the framework and the affected usages retry on a fresh resource; only outer caller cancellation propagates back to the caller. If the cancelled attempt is the last in the budget, `PoolExhausted` carries a `CooldownResource` or `DisableResource` (the health signal that cancelled the usage), not `CancelledError`.

One known edge: if an outer cancellation lands in the same event-loop tick as an internal sibling cancellation, only one `CancelledError` is delivered and it is classified as internal -- the external cancel is absorbed for that attempt and `run()` retries. This is a deliberate trade-off for Python 3.10 compatibility because Python 3.11+ `Task.cancelling()` could disambiguate. The window is a single tick; a caller that must stop can simply cancel again.

## Testing

```bash
# pip (>= 25.1 for --group). The dev group includes prometheus_client so
# collector tests run; the prometheus extra is for applications, not tests.
pip install -e . --group dev
pytest

# uv (default groups include dev; --all-extras pulls agent + prometheus)
uv sync --all-extras
uv run pytest
```

## License

MIT
