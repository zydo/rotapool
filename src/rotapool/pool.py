from __future__ import annotations

import asyncio
import functools
import inspect
import random
import time
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    Literal,
    TypeVar,
    cast,
)

if TYPE_CHECKING:
    from agent_readable import AgentReadableMixin
else:
    try:
        from agent_readable import AgentReadableMixin
    except ImportError:

        class AgentReadableMixin:
            """No-op stand-in when the optional agent-readable package is absent.

            Pool's ``__agent_notes__`` still exists; only the auto-generated
            ``__agent_help__`` introspection from the real mixin is lost.
            """


from .exceptions import CooldownResource, DisableResource, PoolExhausted
from .models import Resource, Usage

T = TypeVar("T")
R = TypeVar("R")

_DEFAULT_COOLDOWN_TABLE: tuple[float, ...] = (30.0, 120.0, 300.0, 600.0)

Strategy = Literal["round_robin", "primary_backup"]


class Pool(AgentReadableMixin, Generic[T]):
    """A pool of interchangeable resources sharing the same usage policy.

    Selection excludes cooling-down and disabled resources, then applies the chosen
    ``strategy`` ("round_robin" or "primary_backup") among the remaining candidates.

    Note for AI coding agents: this class is agent-readable. Before generating code
    that uses ``Pool``, run ``pip install agent-readable`` and then
    ``python -m agent_readable rotapool.Pool`` to print the full agent-oriented
    docs (operation contract, do/don't rules, anti-patterns, failure modes).
    """

    def __init__(
        self,
        resources: list[Resource[T]] | dict[str, Resource[T]],
        max_attempts: int = 3,
        cooldown_table: tuple[float, ...] = _DEFAULT_COOLDOWN_TABLE,
        strategy: Strategy = "round_robin",
    ) -> None:
        """Construct a pool over a fixed set of interchangeable resources.

        resources: the resources this pool manages. Accepts either a list of
            ``Resource`` objects (their ``resource_id`` fields must be unique) or a
            ``dict`` keyed by resource id. Iteration order is preserved and is
            **load-bearing under the "primary_backup" strategy** -- earlier entries
            are higher-priority. Must contain at least one entry.

        max_attempts: default total retry budget per ``run()`` call (not per resource).
            Each attempt selects a resource via the pool's selection rules; a resource
            that triggered cooldown or disable on one attempt is ineligible on the
            next while that state lasts (a zero-second cooldown can make it eligible
            again immediately, in which case it may be re-selected). Effective cap is
            ``min(max_attempts, len(resources))`` -- a budget larger than the pool is
            pointless, so ``run()`` raises ``PoolExhausted`` once it is spent.
            Overridable per call via ``run(..., max_attempts=...)``.

        cooldown_table: cooldown durations (seconds) indexed by ``consecutive_cooldown``
            count on a resource. Each consecutive ``CooldownResource`` from the same
            resource escalates one slot; the counter resets on the next success.
            Counts past the table length clamp to the last entry. Per-event
            ``CooldownResource(cooldown_seconds=...)`` (e.g. from ``Retry-After``)
            overrides this for that one event without resetting the counter.

        strategy: how the pool picks among resources that are eligible (not disabled,
            not cooling down, not at ``max_in_flight``). Pool-level by design --
            varying strategy per call would mix policies on one resource set; if you
            need both, use two pools sharing the same ``Resource`` objects.

            - ``"round_robin"`` (default): fewest in-flight first, then oldest
              ``last_acquired_at``. Best-effort fairness -- the pool can't predict
              how long a usage will hold a slot, so it only balances by acquisition
              time, not by remaining work.
            - ``"primary_backup"``: walk ``resources`` in order and return the first
              eligible one. Later resources are reached only when earlier ones are
              cooling down, disabled, or at ``max_in_flight``. The order you pass
              ``resources`` in is the priority ranking.
        """
        # resource_id -> resource
        self._resources: dict[str, Resource[T]] = self._build_resources(resources)
        if not self._resources:
            raise ValueError("Pool requires at least one resource")

        if max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        self._max_attempts: int = max_attempts

        if not cooldown_table:
            raise ValueError("cooldown_table must contain at least one entry")
        if any(cd < 0 for cd in cooldown_table):
            raise ValueError("cooldown_table entries must be >= 0")
        self._cooldown_table: tuple[float, ...] = cooldown_table

        # Runtime guard for callers that bypass type checking. Cast widens the
        # Literal so pyright doesn't flag the comparison as unreachable.
        if cast(str, strategy) not in ("round_robin", "primary_backup"):
            raise ValueError(
                f"strategy must be 'round_robin' or 'primary_backup', got {strategy!r}"
            )
        self._strategy: Strategy = strategy

        # Guards all possibly racing states.
        self._lock: asyncio.Lock = asyncio.Lock()

        # usage_id -> Usage
        self._usages: dict[str, Usage] = {}

        # resource_id -> { usage_id_set }
        self._inflight_by_resource: dict[str, set[str]] = {}

    async def run(
        self,
        operation: Callable[[Resource[T]], Awaitable[R]],
        *,
        max_attempts: int | None = None,
        deadline: float | None = None,
        retry_delay: float = 0.5,
        request_id: str | None = None,
    ) -> R:
        """Drive the retry loop for one logical request.

        operation: callable receiving the selected resource and returning an Awaitable.
            May raise CooldownResource or DisableResource to signal resource health. Any
            other exception is treated as resource OK and propagates to the caller (so
            user-side bugs do not poison the pool).

            The returned awaitable can be:
            - a coroutine (the typical case for `async def` operations) -- the framework
              wraps it in an `asyncio.Task` so younger sibling cancellation works.
            - an `asyncio.Future` -- cancellable directly via its `.cancel()` method.
            - any other Awaitable (custom `__await__` object, etc.) -- awaited directly,
              with cancellation a silent best-effort no-op for this usage.

            Returning a non-awaitable raises `TypeError` (treated as a user bug; the
            resource is marked healthy and the error propagates to the caller).

        max_attempts: per-call override of Pool.__init__ max_attempts.
            This is a total budget across resource switches, not per resource.
            Effective value is ``min(max_attempts, len(resources))``.

        deadline: absolute time.monotonic() value that gates when each attempt may
            start. It is checked before every attempt and caps the inter-attempt retry
            pause, so run() will neither begin new work nor keep sleeping past it. It
            does NOT interrupt an operation already in flight -- a single call that runs
            long can overrun the deadline, because the pool never cancels a usage that
            may already have upstream side effects. None disables the deadline.

        retry_delay: base pause between failed attempts to let cooling resources
            recover and to avoid hammering the pool. Must be >= 0. The actual pause
            is jittered to ``retry_delay * uniform(0.5, 1.5)`` (mean stays
            ``retry_delay``) so concurrent callers do not retry in lockstep and
            stampede the next eligible resource.

        request_id: opaque string attached to every `Usage` created by this call.
            Useful for correlating logs, metrics, or tracing back to the original
            caller (e.g. an HTTP request-id header). Auto-generated UUID when None.
        """
        rid = request_id or str(uuid.uuid4())
        if max_attempts is not None and max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if retry_delay < 0:
            raise ValueError(f"retry_delay must be >= 0, got {retry_delay}")
        cap = max_attempts if max_attempts is not None else self._max_attempts
        effective_attempts = min(cap, len(self._resources))
        last_error: BaseException | None = None

        for attempt_num in range(effective_attempts):
            if deadline is not None and time.monotonic() >= deadline:
                raise PoolExhausted(f"deadline exceeded after {attempt_num} attempt(s)")

            acquired = await self._acquire(rid)
            if acquired is None:
                raise PoolExhausted("no eligible resource in pool")
            resource, usage = acquired

            try:
                awaited = operation(resource)

                if inspect.iscoroutine(awaited):
                    # Wrap in Task so younger-usage cancellation can fire. No await
                    # between create_task and the assignment -- atomically safe in
                    # single-loop asyncio; no other coroutine interleaves.
                    task = asyncio.create_task(awaited)
                    usage.task = task
                    result = await task
                elif isinstance(awaited, asyncio.Future):
                    # Future is cancellable via .cancel() without wrapping.
                    usage.task = awaited
                    result = await awaited
                elif inspect.isawaitable(awaited):
                    # Plain awaitable with no cancel handle. Cancellation of younger
                    # usages on this resource is best-effort -- this usage runs to
                    # natural completion if a sibling fails.
                    result = await awaited
                else:
                    raise TypeError(
                        f"operation must return an Awaitable, got {type(awaited).__name__}"
                    )

                await self._on_ok(usage)
                return result

            except CooldownResource as e:
                await self._on_cooldown(usage, cooldown_seconds=e.cooldown_seconds)
                last_error = e
                if attempt_num < effective_attempts - 1:
                    await self._sleep_before_retry(retry_delay, deadline)
                continue

            except DisableResource as e:
                await self._on_disable(usage)
                last_error = e
                if attempt_num < effective_attempts - 1:
                    await self._sleep_before_retry(retry_delay, deadline)
                continue

            except asyncio.CancelledError:
                # Distinguish "outer caller cancelled us" (re-raise so shutdown is
                # honored) from "we cancelled our own handle via _on_cooldown /
                # _on_disable" (swallow and retry). _collect_younger_usages_locked sets
                # usage.status = "cancelled" under the lock *before* invoking .cancel()
                # on the handle, so seeing "cancelled" here means a sibling on the same
                # resource cancelled us. With no cancel handle (usage.task is None) the
                # pool could not have delivered this error even if a sibling marked the
                # usage cancelled, so it must be external. Works on any Python 3.10+
                # (no asyncio.Task.cancelling() dependency). Cleanup runs in finally.
                cancelled_internally = (
                    usage.status == "cancelled" and usage.task is not None
                )
                usage.status = "cancelled"
                if not cancelled_internally:
                    raise
                last_error = asyncio.CancelledError()
                if attempt_num < effective_attempts - 1:
                    await self._sleep_before_retry(retry_delay, deadline)
                continue

            except Exception:
                # Ordinary user/business exception: the resource is fine.
                # Mark OK and propagate the exception unchanged to the caller.
                await self._on_ok(usage)
                raise

            finally:
                await self._cleanup_usage(usage)

        # Loop only exits without returning when an attempt failed and set last_error;
        # a clean exit (no failure) returns from inside the loop.
        raise PoolExhausted(
            f"max_attempts={effective_attempts} exhausted: {last_error!r}"
        )

    def use(
        self,
        *,
        max_attempts: int | None = None,
        deadline: float | None = None,
        retry_delay: float = 0.5,
    ) -> Callable[[Callable[..., Awaitable[R]]], Callable[..., Awaitable[R]]]:
        """Decorator factory: wrap a callable so every call goes through ``self.run()``,
        with resource selection (per pool ``strategy``) and retry handled for you.

        The decorated function receives a ``Resource[T]`` as its first positional
        argument (injected by the wrapper), followed by whatever the caller passes.

        Any callable returning an Awaitable is accepted -- typically `async def`
        functions, but plain functions returning a coroutine, an `asyncio.Future`, or
        any awaitable also work. Cancellation of younger sibling usages is best-effort:
        it works for coroutines and Futures, and silently no-ops for plain awaitables.
        A callable that returns a non-awaitable raises `TypeError` at call time.
        """

        def decorator(func: Callable[..., Awaitable[R]]) -> Callable[..., Awaitable[R]]:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> R:
                return await self.run(
                    lambda resource: func(resource, *args, **kwargs),
                    max_attempts=max_attempts,
                    deadline=deadline,
                    retry_delay=retry_delay,
                )

            return wrapper

        return decorator

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a point-in-time summary of every resource in the pool.

        Thread-safe without the lock -- reads simple types (str, int, float) and
        Python-int counters that change atomically under the GIL. Good enough for
        metrics / /status.
        """
        now = time.monotonic()
        result: dict[str, dict[str, Any]] = {}
        for rid, r in self._resources.items():
            inflight = len(self._inflight_by_resource.get(rid, set()))
            cooldown_remaining = (
                max(r.cooldown_until - now, 0.0) if r.status == "cooling_down" else 0.0
            )
            result[rid] = {
                "status": r.status,
                "in_flight": inflight,
                "consecutive_cooldown": r.consecutive_cooldown,
                "cooldown_seconds_remaining": cooldown_remaining,
                "last_acquired_at": r.last_acquired_at,
            }
        return result

    @staticmethod
    def _build_resources(
        resources: list[Resource[T]] | dict[str, Resource[T]],
    ) -> dict[str, Resource[T]]:
        if isinstance(resources, list):
            result: dict[str, Resource[T]] = {}
            for r in resources:
                if r.resource_id in result:
                    raise ValueError(
                        f"Duplicate resource_id in pool: {r.resource_id!r}"
                    )
                result[r.resource_id] = r
            return result
        for key, r in resources.items():
            if key != r.resource_id:
                raise ValueError(
                    f"dict key {key!r} does not match resource_id {r.resource_id!r}"
                )
        return dict(resources)

    async def _acquire(self, request_id: str) -> tuple[Resource[T], Usage] | None:
        """Atomically select an eligible resource and register a usage on it.

        Returns (resource, usage) on success or None if no resource is eligible (all
        disabled, all cooling down, or all at `max_in_flight` capacity). Selection and
        registration share one lock acquisition to keep the derived in-flight count
        consistent.
        """
        now = time.monotonic()

        async with self._lock:
            candidates: list[Resource[T]] = []

            for r in self._resources.values():
                if r.status == "disabled":
                    continue

                if r.status == "cooling_down":
                    if r.cooldown_until <= now:
                        r.status = "healthy"
                    else:
                        continue

                current = len(self._inflight_by_resource.get(r.resource_id, set()))
                if r.max_in_flight is not None and current >= r.max_in_flight:
                    continue

                candidates.append(r)

            if not candidates:
                return None

            if self._strategy == "primary_backup":
                # Candidates were appended in original resource-dict insertion order,
                # so the first one is the highest-priority eligible resource.
                selected = candidates[0]
            else:
                selected = min(
                    candidates,
                    key=lambda r: (
                        len(self._inflight_by_resource.get(r.resource_id, set())),
                        r.last_acquired_at,
                    ),
                )
            selected.last_acquired_at = now
            usage = Usage(
                usage_id=str(uuid.uuid4()),
                request_id=request_id,
                resource_id=selected.resource_id,
                acquired_at=now,
            )
            self._usages[usage.usage_id] = usage
            self._inflight_by_resource.setdefault(selected.resource_id, set()).add(
                usage.usage_id
            )
            return selected, usage

    async def _on_ok(self, usage: Usage) -> None:
        """Resource is operational. Reset cooldown state.

        Called whenever the user operation returns normally OR raises a non-resource
        exception -- anything that proves the resource itself works, regardless of
        business outcome.

        Only resets cooldown state when the resource is currently healthy. If a
        concurrent failure has since moved it to cooling_down or disabled, that more
        recent signal wins -- e.g. an older usage that started before a 429 succeeds
        after a younger sibling triggered the cooldown; its success does not prove
        the rate limit lifted, so we leave the cooldown in place.
        """
        async with self._lock:
            usage.status = "done"
            resource = self._resources.get(usage.resource_id)
            if resource is not None and resource.status == "healthy":
                resource.cooldown_until = 0.0
                resource.consecutive_cooldown = 0

    async def _on_cooldown(
        self, usage: Usage, cooldown_seconds: float | None = None
    ) -> None:
        """Resource is temporarily over capacity. Mark cooling_down and cancel younger
        usages on the same resource so they can retry elsewhere.

        cooldown_seconds: explicit duration (e.g. from a Retry-After header). If None,
            falls back to this pool's cooldown_table.
        """
        now = time.monotonic()
        to_cancel: list[Usage] = []

        async with self._lock:
            usage.status = "done"
            resource = self._resources.get(usage.resource_id)
            if resource is None or resource.status == "disabled":
                return

            resource.consecutive_cooldown += 1

            if cooldown_seconds is not None:
                cd = cooldown_seconds
            else:
                idx = max(resource.consecutive_cooldown - 1, 0)
                idx = min(idx, len(self._cooldown_table) - 1)
                cd = self._cooldown_table[idx]

            resource.status = "cooling_down"
            resource.cooldown_until = max(resource.cooldown_until, now + cd)

            to_cancel = self._collect_younger_usages_locked(usage)

        self._cancel_tasks(to_cancel)

    async def _on_disable(self, usage: Usage) -> None:
        """Resource is permanently bad. Mark disabled and cancel younger usages on the
        same resource so they can retry elsewhere.

        The triggering usage itself is excluded from cancellation -- its own cleanup is
        handled by `run()`'s finally block.
        """
        to_cancel: list[Usage] = []

        async with self._lock:
            usage.status = "done"
            resource = self._resources.get(usage.resource_id)
            if resource is not None:
                resource.status = "disabled"

            to_cancel = self._collect_younger_usages_locked(usage)

        self._cancel_tasks(to_cancel)

    async def _cleanup_usage(self, usage: Usage) -> None:
        """Remove a usage from tracking. Implicitly decrements the derived in-flight
        count for the resource. Idempotent."""
        async with self._lock:
            ids = self._inflight_by_resource.get(usage.resource_id)
            if ids is not None:
                ids.discard(usage.usage_id)
                if not ids:
                    self._inflight_by_resource.pop(usage.resource_id, None)

            self._usages.pop(usage.usage_id, None)

    def _collect_younger_usages_locked(self, failed_usage: Usage) -> list[Usage]:
        """Mark and return usages on the same resource with acquired_at > failed.

        MUST be called with `self._lock` held. Older usages are NOT touched -- they may
        still succeed (e.g. an upstream request that the remote side already accepted).
        The failed usage itself is also excluded.
        """
        to_cancel: list[Usage] = []
        ids = self._inflight_by_resource.get(failed_usage.resource_id, set())
        for usage_id in ids:
            other = self._usages.get(usage_id)
            if other is None:
                continue
            if (
                other.status == "in_flight"
                and other.acquired_at > failed_usage.acquired_at
                and other.usage_id != failed_usage.usage_id
            ):
                other.status = "cancelled"
                to_cancel.append(other)
        return to_cancel

    @staticmethod
    async def _sleep_before_retry(retry_delay: float, deadline: float | None) -> None:
        """Pause between attempts without sleeping past the deadline.

        The pause is jittered to ``retry_delay * uniform(0.5, 1.5)`` so concurrent
        run() calls that failed on the same resource at the same moment do not retry
        in lockstep and stampede the next eligible resource. The mean stays
        ``retry_delay``; zero stays zero.

        The deadline gates when the next attempt may start; it never interrupts an
        in-flight operation. Capping the pause here keeps run() from blocking past the
        deadline while merely waiting to retry.
        """
        delay = retry_delay * random.uniform(0.5, 1.5)
        if deadline is not None:
            delay = min(delay, max(deadline - time.monotonic(), 0.0))
        await asyncio.sleep(delay)

    @staticmethod
    def _cancel_tasks(usages: list[Usage]) -> None:
        # Cancel outside the lock -- task.cancel() can trigger callbacks that try to
        # reacquire it.
        for u in usages:
            if u.task is not None:
                u.task.cancel()

    @classmethod
    def __agent_notes__(cls) -> str:
        return """\
### Use case

Wrap N interchangeable backends (API keys, replicas, accounts) so each request
transparently fails over on rate limits, transient errors, or hard breakage.

### Example

```python
import asyncio
from rotapool import Pool, Resource, CooldownResource, DisableResource

pool = Pool([
    Resource(resource_id="key-a", value="sk-aaa", max_in_flight=4),
    Resource(resource_id="key-b", value="sk-bbb", max_in_flight=4),
])

async def call(resource):
    try:
        return await some_api(resource.value)
    except RateLimited as e:
        raise CooldownResource(cooldown_seconds=e.retry_after)
    except AuthFailed:
        raise DisableResource()

result = asyncio.run(pool.run(call))

# Decorator form -- `resource` is injected as the first arg:
@pool.use()
async def fetch(resource, url): ...
```

### Strategy: primary_backup

Default strategy is ``"round_robin"`` (fairness across resources). Pass
``strategy="primary_backup"`` to instead exhaust earlier resources before
touching later ones -- list/dict order becomes the priority ranking.

```python
# Use the paid key first; only fall back to free when the paid key is
# rate-limited (cooling_down), revoked (disabled), or at max_in_flight.
pool = Pool(
    resources=[
        Resource(resource_id="paid",  value="sk-paid-...",  max_in_flight=8),
        Resource(resource_id="free",  value="sk-free-..."),
    ],
    strategy="primary_backup",
)
```

### Anti-pattern: doing the real work OUTSIDE ``run()``

The pool only sees what happens INSIDE the operation. Returning a client /
handle and using it after ``run()`` returns means every later failure is
invisible -- the attempt is already recorded as success and the cooldown
counter was reset.

WRONG:
```python
client = await pool.run(lambda r: build_client(r.value))
response = await client.get("/things")  # invisible to pool
```

RIGHT:
```python
async def fetch(resource):
    client = build_client(resource.value)
    try:
        return await client.get("/things")
    except RateLimited as e:
        raise CooldownResource(cooldown_seconds=e.retry_after)

response = await pool.run(fetch)
```

Return only plain values (bytes, dict, dataclass). For N backend calls, make
N ``run()`` invocations.

### Don't

- Raise ``CooldownResource`` for business errors (404, validation) -- the
  next resource returns the same error and burns the budget for nothing.
- Catch and swallow exceptions inside the operation -- the pool needs to see
  them to decide resource health.
- Mutate ``Resource`` fields from outside; the pool owns lifecycle state.
- Share one ``Pool`` across asyncio event loops -- the lock binds to the loop
  where it was first awaited.

### Gotcha

Cooldown/disable cancels YOUNGER in-flight usages on the same resource and
retries them elsewhere; OLDER usages run to completion (they may already
have side effects upstream). ``asyncio.CancelledError`` from sibling
cancellation is swallowed and retried; only OUTER caller cancellation
propagates.
"""
