from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import math
import random
import time
import uuid
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
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


from .exceptions import (
    CooldownResource,
    DisableResource,
    PoolExhausted,
    RetryOperation,
)
from .models import (
    RESOURCE_STATUSES,
    PoolStats,
    Resource,
    ResourceStats,
    ResourceStatus,
    RunOutcome,
    Usage,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")

_DEFAULT_COOLDOWN_TABLE: tuple[float, ...] = (30.0, 120.0, 300.0, 600.0)

Strategy = Literal["round_robin", "primary_backup"]


@dataclass
class _ResourceCounters:
    """Per-resource monotonic counters. Lives on the pool, not on Resource.

    Two pools may share Resource objects; mixing counters there would be wrong.
    Membership-scoped: dropped on remove(), zeroed on a later add() of the same id.
    """

    acquires: int = 0
    successes: int = 0
    cooldowns: int = 0
    disables: int = 0
    sibling_cancels: int = 0


@dataclass
class _PoolCounters:
    """Process-lifetime pool counters. Survive add()/remove(); never decrease."""

    attempts: int = 0
    successes: int = 0
    cooldowns: int = 0
    disables: int = 0
    sibling_cancels: int = 0
    retries: int = 0
    runs_ok: int = 0
    runs_exhausted: int = 0
    runs_error: int = 0
    runs_cancelled: int = 0


class Pool(AgentReadableMixin, Generic[T]):
    """A pool of interchangeable resources sharing the same usage policy.

    Selection excludes cooling-down and disabled resources, then applies the chosen
    ``strategy`` ("round_robin" or "primary_backup") among the remaining candidates.
    """

    def __init__(
        self,
        resources: list[Resource[T]] | dict[str, Resource[T]],
        max_attempts: int = 3,
        cooldown_table: tuple[float, ...] = _DEFAULT_COOLDOWN_TABLE,
        strategy: Strategy = "round_robin",
        on_state_change: Callable[..., None] | None = None,
        cancel_siblings: bool = True,
        probe_on_recovery: bool = False,
    ) -> None:
        """Construct a pool over a set of interchangeable resources.

        resources: the resources this pool manages. Accepts either a list of
            ``Resource`` objects (their ``resource_id`` fields must be unique) or a
            ``dict`` keyed by resource id. Iteration order is preserved and is
            **load-bearing under the "primary_backup" strategy** -- earlier entries
            are higher-priority. Must contain at least one entry.

        max_attempts: default total retry budget per ``run()`` call (not per resource).
            Each attempt selects a resource via the pool's selection rules; a resource
            that triggered cooldown or disable on one attempt is ineligible on the
            next while that state lasts (a zero-second cooldown can make it eligible
            again immediately, in which case it may be re-selected). In fail-fast
            mode the effective cap is ``min(max_attempts, len(resources))``. With
            ``wait_for_cooldown=True`` the cap is ``max_attempts`` only, so a
            single-resource pool can wait out its own cooldown. Overridable per
            call via ``run(..., max_attempts=...)``.

        cooldown_table: cooldown durations (seconds) indexed by ``consecutive_cooldown``
            count on a resource. Each consecutive ``CooldownResource`` from the same
            resource escalates one slot; the counter resets on the next success.
            Counts past the table length clamp to the last entry. Per-event
            ``CooldownResource(cooldown_seconds=...)`` (e.g. from ``Retry-After``)
            overrides this for that one event without resetting the counter. Entries
            must be finite and >= 0.

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

        on_state_change: optional monitoring hook invoked as
            ``on_state_change(resource_id, old_status, new_status)`` the moment
            a resource's health status changes, so operators can log or alert
            without polling ``snapshot()``. Delivery rules:

            - Fired for: an operation raising ``CooldownResource`` (healthy ->
              cooling_down, and cooling_down -> cooling_down when an escalation
              or extension lands while already cooling -- the status alone
              cannot express magnitude, so every cooldown event is delivered),
              ``DisableResource`` or admin ``disable()`` (any -> disabled),
              admin ``enable()`` (any -> healthy), and lazy cooldown expiry at
              selection time (cooling_down -> healthy).
            - Not fired for: ``add()`` / ``remove()`` (membership, not a status
              transition), cooldown-state resets on success, or no-op admin
              calls (``enable()`` on an already-healthy resource).

            The hook is invoked after the pool lock is released, as
            ``on_state_change(resource_id, old_status, new_status)`` or
            ``(..., seq)`` with a pool-level monotonic ``seq``. A 3-parameter
            hook still works and emits ``DeprecationWarning``. Keep it fast and
            never call the pool's ``async`` methods (``snapshot()`` and
            ``stats()`` are safe). An exception is logged to the ``rotapool``
            logger and swallowed. Do not use it as a metrics bus.

        cancel_siblings: when True (default), a cooldown or disable *signal*
            cancels strictly-younger in-flight usages on the same resource so
            they can retry elsewhere. When False, those younger usages run to
            completion and deliver their own result to their own caller.
            Administrative ``disable()`` never cancels in-flight work, regardless
            of this flag. Sibling cancellation is at-least-once: the cancelled
            operation may already have reached the backend, so non-idempotent
            operations MUST set this to False.

        probe_on_recovery: when True, a resource whose cooldown has just expired
            is admitted with an effective ``max_in_flight`` of 1 until a success
            (half-open probe). A failed probe re-enters cooldown and advances
            escalation. Default False: expiry restores the configured cap for
            every caller at once.
        """
        # resource_id -> resource
        self._resources: dict[str, Resource[T]] = self._build_resources(resources)
        if not self._resources:
            raise ValueError("Pool requires at least one resource")

        # `not >= 1` instead of `< 1`: also rejects NaN, which would otherwise
        # pass and surface later as `range(nan)` TypeError inside run().
        if not max_attempts >= 1:  # noqa: S1940
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        self._max_attempts: int = max_attempts

        if not cooldown_table:
            raise ValueError("cooldown_table must contain at least one entry")
        if any(not math.isfinite(cd) or cd < 0 for cd in cooldown_table):
            raise ValueError("cooldown_table entries must be finite and >= 0")
        self._cooldown_table: tuple[float, ...] = cooldown_table

        # Runtime guard for callers that bypass type checking. Cast widens the
        # Literal so pyright doesn't flag the comparison as unreachable.
        if cast(str, strategy) not in ("round_robin", "primary_backup"):
            raise ValueError(
                f"strategy must be 'round_robin' or 'primary_backup', got {strategy!r}"
            )
        self._strategy: Strategy = strategy

        if on_state_change is not None and not callable(on_state_change):
            raise TypeError(
                "on_state_change must be callable or None, got "
                f"{type(on_state_change).__name__}"
            )
        self._on_state_change = on_state_change
        self._hook_nparams: int | None = None
        self._event_seq: int = 0
        self._pending_events: list[tuple[str, ResourceStatus, ResourceStatus, int]] = []

        if type(cancel_siblings) is not bool:
            raise TypeError(
                f"cancel_siblings must be bool, got {type(cancel_siblings).__name__}"
            )
        self._cancel_siblings: bool = cancel_siblings

        if type(probe_on_recovery) is not bool:
            raise TypeError(
                "probe_on_recovery must be bool, got "
                f"{type(probe_on_recovery).__name__}"
            )
        self._probe_on_recovery: bool = probe_on_recovery
        self._probing: set[str] = set()

        # Monotonic counters live on the pool, not on Resource (two pools may
        # share Resource objects). Resource counters are membership-scoped;
        # pool counters survive remove().
        self._pool_counters = _PoolCounters()
        self._resource_counters: dict[str, _ResourceCounters] = {
            rid: _ResourceCounters() for rid in self._resources
        }

        # Guards all possibly racing states.
        self._lock: asyncio.Lock = asyncio.Lock()

        # Wakes wait_for_cooldown sleepers when admin enable()/disable() changes
        # eligibility, so they re-evaluate instead of sleeping out a stale plan.
        # Shares self._lock, so holding the lock and holding the condition are
        # the same thing.
        self._admin_changed: asyncio.Condition = asyncio.Condition(lock=self._lock)

        # usage_id -> Usage
        self._usages: dict[str, Usage] = {}

        # resource_id -> { usage_id_set }
        self._inflight_by_resource: dict[str, set[str]] = {}

        # Monotonic per-pool sequence used to order usages exactly. Wall-clock and
        # monotonic timestamps can tie on fast acquisitions; cancellation semantics
        # need a deterministic "younger than" relation.
        self._next_acquisition_order: int = 0

        self._wait_pulse: asyncio.Event = asyncio.Event()

        # Test-only knobs: the conformance virtual clock sets these to its
        # now() / sleep(). Left as None in production, and the fallbacks below
        # call time.monotonic / asyncio.sleep by reference so a test that
        # monkeypatches asyncio.sleep still reaches the real thing.
        self._now: Callable[[], float] | None = None
        self._sleep: Callable[[float], Awaitable[None]] | None = None

    def _timestamp(self) -> float:
        return self._now() if self._now is not None else time.monotonic()

    async def _sleep_for(self, delay: float) -> None:
        if self._sleep is not None:
            await self._sleep(delay)
        else:
            await asyncio.sleep(delay)

    async def run(
        self,
        operation: Callable[[Resource[T]], Awaitable[R]],
        *,
        max_attempts: int | None = None,
        deadline: float | None = None,
        retry_delay: float = 0.5,
        wait_for_cooldown: bool = False,
        request_id: str | None = None,
    ) -> R:
        """Drive the retry loop for one logical request.

        operation: callable receiving the selected resource and returning an Awaitable.
            May raise CooldownResource, DisableResource, or RetryOperation to
            signal resource health. Any other exception is treated as resource OK
            and propagates to the caller (so user-side bugs do not poison the pool).

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
            Fail-fast uses ``min(max_attempts, len(resources))``; with
            ``wait_for_cooldown=True`` the budget is ``max_attempts`` only.

        deadline: absolute time.monotonic() value that gates when each attempt may
            start. It is checked before every attempt and caps both the inter-attempt
            retry pause and the opt-in ``wait_for_cooldown`` sleep, so run() will
            neither begin new work nor keep sleeping past it. It
            does NOT interrupt an operation already in flight -- a single call that runs
            long can overrun the deadline, because the pool never cancels a usage that
            may already have upstream side effects. None disables the deadline.
            Non-finite values (NaN, +-inf) are rejected up front: comparisons
            against them never fire, which would silently disable the deadline.

        retry_delay: base pause between failed attempts to let cooling resources
            recover and to avoid hammering the pool. Must be >= 0. The actual pause
            is jittered to ``retry_delay * uniform(0.5, 1.5)`` (mean stays
            ``retry_delay``) so concurrent callers do not retry in lockstep and
            stampede the next eligible resource.

        wait_for_cooldown: when no resource is eligible at the start of an attempt,
            sleep until the earliest ``cooldown_until`` among cooling resources and
            select again, instead of raising ``PoolExhausted`` immediately. Only a
            cooldown gives a known wake-up time, so this never waits on resources
            that are disabled or at ``max_in_flight`` -- if nothing is cooling,
            ``PoolExhausted`` is raised as usual. With a ``deadline``, the wait is
            attempted only when the earliest cooldown ends before it; otherwise
            ``PoolExhausted`` is raised immediately rather than sleeping out a wait
            that provably cannot help. The wake-up is jittered by an extra
            ``retry_delay * uniform(0, 1)`` (capped by ``deadline``) so concurrent
            waiters do not stampede the recovered resource at the exact expiry
            instant. Admin ``enable()`` / ``disable()`` interrupt the wait so the
            sleeper re-evaluates immediately. Defaults to False (fail fast).

        request_id: opaque string attached to every `Usage` created by this call.
            Useful for correlating logs, metrics, or tracing back to the original
            caller (e.g. an HTTP request-id header). Auto-generated UUID when None.
        """
        rid = request_id or str(uuid.uuid4())
        # `not >=` instead of `<`: also rejects NaN, which would otherwise pass
        # and surface far from the bug -- retry_delay as a mid-retry ValueError
        # out of asyncio.sleep, max_attempts as `range(nan)` TypeError.
        if max_attempts is not None and not max_attempts >= 1:  # noqa: S1940
            raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")
        if not retry_delay >= 0:  # noqa: S1940
            raise ValueError(f"retry_delay must be >= 0, got {retry_delay}")
        if deadline is not None and not math.isfinite(deadline):
            raise ValueError(
                f"deadline must be a finite time.monotonic() value, got {deadline}"
            )
        cap = max_attempts if max_attempts is not None else self._max_attempts
        outcome: RunOutcome | None = None
        try:
            result = await self._run_attempts(
                operation, rid, cap, deadline, retry_delay, wait_for_cooldown
            )
        except PoolExhausted:
            outcome = "exhausted"
            raise
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception:
            outcome = "error"
            raise
        else:
            outcome = "ok"
            return result
        finally:
            if outcome is not None:
                self._record_run_outcome(outcome)

    async def _run_attempts(
        self,
        operation: Callable[[Resource[T]], Awaitable[R]],
        rid: str,
        cap: int,
        deadline: float | None,
        retry_delay: float,
        wait_for_cooldown: bool,
    ) -> R:
        """Drive the retry loop. ``run()`` wraps this to record the outcome."""
        if wait_for_cooldown:
            effective_attempts = cap
        else:
            effective_attempts = min(cap, len(self._resources))
        if effective_attempts < 1:
            # Only reachable via remove() emptying the pool (construction
            # requires >= 1 resource). Report the real cause instead of falling
            # through to "max_attempts=0 exhausted: None".
            raise PoolExhausted("no eligible resource in pool")
        last_error: BaseException | None = None

        for attempt_num in range(effective_attempts):
            if deadline is not None and self._timestamp() >= deadline:
                raise PoolExhausted(f"deadline exceeded after {attempt_num} attempt(s)")

            acquired = await self._acquire(rid)
            if acquired is None and wait_for_cooldown:
                acquired = await self._acquire_after_cooldown(
                    rid, retry_delay, deadline
                )
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

            except RetryOperation as e:
                self._pool_counters.retries += 1
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
                # (no asyncio.Task.cancelling() dependency); the trade-off is that an
                # external cancel landing in the same tick as an internal one is
                # classified internal and absorbed for that attempt -- 3.11+
                # Task.cancelling() could disambiguate. Cleanup runs in finally.
                cancelled_internally = (
                    usage.status == "cancelled" and usage.task is not None
                )
                usage.status = "cancelled"
                if not cancelled_internally:
                    raise
                # SPEC CANCEL-03: exhausted last-error is the health signal
                # that cancelled us, not a raw CancelledError.
                res = self._resources.get(usage.resource_id)
                if res is not None and res.status == "disabled":
                    last_error = DisableResource(
                        reason="cancelled by a sibling usage failure"
                    )
                else:
                    last_error = CooldownResource(
                        reason="cancelled by a sibling usage failure"
                    )
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

    def _record_run_outcome(self, outcome: RunOutcome) -> None:
        """Increment the matching pool-level run counter. GIL-atomic, no lock."""
        c = self._pool_counters
        if outcome == "ok":
            c.runs_ok += 1
        elif outcome == "exhausted":
            c.runs_exhausted += 1
        elif outcome == "error":
            c.runs_error += 1
        else:
            c.runs_cancelled += 1

    def use(
        self,
        *,
        max_attempts: int | None = None,
        deadline: float | None = None,
        retry_delay: float = 0.5,
        wait_for_cooldown: bool = False,
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
                    wait_for_cooldown=wait_for_cooldown,
                )

            return wrapper

        return decorator

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return a point-in-time summary of every resource in the pool.

        Thread-safe without the lock -- iterates a copy of the resource dict
        (``add()`` can grow it from the event loop while another thread polls)
        and reads simple types (str, int, float) that change atomically under
        the GIL. Operator / JSON view: includes ``last_acquired_at`` (a
        ``time.monotonic()`` reading, not a scrape timestamp). For counters
        and Prometheus-ready gauges, use :meth:`stats`.
        """
        now = self._timestamp()
        result: dict[str, dict[str, Any]] = {}
        for rid, r in list(self._resources.items()):  # noqa: S7504
            status, inflight, cooldown_remaining = self._resource_gauges(r, now)
            result[rid] = {
                "status": status,
                "in_flight": inflight,
                "max_in_flight": r.max_in_flight,
                "consecutive_cooldown": r.consecutive_cooldown,
                "cooldown_seconds_remaining": cooldown_remaining,
                "last_acquired_at": r.last_acquired_at,
            }
        return result

    def stats(self) -> PoolStats:
        """Return Prometheus-ready gauges and monotonic counters.

        Lock-free and thread-safe, same contract as :meth:`snapshot`. Gauges
        describe current membership (expired cooldowns report as healthy).
        Pool-level counters are process-lifetime and survive ``remove()``;
        per-resource counters are membership-scoped and start at 0 on
        ``add()``. Does not include ``last_acquired_at`` -- that value is
        process-monotonic and is not a scrape timestamp.
        """
        now = self._timestamp()
        resources: dict[str, ResourceStats] = {}
        in_flight = 0
        eligible = 0
        saturated = 0
        by_status: dict[ResourceStatus, int] = {s: 0 for s in RESOURCE_STATUSES}  # noqa: S7519
        empty = _ResourceCounters()

        for rid, r in list(self._resources.items()):  # noqa: S7504
            status, inflight, cooldown_remaining = self._resource_gauges(r, now)
            c = self._resource_counters.get(rid, empty)
            rs = ResourceStats(
                resource_id=rid,
                status=status,
                in_flight=inflight,
                max_in_flight=r.max_in_flight,
                consecutive_cooldown=r.consecutive_cooldown,
                cooldown_seconds_remaining=cooldown_remaining,
                acquires=c.acquires,
                successes=c.successes,
                cooldowns=c.cooldowns,
                disables=c.disables,
                sibling_cancels=c.sibling_cancels,
            )
            resources[rid] = rs
            in_flight += inflight
            by_status[status] += 1
            cap = self._effective_max_in_flight(r)
            if status == "healthy" and (cap is None or inflight < cap):
                eligible += 1
            if status == "healthy" and cap is not None and inflight >= cap:
                saturated += 1

        pc = self._pool_counters
        return PoolStats(
            resources=resources,
            in_flight=in_flight,
            eligible=eligible,
            saturated=saturated,
            by_status=by_status,
            attempts=pc.attempts,
            successes=pc.successes,
            cooldowns=pc.cooldowns,
            disables=pc.disables,
            sibling_cancels=pc.sibling_cancels,
            retries=pc.retries,
            runs_ok=pc.runs_ok,
            runs_exhausted=pc.runs_exhausted,
            runs_error=pc.runs_error,
            runs_cancelled=pc.runs_cancelled,
        )

    def _resource_gauges(
        self, r: Resource[T], now: float
    ) -> tuple[ResourceStatus, int, float]:
        """Effective status, in-flight count, and cooldown remaining.

        The stored status flips to ``healthy`` lazily inside ``_acquire``, so an
        expired cooldown can linger as ``cooling_down`` on an idle pool. Report
        the effective status without mutating state (callers are lock-free).
        """
        inflight = len(self._inflight_by_resource.get(r.resource_id, set()))
        status = r.status
        if status == "cooling_down" and r.cooldown_until <= now:
            status = "healthy"
        cooldown_remaining = (
            max(r.cooldown_until - now, 0.0) if status == "cooling_down" else 0.0
        )
        return status, inflight, cooldown_remaining

    def _effective_max_in_flight(self, r: Resource[T]) -> int | None:
        if r.resource_id in self._probing:
            configured = r.max_in_flight
            return 1 if configured is None else min(configured, 1)
        return r.max_in_flight

    async def add(
        self,
        resource_id: str | Resource[T],
        value: T | None = None,
        *,
        max_in_flight: int | None = None,
    ) -> Resource[T]:
        """Add a new healthy resource to the pool.

        Two call shapes:

        - ``add(resource_id, value, max_in_flight=None)`` -- constructs a
          ``Resource`` from parts (0.4.0 form).
        - ``add(Resource(...))`` -- same lifecycle defaults; ``value`` must
          be omitted.

        Lifecycle state is always initialized from the ``Resource`` defaults:
        healthy, no cooldown, no prior acquisition, and no consecutive cooldown
        count. The new resource is appended to the pool's insertion order, so it
        is the lowest-priority fallback under ``"primary_backup"``.

        Wakes any ``run(wait_for_cooldown=True)`` sleepers so a newly added healthy
        resource can satisfy them immediately.

        Raises ValueError for a duplicate ``resource_id`` or invalid ``Resource``
        constructor arguments. Raises TypeError if both a ``Resource`` and
        ``value`` are passed, or if ``value`` is omitted for the triple form.
        """
        if isinstance(resource_id, Resource):
            if value is not None:
                raise TypeError("do not pass value when adding a Resource")
            src = resource_id
            cap = max_in_flight if max_in_flight is not None else src.max_in_flight
            resource = Resource(
                resource_id=src.resource_id,
                value=src.value,
                max_in_flight=cap,
            )
        else:
            if value is None:
                raise TypeError("value is required when resource_id is a string")
            resource = Resource(
                resource_id=resource_id,
                value=value,
                max_in_flight=max_in_flight,
            )
        async with self._admin_changed:
            if resource.resource_id in self._resources:
                raise ValueError(
                    f"Duplicate resource_id in pool: {resource.resource_id!r}"
                )
            # Counters first so a lock-free stats() that sees the new resource
            # never observes a missing counter dict entry.
            self._resource_counters[resource.resource_id] = _ResourceCounters()
            self._resources[resource.resource_id] = resource
            self._admin_changed.notify_all()
            self._wait_pulse.set()
        return resource

    async def enable(self, resource_id: str) -> None:
        """Administratively return a resource to selection.

        Clears both the disabled state and any active cooldown -- enable means "the
        operator says this resource is usable now" (e.g. a rotated key). Also resets
        ``consecutive_cooldown`` to 0, so if the operator is wrong the escalation
        restarts from the first ``cooldown_table`` slot instead of resuming where it
        left off. Wakes any ``run(wait_for_cooldown=True)`` sleepers so they can
        acquire the resource immediately. Idempotent on an already-healthy resource.

        Raises KeyError for an unknown resource_id.
        """
        async with self._admin_changed:
            resource = self._get_resource(resource_id)
            # Clear cooldown state before the flip so the on_state_change
            # callback (if any) observes the fully-recovered resource.
            resource.cooldown_until = 0.0
            resource.consecutive_cooldown = 0
            self._probing.discard(resource.resource_id)
            self._set_resource_status_locked(resource, "healthy")
            self._admin_changed.notify_all()
            self._wait_pulse.set()
        self._flush_state_change_events()

    async def disable(self, resource_id: str) -> None:
        """Administratively remove a resource from selection until ``enable()``.

        Unlike an operation raising ``DisableResource``, in-flight usages on the
        resource are NOT cancelled: admin disable is policy, not failure evidence,
        so running work (which may already have upstream side effects) finishes
        naturally. Wakes any ``run(wait_for_cooldown=True)`` sleepers so they can
        re-evaluate (and fail fast) instead of sleeping out a cooldown that no
        longer matters. Idempotent on an already-disabled resource.

        Raises KeyError for an unknown resource_id.
        """
        async with self._admin_changed:
            resource = self._get_resource(resource_id)
            self._set_resource_status_locked(resource, "disabled")
            self._admin_changed.notify_all()
            self._wait_pulse.set()
        self._flush_state_change_events()

    async def remove(self, resource_id: str) -> None:
        """Drop a resource from the pool entirely -- the counterpart to ``add()``.

        The resource disappears from selection, ``snapshot()``, and
        ``stats().resources`` immediately, and the pool stops referencing its
        ``value`` -- unlike ``disable()``, which keeps the (often secret) value
        in memory. In-flight usages
        acquired before removal finish naturally, exactly like admin
        ``disable()``: removal is policy, not failure evidence, so running work
        that may already have upstream side effects is never cancelled. Their
        bookkeeping drains on completion; a late ``CooldownResource`` from such
        a usage updates nothing (the resource is gone), while a
        ``DisableResource`` still cancels younger sibling usages on it -- they
        are talking to the same dead backend. Does not fire
        ``on_state_change``: removal is a membership change, not a status
        transition.

        Wakes any ``run(wait_for_cooldown=True)`` sleepers so they re-evaluate
        -- a cooldown they were waiting on may have belonged to the removed
        resource, and sleeping it out would provably not help.

        Raises KeyError for an unknown resource_id. Re-adding the same
        ``resource_id`` later via ``add()`` starts from fresh default state,
        with one overlap caveat: usages still draining from the removed
        resource count toward the re-added one's in-flight total (and hence
        its ``max_in_flight``) until they finish, so capacity is enforced
        conservatively during the overlap.
        """
        async with self._admin_changed:
            self._get_resource(resource_id)
            # Drop the resource first so stats() membership is the source of
            # truth; a racy stats() may briefly see zeros for a still-listed
            # id, never a ghost series for a removed one.
            del self._resources[resource_id]
            self._resource_counters.pop(resource_id, None)
            self._probing.discard(resource_id)
            self._admin_changed.notify_all()
            self._wait_pulse.set()

    def _get_resource(self, resource_id: str) -> Resource[T]:
        resource = self._resources.get(resource_id)
        if resource is None:
            raise KeyError(f"unknown resource_id: {resource_id!r}")
        return resource

    def _set_resource_status_locked(
        self, resource: Resource[T], new_status: ResourceStatus
    ) -> None:
        """Flip ``resource.status`` and fire ``on_state_change`` on a real change.

        MUST be called with the pool lock held. A no-op (and no event) when the
        status is unchanged -- e.g. a second ``DisableResource`` from a usage on
        an already-disabled resource, or an idempotent admin call.
        """
        old_status = resource.status
        if old_status == new_status:
            return
        resource.status = new_status
        if new_status == "disabled":
            self._pool_counters.disables += 1
            rc = self._resource_counters.get(resource.resource_id)
            if rc is not None:
                rc.disables += 1
        self._queue_state_change_locked(resource.resource_id, old_status, new_status)

    def _queue_state_change_locked(
        self, resource_id: str, old_status: ResourceStatus, new_status: ResourceStatus
    ) -> None:
        """Queue a hook event. MUST hold the pool lock; flush after release."""
        self._event_seq += 1
        self._pending_events.append(
            (resource_id, old_status, new_status, self._event_seq)
        )

    def _flush_state_change_events(self) -> None:
        """Dispatch queued events after the pool lock is released."""
        events = self._pending_events
        self._pending_events = []
        if not events or self._on_state_change is None:
            return
        if self._hook_nparams is None:
            try:
                self._hook_nparams = len(
                    inspect.signature(self._on_state_change).parameters
                )
            except (TypeError, ValueError):
                self._hook_nparams = 3
        nparams = self._hook_nparams
        if nparams < 4 and not getattr(self, "_hook_sig_warned", False):
            self._hook_sig_warned = True
            warnings.warn(
                "on_state_change(resource_id, old, new) is deprecated; "
                "accept a fourth seq argument",
                DeprecationWarning,
                stacklevel=2,
            )
        for resource_id, old_status, new_status, seq in events:
            try:
                if nparams >= 4:
                    self._on_state_change(resource_id, old_status, new_status, seq)
                else:
                    self._on_state_change(resource_id, old_status, new_status)
            except Exception:
                logger.exception("on_state_change callback failed for %s", resource_id)

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
        now = self._timestamp()
        acquired: tuple[Resource[T], Usage] | None = None

        async with self._lock:
            candidates: list[Resource[T]] = []

            for r in self._resources.values():
                if r.status == "disabled":
                    continue

                if r.status == "cooling_down":
                    if r.cooldown_until <= now:
                        self._set_resource_status_locked(r, "healthy")
                        if self._probe_on_recovery:
                            self._probing.add(r.resource_id)
                    else:
                        continue

                current = len(self._inflight_by_resource.get(r.resource_id, set()))
                cap = self._effective_max_in_flight(r)
                if cap is not None and current >= cap:
                    continue

                candidates.append(r)

            if candidates:
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
                self._next_acquisition_order += 1
                self._pool_counters.attempts += 1
                rc = self._resource_counters.get(selected.resource_id)
                if rc is not None:
                    rc.acquires += 1
                usage = Usage(
                    usage_id=str(uuid.uuid4()),
                    request_id=request_id,
                    resource_id=selected.resource_id,
                    acquired_at=now,
                    acquisition_order=self._next_acquisition_order,
                )
                self._usages[usage.usage_id] = usage
                self._inflight_by_resource.setdefault(selected.resource_id, set()).add(
                    usage.usage_id
                )
                acquired = selected, usage
        self._flush_state_change_events()
        return acquired

    async def _acquire_after_cooldown(
        self, request_id: str, retry_delay: float, deadline: float | None
    ) -> tuple[Resource[T], Usage] | None:
        """Sleep until the earliest cooldown expiry, then retry `_acquire`.

        Loops because waking up does not guarantee eligibility: the expired resource
        may have been re-cooled by a concurrent failure (possibly extending its
        cooldown), or it may sit at `max_in_flight` while another resource is still
        cooling. Each iteration either acquires, sleeps toward a cooldown expiry, or
        gives up:

        - returns None when nothing is cooling -- only a cooldown gives a known
          wake-up time, so disabled or saturated resources are never waited on;
        - raises PoolExhausted when the earliest expiry is at or past `deadline`,
          since sleeping out the deadline provably cannot help.

        The wake-up is jittered to ``wake + retry_delay * uniform(0, 1)`` so
        concurrent waiters do not all fire at the exact cooldown expiry and stampede
        the recovered resource. The jitter is additive (waking early would just fail
        and loop), reuses ``retry_delay`` as the pause-granularity knob (0 disables
        it, like every other pause), and is capped by ``deadline``.

        The sleep is interruptible: admin ``enable()`` / ``disable()`` notify
        ``self._admin_changed``, so the waiter re-evaluates immediately instead of
        sleeping out a plan those calls just invalidated. A spurious wake-up (the
        admin change did not affect this waiter) is harmless -- the loop recomputes.
        """
        acquired: tuple[Resource[T], Usage] | None = None
        while acquired is None:
            async with self._admin_changed:
                wakes = [
                    r.cooldown_until
                    for r in self._resources.values()
                    if r.status == "cooling_down"
                ]
                if not wakes:
                    return None
                wake = min(wakes)
                if deadline is not None and wake >= deadline:
                    raise PoolExhausted(
                        f"earliest cooldown ends {wake - deadline:.3f}s after deadline"
                    )
                target = wake + retry_delay * random.uniform(0.0, 1.0)
                if deadline is not None:
                    target = min(target, deadline)
                delay = max(target - self._timestamp(), 0.0)
                self._wait_pulse.clear()
            if delay > 0:
                await self._race_wait_pulse(delay)
            acquired = await self._acquire(request_id)
        return acquired

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
            self._pool_counters.successes += 1
            rc = self._resource_counters.get(usage.resource_id)
            if rc is not None:
                rc.successes += 1
            resource = self._resources.get(usage.resource_id)
            if resource is not None and resource.status == "healthy":
                resource.cooldown_until = 0.0
                resource.consecutive_cooldown = 0
                self._probing.discard(usage.resource_id)

    async def _on_cooldown(
        self, usage: Usage, cooldown_seconds: float | None = None
    ) -> None:
        """Resource is temporarily over capacity. Mark cooling_down and cancel younger
        usages on the same resource so they can retry elsewhere.

        cooldown_seconds: explicit duration (e.g. from a Retry-After header). If None,
            falls back to this pool's cooldown_table.
        """
        now = self._timestamp()
        to_cancel: list[Usage] = []

        async with self._lock:
            usage.status = "done"
            resource = self._resources.get(usage.resource_id)
            if resource is None or resource.status == "disabled":
                pass
            else:
                old_status = resource.status
                resource.consecutive_cooldown += 1

                if cooldown_seconds is not None:
                    cd = cooldown_seconds
                else:
                    idx = max(resource.consecutive_cooldown - 1, 0)
                    idx = min(idx, len(self._cooldown_table) - 1)
                    cd = self._cooldown_table[idx]

                resource.status = "cooling_down"
                resource.cooldown_until = max(resource.cooldown_until, now + cd)
                self._probing.discard(resource.resource_id)

                # Delivered even when old_status is already "cooling_down": an
                # escalation or extension changes magnitude, not status, and the
                # hook is the only push-notification channel for it.
                self._queue_state_change_locked(
                    resource.resource_id, old_status, "cooling_down"
                )

                self._pool_counters.cooldowns += 1
                rc = self._resource_counters.get(resource.resource_id)
                if rc is not None:
                    rc.cooldowns += 1

                if self._cancel_siblings:
                    to_cancel = self._collect_younger_usages_locked(usage)
                    n_cancel = len(to_cancel)
                    if n_cancel:
                        self._pool_counters.sibling_cancels += n_cancel
                        if rc is not None:
                            rc.sibling_cancels += n_cancel
                self._admin_changed.notify_all()
                self._wait_pulse.set()

        self._flush_state_change_events()
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
                self._set_resource_status_locked(resource, "disabled")

            if self._cancel_siblings:
                to_cancel = self._collect_younger_usages_locked(usage)
            n_cancel = len(to_cancel)
            if n_cancel:
                self._pool_counters.sibling_cancels += n_cancel
                rc = self._resource_counters.get(usage.resource_id)
                if rc is not None:
                    rc.sibling_cancels += n_cancel

        self._flush_state_change_events()
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
        """Mark and return usages on the same resource acquired after failed.

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
                and other.acquisition_order > failed_usage.acquisition_order
                and other.usage_id != failed_usage.usage_id
            ):
                other.status = "cancelled"
                to_cancel.append(other)
        return to_cancel

    async def _race_wait_pulse(self, delay: float) -> None:
        """Wait for admin pulse or ``delay`` seconds, whichever first.

        Used instead of ``Condition.wait`` + ``wait_for`` so tests can inject
        ``_sleep`` (virtual time) without blocking on loop wall-clock timeouts.
        The two racers resolve one shared future rather than going through
        ``asyncio.wait``: that keeps the caller's resume line traceable for
        coverage on CPython 3.11.
        """
        loop = asyncio.get_running_loop()
        first_done: asyncio.Future[None] = loop.create_future()

        def _finish(_: object) -> None:
            if not first_done.done():
                first_done.set_result(None)

        sleep_t = asyncio.ensure_future(self._sleep_for(delay))
        pulse_t = asyncio.ensure_future(self._wait_pulse.wait())
        sleep_t.add_done_callback(_finish)
        pulse_t.add_done_callback(_finish)
        try:
            await first_done
        finally:
            for t in (sleep_t, pulse_t):
                if not t.done():
                    t.cancel()

    async def _sleep_before_retry(
        self, retry_delay: float, deadline: float | None
    ) -> None:
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
            delay = min(delay, max(deadline - self._timestamp(), 0.0))
        await self._sleep_for(delay)

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

### Exhaustion: fail fast vs wait

When no resource is eligible, ``run()`` raises ``PoolExhausted`` immediately --
even if a ``deadline`` would outlive the cooldowns. Pass ``wait_for_cooldown=True``
(on ``run()`` or ``use()``) to instead sleep until the earliest cooldown expiry
and select again -- useful for batch jobs that prefer waiting over failing. It
never waits on disabled or saturated resources (no known wake-up time), and with
a ``deadline`` it raises immediately when the earliest expiry lands at or after
it, rather than sleeping out a wait that cannot help. Admin ``enable()`` /
``disable()`` interrupt the wait so the sleeper re-evaluates immediately. Admin
``add()`` also wakes waiters because newly added healthy capacity may satisfy
them immediately, and ``remove()`` wakes them because a cooldown they were
waiting on may have belonged to the removed resource.

### Dynamic add

Use ``await pool.add(resource_id, value, max_in_flight=None)`` to add new
capacity at runtime. The pool constructs a fresh ``Resource`` with default
lifecycle state (healthy, no cooldown, no acquisition history, no cooldown
counter). Duplicate ``resource_id`` values raise ``ValueError``. Added resources
append to pool order, so they are the lowest-priority fallback under
``primary_backup`` unless earlier resources are unavailable.

``await pool.remove(resource_id)`` is the counterpart: the resource and its
value leave selection, ``snapshot()``, and ``stats().resources`` entirely
(``disable()`` keeps the value in memory -- remove is for rotated/revoked
secrets). In-flight usages drain naturally, like admin disable. Raises
``KeyError`` for unknown ids.

### Observability: snapshot, stats, on_state_change

Three surfaces -- pick the right one:

- ``snapshot()`` -- operator/JSON view. Includes process-monotonic
  ``last_acquired_at`` (not a scrape timestamp) and has **no counters**.
- ``stats()`` -- metrics view. Returns a ``PoolStats`` (do not construct one).
  Numeric gauges, ``status_one_hot()`` / ``max_in_flight_gauge`` (``+Inf`` if
  unbounded), plus monotonic counters. Pool-level counters survive
  ``remove()``; per-resource counters live on each ``ResourceStats`` and
  reset on re-``add()`` of the same id.
- ``on_state_change`` -- push hook for health flips, not a metrics bus.

```python
from rotapool import Pool, PoolStats  # PoolStats is the stats() return type

s = pool.stats()
s.by_status["healthy"]          # gauge, always all three status keys
s.eligible, s.saturated         # derived gauges
s.runs_ok, s.runs_exhausted, s.runs_error, s.runs_cancelled
s.resources["key-a"].acquires   # membership-scoped counter
s.resources["key-a"].status_one_hot()
```

Optional Prometheus extra (``pip install "rotapool[prometheus]"``) -- not a
core dependency. Import ``PoolCollector`` from ``rotapool.prometheus``,
**not** from ``rotapool``:

```python
from prometheus_client import REGISTRY
from rotapool.prometheus import PoolCollector
REGISTRY.register(PoolCollector(pool, pool_name="api_keys"))
```

One ``PoolCollector`` per registry (a second collides on metric names); pass
``pools={"a": p1, "b": p2}`` or ``add()`` for several pools. Frozen names
are in ``rotapool.prometheus``. For any other backend, scrape ``stats()``.

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
- Mutate ``Resource`` fields from outside; the pool owns lifecycle state. For
  administrative control use ``await pool.add(id, value)`` /
  ``await pool.enable(id)`` / ``await pool.disable(id)`` /
  ``await pool.remove(id)`` (enable also clears any cooldown and resets the
  escalation counter; disable never cancels in-flight work; remove drops the
  resource and its value entirely while in-flight work drains).
- Share one ``Pool`` across asyncio event loops -- the lock binds to the loop
  where it was first awaited.
- Scrape ``snapshot()`` for Prometheus / rates -- it has no counters and
  ``last_acquired_at`` is process-monotonic. Use ``stats()``.
- Construct ``PoolStats`` / ``ResourceStats`` -- call ``stats()``.
- Sum per-resource counters to rebuild pool totals -- that drops on
  ``remove()`` and breaks ``rate()``.
- Register two ``PoolCollector`` instances on one registry, or import
  ``PoolCollector`` from ``rotapool`` (it lives in ``rotapool.prometheus``).
- Increment counters from ``on_state_change`` -- it misses success,
  exhaustion, retries, and membership.

### Gotcha

Cooldown/disable cancels YOUNGER in-flight usages on the same resource and
retries them elsewhere unless ``cancel_siblings=False``; OLDER usages run to
completion (they may already have side effects upstream). The default is
at-least-once: cancelled work may already have reached the backend. ``asyncio.CancelledError`` from sibling
cancellation is swallowed and retried; only OUTER caller cancellation
propagates. Rare edge: an outer cancel landing in the same event-loop
tick as an internal sibling cancel is classified internal and absorbed
for that attempt (3.10-compat trade-off) -- cancel again to stop.
"""
