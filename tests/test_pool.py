"""Unit tests for rotapool.Pool, parametrized over the three operation shapes.

A single ``async def`` body expresses the work for every test; the ``ops`` fixture
wraps it into each shape the pool supports:

- ``coroutine`` -- the body coroutine is returned directly (pool wraps it in a Task,
  so younger-sibling cancellation works).
- ``awaitable`` -- the body is wrapped in a plain ``__await__`` object with no
  ``.cancel()`` handle (cancellation of this usage is a best-effort no-op).
- ``future`` -- the body is driven by a tracked background task that resolves an
  ``asyncio.Future`` (cancellable directly via ``.cancel()``).

Tests that do not exercise an operation (construction/strategy validation, the
non-awaitable TypeError, and the self-contained multi-shape ``use()`` test) are not
parametrized and run once.

E1 and E5 are the documented inversions: with no ``.cancel()`` handle the awaitable
shape cannot cancel an in-flight usage, so the would-be cancel target runs to natural
completion instead. Those two tests branch on ``ops.shape`` for both the operation
construction and the assertions.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    TypeVar,
    cast,
)

import pytest

from rotapool import CooldownResource, DisableResource, Pool, PoolExhausted, Resource

FAST_TABLE = (0.05, 0.10, 0.15, 0.20)

_T = TypeVar("_T")

# Strong-ref set so the body tasks driving futures are not GC'd before they resolve.
# `_spawn` registers a task here and removes it on completion.
_BG_TASKS: set[asyncio.Task[Any]] = set()


def _spawn(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


class _Aw(Generic[_T]):
    """Minimal awaitable wrapping a coroutine, without a `.cancel()` handle.

    The framework awaits this directly and has no way to cancel it -- by design,
    so the awaitable shape tests the best-effort branch of cancellation.
    """

    def __init__(self, coro: Coroutine[Any, Any, _T]) -> None:
        self._coro = coro

    def __await__(self) -> Generator[Any, Any, _T]:
        return self._coro.__await__()


def _coro_to_future(coro: Coroutine[Any, Any, _T]) -> asyncio.Future[_T]:
    """Drive `coro` in a background task that resolves a fresh Future.

    Decoupling the body from the Future mirrors real Future-returning APIs: when the
    framework cancels the Future via `.cancel()`, the body keeps running but its result
    is dropped (the `fut.done()` guards no-op on an already-cancelled Future).
    """
    fut: asyncio.Future[_T] = asyncio.get_event_loop().create_future()

    async def _drive() -> None:
        try:
            result = await coro
        except Exception as exc:
            if not fut.done():
                fut.set_exception(exc)
        else:
            if not fut.done():
                fut.set_result(result)

    _spawn(_drive())
    return fut


Operation = Callable[[Resource[str]], Awaitable[_T]]
OpBody = Callable[[Resource[str]], Coroutine[Any, Any, _T]]


class Ops:
    """Builds operations of one shape (coroutine / awaitable / future) from a body."""

    def __init__(self, shape: str) -> None:
        self.shape = shape

    def op(self, body: OpBody[_T]) -> Operation[_T]:
        """Wrap an ``async def`` body into the shape-appropriate operation callable."""
        if self.shape == "coroutine":

            def _coro_op(r: Resource[str]) -> Awaitable[_T]:
                return body(r)

            return _coro_op

        if self.shape == "awaitable":

            def _aw_op(r: Resource[str]) -> Awaitable[_T]:
                return _Aw(body(r))

            return _aw_op

        def _fut_op(r: Resource[str]) -> Awaitable[_T]:
            return _coro_to_future(body(r))

        return _fut_op

    def identity(self) -> Operation[str]:
        """Operation that returns ``resource.value``."""

        async def body(r: Resource[str]) -> str:  # NOSONAR
            return r.value

        return self.op(body)

    def raising(self, make_exc: Callable[[], BaseException]) -> Operation[Any]:
        """Operation that raises a fresh exception from ``make_exc`` each call."""

        async def body(_: Resource[str]) -> Any:
            raise make_exc()

        return self.op(body)

    def never(self) -> Operation[Any]:
        """Operation that blocks until cancelled (or the outer run() is cancelled)."""
        if self.shape == "future":

            def _bare_future(_: Resource[str]) -> Awaitable[Any]:
                return asyncio.get_event_loop().create_future()

            return _bare_future

        async def body(_: Resource[str]) -> Any:
            await asyncio.Event().wait()

        return self.op(body)


def _res(n: int, **kw: Any) -> list[Resource[str]]:
    return [Resource(resource_id=f"r{i}", value=f"v{i}", **kw) for i in range(n)]


@pytest.fixture(params=["coroutine", "awaitable", "future"])
def ops(request: pytest.FixtureRequest) -> Ops:
    return Ops(cast(str, request.param))


# ===================================================================
# Group A — Selection & fairness
# ===================================================================


class TestSelection:
    async def test_a1_round_robin_fairness_concurrent(self, ops: Ops) -> None:
        """6 concurrent ops on 3 healthy resources → each used exactly 2x."""
        pool = Pool(resources=_res(3), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()
        gate = asyncio.Event()

        async def body(r: Resource[str]) -> str:
            tally[r.resource_id] += 1
            await gate.wait()
            return r.resource_id

        op = pool.use()(ops.op(body))
        tasks = [asyncio.create_task(cast("Any", op())) for _ in range(6)]
        await asyncio.sleep(0.05)
        gate.set()
        results = await asyncio.gather(*tasks)

        assert sorted(results) == sorted(["r0", "r0", "r1", "r1", "r2", "r2"])
        assert dict(tally) == {"r0": 2, "r1": 2, "r2": 2}

    async def test_a2_prefers_fewest_inflight_then_oldest(self, ops: Ops) -> None:
        """Pick fewest in-flight; break ties by oldest last_acquired_at."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        hold = asyncio.Event()
        acquired: list[str] = []

        async def body(r: Resource[str]) -> str:
            acquired.append(r.resource_id)
            await hold.wait()
            return r.resource_id

        op = ops.op(body)
        t0 = asyncio.create_task(pool.run(op))
        t1 = asyncio.create_task(pool.run(op))
        await asyncio.sleep(0.05)
        assert set(acquired) == {"r0", "r1"}

        hold.set()
        await asyncio.gather(t0, t1)

        acquired.clear()

        async def quick(r: Resource[str]) -> str:  # NOSONAR
            acquired.append(r.resource_id)
            return r.resource_id

        result = await pool.run(ops.op(quick))
        assert result == "r0"

    async def test_a3_primary_backup_prefers_first_eligible(self, ops: Ops) -> None:
        """primary_backup: pick the first eligible resource in list order."""
        pool = Pool(
            resources=_res(3),
            cooldown_table=FAST_TABLE,
            strategy="primary_backup",
        )
        # All healthy: every call should land on r0.
        results = [await pool.run(ops.identity()) for _ in range(5)]
        assert results == ["v0", "v0", "v0", "v0", "v0"]

        # When r0 is at capacity, r1 is chosen; r2 stays untouched.
        cap_pool: Pool[str] = Pool(
            resources=[
                Resource(resource_id="r0", value="v0", max_in_flight=1),
                Resource(resource_id="r1", value="v1"),
                Resource(resource_id="r2", value="v2"),
            ],
            cooldown_table=FAST_TABLE,
            strategy="primary_backup",
        )
        hold = asyncio.Event()
        acquired: list[str] = []

        async def body(r: Resource[str]) -> str:
            acquired.append(r.resource_id)
            await hold.wait()
            return r.resource_id

        op = ops.op(body)
        t0 = asyncio.create_task(cap_pool.run(op))
        t1 = asyncio.create_task(cap_pool.run(op))
        await asyncio.sleep(0.05)
        assert acquired == ["r0", "r1"]
        hold.set()
        await asyncio.gather(t0, t1)

    def test_a4_invalid_strategy_raises(self) -> None:
        """Pool rejects unknown strategy strings at construction."""
        with pytest.raises(ValueError, match="strategy must be"):
            Pool(resources=_res(1), strategy="bogus")  # type: ignore[arg-type]


# ===================================================================
# Group B — Retry & transparent failover
# ===================================================================


class TestRetry:
    async def test_b1_transparent_failover(self, ops: Ops) -> None:
        """Cooldown on first two resources; third succeeds. Caller is unaware."""
        pool = Pool(resources=_res(3), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> str:  # NOSONAR
            tally[r.resource_id] += 1
            if r.resource_id in ("r0", "r1"):
                raise CooldownResource(reason="busy")
            return r.value

        result = await pool.run(ops.op(body))
        assert result == "v2"
        assert tally == {"r0": 1, "r1": 1, "r2": 1}

    async def test_b2_pool_exhausted_all_out(self, ops: Ops) -> None:
        """All resources unusable → PoolExhausted with last error info."""
        pool = Pool(resources=_res(3), max_attempts=3, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> Any:  # NOSONAR
            tally[r.resource_id] += 1
            if r.resource_id == "r0":
                raise DisableResource(reason="dead")
            raise CooldownResource(reason="busy")

        with pytest.raises(PoolExhausted, match="max_attempts=3 exhausted"):
            await pool.run(ops.op(body), retry_delay=0.01)

        assert set(tally.keys()) == {"r0", "r1", "r2"}

    async def test_b3_max_attempts_hard_cap(self, ops: Ops) -> None:
        """max_attempts=2 with 3 resources → third never accessed."""
        pool = Pool(resources=_res(3), max_attempts=2, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> Any:  # NOSONAR
            tally[r.resource_id] += 1
            raise CooldownResource(reason="busy")

        with pytest.raises(PoolExhausted, match="max_attempts=2 exhausted"):
            await pool.run(ops.op(body), retry_delay=0.01)

        assert "r2" not in tally

    async def test_b4_deadline_already_past(self, ops: Ops) -> None:
        """Deadline in the past → immediate PoolExhausted, no op invoked."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        called = False

        async def body(_: Resource[str]) -> str:  # NOSONAR
            nonlocal called
            called = True
            return "x"

        with pytest.raises(PoolExhausted, match="deadline exceeded after 0"):
            await pool.run(ops.op(body), deadline=time.monotonic() - 1)

        assert not called

    async def test_b5_deadline_crossed_mid_retry(self, ops: Ops) -> None:
        """Deadline crossed between attempts → PoolExhausted."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> Any:  # NOSONAR
            tally[r.resource_id] += 1
            raise CooldownResource(reason="busy")

        with pytest.raises(PoolExhausted, match="deadline exceeded after"):
            await pool.run(
                ops.op(body), deadline=time.monotonic() + 0.02, retry_delay=0.05
            )

        assert sum(tally.values()) >= 1


# ===================================================================
# Group C — Cooldown semantics
# ===================================================================


class TestCooldown:
    async def test_c1_escalation_walks_table(self, ops: Ops) -> None:
        """Consecutive cooldowns escalate through the table."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        op = ops.raising(lambda: CooldownResource(reason="hot"))

        for i, expected_cd in enumerate(FAST_TABLE, start=1):
            with pytest.raises(PoolExhausted):
                await pool.run(op, max_attempts=1)

            snap = pool.snapshot()["r0"]
            assert snap["consecutive_cooldown"] == i
            assert snap["cooldown_seconds_remaining"] == pytest.approx(
                expected_cd, abs=0.02
            )
            assert snap["status"] == "cooling_down"

            await asyncio.sleep(FAST_TABLE[i - 1] + 0.02)

    async def test_c2_explicit_cooldown_overrides_table(self, ops: Ops) -> None:
        """CooldownResource(cooldown_seconds=N) ignores the table."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        op = ops.raising(
            lambda: CooldownResource(cooldown_seconds=10.0, reason="custom")
        )

        with pytest.raises(PoolExhausted):
            await pool.run(op, max_attempts=1)

        assert pool.snapshot()["r0"]["cooldown_seconds_remaining"] == pytest.approx(
            10.0, abs=0.1
        )

    async def test_c3_success_resets_cooldown(self, ops: Ops) -> None:
        """Successful op resets consecutive_cooldown and clears cooldown state."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: CooldownResource(reason="hot")), max_attempts=1
            )

        assert pool.snapshot()["r0"]["consecutive_cooldown"] == 1

        await asyncio.sleep(FAST_TABLE[0] + 0.02)

        result = await pool.run(ops.identity())
        assert result == "v0"
        snap = pool.snapshot()["r0"]
        assert snap["consecutive_cooldown"] == 0
        assert snap["cooldown_seconds_remaining"] == pytest.approx(0.0)
        assert snap["status"] == "healthy"

    async def test_c4_older_success_preserves_younger_cooldown(self, ops: Ops) -> None:
        """Regression: older usage's OK must not wipe a younger usage's cooldown."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        younger_fired = asyncio.Event()
        release_older = asyncio.Event()

        async def older_body(_: Resource[str]) -> str:
            older_started.set()
            await younger_fired.wait()
            await release_older.wait()
            return "older-ok"

        async def younger_body(_: Resource[str]) -> Any:  # NOSONAR
            younger_fired.set()
            raise CooldownResource(reason="hot")

        older_task = asyncio.create_task(pool.run(ops.op(older_body)))
        await older_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(ops.op(younger_body), max_attempts=1)

        release_older.set()
        await older_task

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "cooling_down"
        assert snap["consecutive_cooldown"] >= 1
        assert snap["cooldown_seconds_remaining"] > 0

    async def test_c5_cooling_auto_revives(self, ops: Ops) -> None:
        """Resource in cooling_down revives to healthy after cooldown expires."""
        pool = Pool(resources=_res(1), cooldown_table=(0.02,))

        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: CooldownResource(reason="hot")), max_attempts=1
            )

        assert pool.snapshot()["r0"]["status"] == "cooling_down"

        await asyncio.sleep(0.04)

        result = await pool.run(ops.identity())
        assert result == "v0"
        assert pool.snapshot()["r0"]["status"] == "healthy"


# ===================================================================
# Group D — Disable semantics
# ===================================================================


class TestDisable:
    async def test_d1_disable_is_permanent(self, ops: Ops) -> None:
        """Disabled resource is never reselected."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> str:  # NOSONAR
            tally[r.resource_id] += 1
            if r.resource_id == "r0":
                raise DisableResource(reason="dead")
            return r.value

        op = ops.op(body)
        result = await pool.run(op)
        assert result == "v1"

        for _ in range(3):
            assert (await pool.run(op)) == "v1"

        assert tally["r0"] == 1
        assert tally["r1"] == 4

    async def test_d2_disabled_final_despite_concurrent_ok_and_cooldown(
        self, ops: Ops
    ) -> None:
        """`_on_ok` AND `_on_cooldown` cannot reactivate a disabled resource.

        Two usages acquired before disable: an older one that returns OK (hits the
        `_on_ok` no-reset branch) and a younger one that raises CooldownResource
        (hits `_on_cooldown`'s disabled-skip early return).
        """
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        cooler_started = asyncio.Event()
        proceed = asyncio.Event()

        async def older_body(_: Resource[str]) -> str:
            older_started.set()
            await proceed.wait()
            return "older-ok"

        async def cooler_body(_: Resource[str]) -> Any:
            cooler_started.set()
            await proceed.wait()
            raise CooldownResource(reason="hot")

        older_task = asyncio.create_task(pool.run(ops.op(older_body)))
        await older_started.wait()
        cooler_task = asyncio.create_task(pool.run(ops.op(cooler_body)))
        await cooler_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: DisableResource(reason="dead")), max_attempts=1
            )
        assert pool.snapshot()["r0"]["status"] == "disabled"

        proceed.set()
        assert (await older_task) == "older-ok"
        with pytest.raises(PoolExhausted):
            await cooler_task

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "disabled"
        # _on_cooldown's early return left consecutive_cooldown unchanged.
        assert snap["consecutive_cooldown"] == 0


# ===================================================================
# Group E — In-flight cancellation (younger-only, best-effort)
# ===================================================================


class TestCancellation:
    async def test_e1_younger_cancelled_on_cooldown(self, ops: Ops) -> None:
        """Middle worker raises CooldownResource; youngest is cancelled.

        Inverted for the awaitable shape: with no cancel handle the youngest sibling
        runs to natural completion instead of being cancelled.
        """
        pool = Pool(resources=_res(1), max_attempts=3, cooldown_table=FAST_TABLE)

        youngest_cancelled = False
        youngest_completed = False
        oldest_started = asyncio.Event()
        middle_started = asyncio.Event()

        async def oldest_body(_: Resource[str]) -> str:
            oldest_started.set()
            await middle_started.wait()
            await asyncio.sleep(0.2)
            return "oldest-ok"

        async def middle_body(_: Resource[str]) -> Any:
            middle_started.set()
            await asyncio.sleep(0.03)
            raise CooldownResource(cooldown_seconds=10.0, reason="hot")

        youngest_op: Operation[Any]
        if ops.shape == "awaitable":

            async def youngest_runs(_: Resource[str]) -> str:
                nonlocal youngest_completed
                await asyncio.sleep(0.15)
                youngest_completed = True
                return "youngest-ok"

            youngest_op = ops.op(youngest_runs)
        elif ops.shape == "coroutine":

            async def youngest_catches(_: Resource[str]) -> str:
                nonlocal youngest_cancelled
                try:
                    await asyncio.sleep(1.0)
                    return "youngest-ok"
                except asyncio.CancelledError:
                    youngest_cancelled = True
                    raise

            youngest_op = ops.op(youngest_catches)
        else:

            def youngest_future(_: Resource[str]) -> asyncio.Future[str]:
                # The framework cancels the Future via .cancel(); detect that via a
                # done callback (a body task would be decoupled from cancellation).
                fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

                def on_done(f: asyncio.Future[str]) -> None:
                    nonlocal youngest_cancelled
                    if f.cancelled():
                        youngest_cancelled = True

                fut.add_done_callback(on_done)
                return fut

            youngest_op = youngest_future

        oldest_task = asyncio.create_task(pool.run(ops.op(oldest_body)))
        await oldest_started.wait()

        middle_task = asyncio.create_task(pool.run(ops.op(middle_body)))
        await middle_started.wait()

        youngest_task = asyncio.create_task(pool.run(youngest_op))
        await asyncio.sleep(0.01)

        oldest_result = await oldest_task

        with pytest.raises((PoolExhausted, asyncio.CancelledError)):
            await middle_task

        if ops.shape == "awaitable":
            # Awaitable inversion: no cancel handle → youngest runs to completion.
            assert (await youngest_task) == "youngest-ok"
            assert youngest_completed
        else:
            with pytest.raises((PoolExhausted, asyncio.CancelledError)):
                await youngest_task
            assert youngest_cancelled

        assert oldest_result == "oldest-ok"
        assert pool.snapshot()["r0"]["in_flight"] == 0

    async def test_e2_older_not_cancelled_by_younger_failure(self, ops: Ops) -> None:
        """Younger raises CooldownResource; older completes normally."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        release_older = asyncio.Event()

        async def older_body(_: Resource[str]) -> str:
            older_started.set()
            await release_older.wait()
            return "older-ok"

        older_task = asyncio.create_task(pool.run(ops.op(older_body)))
        await older_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: CooldownResource(reason="hot")), max_attempts=1
            )

        release_older.set()
        assert (await older_task) == "older-ok"

    async def test_e3_triggering_usage_not_cancelled(self, ops: Ops) -> None:
        """The usage that raises CooldownResource is not in the cancel list."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        # If the triggering usage were cancelled, we'd see CancelledError instead
        # of the cooldown branch flowing through to PoolExhausted.
        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: CooldownResource(reason="hot")), max_attempts=1
            )

    async def test_e4_outer_cancellation_propagates(self, ops: Ops) -> None:
        """Caller cancelling run() → CancelledError re-raised; cleanup runs."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        task = asyncio.create_task(pool.run(ops.never()))
        await asyncio.sleep(0.05)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert pool.snapshot()["r0"]["in_flight"] == 0

    async def test_e5_internal_cancel_swallowed_retried(self, ops: Ops) -> None:
        """Internal CancelledError is swallowed and retried; caller never sees it.

        Inverted for the awaitable shape: with no cancel handle the waiter is not
        cancelled at all -- it runs to natural completion and the caller observes its
        successful result, not a swallowed retry.
        """
        pool = Pool(resources=_res(2), max_attempts=3, cooldown_table=FAST_TABLE)
        # Force every usage onto r0.
        pool._resources["r1"].status = "disabled"

        blocker_started = asyncio.Event()
        blocker_release = asyncio.Event()
        trigger_ready = asyncio.Event()
        waiter_started = asyncio.Event()
        waiter_release = asyncio.Event()  # only used by the awaitable shape
        waiter_cancelled = False
        attempt = 0

        async def blocker_body(_: Resource[str]) -> str:
            blocker_started.set()
            await blocker_release.wait()
            return "blocker-ok"

        async def trigger_body(_: Resource[str]) -> Any:
            trigger_ready.set()
            await asyncio.sleep(0.05)
            raise CooldownResource(cooldown_seconds=10.0, reason="hot")

        waiter_op: Operation[Any]
        if ops.shape == "awaitable":

            async def waiter_runs(r: Resource[str]) -> str:
                waiter_started.set()
                await waiter_release.wait()
                return r.value

            waiter_op = ops.op(waiter_runs)
        elif ops.shape == "coroutine":

            async def waiter_catches(r: Resource[str]) -> str:
                nonlocal attempt, waiter_cancelled
                attempt += 1
                if attempt == 1:
                    waiter_started.set()
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        waiter_cancelled = True
                        raise
                return r.value

            waiter_op = ops.op(waiter_catches)
        else:

            def waiter_future(r: Resource[str]) -> asyncio.Future[str]:
                nonlocal attempt
                attempt += 1
                if attempt == 1:
                    waiter_started.set()
                    fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

                    def on_done(f: asyncio.Future[str]) -> None:
                        nonlocal waiter_cancelled
                        if f.cancelled():
                            waiter_cancelled = True

                    fut.add_done_callback(on_done)
                    return fut
                done: asyncio.Future[str] = asyncio.get_event_loop().create_future()
                done.set_result(r.value)
                return done

            waiter_op = waiter_future

        blocker_task = asyncio.create_task(pool.run(ops.op(blocker_body)))
        await blocker_started.wait()
        trigger_task = asyncio.create_task(pool.run(ops.op(trigger_body)))
        await trigger_ready.wait()
        waiter_task = asyncio.create_task(pool.run(waiter_op, retry_delay=0.01))
        await waiter_started.wait()

        with pytest.raises((PoolExhausted, asyncio.CancelledError)):
            await trigger_task

        if ops.shape == "awaitable":
            # Awaitable inversion: waiter's cancel was a no-op; it runs naturally
            # and returns its value rather than retrying.
            waiter_release.set()
            assert (await waiter_task) == "v0"
        else:
            # The waiter's internal CancelledError was swallowed; the retry then hit
            # PoolExhausted (r0 cooling, r1 disabled).
            with pytest.raises(PoolExhausted):
                await waiter_task
            assert waiter_cancelled

        blocker_release.set()
        await blocker_task

    async def test_e6_stale_inflight_id_skipped_in_younger_collection(
        self, ops: Ops
    ) -> None:
        """White-box: a stale usage_id in `_inflight_by_resource` (no matching
        record in `_usages`) is silently skipped by `_collect_younger_usages_locked`.
        """
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        # Inject a stale usage_id with no matching Usage record.
        pool._inflight_by_resource.setdefault("r0", set()).add("ghost-id")

        with pytest.raises(PoolExhausted):
            await pool.run(
                ops.raising(lambda: CooldownResource(reason="hot")), max_attempts=1
            )

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "cooling_down"
        assert snap["consecutive_cooldown"] == 1

    async def test_e7_external_cancel_propagates_for_uncancellable_awaitable(
        self,
    ) -> None:
        """Awaitable shape only: a sibling cooldown marks the usage 'cancelled' but
        has no handle to actually cancel it. A subsequent CancelledError can therefore
        only be external (caller cancellation) and must propagate, not be swallowed
        as an internal retry.
        """
        pool = Pool(resources=_res(1), max_attempts=3, cooldown_table=FAST_TABLE)

        trigger_started = asyncio.Event()
        waiter_started = asyncio.Event()
        release = asyncio.Event()  # never set; waiter blocks until cancelled

        async def trigger_body(_: Resource[str]) -> Any:
            trigger_started.set()
            await asyncio.sleep(0.05)
            raise CooldownResource(cooldown_seconds=10.0, reason="hot")

        async def waiter_body(_: Resource[str]) -> str:
            waiter_started.set()
            await release.wait()
            return "never"

        def waiter_op(r: Resource[str]) -> Awaitable[str]:
            return _Aw(waiter_body(r))

        trigger_task = asyncio.create_task(pool.run(trigger_body))
        await trigger_started.wait()
        waiter_task = asyncio.create_task(pool.run(waiter_op))
        await waiter_started.wait()

        # Trigger's cooldown marks the younger waiter 'cancelled' (cancel no-ops).
        with pytest.raises(PoolExhausted):
            await trigger_task

        waiter_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter_task

        assert pool.snapshot()["r0"]["in_flight"] == 0


# ===================================================================
# Group F — Concurrency & saturation
# ===================================================================


class TestConcurrency:
    async def test_f1_max_inflight_saturation_fails_fast(self, ops: Ops) -> None:
        """max_in_flight=1 x 2 resources; 3rd worker gets PoolExhausted."""
        pool = Pool(resources=_res(2, max_in_flight=1), cooldown_table=FAST_TABLE)
        hold = asyncio.Event()

        async def body(r: Resource[str]) -> str:
            await hold.wait()
            return r.resource_id

        op = ops.op(body)
        t1 = asyncio.create_task(pool.run(op))
        t2 = asyncio.create_task(pool.run(op))
        await asyncio.sleep(0.05)

        with pytest.raises(PoolExhausted, match="no eligible resource"):
            await pool.run(op)

        hold.set()
        await asyncio.gather(t1, t2)

    async def test_f2_released_capacity_reusable(self, ops: Ops) -> None:
        """After a slot frees up, a new worker can acquire it."""
        pool = Pool(resources=_res(1, max_in_flight=1), cooldown_table=FAST_TABLE)
        release = asyncio.Event()

        async def blocker_body(_: Resource[str]) -> None:
            await release.wait()

        blocker_task = asyncio.create_task(pool.run(ops.op(blocker_body)))
        await asyncio.sleep(0.02)

        with pytest.raises(PoolExhausted):
            await pool.run(ops.identity())

        release.set()
        await blocker_task

        result = await pool.run(ops.identity())
        assert result == "v0"


# ===================================================================
# Group G — Operation-shape contract
# ===================================================================


class TestOperationShape:
    async def test_g1_non_awaitable_raises_typeerror(self) -> None:
        """Returning non-awaitable → TypeError; resource stays healthy."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        with pytest.raises(TypeError, match="operation must return an Awaitable"):
            await pool.run(cast("Any", lambda _: 42))

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "healthy"
        assert snap["consecutive_cooldown"] == 0

    async def test_g2_business_exception_propagates(self, ops: Ops) -> None:
        """ValueError propagates; resource stays healthy."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        with pytest.raises(ValueError, match="boom"):
            await pool.run(ops.raising(lambda: ValueError("boom")))

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "healthy"
        assert snap["in_flight"] == 0

    async def test_g3_use_accepts_sync_and_async(self) -> None:
        """@use works with async def, sync returning coroutine, sync returning Future."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        @pool.use()
        async def op_async(r: Resource[str]) -> str:
            return f"async-{r.value}"

        async def _helper(r: Resource[str]) -> str:  # NOSONAR
            return f"coro-{r.value}"

        @pool.use()
        def op_sync_coro(r: Resource[str]):
            return _helper(r)

        @pool.use()
        def op_sync_future(r: Resource[str]):
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()
            fut.set_result(f"future-{r.value}")
            return fut

        assert await op_async() == "async-v0"
        assert await op_sync_coro() == "coro-v0"
        assert await op_sync_future() == "future-v0"


# ===================================================================
# Group H — Constructor & API surface
# ===================================================================


class TestAPI:
    def test_h1_empty_pool(self) -> None:
        """Empty pool → ValueError at construction."""
        with pytest.raises(ValueError, match="at least one resource"):
            Pool(resources=[])

    def test_h2_duplicate_resource_id(self) -> None:
        """Duplicate resource_id in list form → ValueError."""
        with pytest.raises(ValueError, match="Duplicate resource_id"):
            Pool(
                resources=[
                    Resource(resource_id="a", value="x"),
                    Resource(resource_id="a", value="y"),
                ]
            )

    async def test_h3_dict_form_equivalent(self, ops: Ops) -> None:
        """Dict-form constructor behaves identically to list-form."""
        pool = Pool(
            resources={
                "r0": Resource(resource_id="r0", value="v0"),
                "r1": Resource(resource_id="r1", value="v1"),
            },
            cooldown_table=FAST_TABLE,
        )

        results: set[str] = set()
        for _ in range(4):
            results.add(await pool.run(ops.identity()))

        assert results == {"v0", "v1"}

    async def test_h4_request_id_propagated(self, ops: Ops) -> None:
        """request_id is attached to every Usage created during the run."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        captured_rids: list[str] = []

        async def body(r: Resource[str]) -> str:  # NOSONAR
            for u in pool._usages.values():
                captured_rids.append(u.request_id)
            return r.value

        await pool.run(ops.op(body), request_id="req-xyz")
        assert captured_rids
        assert all(rid == "req-xyz" for rid in captured_rids)

    async def test_h5_snapshot_schema(self) -> None:
        """snapshot() returns the documented keys and types."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        snap = pool.snapshot()
        assert set(snap["r0"].keys()) == {
            "status",
            "in_flight",
            "consecutive_cooldown",
            "cooldown_seconds_remaining",
            "last_acquired_at",
        }
        assert snap["r0"]["status"] == "healthy"
        assert isinstance(snap["r0"]["in_flight"], int)
        assert isinstance(snap["r0"]["consecutive_cooldown"], int)
        assert isinstance(snap["r0"]["cooldown_seconds_remaining"], float)
        assert isinstance(snap["r0"]["last_acquired_at"], float)

        async def cool(_: Resource[str]) -> None:
            raise CooldownResource(reason="hot")

        with pytest.raises(PoolExhausted):
            await pool.run(cool, max_attempts=1)

        snap = pool.snapshot()
        assert snap["r0"]["status"] == "cooling_down"
        assert snap["r0"]["cooldown_seconds_remaining"] > 0

    async def test_h6_use_forwards_args(self) -> None:
        """@use passes positional and keyword args through."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        captured: dict[str, Any] = {}

        @pool.use()
        async def op(r: Resource[str], x: int, y: int, *, opt: int = 1) -> str:
            captured.update(x=x, y=y, opt=opt, rid=r.resource_id)
            return r.value

        result = await op(10, 20, opt=99)
        assert result == "v0"
        assert captured == {"x": 10, "y": 20, "opt": 99, "rid": "r0"}

    async def test_h7_per_call_overrides(self, ops: Ops) -> None:
        """Per-call max_attempts overrides pool-level default."""
        pool = Pool(resources=_res(2), max_attempts=5, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        async def body(r: Resource[str]) -> Any:  # NOSONAR
            tally[r.resource_id] += 1
            raise CooldownResource(reason="hot")

        with pytest.raises(PoolExhausted, match="max_attempts=1 exhausted"):
            await pool.run(ops.op(body), max_attempts=1, retry_delay=0.01)

        assert sum(tally.values()) == 1

    def test_h8_construction_rejects_bad_max_attempts(self) -> None:
        """max_attempts < 1 at construction → ValueError."""
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            Pool(resources=_res(1), max_attempts=0)

    def test_h9_construction_rejects_empty_cooldown_table(self) -> None:
        """Empty cooldown_table → ValueError at construction."""
        with pytest.raises(ValueError, match="cooldown_table must contain"):
            Pool(resources=_res(1), cooldown_table=())

    def test_h11_construction_rejects_negative_cooldown(self) -> None:
        """Negative cooldown_table entry → ValueError at construction."""
        with pytest.raises(ValueError, match="cooldown_table entries must be >= 0"):
            Pool(resources=_res(1), cooldown_table=(30.0, -1.0))

    def test_h12_dict_key_must_match_resource_id(self) -> None:
        """Dict key differing from resource_id → ValueError at construction.

        A mismatch would silently break health tracking: usages are keyed by
        resource_id, so cooldown/disable lookups would miss the resource.
        """
        with pytest.raises(ValueError, match="does not match resource_id"):
            Pool(resources={"alias": Resource(resource_id="real", value="v")})

    async def test_h10_run_rejects_bad_max_attempts(self) -> None:
        """Per-call max_attempts < 1 → ValueError before any attempt."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        async def op(r: Resource[str]) -> str:  # NOSONAR
            return r.value

        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            await pool.run(op, max_attempts=0)

    async def test_h13_run_rejects_negative_retry_delay(self) -> None:
        """Per-call retry_delay < 0 → ValueError before any attempt."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        async def op(r: Resource[str]) -> str:  # NOSONAR
            return r.value

        with pytest.raises(ValueError, match="retry_delay must be >= 0"):
            await pool.run(op, retry_delay=-0.1)

    async def test_h14_works_without_agent_readable(self) -> None:
        """agent-readable is optional: with its import blocked, the module falls
        back to the no-op mixin and the pool still runs operations end to end."""
        import importlib
        import sys

        import rotapool.pool as pool_module

        saved = sys.modules.get("agent_readable")
        # A None entry in sys.modules makes `import agent_readable` raise
        # ModuleNotFoundError, exercising the fallback branch.
        sys.modules["agent_readable"] = None  # type: ignore[assignment]
        try:
            reloaded = importlib.reload(pool_module)
            assert "agent_readable" not in reloaded.AgentReadableMixin.__module__

            pool = reloaded.Pool(
                resources=[Resource(resource_id="r0", value="v0")],
                cooldown_table=FAST_TABLE,
            )

            async def op(r: Resource[str]) -> str:  # NOSONAR
                return r.value

            assert await pool.run(op) == "v0"
        finally:
            if saved is None:
                sys.modules.pop("agent_readable", None)
            else:
                sys.modules["agent_readable"] = saved
            importlib.reload(pool_module)
