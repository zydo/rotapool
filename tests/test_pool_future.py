"""Unit tests for rotapool.Pool — Future-returning operations.

Mirror of test_pool_coroutine.py 1:1. Class names, method names, setup, and assertions
match exactly; only the operation construction differs (sync `def` returning an
`asyncio.Future`, with body work driven by tracked background tasks via `_spawn`).
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import Any, Coroutine, cast

import pytest

from rotapool import CooldownResource, DisableResource, Pool, PoolExhausted, Resource

FAST_TABLE = (0.05, 0.10, 0.15, 0.20)

# Strong-ref set so the body tasks driving futures are not GC'd before they resolve.
# `_spawn` registers a task here and removes it on completion.
_BG_TASKS: set[asyncio.Task[Any]] = set()


def _res(n: int, **kw: Any) -> list[Resource[str]]:
    return [Resource(resource_id=f"r{i}", value=f"v{i}", **kw) for i in range(n)]


def _spawn(coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    task = asyncio.create_task(coro)
    _BG_TASKS.add(task)
    task.add_done_callback(_BG_TASKS.discard)
    return task


def _ok_future(value: Any) -> asyncio.Future[Any]:
    fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
    fut.set_result(value)
    return fut


def _err_future(exc: BaseException) -> asyncio.Future[Any]:
    fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
    fut.set_exception(exc)
    return fut


def _id_future(r: Resource[str]) -> asyncio.Future[str]:
    return _ok_future(r.value)


# ===================================================================
# Group A — Selection & fairness
# ===================================================================


class TestSelection:
    async def test_a1_round_robin_fairness_concurrent(self):
        """6 concurrent ops on 3 healthy resources → each used exactly 2x."""
        pool = Pool(resources=_res(3), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()
        gate = asyncio.Event()

        @pool.rotated()
        def op(r: Resource[str]) -> asyncio.Future[str]:
            tally[r.resource_id] += 1
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await gate.wait()
                if not fut.done():
                    fut.set_result(r.resource_id)

            _spawn(_wait())
            return fut

        tasks = [asyncio.create_task(cast("Any", op())) for _ in range(6)]
        await asyncio.sleep(0.05)
        gate.set()
        results = await asyncio.gather(*tasks)

        assert sorted(results) == sorted(["r0", "r0", "r1", "r1", "r2", "r2"])
        assert dict(tally) == {"r0": 2, "r1": 2, "r2": 2}

    async def test_a2_prefers_fewest_inflight_then_oldest(self):
        """Pick fewest in-flight; break ties by oldest last_acquired_at."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        hold = asyncio.Event()
        acquired: list[str] = []

        def op(r: Resource[str]) -> asyncio.Future[str]:
            acquired.append(r.resource_id)
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await hold.wait()
                if not fut.done():
                    fut.set_result(r.resource_id)

            _spawn(_wait())
            return fut

        t0 = asyncio.create_task(pool.run(op))
        t1 = asyncio.create_task(pool.run(op))
        await asyncio.sleep(0.05)
        assert set(acquired) == {"r0", "r1"}

        hold.set()
        await asyncio.gather(t0, t1)

        acquired.clear()

        def quick(r: Resource[str]) -> asyncio.Future[str]:
            acquired.append(r.resource_id)
            return _ok_future(r.resource_id)

        result = await pool.run(quick)
        assert result == "r0"


# ===================================================================
# Group B — Retry & transparent failover
# ===================================================================


class TestRetry:
    async def test_b1_transparent_failover(self):
        """Cooldown on first two resources; third succeeds. Caller is unaware."""
        pool = Pool(resources=_res(3), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def op(r: Resource[str]) -> asyncio.Future[str]:
            tally[r.resource_id] += 1
            if r.resource_id in ("r0", "r1"):
                return _err_future(CooldownResource(reason="busy"))
            return _ok_future(r.value)

        result = await pool.run(op)
        assert result == "v2"
        assert tally == {"r0": 1, "r1": 1, "r2": 1}

    async def test_b2_pool_exhausted_all_out(self):
        """All resources unusable → PoolExhausted with last error info."""
        pool = Pool(resources=_res(3), max_attempts=3, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def op(r: Resource[str]) -> asyncio.Future[Any]:
            tally[r.resource_id] += 1
            if r.resource_id == "r0":
                return _err_future(DisableResource(reason="dead"))
            return _err_future(CooldownResource(reason="busy"))

        with pytest.raises(PoolExhausted, match="max_attempts=3 exhausted"):
            await pool.run(op, retry_delay=0.01)

        assert set(tally.keys()) == {"r0", "r1", "r2"}

    async def test_b3_max_attempts_hard_cap(self):
        """max_attempts=2 with 3 resources → third never accessed."""
        pool = Pool(resources=_res(3), max_attempts=2, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def op(r: Resource[str]) -> asyncio.Future[Any]:
            tally[r.resource_id] += 1
            return _err_future(CooldownResource(reason="busy"))

        with pytest.raises(PoolExhausted, match="max_attempts=2 exhausted"):
            await pool.run(op, retry_delay=0.01)

        assert "r2" not in tally

    async def test_b4_deadline_already_past(self):
        """Deadline in the past → immediate PoolExhausted, no op invoked."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        called = False

        def op(_: Resource[str]) -> asyncio.Future[str]:
            nonlocal called
            called = True
            return _ok_future("x")

        with pytest.raises(PoolExhausted, match="deadline exceeded after 0"):
            await pool.run(op, deadline=time.monotonic() - 1)

        assert not called

    async def test_b5_deadline_crossed_mid_retry(self):
        """Deadline crossed between attempts → PoolExhausted."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def op(r: Resource[str]) -> asyncio.Future[Any]:
            tally[r.resource_id] += 1
            return _err_future(CooldownResource(reason="busy"))

        with pytest.raises(PoolExhausted, match="deadline exceeded after"):
            await pool.run(op, deadline=time.monotonic() + 0.02, retry_delay=0.05)

        assert sum(tally.values()) >= 1


# ===================================================================
# Group C — Cooldown semantics
# ===================================================================


class TestCooldown:
    async def test_c1_escalation_walks_table(self):
        """Consecutive cooldowns escalate through the table."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

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

    async def test_c2_explicit_cooldown_overrides_table(self):
        """CooldownResource(cooldown_seconds=N) ignores the table."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(cooldown_seconds=10.0, reason="custom"))

        with pytest.raises(PoolExhausted):
            await pool.run(op, max_attempts=1)

        assert pool.snapshot()["r0"]["cooldown_seconds_remaining"] == pytest.approx(
            10.0, abs=0.1
        )

    async def test_c3_success_resets_cooldown(self):
        """Successful op resets consecutive_cooldown and clears cooldown state."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def cool(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted):
            await pool.run(cool, max_attempts=1)

        assert pool.snapshot()["r0"]["consecutive_cooldown"] == 1

        await asyncio.sleep(FAST_TABLE[0] + 0.02)

        result = await pool.run(_id_future)
        assert result == "v0"
        snap = pool.snapshot()["r0"]
        assert snap["consecutive_cooldown"] == 0
        assert snap["cooldown_seconds_remaining"] == pytest.approx(0.0)
        assert snap["status"] == "healthy"

    async def test_c4_older_success_preserves_younger_cooldown(self):
        """Regression: older usage's OK must not wipe a younger usage's cooldown."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        younger_fired = asyncio.Event()
        release_older = asyncio.Event()

        def older_op(_: Resource[str]) -> asyncio.Future[str]:
            older_started.set()
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await younger_fired.wait()
                await release_older.wait()
                if not fut.done():
                    fut.set_result("older-ok")

            _spawn(_wait())
            return fut

        def younger_op(_: Resource[str]) -> asyncio.Future[Any]:
            younger_fired.set()
            return _err_future(CooldownResource(reason="hot"))

        older_task = asyncio.create_task(pool.run(older_op))
        await older_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(younger_op, max_attempts=1)

        release_older.set()
        await older_task

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "cooling_down"
        assert snap["consecutive_cooldown"] >= 1
        assert snap["cooldown_seconds_remaining"] > 0

    async def test_c5_cooling_auto_revives(self):
        """Resource in cooling_down revives to healthy after cooldown expires."""
        pool = Pool(resources=_res(1), cooldown_table=(0.02,))

        def cool(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted):
            await pool.run(cool, max_attempts=1)

        assert pool.snapshot()["r0"]["status"] == "cooling_down"

        await asyncio.sleep(0.04)

        result = await pool.run(_id_future)
        assert result == "v0"
        assert pool.snapshot()["r0"]["status"] == "healthy"


# ===================================================================
# Group D — Disable semantics
# ===================================================================


class TestDisable:
    async def test_d1_disable_is_permanent(self):
        """Disabled resource is never reselected."""
        pool = Pool(resources=_res(2), cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def op(r: Resource[str]) -> asyncio.Future[str]:
            tally[r.resource_id] += 1
            if r.resource_id == "r0":
                return _err_future(DisableResource(reason="dead"))
            return _ok_future(r.value)

        result = await pool.run(op)
        assert result == "v1"

        for _ in range(3):
            assert (await pool.run(op)) == "v1"

        assert tally["r0"] == 1
        assert tally["r1"] == 4

    async def test_d2_disabled_final_despite_concurrent_ok_and_cooldown(self):
        """`_on_ok` AND `_on_cooldown` cannot reactivate a disabled resource."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        cooler_started = asyncio.Event()
        proceed = asyncio.Event()

        def older_op(_: Resource[str]) -> asyncio.Future[str]:
            older_started.set()
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await proceed.wait()
                if not fut.done():
                    fut.set_result("older-ok")

            _spawn(_wait())
            return fut

        def cooler_op(_: Resource[str]) -> asyncio.Future[Any]:
            cooler_started.set()
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await proceed.wait()
                if not fut.done():
                    fut.set_exception(CooldownResource(reason="hot"))

            _spawn(_wait())
            return fut

        def disabler(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(DisableResource(reason="dead"))

        older_task = asyncio.create_task(pool.run(older_op))
        await older_started.wait()
        cooler_task = asyncio.create_task(pool.run(cooler_op))
        await cooler_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(disabler, max_attempts=1)
        assert pool.snapshot()["r0"]["status"] == "disabled"

        proceed.set()
        assert (await older_task) == "older-ok"
        with pytest.raises(PoolExhausted):
            await cooler_task

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "disabled"
        assert snap["consecutive_cooldown"] == 0


# ===================================================================
# Group E — In-flight cancellation (younger-only, best-effort)
# ===================================================================


class TestCancellation:
    async def test_e1_younger_cancelled_on_cooldown(self):
        """Middle worker raises CooldownResource; youngest is cancelled."""
        pool = Pool(resources=_res(1), max_attempts=3, cooldown_table=FAST_TABLE)

        youngest_cancelled = False
        oldest_started = asyncio.Event()
        middle_started = asyncio.Event()

        def oldest_op(_: Resource[str]) -> asyncio.Future[str]:
            oldest_started.set()
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _body() -> None:
                await middle_started.wait()
                await asyncio.sleep(0.2)
                if not fut.done():
                    fut.set_result("oldest-ok")

            _spawn(_body())
            return fut

        def middle_op(_: Resource[str]) -> asyncio.Future[Any]:
            middle_started.set()
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()

            async def _body() -> None:
                await asyncio.sleep(0.03)
                if not fut.done():
                    fut.set_exception(
                        CooldownResource(cooldown_seconds=10.0, reason="hot")
                    )

            _spawn(_body())
            return fut

        def youngest_op(_: Resource[str]) -> asyncio.Future[str]:
            # The framework cancels the Future via .cancel(); detect that via a
            # done callback (a body task would be decoupled from cancellation).
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            def on_done(f: asyncio.Future[str]) -> None:
                nonlocal youngest_cancelled
                if f.cancelled():
                    youngest_cancelled = True

            fut.add_done_callback(on_done)
            return fut

        oldest_task = asyncio.create_task(pool.run(oldest_op))
        await oldest_started.wait()

        middle_task = asyncio.create_task(pool.run(middle_op))
        await middle_started.wait()

        youngest_task = asyncio.create_task(pool.run(youngest_op))
        await asyncio.sleep(0.01)

        oldest_result = await oldest_task

        with pytest.raises((PoolExhausted, asyncio.CancelledError)):
            await middle_task
        with pytest.raises((PoolExhausted, asyncio.CancelledError)):
            await youngest_task

        assert oldest_result == "oldest-ok"
        assert youngest_cancelled
        assert pool.snapshot()["r0"]["in_flight"] == 0

    async def test_e2_older_not_cancelled_by_younger_failure(self):
        """Younger raises CooldownResource; older completes normally."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        older_started = asyncio.Event()
        release_older = asyncio.Event()

        def older_op(_: Resource[str]) -> asyncio.Future[str]:
            older_started.set()
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await release_older.wait()
                if not fut.done():
                    fut.set_result("older-ok")

            _spawn(_wait())
            return fut

        def younger_op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        older_task = asyncio.create_task(pool.run(older_op))
        await older_started.wait()

        with pytest.raises(PoolExhausted):
            await pool.run(younger_op, max_attempts=1)

        release_older.set()
        assert (await older_task) == "older-ok"

    async def test_e3_triggering_usage_not_cancelled(self):
        """The usage that raises CooldownResource is not in the cancel list."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted):
            await pool.run(op, max_attempts=1)

    async def test_e4_outer_cancellation_propagates(self):
        """Caller cancelling run() → CancelledError re-raised; cleanup runs."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return asyncio.get_event_loop().create_future()

        task = asyncio.create_task(pool.run(op))
        await asyncio.sleep(0.05)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert pool.snapshot()["r0"]["in_flight"] == 0

    async def test_e5_internal_cancel_swallowed_retried(self):
        """Internal CancelledError is swallowed and retried; caller never sees it."""
        pool = Pool(resources=_res(2), max_attempts=3, cooldown_table=FAST_TABLE)
        pool._resources["r1"].status = "disabled"

        blocker_started = asyncio.Event()
        blocker_release = asyncio.Event()
        trigger_ready = asyncio.Event()
        waiter_started = asyncio.Event()
        waiter_cancelled = False
        attempt = 0

        def blocker_op(_: Resource[str]) -> asyncio.Future[str]:
            blocker_started.set()
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await blocker_release.wait()
                if not fut.done():
                    fut.set_result("blocker-ok")

            _spawn(_wait())
            return fut

        def trigger_op(_: Resource[str]) -> asyncio.Future[Any]:
            trigger_ready.set()
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()

            async def _body() -> None:
                await asyncio.sleep(0.05)
                if not fut.done():
                    fut.set_exception(
                        CooldownResource(cooldown_seconds=10.0, reason="hot")
                    )

            _spawn(_body())
            return fut

        def waiter_op(r: Resource[str]) -> asyncio.Future[str]:
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
            return _ok_future(r.value)

        blocker_task = asyncio.create_task(pool.run(blocker_op))
        await blocker_started.wait()
        trigger_task = asyncio.create_task(pool.run(trigger_op))
        await trigger_ready.wait()
        waiter_task = asyncio.create_task(pool.run(waiter_op, retry_delay=0.01))
        await waiter_started.wait()

        with pytest.raises((PoolExhausted, asyncio.CancelledError)):
            await trigger_task

        with pytest.raises(PoolExhausted):
            await waiter_task

        assert waiter_cancelled
        blocker_release.set()
        await blocker_task

    async def test_e6_stale_inflight_id_skipped_in_younger_collection(self):
        """White-box: a stale usage_id in `_inflight_by_resource` (no matching
        record in `_usages`) is silently skipped by `_collect_younger_usages_locked`.
        """
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        pool._inflight_by_resource.setdefault("r0", set()).add("ghost-id")

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted):
            await pool.run(op, max_attempts=1)

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "cooling_down"
        assert snap["consecutive_cooldown"] == 1


# ===================================================================
# Group F — Concurrency & saturation
# ===================================================================


class TestConcurrency:
    async def test_f1_max_inflight_saturation_fails_fast(self):
        """max_in_flight=1 x 2 resources; 3rd worker gets PoolExhausted."""
        pool = Pool(resources=_res(2, max_in_flight=1), cooldown_table=FAST_TABLE)
        hold = asyncio.Event()

        def op(r: Resource[str]) -> asyncio.Future[str]:
            fut: asyncio.Future[str] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await hold.wait()
                if not fut.done():
                    fut.set_result(r.resource_id)

            _spawn(_wait())
            return fut

        t1 = asyncio.create_task(pool.run(op))
        t2 = asyncio.create_task(pool.run(op))
        await asyncio.sleep(0.05)

        with pytest.raises(PoolExhausted, match="no eligible resource"):
            await pool.run(op)

        hold.set()
        await asyncio.gather(t1, t2)

    async def test_f2_released_capacity_reusable(self):
        """After a slot frees up, a new worker can acquire it."""
        pool = Pool(resources=_res(1, max_in_flight=1), cooldown_table=FAST_TABLE)
        release = asyncio.Event()

        def blocker(_: Resource[str]) -> asyncio.Future[Any]:
            fut: asyncio.Future[Any] = asyncio.get_event_loop().create_future()

            async def _wait() -> None:
                await release.wait()
                if not fut.done():
                    fut.set_result(None)

            _spawn(_wait())
            return fut

        blocker_task = asyncio.create_task(pool.run(blocker))
        await asyncio.sleep(0.02)

        with pytest.raises(PoolExhausted):
            await pool.run(_id_future)

        release.set()
        await blocker_task

        result = await pool.run(_id_future)
        assert result == "v0"


# ===================================================================
# Group G — Operation-shape contract
# ===================================================================


class TestOperationShape:
    async def test_g1_non_awaitable_raises_typeerror(self):
        """Returning non-awaitable → TypeError; resource stays healthy."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        with pytest.raises(TypeError, match="operation must return an Awaitable"):
            await pool.run(cast("Any", lambda _: 42))

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "healthy"
        assert snap["consecutive_cooldown"] == 0

    async def test_g2_business_exception_propagates(self):
        """ValueError propagates; resource stays healthy."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        def op(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(ValueError("boom"))

        with pytest.raises(ValueError, match="boom"):
            await pool.run(op)

        snap = pool.snapshot()["r0"]
        assert snap["status"] == "healthy"
        assert snap["in_flight"] == 0

    async def test_g3_rotated_accepts_sync_and_async(self):
        """@rotated works with async def, sync returning coroutine, sync returning Future."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)

        @pool.rotated()
        async def op_async(r: Resource[str]) -> str:
            return f"async-{r.value}"

        async def _helper(r: Resource[str]) -> str:  # NOSONAR S7503
            return f"coro-{r.value}"

        @pool.rotated()
        def op_sync_coro(r: Resource[str]):
            return _helper(r)

        @pool.rotated()
        def op_sync_future(r: Resource[str]):
            return _ok_future(f"future-{r.value}")

        assert await op_async() == "async-v0"
        assert await op_sync_coro() == "coro-v0"
        assert await op_sync_future() == "future-v0"


# ===================================================================
# Group H — Constructor & API surface
# ===================================================================


class TestAPI:
    def test_h1_empty_pool(self):
        """Empty pool → ValueError at construction."""
        with pytest.raises(ValueError, match="at least one resource"):
            Pool(resources=[])

    def test_h2_duplicate_resource_id(self):
        """Duplicate resource_id in list form → ValueError."""
        with pytest.raises(ValueError, match="Duplicate resource_id"):
            Pool(
                resources=[
                    Resource(resource_id="a", value="x"),
                    Resource(resource_id="a", value="y"),
                ]
            )

    async def test_h3_dict_form_equivalent(self):
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
            results.add(await pool.run(_id_future))

        assert results == {"v0", "v1"}

    async def test_h4_request_id_propagated(self):
        """request_id is attached to every Usage created during the run."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        captured_rids: list[str] = []

        def op(r: Resource[str]) -> asyncio.Future[str]:
            for u in pool._usages.values():
                captured_rids.append(u.request_id)
            return _ok_future(r.value)

        await pool.run(op, request_id="req-xyz")
        assert captured_rids
        assert all(rid == "req-xyz" for rid in captured_rids)

    async def test_h5_snapshot_schema(self):
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

        def cool(_: Resource[str]) -> asyncio.Future[Any]:
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted):
            await pool.run(cool, max_attempts=1)

        snap = pool.snapshot()
        assert snap["r0"]["status"] == "cooling_down"
        assert snap["r0"]["cooldown_seconds_remaining"] > 0

    async def test_h6_rotated_forwards_args(self):
        """@rotated passes positional and keyword args through."""
        pool = Pool(resources=_res(1), cooldown_table=FAST_TABLE)
        captured: dict[str, Any] = {}

        @pool.rotated()
        def op(
            r: Resource[str], x: int, y: int, *, opt: int = 1
        ) -> asyncio.Future[str]:
            captured.update(x=x, y=y, opt=opt, rid=r.resource_id)
            return _ok_future(r.value)

        result = await op(10, 20, opt=99)
        assert result == "v0"
        assert captured == {"x": 10, "y": 20, "opt": 99, "rid": "r0"}

    async def test_h7_per_call_overrides(self):
        """Per-call max_attempts overrides pool-level default."""
        pool = Pool(resources=_res(2), max_attempts=5, cooldown_table=FAST_TABLE)
        tally: Counter[str] = Counter()

        def always_cool(r: Resource[str]) -> asyncio.Future[Any]:
            tally[r.resource_id] += 1
            return _err_future(CooldownResource(reason="hot"))

        with pytest.raises(PoolExhausted, match="max_attempts=1 exhausted"):
            await pool.run(always_cool, max_attempts=1, retry_delay=0.01)

        assert sum(tally.values()) == 1
