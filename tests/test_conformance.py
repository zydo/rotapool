"""Drive rotapool-spec/conformance/scenarios.yaml against this implementation."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from rotapool import CooldownResource, DisableResource, Pool, PoolExhausted, Resource

STRATEGY = {"least_in_flight": "round_robin", "primary_backup": "primary_backup"}


def _scenarios_path() -> Path | None:
    env = os.environ.get("ROTAPOOL_SCENARIOS")
    if env:
        path = Path(env)
        return path if path.is_file() else None
    sibling = Path(__file__).resolve().parents[2] / "rotapool-spec/conformance/scenarios.yaml"
    return sibling if sibling.is_file() else None


class VirtualClock:
    def __init__(self) -> None:
        self.t = 0.0
        self._sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self.t

    @property
    def n_sleepers(self) -> int:
        return sum(1 for _, fut in self._sleepers if not fut.done())

    def advance(self, dt: float) -> None:
        self.t += dt
        left: list[tuple[float, asyncio.Future[None]]] = []
        for when, fut in self._sleepers:
            if fut.done():
                continue
            if when <= self.t:
                fut.set_result(None)
            else:
                left.append((when, fut))
        self._sleepers = left

    async def sleep(self, delay: float) -> None:
        if delay <= 0:
            await asyncio.sleep(0)
            return
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._sleepers.append((self.t + delay, fut))
        try:
            await fut
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            raise


def _load_scenarios() -> list[dict[str, Any]]:
    path = _scenarios_path()
    if path is None:
        return []
    data = yaml.safe_load(path.read_text())
    return list(data["scenarios"])


_SCENARIOS = _load_scenarios()


def _resource_from(spec: dict[str, Any]) -> Resource[str]:
    rid = spec["id"]
    cap = spec.get("max_in_flight")
    return Resource(resource_id=rid, value=rid, max_in_flight=cap)


def _build_pool(cfg: dict[str, Any], clock: VirtualClock) -> Pool[str]:
    resources = [_resource_from(r) for r in cfg["resources"]]
    kwargs: dict[str, Any] = {
        "resources": resources,
        "max_attempts": cfg.get("max_attempts", 3),
        "cooldown_table": tuple(float(x) for x in cfg["cooldown_table"]),
        "strategy": STRATEGY[cfg.get("strategy", "least_in_flight")],
    }
    if cfg.get("cancel_siblings") is False:
        kwargs["cancel_siblings"] = False
    pool = Pool(**kwargs)
    pool._now = clock.now  # noqa: SLF001
    pool._sleep = clock.sleep  # noqa: SLF001
    return pool


class _Run:
    def __init__(self, run_id: str) -> None:
        self.id = run_id
        self.attempt = 0
        self.acquired: str | None = None
        self.gate = asyncio.Event()
        self.task: asyncio.Task[Any] | None = None
        self.outcome: str | None = None
        self.max_attempts: int | None = None
        self.hold = False


class _Driver:
    def __init__(self, scenario: dict[str, Any]) -> None:
        self.scenario = scenario
        self.clock = VirtualClock()
        self.pool = _build_pool(scenario["pool"], self.clock)
        self.script: list[dict[str, Any]] = scenario.get("script") or []
        self.runs: dict[str, _Run] = {}
        self.acquired_order: list[str] = []

    def _row(self, run_id: str, attempt: int, resource_id: str) -> dict[str, Any]:
        for row in self.script:
            if (
                row.get("run") == run_id
                and int(row.get("attempt", 0)) == attempt
                and row.get("resource") == resource_id
            ):
                return row
        return {"signal": "ok"}

    def _operation(self, run: _Run) -> Any:
        async def op(resource: Resource[str]) -> str:
            run.attempt += 1
            run.acquired = resource.resource_id
            row = self._row(run.id, run.attempt, resource.resource_id)
            if row.get("hold"):
                await run.gate.wait()
            signal = row.get("signal", "ok")
            if signal == "cooldown":
                dur = row.get("duration")
                raise CooldownResource(
                    cooldown_seconds=None if dur is None else float(dur)
                )
            if signal == "disable":
                raise DisableResource()
            if signal == "error":
                raise RuntimeError("conformance-operation-error")
            return resource.value

        return op

    def _opts(self, run: _Run) -> dict[str, Any]:
        cfg = self.scenario["pool"]
        opts: dict[str, Any] = {
            "retry_delay": float(cfg.get("retry_delay", 0)),
            "wait_for_cooldown": bool(cfg.get("wait_for_cooldown", False)),
        }
        if run.max_attempts is not None:
            opts["max_attempts"] = run.max_attempts
        return opts

    async def _start(self, run_id: str, hold: bool, max_attempts: int | None) -> _Run:
        run = self.runs.setdefault(run_id, _Run(run_id))
        run.hold = hold
        run.max_attempts = max_attempts
        run.gate = asyncio.Event()
        if not hold:
            run.gate.set()

        async def wrapped() -> None:
            try:
                await self.pool.run(self._operation(run), **self._opts(run))
            except PoolExhausted:
                run.outcome = "exhausted"
            except RuntimeError:
                run.outcome = "error"
            except asyncio.CancelledError:
                run.outcome = "cancelled"
                raise
            else:
                run.outcome = "ok"

        run.task = asyncio.create_task(wrapped())
        await self._settle(run)
        return run

    async def _settle(self, run: _Run) -> None:
        for _ in range(10_000):
            assert run.task is not None
            if run.task.done() or run.acquired is not None or self.clock.n_sleepers:
                await asyncio.sleep(0)
                return
            await asyncio.sleep(0)
        raise RuntimeError(f"run {run.id} did not start")

    async def _await_run(self, run_id: str) -> None:
        run = self.runs[run_id]
        assert run.task is not None
        await run.task

    async def _pump_inflight(self, resource_id: str, count: int) -> None:
        for _ in range(10_000):
            snap = self.pool.snapshot()
            if snap.get(resource_id, {}).get("in_flight") == count:
                return
            await asyncio.sleep(0)
        raise RuntimeError(
            f"in_flight {resource_id} never reached {count}: {self.pool.snapshot()}"
        )

    async def run_steps(self) -> None:
        for step in self.scenario["steps"]:
            action = step["action"]
            if action == "run":
                run = await self._start(
                    step["id"], hold=False, max_attempts=step.get("max_attempts")
                )
                await self._await_run(run.id)
            elif action == "spawn":
                await self._start(
                    step["id"],
                    hold=bool(step.get("hold", False)),
                    max_attempts=step.get("max_attempts"),
                )
            elif action == "await":
                await self._await_run(step["id"])
            elif action == "release":
                self.runs[step["id"]].gate.set()
                await asyncio.sleep(0)
            elif action == "advance":
                self.clock.advance(float(step["seconds"]))
                await asyncio.sleep(0)
            elif action == "wait_in_flight":
                await self._pump_inflight(step["resource"], int(step["count"]))
            elif action == "enable":
                await self.pool.enable(step["resource"])
            elif action == "disable":
                await self.pool.disable(step["resource"])
            elif action == "remove":
                await self.pool.remove(step["resource"])
            elif action == "add":
                await self.pool.add(
                    step["resource"],
                    step["resource"],
                    max_in_flight=step.get("max_in_flight"),
                )
            elif action == "snapshot_acquired":
                self.acquired_order = [
                    self.runs[s["id"]].acquired or ""
                    for s in self.scenario["steps"]
                    if s["action"] == "spawn"
                ]
            else:
                raise ValueError(f"unknown action {action}")

    def actual(self) -> dict[str, Any]:
        snap = self.pool.snapshot()
        stats = self.pool.stats()
        resources = {}
        for rid, row in snap.items():
            resources[rid] = {
                "status": row["status"],
                "consecutive_cooldown": row["consecutive_cooldown"],
                "in_flight": row["in_flight"],
                "cooldown_remaining": row["cooldown_seconds_remaining"],
            }
        calls = [
            {"id": rid, "outcome": run.outcome}
            for rid, run in self.runs.items()
        ]
        return {
            "calls": calls,
            "resources": resources,
            "pool": {
                "attempts": stats.attempts,
                "successes": stats.successes,
                "cooldowns": stats.cooldowns,
                "disables": stats.disables,
                "sibling_cancels": stats.sibling_cancels,
                "runs_ok": stats.runs_ok,
                "runs_exhausted": stats.runs_exhausted,
                "runs_error": stats.runs_error,
                "in_flight": stats.in_flight,
            },
            "acquired": self.acquired_order,
        }


def _check(actual: dict[str, Any], expect: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    exp_calls = {c["id"]: c["outcome"] for c in expect.get("calls") or []}
    got_calls = {c["id"]: c["outcome"] for c in actual["calls"]}
    for cid, outcome in exp_calls.items():
        if got_calls.get(cid) != outcome:
            errors.append(f"call {cid}: expected {outcome}, got {got_calls.get(cid)}")
    for rid, exp in (expect.get("resources") or {}).items():
        got = actual["resources"].get(rid)
        if got is None:
            errors.append(f"resource {rid} missing")
            continue
        for key, val in exp.items():
            if key == "cooldown_remaining":
                if abs(float(got[key]) - float(val)) > 1e-6:
                    errors.append(
                        f"{rid}.{key}: expected {val}, got {got[key]}"
                    )
            elif got[key] != val:
                errors.append(f"{rid}.{key}: expected {val}, got {got[key]}")
    for key, val in (expect.get("pool") or {}).items():
        if actual["pool"].get(key) != val:
            errors.append(
                f"pool.{key}: expected {val}, got {actual['pool'].get(key)}"
            )
    if "acquired" in expect and actual.get("acquired") != expect["acquired"]:
        errors.append(f"acquired: expected {expect['acquired']}, got {actual.get('acquired')}")
    return errors


@pytest.mark.skipif(not _SCENARIOS, reason="rotapool-spec scenarios.yaml not found")
@pytest.mark.parametrize("scenario", _SCENARIOS, ids=lambda s: s["id"])
async def test_conformance_scenario(scenario: dict[str, Any]) -> None:
    try:
        driver = _Driver(scenario)
    except TypeError as exc:
        pytest.fail(f"pool construction failed ({exc}); actual=construction_error")
    await driver.run_steps()
    actual = driver.actual()
    errors = _check(actual, scenario["expect"])
    if errors:
        pytest.fail("\n".join(errors) + f"\nactual={actual!r}")
