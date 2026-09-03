"""Tests for rotapool.prometheus.PoolCollector."""

from __future__ import annotations

import math

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from rotapool import CooldownResource, Pool, PoolExhausted, Resource
from rotapool.models import RESOURCE_STATUSES, RUN_OUTCOMES
from rotapool.prometheus import (
    ATTEMPTS_TOTAL,
    COOLDOWNS_TOTAL,
    DISABLES_TOTAL,
    ELIGIBLE,
    IN_FLIGHT,
    RESOURCE_ACQUIRES_TOTAL,
    RESOURCE_COOLDOWN_REMAINING_SECONDS,
    RESOURCE_COOLDOWNS_TOTAL,
    RESOURCE_IN_FLIGHT,
    RESOURCE_MAX_IN_FLIGHT,
    RESOURCE_STATUS,
    RESOURCES,
    RUNS_TOTAL,
    SATURATED,
    SUCCESSES_TOTAL,
    PoolCollector,
)


def _res(n: int, **kw: object) -> list[Resource[str]]:
    return [
        Resource(resource_id=f"r{i}", value=f"v{i}", **kw)  # type: ignore[arg-type]
        for i in range(n)
    ]


def _sample(
    registry: CollectorRegistry, name: str, labels: dict[str, str]
) -> float | None:
    return registry.get_sample_value(name, labels)


def _registered(*pools: tuple[Pool[str], str]) -> CollectorRegistry:
    registry = CollectorRegistry()
    if len(pools) == 1:
        pool, name = pools[0]
        PoolCollector(pool, pool_name=name).register(registry)
    else:
        PoolCollector(pools={name: pool for pool, name in pools}).register(registry)
    return registry


class TestPoolCollector:
    def test_empty_collector_scrapes(self) -> None:
        registry = CollectorRegistry()
        PoolCollector().register(registry)
        payload = generate_latest(registry).decode()
        assert RUNS_TOTAL in payload

    def test_rejects_both_pool_and_pools(self) -> None:
        pool = Pool(resources=_res(1))
        with pytest.raises(TypeError, match="not both"):
            PoolCollector(pool, pools={"x": pool})

    def test_rejects_empty_pool_name(self) -> None:
        pool = Pool(resources=_res(1))
        with pytest.raises(ValueError, match="non-empty"):
            PoolCollector(pool, pool_name="")
        c = PoolCollector()
        with pytest.raises(ValueError, match="non-empty"):
            c.add(pool, pool_name="")

    def test_rejects_duplicate_pool_name(self) -> None:
        p1 = Pool(resources=_res(1))
        p2 = Pool(resources=_res(1))
        c = PoolCollector(p1, pool_name="a")
        with pytest.raises(ValueError, match="duplicate pool_name"):
            c.add(p2, pool_name="a")
        assert c.add(p2, pool_name="b") is c

    def test_agent_notes_warn_against_double_register(self) -> None:
        notes = PoolCollector.__agent_notes__()
        assert "rotapool.prometheus" in notes
        assert "not ``rotapool``" in notes or "not from ``rotapool``" in notes
        assert "stats()" in notes
        assert "snapshot()" in notes
        assert "second" in notes.lower() or "collide" in notes.lower()

    def test_second_collector_collides_on_same_registry(self) -> None:
        registry = CollectorRegistry()
        PoolCollector(Pool(resources=_res(1)), pool_name="a").register(registry)
        with pytest.raises(ValueError):
            PoolCollector(Pool(resources=_res(1)), pool_name="b").register(registry)

    def test_register_defaults_to_global_registry(self) -> None:
        from prometheus_client import REGISTRY

        collector = PoolCollector(Pool(resources=_res(1)), pool_name="global-default")
        collector.register()
        try:
            payload = generate_latest(REGISTRY).decode()
            assert RUNS_TOTAL in payload
        finally:
            REGISTRY.unregister(collector)

    async def test_gauges_and_run_ok(self) -> None:
        pool = Pool(
            resources=[
                Resource(resource_id="r0", value="v0", max_in_flight=4),
                Resource(resource_id="r1", value="v1"),
            ]
        )

        async def ok(r: Resource[str]) -> str:
            return r.value

        assert await pool.run(ok) == "v0"
        registry = _registered((pool, "api_keys"))

        assert _sample(registry, RUNS_TOTAL, {"pool": "api_keys", "outcome": "ok"}) == 1
        for outcome in RUN_OUTCOMES:
            value = _sample(
                registry, RUNS_TOTAL, {"pool": "api_keys", "outcome": outcome}
            )
            assert value == (1.0 if outcome == "ok" else 0.0)
        assert _sample(registry, ATTEMPTS_TOTAL, {"pool": "api_keys"}) == 1
        assert _sample(registry, SUCCESSES_TOTAL, {"pool": "api_keys"}) == 1
        assert _sample(registry, IN_FLIGHT, {"pool": "api_keys"}) == 0
        assert _sample(registry, ELIGIBLE, {"pool": "api_keys"}) == 2
        assert _sample(registry, SATURATED, {"pool": "api_keys"}) == 0
        assert (
            _sample(registry, RESOURCES, {"pool": "api_keys", "status": "healthy"}) == 2
        )
        for status in RESOURCE_STATUSES:
            expected = 1.0 if status == "healthy" else 0.0
            assert (
                _sample(
                    registry,
                    RESOURCE_STATUS,
                    {"pool": "api_keys", "resource_id": "r0", "status": status},
                )
                == expected
            )
        assert (
            _sample(
                registry,
                RESOURCE_IN_FLIGHT,
                {"pool": "api_keys", "resource_id": "r0"},
            )
            == 0
        )
        assert (
            _sample(
                registry,
                RESOURCE_MAX_IN_FLIGHT,
                {"pool": "api_keys", "resource_id": "r0"},
            )
            == 4
        )
        inf = _sample(
            registry,
            RESOURCE_MAX_IN_FLIGHT,
            {"pool": "api_keys", "resource_id": "r1"},
        )
        assert inf is not None
        assert math.isinf(inf)
        assert (
            _sample(
                registry,
                RESOURCE_ACQUIRES_TOTAL,
                {"pool": "api_keys", "resource_id": "r0"},
            )
            == 1
        )
        assert (
            _sample(
                registry,
                RESOURCE_ACQUIRES_TOTAL,
                {"pool": "api_keys", "resource_id": "r1"},
            )
            == 0
        )

    async def test_cooldown_and_remove_keeps_pool_counter(self) -> None:
        pool = Pool(
            resources=_res(2),
            cooldown_table=(30.0, 60.0),
        )

        async def cool(_: Resource[str]) -> None:
            raise CooldownResource(reason="hot")

        with pytest.raises(PoolExhausted):
            await pool.run(cool, max_attempts=1)

        registry = CollectorRegistry()
        collector = PoolCollector(pool, pool_name="keys")
        collector.register(registry)
        assert _sample(registry, COOLDOWNS_TOTAL, {"pool": "keys"}) == 1
        assert (
            _sample(
                registry,
                RESOURCE_COOLDOWNS_TOTAL,
                {"pool": "keys", "resource_id": "r0"},
            )
            == 1
        )
        remaining = _sample(
            registry,
            RESOURCE_COOLDOWN_REMAINING_SECONDS,
            {"pool": "keys", "resource_id": "r0"},
        )
        assert remaining is not None
        assert remaining > 0
        assert (
            _sample(
                registry,
                RESOURCE_STATUS,
                {"pool": "keys", "resource_id": "r0", "status": "cooling_down"},
            )
            == 1
        )

        await pool.remove("r0")
        assert _sample(registry, COOLDOWNS_TOTAL, {"pool": "keys"}) == 1
        assert (
            _sample(
                registry,
                RESOURCE_COOLDOWNS_TOTAL,
                {"pool": "keys", "resource_id": "r0"},
            )
            is None
        )
        assert (
            _sample(
                registry,
                RESOURCE_STATUS,
                {"pool": "keys", "resource_id": "r0", "status": "cooling_down"},
            )
            is None
        )

    async def test_two_pools_one_collector(self) -> None:
        a = Pool(resources=_res(1))
        b = Pool(resources=_res(1))

        async def ok(r: Resource[str]) -> str:
            return r.value

        await a.run(ok)
        registry = _registered((a, "alpha"), (b, "beta"))
        assert _sample(registry, RUNS_TOTAL, {"pool": "alpha", "outcome": "ok"}) == 1
        assert _sample(registry, RUNS_TOTAL, {"pool": "beta", "outcome": "ok"}) == 0
        assert _sample(registry, ELIGIBLE, {"pool": "alpha"}) == 1
        assert _sample(registry, ELIGIBLE, {"pool": "beta"}) == 1

    async def test_admin_disable(self) -> None:
        pool = Pool(resources=_res(1))
        await pool.disable("r0")
        registry = _registered((pool, "p"))
        assert _sample(registry, DISABLES_TOTAL, {"pool": "p"}) == 1
        assert _sample(registry, RESOURCES, {"pool": "p", "status": "disabled"}) == 1
        assert _sample(registry, ELIGIBLE, {"pool": "p"}) == 0
