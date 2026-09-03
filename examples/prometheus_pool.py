"""Minimal Prometheus scrape of a rotapool.Pool.

Requires the optional extra (not a core dependency):

    pip install "rotapool[prometheus]"
    python examples/prometheus_pool.py

Print one scrape to stdout instead of serving:

    python examples/prometheus_pool.py --once

Then:

    curl -s http://127.0.0.1:8000/metrics | grep rotapool_
"""

from __future__ import annotations

import argparse
import asyncio
import random

from prometheus_client import CollectorRegistry, generate_latest, start_http_server

from rotapool import CooldownResource, DisableResource, Pool, PoolExhausted, Resource
from rotapool.prometheus import PoolCollector

# Short table so --once and a few seconds of serving show cooling_down series.
_POOL = Pool(
    resources=[
        Resource(resource_id="key-a", value="sk-aaa", max_in_flight=4),
        Resource(resource_id="key-b", value="sk-bbb", max_in_flight=4),
        Resource(resource_id="key-c", value="sk-ccc"),
    ],
    max_attempts=3,
    cooldown_table=(2.0, 5.0, 10.0),
)


async def call_upstream(resource: Resource[str]) -> str:
    """Stand-in for an HTTP call. Signals health the way a real client would."""
    await asyncio.sleep(random.uniform(0.01, 0.05))
    roll = random.random()
    if roll < 0.12:
        raise CooldownResource(reason="rate limited")
    if roll < 0.13:
        raise DisableResource(reason="revoked")
    if roll < 0.18:
        raise RuntimeError("business error")
    return resource.value


async def worker(stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            await _POOL.run(call_upstream)
        except (PoolExhausted, RuntimeError):
            pass
        await asyncio.sleep(random.uniform(0.02, 0.1))


async def drive(seconds: float) -> None:
    stop = asyncio.Event()
    tasks = [asyncio.create_task(worker(stop)) for _ in range(8)]
    try:
        await asyncio.sleep(seconds)
    finally:
        stop.set()
        await asyncio.gather(*tasks)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port", type=int, default=8000, help="HTTP port for /metrics (default 8000)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a short burst and print generate_latest() instead of serving",
    )
    args = parser.parse_args()

    # Dedicated registry so /metrics is only rotapool series (no process_*).
    # One collector per registry; the pool label distinguishes this pool.
    registry = CollectorRegistry()
    PoolCollector(_POOL, pool_name="demo").register(registry)

    if args.once:
        asyncio.run(drive(0.8))
        print(generate_latest(registry).decode(), end="")
        return

    start_http_server(args.port, registry=registry)
    print(f"scrape http://127.0.0.1:{args.port}/metrics  (Ctrl-C to stop)")
    try:
        asyncio.run(drive(3600.0))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
