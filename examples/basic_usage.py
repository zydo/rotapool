"""Minimal ``@pool.use()`` and ``pool.run()`` -- no extra dependencies.

    python examples/basic_usage.py

The markdown snippets in the README and usage guide are the annotated API
shape (they assume httpx and a live URL). This file is the same two entry
points, runnable as a script, with a fake upstream.
"""

from __future__ import annotations

import asyncio
import random

from rotapool import CooldownResource, DisableResource, Pool, Resource

pool = Pool(
    resources=[
        Resource(resource_id="key-1", value="sk-aaa"),
        Resource(resource_id="key-2", value="sk-bbb"),
        Resource(resource_id="key-3", value="sk-ccc"),
    ],
    max_attempts=3,
    cooldown_table=(1.0, 2.0, 5.0),
)


def fake_upstream(api_key: str, payload: str) -> str:
    """Stand-in for an HTTP call. Raise the same signals a real client would."""
    roll = random.random()
    if roll < 0.25:
        raise CooldownResource(reason="rate limited")
    if roll < 0.27:
        raise DisableResource(reason="revoked")
    return f"{payload} via {api_key}"


# Option 1 -- decorator. Resource is injected as the first argument.
# Policy knobs here are fixed at decoration time.
@pool.use()
async def call_upstream(resource: Resource[str], payload: str) -> str:
    return fake_upstream(resource.value, payload)


# Option 2 -- direct run(). Use this when a knob must vary per call
# (request_id, a deadline, a one-off max_attempts).
async def call_upstream_run(resource: Resource[str], payload: str) -> str:  # noqa: S7503
    return fake_upstream(resource.value, payload)


async def main() -> None:
    via_use = await call_upstream("hello")
    print("use():", via_use)

    via_run = await pool.run(
        lambda resource: call_upstream_run(resource, "hello"),
        request_id="req-demo",
    )
    print("run():", via_run)


if __name__ == "__main__":
    asyncio.run(main())
