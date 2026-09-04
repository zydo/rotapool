# rotapool

[![CI](https://github.com/zydo/rotapool/actions/workflows/ci.yml/badge.svg)](https://github.com/zydo/rotapool/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/rotapool.svg)](https://pypi.org/project/rotapool/)

Generic async resource pool with health-aware selection, cooldown, and retry

A pool of arbitrary resources — API keys, OAuth credentials, proxy URLs, HTTP
clients, LLM providers, inference or RPC endpoints, browser sessions, GPU
workers — that rotates across them as they become unhealthy. Every call is
also health evidence: callers signal whether the selected resource should stay
healthy, cool down temporarily, or be disabled.

| Signal                              | Meaning                                |
| ----------------------------------- | -------------------------------------- |
| normal return / any other exception | Resource is healthy                    |
| `RetryOperation`                    | Transient glitch; retry, no cooldown   |
| `CooldownResource`                  | Temporarily overloaded, e.g. HTTP 429  |
| `DisableResource`                   | Permanently unusable, e.g. revoked key |

> **Designed for AI coding agents.** `rotapool` exposes machine-readable usage notes via [agent-readable](https://github.com/zydo/agent-readable), including operation contracts, do/don't rules, anti-patterns, and failure modes.
>
> ```bash
> npx skills add zydo/skills --skill agent-readable
> ```

rotapool provides **at-least-once** execution semantics. When a usage signals
cooldown or disable, younger in-flight usages on the same resource are
cancelled and retried elsewhere; those cancelled operations MAY already have
reached the backend. Operations MUST be idempotent, or you MUST construct the
pool with `cancel_siblings=False`.

## What it is not

- **not a database connection pool** -- resources are not checked out and returned; a resource serves many concurrent usages simultaneously.
- **not a generic object-leasing pool** -- there is no lease/return model.
- **not merely a rate limiter** -- health feedback comes from your operation's outcome, not a token bucket.
- **not merely a retry library** -- retry selection is health-aware and per-resource, with cooldown escalation and sibling cancellation.

## Install

```bash
pip install rotapool
# or
uv add rotapool
```

Requires Python 3.10+. Zero runtime dependencies. Optional extras: `pip install "rotapool[agent]"` for [agent-readable](https://github.com/zydo/agent-readable), `pip install "rotapool[prometheus]"` for a Prometheus collector over `pool.stats()`. A runnable scrape is in [`examples/prometheus_pool.py`](examples/prometheus_pool.py).

## Quick Start

```python
import httpx

from rotapool import CooldownResource, DisableResource, Pool, Resource, RetryOperation

client = httpx.AsyncClient()

pool = Pool(
    resources=[
        Resource(resource_id="key-1", value="sk-aaa"),
        Resource(resource_id="key-2", value="sk-bbb"),
        Resource(resource_id="key-3", value="sk-ccc"),
    ],
    max_attempts=3,
    cooldown_table=(30.0, 120.0, 300.0, 600.0),
)

async def call_upstream(resource, url, payload):
    try:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {resource.value}"},
            json=payload,
        )
    except httpx.TransportError:
        raise RetryOperation(reason="transport")

    if resp.status_code == 429:
        raise CooldownResource(reason="rate limited")
    if resp.status_code == 401:
        raise DisableResource(reason="invalid key")

    return resp.json()

result = await pool.run(
    lambda resource: call_upstream(
        resource, "https://api.example.com/v1/chat", {"prompt": "hi"}
    ),
)
```

The HTTP client lives **outside** the operation and is captured by the
closure. `@pool.use()` is a decorator shim over `pool.run()`; see
[usage](docs/usage.md). Runnable versions of this (no httpx) and of the
Prometheus extra live in [`examples/`](examples/).

## Documentation

- [Usage guide](docs/usage.md) covers pool initialization, `@pool.use()`, direct `pool.run()`, resource types, accepted operation shapes, and observability.
- [Behavior guide](docs/behavior.md) explains selection strategies, cooldown escalation, retry behavior, in-flight cancellation, cancellation discrimination, admin control, and metrics (`snapshot()`, `stats()`, optional Prometheus collector).
- [API reference](docs/api.md) documents `Pool`, `Resource`, `snapshot()`, `stats()`, admin methods, exceptions, and the optional Prometheus collector.
- [Pitfalls and testing](docs/pitfalls-and-testing.md) lists common anti-patterns, cancellation gotchas, test commands, and license information.
- [Changelog](CHANGELOG.md) lists released versions.
- [`examples/`](examples/) has runnable scripts: `basic_usage.py` (`use()` / `run()`), `prometheus_pool.py` (optional scrape).

## Core Concepts

- Each `Pool` owns a set of `Resource[T]` objects. `T` can be a bearer token, proxy URL, browser session, GPU worker, client object, or any other value.
- Each `run()` attempt receives one selected `Resource`.
- Raising `CooldownResource` marks that resource temporarily unavailable and retries elsewhere when possible.
- Raising `DisableResource` removes that resource from selection until `enable()` is called.
- Raising `RetryOperation` retries without cooldown or sibling cancel.
- Any other exception is treated as caller or business failure, not resource failure, and propagates.
- `@pool.use()` is a decorator convenience over `pool.run()`.

## Testing

```bash
uv sync --all-extras
uv run pytest --cov
```

See [pitfalls and testing](docs/pitfalls-and-testing.md) for pip-based setup and additional notes.

## License

MIT
