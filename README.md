# rotapool

[![CI](https://github.com/zydo/rotapool/actions/workflows/ci.yml/badge.svg)](https://github.com/zydo/rotapool/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/rotapool.svg)](https://pypi.org/project/rotapool/)

Async resource pool with inline health feedback, automatic cooldown, and retry -- for API keys, proxies, GPU workers, or anything that can rate-limit you or go down.

`rotapool` is designed for resources where every call is also useful health evidence. Instead of relying on a separate prober, callers signal whether the selected resource should stay healthy, cool down temporarily, or be disabled.

| Signal                              | Meaning                                 |
| ----------------------------------- | --------------------------------------- |
| normal return / any other exception | Resource is healthy                     |
| `CooldownResource`                  | Temporarily overloaded, e.g. HTTP 429   |
| `DisableResource`                   | Permanently unusable, e.g. revoked key  |

> **Designed for AI coding agents.** `rotapool` exposes machine-readable usage notes via [agent-readable](https://github.com/zydo/agent-readable), including operation contracts, do/don't rules, anti-patterns, and failure modes.
>
> ```bash
> npx skills add zydo/skills --skill agent-readable
> ```

## Install

```bash
pip install rotapool
# or
uv add rotapool
```

Requires Python 3.10+. Zero runtime dependencies; `pip install "rotapool[agent]"` adds the optional [agent-readable](https://github.com/zydo/agent-readable) integration.

## Quick Start

```python
import httpx

from rotapool import CooldownResource, DisableResource, Pool, Resource

pool = Pool(
    resources=[
        Resource(resource_id="key-1", value="sk-aaa"),
        Resource(resource_id="key-2", value="sk-bbb"),
        Resource(resource_id="key-3", value="sk-ccc"),
    ],
    max_attempts=3,
    cooldown_table=(30.0, 120.0, 300.0, 600.0),
)

@pool.use()
async def call_upstream(resource, url, payload):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={"Authorization": f"Bearer {resource.value}"},
            json=payload,
        )

    if resp.status_code == 429:
        raise CooldownResource(reason="rate limited")
    if resp.status_code == 401:
        raise DisableResource(reason="invalid key")

    return resp.json()

result = await call_upstream("https://api.example.com/v1/chat", {"prompt": "hi"})
```

## Documentation

- [Usage guide](docs/usage.md) covers pool initialization, `@pool.use()`, direct `pool.run()`, resource types, and accepted operation shapes.
- [Behavior guide](docs/behavior.md) explains selection strategies, cooldown escalation, retry behavior, in-flight cancellation, cancellation discrimination, and admin control.
- [API reference](docs/api.md) documents `Pool`, `Resource`, `snapshot()`, admin methods, and exceptions.
- [Pitfalls and testing](docs/pitfalls-and-testing.md) lists common anti-patterns, cancellation gotchas, test commands, and license information.

## Core Concepts

- Each `Pool` owns a set of `Resource[T]` objects. `T` can be a bearer token, proxy URL, browser session, GPU worker, client object, or any other value.
- Each `run()` attempt receives one selected `Resource`.
- Raising `CooldownResource` marks that resource temporarily unavailable and retries elsewhere when possible.
- Raising `DisableResource` removes that resource from selection until `enable()` is called.
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
