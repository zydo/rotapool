# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Pool(..., cancel_siblings=True)` -- when False, cooldown/disable signals
  do not cancel younger in-flight usages on the same resource. Default True
  matches 0.4.0. Non-idempotent operations should pass False (at-least-once
  sibling cancellation).

### Changed

- `on_state_change` is invoked after the pool lock is released. A fourth
  monotonic `seq` argument is accepted; 3-parameter hooks still work with a
  `DeprecationWarning`.
- Internally cancelled attempts that exhaust the budget surface a
  `CooldownResource` / `DisableResource` as the last error, not
  `CancelledError`.
- Applying a cooldown signal wakes `wait_for_cooldown` sleepers so they
  recompute the earliest expiry.

## [0.4.0] - 2026-09-02

### Added

- `pool.stats()` -- lock-free metrics snapshot. Prometheus-ready gauges
  (numeric, `by_status` always has all three keys, `status_one_hot()`,
  `max_in_flight_gauge` is `+Inf` when unbounded) plus monotonic counters.
  Pool-level counters survive `remove()`; per-resource counters are
  membership-scoped. `last_acquired_at` is omitted (process-monotonic, not a
  scrape timestamp). Returns frozen `PoolStats` / `ResourceStats` (do not
  construct them; import from `rotapool`).
- Optional extra `rotapool[prometheus]` (`prometheus_client`, not a core
  dependency). `from rotapool.prometheus import PoolCollector` -- register
  **one** collector per registry; distinguish pools with the `pool` label
  (`pools={...}` or `add()`). Frozen metric names are part of this extra's
  API; renaming them is a dashboard-breaking change. See `docs/api.md`.
- Runnable examples: `examples/basic_usage.py` (`use()` / `run()`) and
  `examples/prometheus_pool.py` (HTTP `/metrics` or `--once`).
- Agent-readable notes on `Pool`, `PoolStats`, `ResourceStats`, and
  `PoolCollector` covering scrape vs `snapshot()`, one collector per
  registry, and "do not construct stats dataclasses".

### Changed

- `snapshot()` stays the operator/JSON view. Use `stats()` (or
  `PoolCollector`) for Prometheus; do not scrape `last_acquired_at`.
- `remove()` drops the resource from `snapshot()` and `stats().resources`;
  pool-level counters are unchanged.
- Typing: `Callable` / `Awaitable` imported from `collections.abc`.
- Optional extra `agent-readable` floor raised to `>=0.3.1`.
- CI lints `examples/` and runs both example scripts on the test matrix.

### Notes

- Do not sum per-resource counters to rebuild pool totals (`rate()` breaks
  after `remove()`).
- Do not use `on_state_change` as a metrics bus (runs under the lock; misses
  success, exhaustion, retries, and membership).
- `CooldownResource.reason` / `DisableResource.reason` are not metrics
  labels.

## [0.3.0] - 2026-08-22

- `remove()` drops a resource and its value from the pool; in-flight usages
  drain. Re-add of the same id starts fresh except draining usages still
  count toward `max_in_flight`.
- `on_state_change` constructor hook for health-status transitions.
- `snapshot()` includes `max_in_flight`.

## [0.2.2] - 2026-07-04

- Harden pool input validation and usage ordering.

## [0.2.1] - 2026-06-26

- Dynamic `add()` of resources at runtime.

## [0.2.0] - 2026-06-11

- Opt-in `wait_for_cooldown` on `run()` / `use()` (jittered, admin-
  interruptible waits).
- Admin `enable()` / `disable()`.
- Validate `CooldownResource.cooldown_seconds` at construction.

## [0.1.2] - 2026-05-11

- Expand docs; loosen `agent-readable` pin to `>=0.1.0`.

## [0.1.1] - 2026-05-10

- `AgentReadableMixin` / `__agent_notes__` on `Pool`.
- PyPI publish workflow.

## [0.1.0] - 2026-05-06

- Initial release.

[0.4.0]: https://github.com/zydo/rotapool/releases/tag/v0.4.0
[0.3.0]: https://github.com/zydo/rotapool/releases/tag/v0.3.0
[0.2.2]: https://github.com/zydo/rotapool/releases/tag/v0.2.2
[0.2.1]: https://github.com/zydo/rotapool/releases/tag/v0.2.1
[0.2.0]: https://github.com/zydo/rotapool/releases/tag/v0.2.0
[0.1.2]: https://github.com/zydo/rotapool/releases/tag/v0.1.2
[0.1.1]: https://github.com/zydo/rotapool/releases/tag/v0.1.1
[0.1.0]: https://github.com/zydo/rotapool/releases/tag/v0.1.0
