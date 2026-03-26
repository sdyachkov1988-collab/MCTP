# MCTP — Контекст для нового чата

## Назначение
MCTP — модульная spot-платформа для deterministic backtest, paper execution и Binance Spot TESTNET runtime.

## Подтвержденная стадия
- подтвержденная стадия: `v1.7`

## Что завершено
- `v0.0`-`v0.12`: core, execution, risk, sizing, portfolio/accounting, storage, streams, backtest, analytics, indicators, strategy contract, paper runtime
- `v1.0`: testnet adapter v1, authenticated REST/WS path, private user stream, listenKey lifecycle, real OCO submit, bounded account refresh, delisting detection plumbing
- `v1.1`: exchange-authoritative balance cache handling, external OCO cancel monitoring, software-stop fallback, symbol-change procedure
- `v1.2`: startup synchronization gate, startup balance refresh, missing-basis handling, startup OCO consistency, gap-risk startup handling, restart-time protection guarantee
- `v1.3`: restart reconciliation, reconnect balance refresh, OCO outage-fill reconciliation with cached `bnb_rate`, manual-trade detection and basis-adjustment path
- `v1.4`: structured JSON logs, hash-chain audit log, before/after decision capture, heartbeat, latency/memory monitoring, smoke-only Strategy Performance Monitor on testnet
- `v1.5`: structured alerting with primary/backup delivery and runtime-owned heartbeat-timeout watchdog
- `v1.6`: safety controls, closed-candle enforcement, OCO pre-submit validation, controlled snapshot transitions, monotonic order-status handling, supervised critical background tasks, retry-safe delisting forced exit, bounded execution-state retention, conservative startup OCO ambiguity handling
- post-`v1.6` consistency hardening: single-source exchange-truth application split, pending/in-flight submit guard, cancel-active-OCO-before-direct-sell handling, conservative single-unknown startup OCO handling, broader restart consistency for outstanding order / partial-fill state
- `v1.7` verification steps completed so far: scenario matrix, chaos/integration coverage, 4 independent WS stream checks, and operator readiness artifacts (runbook, checklist, incident journal, operator intervention rules, BALANCE_CACHE_TTL verification)
- transition gate added: explicit `v1.7` -> `v2.0` readiness boundary document and docs index for artifact discovery

## Важные границы
- только `Decimal` для финансовой логики
- только UTC-aware timestamps
- `Symbol` остается типизированным
- strategy layer остается read-only
- paper mode остается отдельным от testnet mode
- Binance Spot TESTNET используется только для integration/mechanics, не для доказательства market realism

## Что явно вне текущего scope
- `v2.0+` first-live implementation work beyond the transition boundary
- production live trading readiness
- multi-pair
- futures
