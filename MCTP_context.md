# MCTP — Контекст для нового чата

## Назначение
MCTP — модульная spot-платформа для deterministic backtest, paper execution и Binance Spot TESTNET runtime.

## Подтверждённая стадия
- подтверждённая стадия: `v2.0-step2-fix` (accepted baseline)
- 503 теста — все зелёные (проверено локально)

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
- `v1.7`: scenario matrix, chaos/integration coverage, 4 independent WS stream checks, operator readiness artifacts (runbook, checklist, incident journal, operator intervention rules, BALANCE_CACHE_TTL verification), transition gate document
- `v2.0-step1`: `BtcUsdtMtfV20Strategy` (BTCUSDT only, read-only, D1/H4/H1/M15), MTF агрегатор `mctp/strategy/mtf.py` (M15→H1/H4/D1, closed candles only, UTC aligned), strategy plugin в `BacktestEngine` (STRATEGY_ID_V20_BTCUSDT_MTF), MTF wiring в `PaperRuntime`, тесты `test_v2_0_mtf_strategy.py` (10 сценариев)
- `v2.0-patch1`: три CRITICAL фикса — `run_testnet_platform.py` использует `BtcUsdtMtfV20Strategy`, `_persist_snapshot()` защищён try/catch + alert, boundary leakage устранён (`ExchangeOrderStatus`/`ListOrderStatus`/`ListStatusType`/`ContingencyType` enums добавлены)
- `v2.0-step2`: testnet wiring — `LiveMtfAggregator`, `MtfKlineManager`, 4 независимых kline канала M15/H1/H4/D1, REST priming, startup gate блокирует READY до warmup, M15 gap detection, per-TF staleness, 15 integration тестов
- `v2.0-step2-fix`: 5 audit fixes поверх `v2.0-step2`; текущий `HEAD`/tag репозитория
- `v2.0 backtest wiring` (accepted baseline): `run_backtest_csv.py` поддерживает `--strategy`, default остаётся legacy path, v2.0 backtest path использует согласованный protective OCO lifecycle и direct SELL явно отменяет локальный protective OCO без противоречивого двойного exit state

## Архитектурные инварианты
- только `Decimal` для финансовой логики
- только UTC-aware timestamps
- `Symbol` остаётся типизированным объектом
- strategy layer остаётся read-only
- exchange-specific преобразования остаются внутри adapter/runtime boundary
- paper mode и testnet mode остаются разделёнными
- константы только из `mctp/core/constants.py`

## Известные проблемы из аудита

Подтверждённый stabilization batch закрыт:
- `schema_version` добавлен в `BalanceCacheStore` и `OrderStore`
- M15 gap / dropped bucket path в `mctp/strategy/mtf.py` теперь пишет warning
- `Timeframe.MONTHLY` добавлен на enum/constants уровне
- `float(T_CANCEL)` убран без изменения семантики
- EMA использует явный SMA seed
- CCI scaling constant вынесен в `mctp/core/constants.py`

## Текущий фокус
Accepted working baseline зафиксирован на `v2.0-step2-fix` с завершённым `v2.0 backtest wiring`. Новый feature corridor после freeze ещё не зафиксирован.

## Роли инструментов в работе
- **Claude (чат)** — архитектурные решения, roadmap compliance, системный аудит, стратегические решения
- **Claude Code** — реализация, file-level аудит, запуск тестов, работа с Git
- **ChatGPT** — матрица контрактов 01-57 (только при наличии GitHub доступа)
- **DeepSeek** — второе мнение по коду
- **Gemini / Grok** — не использовать для строгого аудита (позитивное смещение)
- **Cursor** — локальная работа с конкретными файлами

## Матрица контрактов (статус на v2.0-step2-fix)
Контракты 44-53 — плановые заглушки согласно roadmap (не баги):
- 44: критерии фьючерсов — оценивается при v2.2
- 45-53: мультипары, ML, on-chain, anomaly, research — фазы v2.3-v5.0
- 54: адаптивный риск — реализован частично (уровни 1,3,5.1,5.2,6,7,9), остальное фазируется
- 07: 7 TF — `MONTHLY` теперь добавлен на enum/constants уровне; широкий downstream scope не расширялся

## Что явно вне текущего scope
- production live trading readiness
- multi-pair
- futures
- regime / anomaly / on-chain / ML
- allocation engine

## Сохранённые версии
- `v1.7-final` — чистая база до v2.0 (zip сохранён отдельно)
- `v2.0-step1` — v1.7 + стратегия + MTF агрегатор (458 тестов зелёные)
- `v2.0-step2` — testnet wiring (478 тестов зелёные)
- `v2.0-step2-fix` — accepted baseline: audit fixes over step2 + completed `v2.0 backtest wiring` + closed stabilization tails (503 теста зелёные)
