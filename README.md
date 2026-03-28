# MCTP

## Что это
MCTP (`Modular Crypto Trading Platform`) — модульная spot-платформа для deterministic backtest, paper runtime и Binance Spot TESTNET runtime. Репозиторий построен вокруг типизированных доменных моделей, строгого учета, adapter-driven execution boundary и поэтапного roadmap-подхода.

## Текущая подтвержденная стадия
Подтвержденная стадия репозитория: `v2.0-step2-fix` (accepted baseline).

## Что уже реализовано
- `v0.0`-`v0.12`: core domain contracts, execution/risk/sizing, portfolio/accounting, storage, streams, backtest, analytics, indicator engine, strategy contract, paper runtime
- post-v0.12 maintenance: accounting/runtime polish, CSV backtest input, trade export, affordability fixes
- `v1.0`: Binance Spot TESTNET adapter v1, authenticated REST/WS path, private user stream, listenKey lifecycle, real OCO submit, 4 независимых stream lifecycles, bounded account refresh, delisting detection plumbing
- `v1.1`: exchange-authoritative balance cache handling, external OCO cancel monitoring, software-stop fallback, controlled symbol-change procedure
- `v1.2`: startup synchronization gate, startup balance refresh, missing-basis handling, startup OCO consistency, startup gap-risk logic, restart-time protection guarantee
- `v1.3`: restart reconciliation, reconnect balance refresh, outage OCO fill reconciliation with cached `bnb_rate`, manual-trade detection and basis-adjustment path
- `v1.4`: structured JSON logs, hash-chain audit log, before/after decision capture, heartbeat, latency/memory monitoring, Strategy Performance Monitor with smoke-only testnet semantics
- `v1.5`: alert severities, runtime alert generation, primary/backup delivery redundancy, runtime-owned heartbeat-timeout watchdog
- `v1.6`: explicit `OperationalMode`, drawdown/daily-loss/consecutive-loss controls, logging-only `RecoveryMode` on paper/testnet, BNB guard, symbol-change guard, forced delisting exit with persistent no-reentry window, `regime_unknown` pause/zero-size priority
- post-`v1.6` hardening:
  - strict closed-candle enforcement in runtime decision flow
  - pre-submit OCO validation against fresh market reference
  - controlled snapshot transitions through `PortfolioTracker`
  - monotonic REST/WS order-status handling
  - supervised critical background tasks
  - retry-safe delisting forced exit
  - bounded execution-state retention
  - conservative startup OCO ambiguity handling
  - single-source economic application split between balance-sync and fill paths
  - pending/in-flight order guard against duplicate submit
  - active protective OCO cancel-before-direct-sell handling
  - conservative single-unknown startup OCO handling
  - broader restart consistency handling for outstanding plain orders and partial-fill state
- `v1.7`: scenario matrix, chaos/integration coverage, operator-facing readiness artifacts, transition gate
- `v2.0-step1`: `BtcUsdtMtfV20Strategy`, closed-candle MTF aggregation for D1/H4/H1/M15, backtest/paper wiring
- `v2.0-patch1`: critical fixes for `run_testnet_platform.py`, guarded `_persist_snapshot()`, boundary leakage cleanup via exchange enums
- `v2.0-step2`: live testnet MTF wiring with `LiveMtfAggregator`, `MtfKlineManager`, 4 independent kline channels (M15/H1/H4/D1), REST priming, startup warmup gate
- `v2.0-step2-fix`: accepted working baseline after audit fixes and completed `v2.0 backtest wiring`
- completed `v2.0 backtest wiring`: explicit `--strategy` selection in `run_backtest_csv.py`, backward-compatible legacy default, consistent protective/OCO handling in the `v20_btcusdt_mtf` backtest path

## Что не реализовано
- `v2.0+` first-live implementation scope
- production live trading readiness
- multi-pair
- futures

## Архитектурные инварианты
- все финансовые значения — только `Decimal`
- все timestamps — только UTC-aware `datetime`
- `Symbol` остается типизированным объектом
- strategy layer остается read-only
- exchange-specific преобразования остаются внутри adapter/runtime boundary
- paper mode и testnet mode остаются разделенными

## Запуск тестов
Полный прогон:

```bash
python -m pytest tests/ -v
```

Быстрый прогон:

```bash
python -m pytest tests/ -q
```

Current local baseline: `503 passed`.

## Скрипты
Deterministic demo backtest:

```bash
python run_backtest.py
```

CSV backtest:

```bash
python run_backtest_csv.py --csv data/market/spot/BTCUSDT-1m-2025-01.csv --symbol BTCUSDT
```

CSV backtest with explicit v2.0 MTF strategy:

```bash
python run_backtest_csv.py --csv data/market/spot/BTCUSDT-1m-2025-01.csv --symbol BTCUSDT --strategy v20_btcusdt_mtf
```

Local paper runtime demo:

```bash
python run_paper_runtime.py
```

Testnet runtime entrypoint:

```bash
python run_testnet_platform.py
```

## Current Baseline Operational Docs

The repository now includes current baseline operator-facing docs:

- [docs/README.md](./docs/README.md)
- [docs/v1_7_operator_runbook.md](./docs/v1_7_operator_runbook.md)
- [docs/v1_7_pre_live_checklist.md](./docs/v1_7_pre_live_checklist.md)
- [docs/v1_7_incident_journal_template.md](./docs/v1_7_incident_journal_template.md)
- [docs/v1_7_operator_intervention_rules.md](./docs/v1_7_operator_intervention_rules.md)
- [docs/v1_7_to_v2_0_readiness_gate.md](./docs/v1_7_to_v2_0_readiness_gate.md)

These artifacts document the accepted testnet/pre-live operator behavior for the current `v2.0-step2-fix` baseline. They do not claim live-trading support.

## Важная оговорка про testnet
Binance Spot TESTNET используется только для проверки integration/mechanics. Его ликвидность, книги и fills не считаются рыночно-реалистичными и не доказывают качество стратегии.
