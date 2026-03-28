# MCTP

## What This Is
MCTP (`Modular Crypto Trading Platform`) is a modular spot trading platform for deterministic backtesting, paper runtime, and Binance Spot TESTNET runtime. The repository is built around typed domain models, strict accounting, adapter-boundary execution, and an incremental roadmap.

## Current Repo State
- historical accepted baseline: `v2.0-step2-fix`
- current local working state: `v2.0-step2-fix` plus accepted `v2.0 backtest wiring`, backtest hot-path optimization, and narrow `v20_btcusdt_mtf` guard hardening
- current full local test baseline: `533 passed, 9 warnings`

## What Is Implemented
- `v0.0`-`v0.12`: core domain contracts, execution, risk, sizing, portfolio/accounting, storage, streams, backtest, analytics, indicators, strategy contract, paper runtime
- post-`v0.12` maintenance: accounting/runtime polish, CSV backtest input, trade export, affordability fixes
- `v1.0`: Binance Spot TESTNET adapter v1, authenticated REST/WS path, private user stream, listenKey lifecycle, real OCO submit, bounded account refresh, delisting plumbing
- `v1.1`: exchange-authoritative balance cache handling, external OCO cancel monitoring, software-stop fallback, controlled symbol-change procedure
- `v1.2`: startup synchronization gate, startup balance refresh, missing-basis handling, startup OCO consistency, startup gap-risk handling
- `v1.3`: restart reconciliation, reconnect balance refresh, OCO outage-fill reconciliation, manual-trade detection and basis-adjustment path
- `v1.4`: structured JSON logs, hash-chain audit log, before/after decision capture, heartbeat, latency/memory monitoring
- `v1.5`: runtime alerting with primary/backup delivery and watchdog coverage
- `v1.6`: safety controls, recovery/reliability hardening, strict closed-candle enforcement
- `v1.7`: scenario matrix, chaos/integration coverage, operator-facing readiness artifacts, transition gate
- `v2.0-step1`: `BtcUsdtMtfV20Strategy`, closed-candle MTF aggregation for `D1/H4/H1/M15`, backtest/paper wiring
- `v2.0-patch1`: critical fixes for `run_testnet_platform.py`, guarded `_persist_snapshot()`, boundary leakage cleanup
- `v2.0-step2`: live testnet MTF wiring with `LiveMtfAggregator`, `MtfKlineManager`, `M15/H1/H4/D1` channels, REST priming, startup warmup gate
- `v2.0-step2-fix`: accepted baseline after audit fixes
- accepted local `v2.0 backtest wiring`: explicit `--strategy` selection in `run_backtest_csv.py`, backward-compatible legacy default, consistent protective/OCO handling in `_run_v20_btcusdt_mtf`
- backtest hot-path optimization: incremental rolling indicator path for heavy CSV backtests, avoiding pathological full-history indicator recalculation in the backtest loop
- current `v20_btcusdt_mtf` guard family:
  - block `D1 >= 30%` with `H4 < 0.5%`
  - block `D1 >= 30%` with `0.5% <= H4 < 1.0%`
  - block `D1 >= 30%` with `H4 >= 2.0%`
  - block `10% <= D1 < 20%` with `0.5% <= H4 < 1.0%`
  - block `10% <= D1 < 20%` with `1.0% <= H4 < 2.0%`

## Current Local Backtest Reference
Full 2024 on current code state:
- `execution_count=34`
- `trade_count=17`
- `end_equity=10099.88783325915940214285715`
- `realized_pnl_total=99.88783325915940214285714652`
- `profit_factor=1.162970696212725768760993567`
- `max_drawdown_pct=0.02300367002048603745724802941`

Full 2025 on current code state:
- `execution_count=18`
- `trade_count=9`
- `end_equity=9769.109399677787024285714303`
- `realized_pnl_total=-230.8906003222129757142856982`
- `profit_factor=0.3724160082990711736579634635`
- `max_drawdown_pct=0.0274036611864884404285714272`

## Architectural Invariants
- all financial values use `Decimal`
- all timestamps are UTC-aware `datetime`
- `Symbol` remains typed
- strategy layer remains read-only
- backtest uses only closed candles
- exchange-specific logic remains inside the adapter/runtime boundary
- paper mode and testnet mode remain separated
- constants come from `mctp/core/constants.py`

## What Is Not Implemented
- production live trading readiness
- multi-pair
- futures
- ML / anomaly / on-chain / allocation scope

## Running Tests
Full suite:

```bash
python -m pytest tests/ -v
```

Fast suite:

```bash
python -m pytest tests/ -q
```

## Scripts
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
- [docs/README.md](./docs/README.md)
- [docs/v1_7_operator_runbook.md](./docs/v1_7_operator_runbook.md)
- [docs/v1_7_pre_live_checklist.md](./docs/v1_7_pre_live_checklist.md)
- [docs/v1_7_incident_journal_template.md](./docs/v1_7_incident_journal_template.md)
- [docs/v1_7_operator_intervention_rules.md](./docs/v1_7_operator_intervention_rules.md)
- [docs/v1_7_to_v2_0_readiness_gate.md](./docs/v1_7_to_v2_0_readiness_gate.md)

These documents describe the current operator-facing baseline and transition boundary. They do not claim live-trading support.

## Important Testnet Note
Binance Spot TESTNET is used only for integration and execution-mechanics validation. Its liquidity, order book, and fills are not treated as market-realistic proof of strategy quality.
