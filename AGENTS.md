# MCTP - Modular Crypto Trading Platform

## Current Historical Baseline
- accepted historical baseline: `v2.0-step2-fix`

## Current Local Working State
- local repo now includes accepted `v2.0 backtest wiring`
- local repo includes backtest hot-path optimization for long CSV runs
- local repo includes a narrow `v20_btcusdt_mtf` guard family shaped by post-baseline research
- current full local test baseline: `533 passed, 9 warnings`

## Confirmed Stages
- `v0.0`-`v1.7` complete
- `v2.0-step1`: strategy + MTF aggregator + backtest/paper wiring
- `v2.0-patch1`: critical fixes for `run_testnet_platform`, `_persist_snapshot()`, and boundary leakage
- `v2.0-step2`: testnet wiring with `LiveMtfAggregator`, `MtfKlineManager`, four independent kline channels, REST priming, startup gate
- `v2.0-step2-fix`: accepted historical baseline
- local accepted `v2.0 backtest wiring`: OCO wiring in `_run_v20_btcusdt_mtf`, `--strategy` flag in `run_backtest_csv.py`
- local post-baseline hardening: backtest hot-path optimization and narrow `v20_btcusdt_mtf` guard family

## Architectural Invariants
- all financial values use `Decimal`
- all timestamps are UTC-aware `datetime`
- `Symbol` remains typed
- async code goes through `asyncio`
- constants come from `mctp/core/constants.py`
- strategy layer remains read-only
- backtest uses only closed candles
- exchange-specific logic stays inside the adapter/runtime boundary
- paper mode and testnet mode stay separated

## What Is Actually Implemented
- core domain models, risk/sizing, portfolio/accounting, order lifecycle, OCO/software-stop
- deterministic backtest, analytics, CSV backtest input, trade export
- paper runtime
- Binance Spot TESTNET adapter v1
- startup sync / restart reconciliation through `v1.3`
- observability through `v1.4`
- alerting through `v1.5`
- `v1.6` safety controls and reliability hardening
- `v1.7` scenario matrix, chaos/integration, operator artifacts, transition gate
- `v2.0-step1` `BtcUsdtMtfV20Strategy`, MTF aggregator, backtest/paper wiring
- `v2.0-step2` testnet MTF wiring
- local `v2.0 backtest wiring`
- local backtest hot-path optimization
- local `v20_btcusdt_mtf` guard family:
  - `D1 >= 30%` with `H4 < 0.5%`
  - `D1 >= 30%` with `0.5% <= H4 < 1.0%`
  - `D1 >= 30%` with `H4 >= 2.0%`
  - `10% <= D1 < 20%` with `0.5% <= H4 < 1.0%`
  - `10% <= D1 < 20%` with `1.0% <= H4 < 2.0%`

## Current Next-Step Guidance
- historical accepted baseline stays `v2.0-step2-fix`
- no new universal feature corridor is locked yet
- after the latest research, another shared cross-year bucket guard is not clearly justified
- if more work is opened later, it should likely be either:
  - deeper entry-logic audit
  - 2024-specific experimental branch
  - 2025-specific experimental branch

## Anti-Scope Rules
- do not add multi-pair scope
- do not add futures
- do not add regime/anomaly/on-chain/ML
- do not add allocation engine
- do not rewrite architecture without direct need
- do not change accepted `v1.7` operator artifacts without explicit instruction

## Workflow
- `audit -> minimal patch -> tests -> audit -> context sync`
- after each meaningful change, update `MCTP_context.md` and `AGENTS.md`
- commit only green tests
