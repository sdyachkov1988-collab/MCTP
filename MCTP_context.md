# MCTP Context

## Purpose
MCTP is a modular spot trading platform for deterministic backtest, paper execution, and Binance Spot TESTNET runtime.

## Historical Accepted Baseline
- accepted historical baseline: `v2.0-step2-fix`

## Current Local Working State
- historical baseline preserved
- accepted local `v2.0 backtest wiring` is present
- backtest hot-path optimization is present
- `v20_btcusdt_mtf` has a narrow guard family shaped by post-baseline research
- current local full test baseline: `533 passed, 9 warnings`

## Implemented Scope
- `v0.0`-`v0.12`: core, execution, risk, sizing, portfolio/accounting, storage, streams, backtest, analytics, indicators, strategy contract, paper runtime
- `v1.0`-`v1.7`: testnet adapter, synchronization/reconciliation, observability, alerting, safety/reliability hardening, scenario matrix, operator artifacts
- `v2.0-step1`: `BtcUsdtMtfV20Strategy`, MTF aggregation, backtest/paper wiring
- `v2.0-patch1`: critical fixes around boundary safety and runtime wiring
- `v2.0-step2`: testnet MTF wiring with `LiveMtfAggregator`, `MtfKlineManager`, independent `M15/H1/H4/D1` channels, REST priming, startup gate
- `v2.0-step2-fix`: accepted baseline after audit fixes
- local `v2.0 backtest wiring`: `--strategy` flag in `run_backtest_csv.py`, backward-compatible legacy default, OCO/protective handling aligned in `_run_v20_btcusdt_mtf`
- local backtest hot-path optimization: incremental rolling indicator path for heavy CSV backtests
- local `v20_btcusdt_mtf` guard family:
  - `D1 >= 30%` and `H4 < 0.5%`
  - `D1 >= 30%` and `0.5% <= H4 < 1.0%`
  - `D1 >= 30%` and `H4 >= 2.0%`
  - `10% <= D1 < 20%` and `0.5% <= H4 < 1.0%`
  - `10% <= D1 < 20%` and `1.0% <= H4 < 2.0%`

## Current Local Backtest Reference
### Full 2024
- `execution_count=34`
- `trade_count=17`
- `end_equity=10099.88783325915940214285715`
- `realized_pnl_total=99.88783325915940214285714652`
- `profit_factor=1.162970696212725768760993567`
- `max_drawdown_pct=0.02300367002048603745724802941`

### Full 2025
- `execution_count=18`
- `trade_count=9`
- `end_equity=9769.109399677787024285714303`
- `realized_pnl_total=-230.8906003222129757142856982`
- `profit_factor=0.3724160082990711736579634635`
- `max_drawdown_pct=0.0274036611864884404285714272`

### Safety Range
For `2024-07_to_2025-03` on the current code state:
- `execution_count=0`
- `trade_count=0`
- `end_equity=10000.00000000`
- `realized_pnl_total=0`

## Architectural Invariants
- only `Decimal` for financial logic
- only UTC-aware timestamps
- `Symbol` remains typed
- strategy layer remains read-only
- exchange-specific logic stays inside adapter/runtime boundary
- paper mode and testnet mode stay separated
- constants come from `mctp/core/constants.py`

## Current Analytical Conclusion
- no clean shared cross-year bucket-level bad regime remains after the current guard family
- `2024` residual weakness is still concentrated in `30%+ x 1.0-2.0% H4`
- `2025` residual weakness is fragmented across smaller cells
- the remaining shared weakness is feature-shaped rather than bucket-clean, especially higher `H1 RSI`
- the current mainline guard chain is likely close to its reasonable limit

## Out of Scope
- production live trading readiness
- multi-pair
- futures
- ML / anomaly / on-chain / allocation scope

## Working Mode
- `audit -> minimal patch -> tests -> audit -> context sync`
- update `MCTP_context.md` and `AGENTS.md` after meaningful changes
- commit only green tests
