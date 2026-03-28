# MCTP Current Baseline Boundary Note

## Purpose

This document defines the current accepted boundary of the repository at `v2.0-step2-fix`.

It records what is already accepted in the codebase, what operational/runtime truth is currently supported, and what remains outside scope. It does not introduce live-trading behavior by itself.

## What Is Accepted In The Current Baseline

The repository has completed the following accepted verification and implementation work:

- scenario matrix completed
- chaos / integration verification completed
- all 4 websocket streams independently verified:
  - `KLINE`
  - `BOOK_TICKER`
  - `BNB_TICKER`
  - `USER_DATA`
- operator runbook present
- pre-live checklist present
- incident journal template present
- operator intervention rules present
- `BALANCE_CACHE_TTL` behavior verified against current runtime semantics
- `BtcUsdtMtfV20Strategy` implemented
- MTF aggregator and testnet wiring accepted through `v2.0-step2`
- local `v2.0` backtest wiring accepted:
  - `--strategy` in `run_backtest_csv.py`
  - backward-compatible legacy default path
  - protective/OCO handling in `_run_v20_btcusdt_mtf`

## What Remains Outside Current Scope

The following remain outside the accepted repository scope described by this baseline note:

- no live production rollout yet
- no multi-pair support
- no futures support
- no regime engine
- no anomaly engine
- no on-chain scope
- no ML scope
- no `v2.3+` allocation logic

## Current Operational Truth

The current baseline truth is:

- current operational/runtime scope is deterministic backtest, paper runtime, and Binance Spot TESTNET runtime
- accepted `v2.0` work is limited to the current single-pair BTCUSDT MTF strategy corridor already present in code
- operator documents remain for testnet/pre-live operation; they are not a live-production playbook
- the repository baseline is accepted as transitional, not final production maturity

## Explicit Non-Goals For This Baseline

This baseline does not imply or accept:

- a live-production deployment
- broader `v2.x` feature expansion
- `v2.3` multi-pair work
- `v3.x` intelligence, anomaly, or regime-engine work
- any change to the accepted paper/testnet separation

## Phase Boundary

- `v1.7` verification artifacts remain part of the baseline history
- accepted repository state is now `v2.0-step2-fix`
- local `v2.0` backtest wiring is part of the accepted baseline
- this boundary note still does not imply live trading by itself
