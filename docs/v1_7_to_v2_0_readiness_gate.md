# MCTP v1.7 to v2.0 Readiness Gate

## Purpose

This document closes the boundary between completed `v1.7` live-readiness verification and the later `v2.0` first-live implementation phase.

It is a transition artifact only. It does not introduce live-trading behavior.

## What Is Accepted At The End Of v1.7

The repository has completed the following accepted pre-live verification work:

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

## What Remains Outside Current Scope

The following remain outside the accepted repository scope described by this transition artifact:

- `BtcUsdtMtfV20Strategy` and its testnet/backtest wiring exist in the repository, but this document does not accept or describe live `v2.0` trading behavior
- no live production rollout yet
- no multi-pair support
- no regime engine
- no anomaly engine
- no `v2.3+` allocation logic

## Exact Entry Conditions For Starting v2.0 Implementation

The next implementation step may begin only under these assumptions:

- current runtime base is accepted for the pre-live/testnet verification scope
- current operator artifacts are accepted and discoverable
- the next coding work is limited to the first-live single-pair scope
- `v2.0` remains limited to:
  - one pair
  - one strategy
  - real-money mechanics only

## Exact Non-Goals For The First v2.0 Implementation Step

The first `v2.0` step must not pull in:

- `v2.1` stabilization work
- `v2.3` multi-pair work
- `v3.x` intelligence, anomaly, or regime-engine work

## Phase Boundary

- `v1.7` live-readiness verification is complete
- subsequent implementation phases begin in `v2.0`
- this transition gate does not add live trading by itself
