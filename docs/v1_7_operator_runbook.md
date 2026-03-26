# MCTP v1.7 Operator Runbook

## Purpose And Scope

This runbook is for the current pre-live verification system only:

- deterministic backtest
- paper runtime
- Binance Spot TESTNET runtime
- accepted startup/restart/protection/safety behavior through v1.7 verification

It is not a live-trading runbook. It does not assume any operator action that the repository cannot actually support today.

## Required Starting Conditions Before Runtime Start

- configuration points to the intended typed symbol and timeframe
- instrument info is present and matches the symbol
- storage paths for snapshot, balance cache, orders, and accounting are writable
- testnet credentials are present for the testnet runtime
- previous unresolved operator obligations are reviewed:
  - missing basis
  - manual trade prompt
  - restart outstanding order / partial-fill halt
  - startup OCO ambiguity halt

## Startup Checks

At startup the accepted runtime should:

- refresh balances from exchange REST
- create listenKey and connect the 4 WS streams
- run startup synchronization before `READY`
- apply startup OCO consistency checks
- apply restart reconciliation checks
- block on missing-basis / partial-fill / outstanding-order / OCO ambiguity conditions where required

If startup ends in `HALT`, do not treat that as recoverable by observation alone. Review the blocking cause first.

## What Healthy Runtime Looks Like

A healthy runtime for this stage generally means:

- runtime status is `READY`
- no unresolved pending symbol change stage
- no unresolved manual basis prompt
- no unexpected pending order marker
- protection state is coherent:
  - `NONE` when flat and no exchange protection exists
  - `EXCHANGE_OCO` when exchange protection is active
  - `SOFTWARE_STOP` only when software-stop fallback is intentionally active
- stream stale flags reflect current stream reality and are not masking each other

## Stream Health And Stale-State Interpretation

- `KLINE` stale:
  - warning-level stale condition
  - runtime is expected to halt decision progression
  - operator should stop and review before continuing
- `BOOK_TICKER` stale:
  - info-level stale condition
  - does not automatically imply balance/accounting corruption
  - operator may continue observation only if protection/order state is otherwise coherent
- `BNB_TICKER` stale:
  - info-level stale condition
  - does not automatically imply position corruption
  - operator should treat fee-rate context as degraded
- `USER_DATA` stale:
  - must not be treated as healthy just because market-data streams are active
  - pending/open-order/protection context requires manual review before trusting runtime state

TTL-based balance refresh does not heal a stale `USER_DATA` stream and must not be interpreted as such.

## Protection State Expectations

- protective OCO should be represented by `EXCHANGE_OCO`
- software-stop fallback should be represented by `SOFTWARE_STOP`
- the runtime should not operate with both as primary protection simultaneously
- direct SELL paths should not bypass active exchange OCO
- external OCO cancellation while exposed is a critical condition and should reactivate software-stop

## Symbol State Expectations

- symbol change is allowed only through the controlled path
- symbol change must not proceed while:
  - position is still open
  - pending order exists
  - exchange protection is still active
  - basis obligations remain unresolved

## What To Inspect After Restart Or Reconciliation

- balance snapshot matches exchange truth
- active local orders and exchange open orders do not disagree silently
- partial-fill-related state did not restart into a falsely clean state
- owned OCO context is still coherent
- pending order markers match real outstanding order context
- protection mode matches actual runtime/exchange context

## How To Interpret Pending / Open-Order / Protection Anomalies

- unknown exchange open order on restart:
  - treat as stop-and-review condition
- unknown exchange partial fill on restart:
  - treat as stop-and-review condition
- startup OCO ambiguity:
  - treat as stop-and-review condition
- active exchange OCO cannot be cancelled before forced direct SELL:
  - treat as stop-and-review condition
- USER_DATA stale with unresolved pending order:
  - do not assume state is safe; stop and review

## Conditions That Require Stopping Runtime Instead Of Continuing

- runtime `HALT` caused by:
  - missing basis
  - manual trade prompt
  - startup OCO ambiguity
  - startup protection conflict
  - restart outstanding order
  - restart partial fill
  - background task failure
  - heartbeat timeout
  - direct SELL OCO cancel failure
- external OCO cancellation while position remains exposed
- unresolved USER_DATA degradation with pending/open-order uncertainty

## Conditions That Are Safe For Observation Only

- isolated `BOOK_TICKER` stale info condition with otherwise coherent state
- isolated `BNB_TICKER` stale info condition with otherwise coherent state
- delisting announcement before the force-exit window, if no force-exit condition is active yet
- post-only rejection info event, if runtime state remains coherent

## Accepted Safeguards Referenced By This Runbook

- startup synchronization gate
- exchange-authoritative balance handling
- restart reconciliation and conservative restart halts
- external OCO cancellation fallback
- pending / in-flight order guard
- symbol change guard
- drawdown / daily-loss / consecutive-loss safety controls
- stream-specific stale handling
- heartbeat watchdog
