# MCTP v1.7 Operator Intervention Rules

## Stop Runtime Immediately

Stop runtime immediately and do not continue observation-only mode when any of the following is true:

- runtime is in `HALT` because of a critical startup or restart condition
- missing basis is unresolved
- manual trade prompt is unresolved
- startup OCO ambiguity or startup protection conflict is present
- restart found outstanding open order or partial-fill state requiring manual review
- heartbeat timeout fired
- critical background task failure fired
- direct SELL could not cancel active exchange OCO safely
- external OCO cancellation occurred while a position remained exposed

## Do Not Switch Symbol

Do not switch symbol when any of the following is true:

- held position is still non-zero
- pending order marker is still present
- active exchange protection still exists
- basis obligations are unresolved
- restart/manual-trade review is still pending

## Do Not Resume After Restart Without Manual Review

Do not resume after restart when:

- runtime halted on unknown exchange open order
- runtime halted on partial-fill-related restart state
- runtime halted on startup OCO ambiguity
- runtime halted on missing basis
- USER_DATA degradation existed together with unresolved pending/open-order context

## Continue Observation Without Immediate Intervention

Observation-only continuation is acceptable only when:

- isolated `BOOK_TICKER` stale condition is present and order/protection state is otherwise coherent
- isolated `BNB_TICKER` stale condition is present and order/protection state is otherwise coherent
- info-only delisting announcement exists but force-exit window is not active yet
- post-only rejection occurred without broader runtime inconsistency

## Protection / Order Inconsistency Escalation

Escalate immediately when:

- protection mode does not match the actual known exchange/local protection context
- active OCO exists but direct close path is attempted without safe cancellation
- pending order marker remains after terminal resolution should already have cleared it
- open-order state remains after `T_CANCEL` path should have cleared it

## USER_DATA Degradation Rule

Treat `USER_DATA` degradation as operationally serious even if market-data streams remain healthy.

- Do not assume exchange truth is healthy just because `KLINE`, `BOOK_TICKER`, and `BNB_TICKER` are active.
- Do not assume TTL-based balance refresh repairs `USER_DATA`.
- If pending/open-order/protection state is not trivially clean, stop and review instead of continuing.

## Stale Stream Rules

- `KLINE` stale:
  - treat as stop-and-review
- `BOOK_TICKER` stale:
  - observation only is acceptable if protection/order state is coherent
- `BNB_TICKER` stale:
  - observation only is acceptable if protection/order state is coherent
- `USER_DATA` stale:
  - requires manual review before trusting order/protection consistency
