# MCTP v1.7 Pre-Live Checklist

Mark each item explicitly as `done / not done / blocked`.

- Scenario matrix completed and reviewed
- Chaos / integration verification completed and reviewed
- All 4 WS streams independently verified:
  - `KLINE`
  - `BOOK_TICKER`
  - `BNB_TICKER`
  - `USER_DATA`
- Restart / reconciliation checks completed
- Protection / OCO scenarios completed
- Symbol change safety scenarios completed
- Cost basis recovery paths verified:
  - manual basis
  - zero basis declaration plus BUY block
  - immediate close
- `T_CANCEL` path verified at runtime level
- BALANCE_CACHE_TTL behavior verified against current runtime semantics
- Operator intervention rules reviewed
- Incident journal template prepared and available
- Startup halt conditions reviewed by operator
- Restart halt conditions reviewed by operator
- USER_DATA degradation handling reviewed by operator
- Direct SELL with active OCO protection behavior reviewed
- Delisting forced-exit and no-reentry behavior reviewed
- Pending / in-flight order guard behavior reviewed
- Full automated test suite green on the target commit

Do not mark this checklist complete based on intuition or ad hoc observation alone.
