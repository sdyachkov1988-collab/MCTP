# MCTP Docs Index

## Historical Accepted Baseline

The historical accepted repository baseline remains `v2.0-step2-fix`.

## Current Local Working State

The current local repo state additionally includes:
- accepted local `v2.0 backtest wiring`
- backtest hot-path optimization for long CSV runs
- narrow `v20_btcusdt_mtf` strategy-guard hardening

## Operator-Facing Docs

- [Operator Runbook](./v1_7_operator_runbook.md)
- [Pre-Live Checklist](./v1_7_pre_live_checklist.md)
- [Incident Journal Template](./v1_7_incident_journal_template.md)
- [Operator Intervention Rules](./v1_7_operator_intervention_rules.md)
- [Boundary and Transition Note](./v1_7_to_v2_0_readiness_gate.md)

These documents describe the current operator-facing truth for deterministic backtest, paper runtime, Binance Spot TESTNET runtime, and the transition boundary around the `v2.0-step2-fix` baseline.

They do not claim production live-trading readiness, multi-pair scope, futures support, or a fixed post-baseline feature roadmap.
