from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from mctp.core.constants import (
    ASSET_BNB,
    BNB_NEAR_ZERO_THRESHOLD,
    CONSECUTIVE_LOSSES_REDUCE,
    CRITICAL_DRAWDOWN_STOP_CODE,
    DAILY_LOSS_LIMIT_PCT,
    MAX_DRAWDOWN_STOP_PCT,
    MAX_DRAWDOWN_WARNING_PCT,
    RISK_REDUCTION_MULTIPLIER,
    WARNING_BNB_NEAR_ZERO_CODE,
    WARNING_CONSECUTIVE_LOSSES_CODE,
    WARNING_DAILY_LOSS_LIMIT_CODE,
    WARNING_DRAWDOWN_CODE,
    WARNING_PERSISTENT_DUST_CODE,
    WARNING_REGIME_UNKNOWN_CODE,
    WARNING_STRATEGY_DEGRADATION_CODE,
)
from mctp.core.enums import AlertSeverity, OperationalMode
from mctp.sizing.models import RiskMultipliers


class TestnetSafetyStateHelper:
    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime

    def current_equity(self) -> Optional[Decimal]:
        runtime = self.runtime
        if runtime.current_bid is None:
            return None
        return runtime.portfolio.snapshot.free_quote + (runtime.portfolio.snapshot.held_qty * runtime.current_bid)

    def evaluate_warning_conditions(self) -> None:
        runtime = self.runtime
        current_equity = self.current_equity()
        start_equity = runtime.adaptive_risk.daily_start_equity
        if (
            current_equity is not None
            and start_equity > Decimal("0")
            and current_equity < start_equity * (Decimal("1") - MAX_DRAWDOWN_WARNING_PCT)
        ):
            runtime._raise_alert(
                AlertSeverity.WARNING,
                WARNING_DRAWDOWN_CODE,
                "Drawdown warning threshold reached",
                context={"current_equity": current_equity, "start_equity": start_equity},
            )
        if runtime.adaptive_risk.consecutive_losses >= CONSECUTIVE_LOSSES_REDUCE:
            runtime._raise_alert(
                AlertSeverity.WARNING,
                WARNING_CONSECUTIVE_LOSSES_CODE,
                "Consecutive losses warning threshold reached",
                context={"consecutive_losses": runtime.adaptive_risk.consecutive_losses},
            )
        cached_balances = runtime.balance_cache_store.load()
        if cached_balances is not None:
            balances, _ = cached_balances
            bnb_balance = balances.get(ASSET_BNB, Decimal("0"))
            if bnb_balance <= BNB_NEAR_ZERO_THRESHOLD:
                runtime._raise_alert(
                    AlertSeverity.WARNING,
                    WARNING_BNB_NEAR_ZERO_CODE,
                    "BNB balance is near zero",
                    context={"bnb_balance": bnb_balance},
                )
        spm_snapshot = runtime.observability.performance_monitor.snapshot()
        if spm_snapshot.warning:
            runtime._raise_alert(
                AlertSeverity.WARNING,
                WARNING_STRATEGY_DEGRADATION_CODE,
                "Strategy degradation warning triggered",
                context={"reasons": tuple(spm_snapshot.warning_reasons)},
            )
        min_notional = Decimal(str(runtime.config.instrument_info.get("min_notional", "0")))
        held_notional = runtime.portfolio.snapshot.held_qty * runtime.portfolio.snapshot.avg_cost_basis
        if (
            runtime.portfolio.snapshot.is_in_position
            and not runtime.portfolio.snapshot.meaningful_position
            and held_notional < min_notional
        ):
            runtime._raise_alert(
                AlertSeverity.WARNING,
                WARNING_PERSISTENT_DUST_CODE,
                "Persistent dust position remains below min notional",
                context={"held_notional": held_notional, "min_notional": min_notional},
            )

    def effective_risk_multipliers(self) -> RiskMultipliers:
        runtime = self.runtime
        base = runtime.adaptive_risk.get_risk_multipliers()
        regime_mult = runtime._regime_mult_override
        anomaly_mult = runtime._anomaly_mult_override
        if runtime._regime_unknown:
            regime_mult = Decimal("0")
            anomaly_mult = Decimal("1")
        return RiskMultipliers(
            loss_mult=base.loss_mult * runtime._drawdown_loss_mult,
            atr_mult=base.atr_mult,
            regime_mult=regime_mult,
            anomaly_mult=anomaly_mult,
        )

    def set_regime_state(
        self,
        regime_unknown: bool,
        *,
        regime_mult: Optional[Decimal] = None,
        anomaly_mult: Optional[Decimal] = None,
    ) -> None:
        runtime = self.runtime
        runtime._regime_unknown = regime_unknown
        if not regime_unknown:
            runtime._regime_unknown_alert_active = False
        if regime_mult is not None:
            runtime._regime_mult_override = regime_mult
        if anomaly_mult is not None:
            runtime._anomaly_mult_override = anomaly_mult
        self.evaluate_safety_controls(runtime.current_runtime_time)

    def manual_resume_after_stop(self) -> None:
        runtime = self.runtime
        if not runtime._manual_resume_required:
            raise ValueError("manual resume is only available after safety STOP")
        runtime._manual_resume_required = False
        runtime._drawdown_stop_active = False
        runtime._drawdown_warning_active = False
        runtime._drawdown_loss_mult = Decimal("1")
        runtime._peak_equity = self.control_equity()
        runtime.last_alert = None
        runtime.status = runtime._status_enum.READY
        self.evaluate_safety_controls(runtime.current_runtime_time)

    def control_equity(self) -> Decimal:
        runtime = self.runtime
        mark_price = runtime.current_bid
        if mark_price is not None:
            return runtime.portfolio.snapshot.free_quote + (runtime.portfolio.snapshot.held_qty * mark_price)
        if runtime.portfolio.snapshot.avg_cost_basis > Decimal("0"):
            return runtime.portfolio.snapshot.free_quote + (
                runtime.portfolio.snapshot.held_qty * runtime.portfolio.snapshot.avg_cost_basis
            )
        return runtime.portfolio.snapshot.free_quote

    def evaluate_safety_controls(self, timestamp: datetime) -> None:
        runtime = self.runtime
        if runtime.adaptive_risk.should_reset_daily(timestamp):
            runtime.adaptive_risk.reset_daily(self.control_equity(), now=timestamp)
            runtime._daily_loss_pause_alert_active = False
        current_equity = self.control_equity()
        if current_equity > runtime._peak_equity:
            runtime._peak_equity = current_equity
        drawdown_pct = (
            (runtime._peak_equity - current_equity) / runtime._peak_equity
            if runtime._peak_equity > Decimal("0")
            else Decimal("0")
        )
        runtime.recovery_mode_controller.observe_drawdown(drawdown_pct, timestamp)
        if runtime.recovery_mode_controller.last_logged_reason is not None:
            runtime._emit_runtime_event("recovery_mode_observe")
        if drawdown_pct >= MAX_DRAWDOWN_STOP_PCT:
            if not runtime._drawdown_stop_active:
                runtime._drawdown_stop_active = True
                runtime._manual_resume_required = True
                runtime._raise_alert(
                    AlertSeverity.CRITICAL,
                    CRITICAL_DRAWDOWN_STOP_CODE,
                    "Drawdown stop threshold reached",
                    context={"drawdown_pct": drawdown_pct},
                    timestamp=timestamp,
                )
            runtime.status = runtime._status_enum.HALT
        elif drawdown_pct >= MAX_DRAWDOWN_WARNING_PCT:
            runtime._drawdown_loss_mult = RISK_REDUCTION_MULTIPLIER
            if not runtime._drawdown_warning_active:
                runtime._drawdown_warning_active = True
                runtime._raise_alert(
                    AlertSeverity.WARNING,
                    WARNING_DRAWDOWN_CODE,
                    "Drawdown warning threshold reached",
                    context={"drawdown_pct": drawdown_pct},
                    timestamp=timestamp,
                )
        else:
            runtime._drawdown_warning_active = False
            runtime._drawdown_loss_mult = Decimal("1")

        cached_balances = runtime.balance_cache_store.load()
        runtime._bnb_guard_active = False
        if cached_balances is not None:
            balances, _ = cached_balances
            bnb_balance = balances.get(ASSET_BNB)
            if bnb_balance is not None and bnb_balance <= BNB_NEAR_ZERO_THRESHOLD:
                runtime._bnb_guard_active = True

        daily_loss_triggered = (
            runtime.adaptive_risk.daily_start_equity > Decimal("0")
            and runtime.adaptive_risk.daily_loss / runtime.adaptive_risk.daily_start_equity >= DAILY_LOSS_LIMIT_PCT
        )
        if daily_loss_triggered and not runtime._daily_loss_pause_alert_active:
            runtime._daily_loss_pause_alert_active = True
            runtime._raise_alert(
                AlertSeverity.WARNING,
                WARNING_DAILY_LOSS_LIMIT_CODE,
                "Daily loss limit reached; new entries paused",
                context={"daily_loss": runtime.adaptive_risk.daily_loss},
                timestamp=timestamp,
            )

        if runtime._regime_unknown:
            if not runtime._regime_unknown_alert_active:
                runtime._regime_unknown_alert_active = True
                runtime._raise_alert(
                    AlertSeverity.WARNING,
                    WARNING_REGIME_UNKNOWN_CODE,
                    "Regime is unknown; new entries paused and final size forced to zero",
                    timestamp=timestamp,
                )

        target_mode = OperationalMode.RUN
        if runtime._manual_resume_required:
            target_mode = OperationalMode.STOP
        elif runtime._delisting_close_only_active:
            target_mode = OperationalMode.CLOSE_ONLY
        elif (
            runtime._regime_unknown
            or runtime._bnb_guard_active
            or runtime.adaptive_risk.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES
        ):
            target_mode = OperationalMode.PAUSE_NEW_ENTRIES
        runtime.operational_mode = target_mode
