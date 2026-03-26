"""
AdaptiveRiskController - Level 3 (daily loss limit) + Level 5.1 (loss_mult).
is_live=False: PAUSE mode is only logged and never enforced.
"""
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from mctp.core.constants import (
    ATR_MULT_MAX,
    ATR_MULT_MIN,
    ATR_REFERENCE_PCT,
    CONSECUTIVE_LOSSES_REDUCE,
    CONSECUTIVE_LOSSES_STOP,
    DAILY_LOSS_LIMIT_PCT,
    RISK_REDUCTION_MULTIPLIER,
)
from mctp.core.enums import OperationalMode
from mctp.sizing.models import RiskMultipliers

logger = logging.getLogger(__name__)


class AdaptiveRiskController:
    """
    Tracks daily loss and consecutive losses.

    Level 3: daily_loss / daily_start_equity >= DAILY_LOSS_LIMIT_PCT
             -> PAUSE_NEW_ENTRIES only when is_live=True

    Level 5.1: consecutive_losses >= CONSECUTIVE_LOSSES_REDUCE
               -> loss_mult = 0.5
               consecutive_losses >= CONSECUTIVE_LOSSES_STOP and is_live
               -> PAUSE_NEW_ENTRIES

    Level 5.2: atr_mult is driven by ATR / price using explicit clamps.
    """

    def __init__(self, initial_equity: Decimal, is_live: bool = False) -> None:
        if not isinstance(initial_equity, Decimal):
            raise AssertionError("initial_equity must be Decimal")
        self._is_live: bool = is_live
        self._daily_start_equity: Decimal = initial_equity
        self._daily_loss: Decimal = Decimal("0")
        self._consecutive_losses: int = 0
        self._loss_mult: Decimal = Decimal("1")
        self._atr_mult: Decimal = Decimal("1")
        self._mode: OperationalMode = OperationalMode.RUN
        self._last_reset_date: date = datetime.now(timezone.utc).date()

    @property
    def operational_mode(self) -> OperationalMode:
        return self._mode

    @property
    def consecutive_losses(self) -> int:
        return self._consecutive_losses

    @property
    def daily_loss(self) -> Decimal:
        return self._daily_loss

    @property
    def daily_start_equity(self) -> Decimal:
        return self._daily_start_equity

    def get_loss_mult(self) -> Decimal:
        return self._loss_mult

    @property
    def loss_mult(self) -> Decimal:
        return self._loss_mult

    @property
    def atr_mult(self) -> Decimal:
        return self._atr_mult

    @property
    def regime_mult(self) -> Decimal:
        return Decimal("1.0")

    @property
    def anomaly_mult(self) -> Decimal:
        return Decimal("1.0")

    def get_risk_multipliers(self) -> RiskMultipliers:
        return RiskMultipliers(
            loss_mult=self.loss_mult,
            atr_mult=self.atr_mult,
            regime_mult=self.regime_mult,
            anomaly_mult=self.anomaly_mult,
        )

    def update_atr_context(
        self,
        atr_value: Optional[Decimal],
        reference_price: Optional[Decimal],
    ) -> None:
        if atr_value is None or reference_price is None or reference_price <= Decimal("0"):
            self._atr_mult = Decimal("1")
            return
        atr_pct = atr_value / reference_price
        if atr_pct <= Decimal("0"):
            self._atr_mult = Decimal("1")
            return
        raw = ATR_REFERENCE_PCT / atr_pct
        if raw < ATR_MULT_MIN:
            self._atr_mult = ATR_MULT_MIN
        elif raw > ATR_MULT_MAX:
            self._atr_mult = ATR_MULT_MAX
        else:
            self._atr_mult = raw

    def on_trade_result(
        self,
        pnl: Decimal,
        equity: Decimal,
        now: Optional[datetime] = None,
    ) -> None:
        if not isinstance(pnl, Decimal):
            raise AssertionError("pnl must be Decimal")

        if pnl < Decimal("0"):
            self._daily_loss += abs(pnl)
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

        if self._consecutive_losses >= CONSECUTIVE_LOSSES_REDUCE:
            self._loss_mult = RISK_REDUCTION_MULTIPLIER
        else:
            self._loss_mult = Decimal("1")

        if self._daily_start_equity > Decimal("0"):
            daily_loss_pct = self._daily_loss / self._daily_start_equity
        else:
            daily_loss_pct = Decimal("0")

        if self._is_live:
            if daily_loss_pct >= DAILY_LOSS_LIMIT_PCT:
                self._mode = OperationalMode.PAUSE_NEW_ENTRIES
                logger.warning(
                    "Daily loss limit reached: %.4f >= %.4f - PAUSE_NEW_ENTRIES",
                    daily_loss_pct,
                    DAILY_LOSS_LIMIT_PCT,
                )
            elif self._consecutive_losses >= CONSECUTIVE_LOSSES_STOP:
                self._mode = OperationalMode.PAUSE_NEW_ENTRIES
                logger.warning(
                    "Consecutive losses %d >= %d - PAUSE_NEW_ENTRIES",
                    self._consecutive_losses,
                    CONSECUTIVE_LOSSES_STOP,
                )
        else:
            if daily_loss_pct >= DAILY_LOSS_LIMIT_PCT:
                logger.info(
                    "[paper] Daily loss limit would trigger: %.4f >= %.4f",
                    daily_loss_pct,
                    DAILY_LOSS_LIMIT_PCT,
                )
            if self._consecutive_losses >= CONSECUTIVE_LOSSES_STOP:
                logger.info(
                    "[paper] Consecutive losses %d >= %d - would PAUSE",
                    self._consecutive_losses,
                    CONSECUTIVE_LOSSES_STOP,
                )

    def reset_daily(
        self,
        equity: Optional[Decimal] = None,
        now: Optional[datetime] = None,
    ) -> None:
        if equity is not None:
            if not isinstance(equity, Decimal):
                raise AssertionError("equity must be Decimal")
            self._daily_start_equity = equity
        self._daily_loss = Decimal("0")
        self._mode = OperationalMode.RUN
        ts = now if now is not None else datetime.now(timezone.utc)
        self._last_reset_date = ts.date()

    def should_reset_daily(self, now: Optional[datetime] = None) -> bool:
        ts = now if now is not None else datetime.now(timezone.utc)
        return ts.date() > self._last_reset_date
