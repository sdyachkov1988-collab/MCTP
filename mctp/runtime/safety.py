from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from mctp.core.constants import MAX_DRAWDOWN_STOP_PCT, MAX_DRAWDOWN_WARNING_PCT
from mctp.core.enums import RecoveryMode


@dataclass
class RecoveryModeController:
    mode: RecoveryMode = RecoveryMode.NORMAL
    live_activation_enabled: bool = False
    last_logged_reason: Optional[str] = None
    last_logged_at: Optional[datetime] = None

    def observe_drawdown(self, drawdown_pct: Decimal, timestamp: datetime) -> None:
        if self.live_activation_enabled:
            return
        if drawdown_pct <= Decimal("0"):
            return
        if drawdown_pct >= MAX_DRAWDOWN_STOP_PCT:
            self.last_logged_reason = "terminal_drawdown_observed"
        elif drawdown_pct >= MAX_DRAWDOWN_WARNING_PCT:
            self.last_logged_reason = "elevated_drawdown_observed"
        else:
            self.last_logged_reason = "recovery_monitor_observed"
        self.last_logged_at = timestamp
