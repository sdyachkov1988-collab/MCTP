"""
EquitySnapshot and EquityTracker for equity and drawdown tracking (v0.6 / fix v0.7).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional

from mctp.core.constants import N_SNAP


@dataclass
class EquitySnapshot:
    timestamp: datetime
    total_equity: Decimal
    free_quote: Decimal
    held_qty: Decimal
    held_value: Decimal
    current_price: Decimal
    is_in_position: bool
    meaningful_position: bool = False

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None:
            raise ValueError("EquitySnapshot.timestamp must be UTC-aware")
        for name, value in [
            ("total_equity", self.total_equity),
            ("free_quote", self.free_quote),
            ("held_qty", self.held_qty),
            ("held_value", self.held_value),
            ("current_price", self.current_price),
        ]:
            if not isinstance(value, Decimal):
                raise AssertionError(f"EquitySnapshot.{name} must be Decimal")


class EquityTracker:
    def __init__(self, initial_equity: Decimal) -> None:
        if not isinstance(initial_equity, Decimal):
            raise AssertionError("initial_equity must be Decimal")
        self._peak_equity: Decimal = initial_equity
        self._history: list[EquitySnapshot] = []

    def record(self, snapshot: EquitySnapshot) -> None:
        self._history.append(snapshot)
        if snapshot.total_equity > self._peak_equity:
            self._peak_equity = snapshot.total_equity

    def should_record(self, now: Optional[datetime] = None) -> bool:
        if not self._history:
            return True
        ts = now if now is not None else datetime.now(timezone.utc)
        return (ts - self._history[-1].timestamp) >= timedelta(minutes=N_SNAP)

    @property
    def peak_equity(self) -> Decimal:
        return self._peak_equity

    @property
    def current_drawdown_pct(self) -> Decimal:
        if not self._history or self._peak_equity == Decimal("0"):
            return Decimal("0")
        current = self._history[-1].total_equity
        return (self._peak_equity - current) / self._peak_equity

    @property
    def history(self) -> list[EquitySnapshot]:
        return list(self._history)

    @staticmethod
    def make_snapshot(
        free_quote: Decimal,
        held_qty: Decimal,
        current_price: Decimal,
        is_in_position: bool,
        now: Optional[datetime] = None,
        meaningful_position: Optional[bool] = None,
    ) -> EquitySnapshot:
        ts = now if now is not None else datetime.now(timezone.utc)
        held_value = held_qty * current_price
        total_equity = free_quote + held_value
        return EquitySnapshot(
            timestamp=ts,
            total_equity=total_equity,
            free_quote=free_quote,
            held_qty=held_qty,
            held_value=held_value,
            current_price=current_price,
            is_in_position=is_in_position,
            meaningful_position=is_in_position if meaningful_position is None else meaningful_position,
        )
