from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from mctp.core.constants import (
    PARTIAL_CLOSE_FRACTION,
    PARTIAL_CLOSE_TRIGGER_ATR,
    SL_EXECUTION_BUFFER,
    TRAILING_ACTIVATION_ATR,
    TRAILING_ATR_MULTIPLIER,
)


@dataclass(frozen=True)
class SoftwareStopState:
    entry_price: Decimal
    stop_price: Decimal
    partial_close_trigger_price: Decimal
    remaining_qty: Decimal
    partial_close_executed: bool


@dataclass(frozen=True)
class SoftwareStopAction:
    new_stop_price: Decimal
    partial_close_qty: Decimal
    partial_close_triggered: bool


class SoftwareTrailingStop:
    def initialize(self, entry_price: Decimal, quantity: Decimal, atr: Decimal) -> SoftwareStopState:
        return SoftwareStopState(
            entry_price=entry_price,
            stop_price=entry_price - (atr * TRAILING_ATR_MULTIPLIER),
            partial_close_trigger_price=entry_price + (atr * PARTIAL_CLOSE_TRIGGER_ATR),
            remaining_qty=quantity,
            partial_close_executed=False,
        )

    def update(
        self,
        state: SoftwareStopState,
        highest_price: Decimal,
        current_price: Decimal,
        atr: Decimal,
    ) -> SoftwareStopAction:
        new_stop = state.stop_price
        if highest_price - state.entry_price >= atr * TRAILING_ACTIVATION_ATR:
            candidate = highest_price - (atr * TRAILING_ATR_MULTIPLIER)
            if candidate > new_stop:
                new_stop = candidate
        partial_qty = Decimal("0")
        partial_triggered = False
        if not state.partial_close_executed and current_price >= state.partial_close_trigger_price:
            partial_qty = state.remaining_qty * PARTIAL_CLOSE_FRACTION
            partial_triggered = True
        return SoftwareStopAction(
            new_stop_price=new_stop,
            partial_close_qty=partial_qty,
            partial_close_triggered=partial_triggered,
        )
