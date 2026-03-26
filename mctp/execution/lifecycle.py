"""
Lifecycle ордера: состояния и агрегация филлов.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from mctp.core.enums import ExecutionResult
from mctp.core.order import Fill


class OrderState(Enum):
    PENDING      = "PENDING"
    ACCEPTED     = "ACCEPTED"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED       = "FILLED"
    CANCELLED    = "CANCELLED"
    REJECTED     = "REJECTED"


TERMINAL_STATES = frozenset({
    OrderState.FILLED,
    OrderState.CANCELLED,
    OrderState.REJECTED,
})

_RESULT_TO_STATE: dict[ExecutionResult, OrderState] = {
    ExecutionResult.ACCEPTED:     OrderState.ACCEPTED,
    ExecutionResult.FILLED:       OrderState.FILLED,
    ExecutionResult.PARTIAL_FILL: OrderState.PARTIAL_FILL,
    ExecutionResult.CANCELLED:    OrderState.CANCELLED,
    ExecutionResult.REJECTED:     OrderState.REJECTED,
}


@dataclass
class OrderRecord:
    client_order_id: str
    state: OrderState = OrderState.PENDING
    fills: list = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        if self.created_at.tzinfo is None:
            raise ValueError("OrderRecord.created_at must be UTC-aware")
        if self.updated_at.tzinfo is None:
            raise ValueError("OrderRecord.updated_at must be UTC-aware")

    # ── Transitions ──────────────────────────────────────────────────────────

    def apply_result(self, result: ExecutionResult) -> None:
        """Применить ExecutionResult → новое состояние."""
        if self.state in TERMINAL_STATES:
            raise ValueError(f"Cannot transition from terminal state {self.state}")
        new_state = _RESULT_TO_STATE.get(result)
        if new_state is None:
            raise ValueError(f"Unknown ExecutionResult: {result}")
        self.state = new_state
        self.updated_at = datetime.now(timezone.utc)

    def apply_fill(self, fill: Fill) -> None:
        """Добавить Fill; PARTIAL_FILL если частичный, FILLED иначе."""
        if self.state in TERMINAL_STATES:
            raise ValueError(f"Cannot apply fill to terminal state {self.state}")
        self.fills.append(fill)
        self.state = OrderState.PARTIAL_FILL if fill.is_partial else OrderState.FILLED
        self.updated_at = datetime.now(timezone.utc)

    def mark_cancelled(self) -> None:
        if self.state in TERMINAL_STATES:
            raise ValueError(f"Cannot cancel in terminal state {self.state}")
        self.state = OrderState.CANCELLED
        self.updated_at = datetime.now(timezone.utc)

    def mark_rejected(self) -> None:
        if self.state in TERMINAL_STATES:
            raise ValueError(f"Cannot reject in terminal state {self.state}")
        self.state = OrderState.REJECTED
        self.updated_at = datetime.now(timezone.utc)

    # ── Properties ───────────────────────────────────────────────────────────

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    @property
    def total_filled_base_qty(self) -> Decimal:
        return sum((f.base_qty_filled for f in self.fills), Decimal("0"))

    @property
    def total_quote_amount(self) -> Decimal:
        return sum((f.quote_qty_filled for f in self.fills), Decimal("0"))

    @property
    def total_commission(self) -> Decimal:
        return sum((f.commission for f in self.fills), Decimal("0"))

    @property
    def fill_count(self) -> int:
        return len(self.fills)

    @property
    def avg_fill_price(self) -> Optional[Decimal]:
        """Средневзвешенная цена исполнения. None если филлов нет."""
        if not self.fills:
            return None
        total_base = self.total_filled_base_qty
        if total_base == Decimal("0"):
            return None
        return self.total_quote_amount / total_base
