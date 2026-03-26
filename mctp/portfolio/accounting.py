from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from mctp.core.enums import CommissionAsset
from mctp.core.order import Fill
from mctp.core.types import Symbol


@dataclass(frozen=True)
class AccountingFillRecord:
    trade_id: str
    order_id: str
    symbol: Symbol
    filled_at: datetime
    fill_price: Decimal
    commission: Decimal
    commission_asset: CommissionAsset
    fee_drag_quote: Decimal
    bnb_rate_at_fill: Optional[Decimal] = None

    def __post_init__(self) -> None:
        if self.filled_at.tzinfo is None:
            raise ValueError("AccountingFillRecord.filled_at must be UTC-aware")
        for name, value in [
            ("fill_price", self.fill_price),
            ("commission", self.commission),
            ("fee_drag_quote", self.fee_drag_quote),
        ]:
            if not isinstance(value, Decimal):
                raise AssertionError(f"AccountingFillRecord.{name} must be Decimal")
        if self.bnb_rate_at_fill is not None and not isinstance(self.bnb_rate_at_fill, Decimal):
            raise AssertionError("AccountingFillRecord.bnb_rate_at_fill must be Decimal or None")


class AccountingLedger:
    def __init__(self) -> None:
        self._fill_history: list[AccountingFillRecord] = []
        self._fee_drag_quote_total: Decimal = Decimal("0")

    @property
    def fill_history(self) -> list[AccountingFillRecord]:
        return list(self._fill_history)

    @property
    def fee_drag_quote_total(self) -> Decimal:
        return self._fee_drag_quote_total

    def restore_history(self, records: list[AccountingFillRecord]) -> None:
        self._fill_history = list(records)
        self._fee_drag_quote_total = sum((record.fee_drag_quote for record in self._fill_history), Decimal("0"))

    def record_fill(
        self,
        fill: Fill,
        bnb_rate_at_fill: Optional[Decimal],
    ) -> AccountingFillRecord:
        fee_drag_quote = self._fee_drag_quote(fill, bnb_rate_at_fill)
        record = AccountingFillRecord(
            trade_id=fill.trade_id,
            order_id=fill.order_id,
            symbol=fill.symbol,
            filled_at=fill.filled_at,
            fill_price=fill.fill_price,
            commission=fill.commission,
            commission_asset=fill.commission_asset,
            fee_drag_quote=fee_drag_quote,
            bnb_rate_at_fill=bnb_rate_at_fill,
        )
        self._fill_history.append(record)
        self._fee_drag_quote_total += fee_drag_quote
        return record

    @staticmethod
    def _fee_drag_quote(fill: Fill, bnb_rate_at_fill: Optional[Decimal]) -> Decimal:
        if fill.commission_asset == CommissionAsset.QUOTE:
            return fill.commission
        if fill.commission_asset == CommissionAsset.BASE:
            return fill.commission * fill.fill_price
        if bnb_rate_at_fill is None or bnb_rate_at_fill <= Decimal("0"):
            raise ValueError("bnb_rate_at_fill required and must be > 0 for BNB commission")
        return fill.commission * bnb_rate_at_fill
