"""
Модели ордера и филла. Контракты 15, 22, 23.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional
import uuid

from .enums import (
    Side, OrderType, TimeInForce, QuantitySource,
    ExecutionResult, CommissionAsset, Market
)
from .types import Symbol


@dataclass
class Order:
    """
    Полная модель спот-ордера.
    quantity_source=QUOTE только для BUY (контракт 15).
    """
    symbol: Symbol
    side: Side
    order_type: OrderType
    quantity: Decimal                          # базовый актив (BASE)
    price: Optional[Decimal] = None            # для LIMIT ордеров
    quote_quantity: Optional[Decimal] = None   # для QUOTE ордеров (только BUY)
    quantity_source: QuantitySource = QuantitySource.BASE
    time_in_force: TimeInForce = TimeInForce.GTC
    post_only: bool = False
    reason: str = ""
    client_order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        # Контракт 15: quote_quantity только для BUY
        if self.quantity_source == QuantitySource.QUOTE and self.side == Side.SELL:
            from .exceptions import MCTPError
            raise MCTPError("quote_quantity forbidden for SELL orders (contract 15)")
        # Контракт 36: Decimal везде
        if not isinstance(self.quantity, Decimal):
            raise AssertionError("Order.quantity must be Decimal")
        if self.price is not None and not isinstance(self.price, Decimal):
            raise AssertionError("Order.price must be Decimal")
        if self.quote_quantity is not None and not isinstance(self.quote_quantity, Decimal):
            raise AssertionError("Order.quote_quantity must be Decimal")
        # Контракт 37: UTC
        if self.created_at.tzinfo is None:
            raise ValueError("Order.created_at must be UTC-aware")


@dataclass
class Fill:
    """
    Спот-модель филла. Контракт 23.
    Все три случая комиссии: BNB / BASE / QUOTE.
    avg_cost_basis обновляется ТОЛЬКО из филла.
    """
    order_id: str
    symbol: Symbol
    side: Side
    base_qty_filled: Decimal      # количество базового актива
    quote_qty_filled: Decimal     # потраченный/полученный котируемый актив
    fill_price: Decimal           # фактическая цена исполнения
    commission: Decimal           # размер комиссии
    commission_asset: CommissionAsset
    is_partial: bool = False
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    filled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self):
        # Контракт 36
        for name, val in [
            ("base_qty_filled", self.base_qty_filled),
            ("quote_qty_filled", self.quote_qty_filled),
            ("fill_price", self.fill_price),
            ("commission", self.commission),
        ]:
            if not isinstance(val, Decimal):
                raise AssertionError(f"Fill.{name} must be Decimal")
        if self.filled_at.tzinfo is None:
            raise ValueError("Fill.filled_at must be UTC-aware")

    def net_base_received(self) -> Decimal:
        """
        Чистое количество базового актива после комиссии.
        Контракт 23 СЛУЧАЙ BASE: net_received = base_qty_filled - commission
        """
        if self.commission_asset == CommissionAsset.BASE:
            return self.base_qty_filled - self.commission
        return self.base_qty_filled

    def net_quote_spent(self) -> Decimal:
        """
        Чистое количество котируемого актива с комиссией.
        Контракт 23 СЛУЧАЙ QUOTE: net_spent = quote_qty_filled + commission
        """
        if self.commission_asset == CommissionAsset.QUOTE:
            return self.quote_qty_filled + self.commission
        return self.quote_qty_filled
