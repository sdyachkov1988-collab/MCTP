"""
OrderStore — персистенция активных ордеров и OCO (v0.7).
Формат: JSON, атомарная запись через tmp-файл + os.replace().
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from mctp.core.types import Symbol
from mctp.core.enums import Market, Side, OrderType, CommissionAsset, QuantitySource, TimeInForce
from mctp.core.order import Order, Fill
from mctp.execution.oco import OCOOrder, OCOStatus
from mctp.storage.exceptions import StorageCorruptedError


# ── Serialisation helpers ─────────────────────────────────────────────────────

def _ser_symbol(symbol: Symbol) -> dict:
    return {"base": symbol.base, "quote": symbol.quote, "market": symbol.market.value}


def _des_symbol(d: dict) -> Symbol:
    return Symbol(base=d["base"], quote=d["quote"], market=Market(d["market"]))


def _ser_fill(fill: Fill) -> dict:
    return {
        "order_id":         fill.order_id,
        "symbol":           _ser_symbol(fill.symbol),
        "side":             fill.side.value,
        "base_qty_filled":  str(fill.base_qty_filled),
        "quote_qty_filled": str(fill.quote_qty_filled),
        "fill_price":       str(fill.fill_price),
        "commission":       str(fill.commission),
        "commission_asset": fill.commission_asset.value,
        "is_partial":       fill.is_partial,
        "trade_id":         fill.trade_id,
        "filled_at":        fill.filled_at.isoformat(),
    }


def _des_fill(d: dict) -> Fill:
    ts = datetime.fromisoformat(d["filled_at"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return Fill(
        order_id=d["order_id"],
        symbol=_des_symbol(d["symbol"]),
        side=Side(d["side"]),
        base_qty_filled=Decimal(d["base_qty_filled"]),
        quote_qty_filled=Decimal(d["quote_qty_filled"]),
        fill_price=Decimal(d["fill_price"]),
        commission=Decimal(d["commission"]),
        commission_asset=CommissionAsset(d["commission_asset"]),
        is_partial=d["is_partial"],
        trade_id=d["trade_id"],
        filled_at=ts,
    )


def _ser_order(order: Order) -> dict:
    return {
        "client_order_id": order.client_order_id,
        "symbol":          _ser_symbol(order.symbol),
        "side":            order.side.value,
        "order_type":      order.order_type.value,
        "quantity":        str(order.quantity),
        "price":           str(order.price) if order.price is not None else None,
        "quote_quantity":  str(order.quote_quantity) if order.quote_quantity is not None else None,
        "quantity_source": order.quantity_source.value,
        "time_in_force":   order.time_in_force.value,
        "post_only":       order.post_only,
        "reason":          order.reason,
        "created_at":      order.created_at.isoformat(),
    }


def _des_order(d: dict) -> Order:
    ts = datetime.fromisoformat(d["created_at"])
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return Order(
        client_order_id=d["client_order_id"],
        symbol=_des_symbol(d["symbol"]),
        side=Side(d["side"]),
        order_type=OrderType(d["order_type"]),
        quantity=Decimal(d["quantity"]),
        price=Decimal(d["price"]) if d["price"] is not None else None,
        quote_quantity=Decimal(d["quote_quantity"]) if d["quote_quantity"] is not None else None,
        quantity_source=QuantitySource(d["quantity_source"]),
        time_in_force=TimeInForce(d["time_in_force"]),
        post_only=d["post_only"],
        reason=d["reason"],
        created_at=ts,
    )


def _ser_oco(oco: OCOOrder) -> dict:
    return {
        "list_order_id":  oco.list_order_id,
        "symbol":         _ser_symbol(oco.symbol),
        "tp_price":       str(oco.tp_price),
        "sl_stop_price":  str(oco.sl_stop_price),
        "sl_limit_price": str(oco.sl_limit_price),
        "quantity":       str(oco.quantity),
        "tp_client_order_id": oco.tp_client_order_id,
        "sl_client_order_id": oco.sl_client_order_id,
        "status":         oco.status.value,
        "created_at":     oco.created_at.isoformat(),
        "updated_at":     oco.updated_at.isoformat(),
        "tp_fills":       [_ser_fill(f) for f in oco.tp_fills],
        "sl_fills":       [_ser_fill(f) for f in oco.sl_fills],
    }


def _des_oco(d: dict) -> OCOOrder:
    def _dt(s: str) -> datetime:
        ts = datetime.fromisoformat(s)
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)

    return OCOOrder(
        list_order_id=d["list_order_id"],
        symbol=_des_symbol(d["symbol"]),
        tp_price=Decimal(d["tp_price"]),
        sl_stop_price=Decimal(d["sl_stop_price"]),
        sl_limit_price=Decimal(d["sl_limit_price"]),
        quantity=Decimal(d["quantity"]),
        tp_client_order_id=d.get("tp_client_order_id"),
        sl_client_order_id=d.get("sl_client_order_id"),
        status=OCOStatus(d["status"]),
        created_at=_dt(d["created_at"]),
        updated_at=_dt(d["updated_at"]),
        tp_fills=[_des_fill(f) for f in d.get("tp_fills", [])],
        sl_fills=[_des_fill(f) for f in d.get("sl_fills", [])],
    )


# ── OrderStore ────────────────────────────────────────────────────────────────

class OrderStore:
    """
    Персистенция активных ордеров и OCO.
    Два независимых раздела: active_orders, active_ocos.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._tmp  = path + ".tmp"
        # in-memory mirrors
        self._orders: dict[str, Order]    = {}
        self._ocos:   dict[str, OCOOrder] = {}
        # lazy-load on first access
        self._loaded = False

    # ── Public API ────────────────────────────────────────────────────────────

    def save_order(self, order: Order) -> None:
        self._ensure_loaded()
        self._orders[order.client_order_id] = order
        self._flush()

    def remove_order(self, client_order_id: str) -> None:
        self._ensure_loaded()
        self._orders.pop(client_order_id, None)
        self._flush()

    def save_oco(self, oco: OCOOrder) -> None:
        self._ensure_loaded()
        self._ocos[oco.list_order_id] = oco
        self._flush()

    def remove_oco(self, list_order_id: str) -> None:
        self._ensure_loaded()
        self._ocos.pop(list_order_id, None)
        self._flush()

    def load(self) -> tuple[dict[str, Order], dict[str, OCOOrder]]:
        """Вернуть (orders, ocos). Если файл не существует — пустые dicts."""
        if not os.path.exists(self._path):
            return {}, {}
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            orders = {coid: _des_order(v) for coid, v in data.get("active_orders", {}).items()}
            ocos   = {lid:  _des_oco(v)   for lid,  v in data.get("active_ocos",   {}).items()}
            return orders, ocos
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            raise StorageCorruptedError(
                f"OrderStore file is corrupted: {self._path}"
            ) from exc

    def exists(self) -> bool:
        return os.path.exists(self._path) and os.path.getsize(self._path) > 0

    # ── Internal ──────────────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            orders, ocos = self.load()
            self._orders.update(orders)
            self._ocos.update(ocos)
            self._loaded = True

    def _flush(self) -> None:
        data = {
            "active_orders": {coid: _ser_order(o) for coid, o in self._orders.items()},
            "active_ocos":   {lid:  _ser_oco(oco)  for lid,  oco in self._ocos.items()},
        }
        with open(self._tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(self._tmp, self._path)
