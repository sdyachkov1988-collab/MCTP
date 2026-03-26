import pytest
import asyncio
from decimal import Decimal
from datetime import datetime, timezone

from mctp.core.types import Symbol
from mctp.core.enums import (
    Market, Side, OrderType, TimeInForce,
    QuantitySource, ExecutionResult, CommissionAsset
)
from mctp.core.order import Order, Fill
from mctp.core.interfaces import ExecutionInterface
from mctp.core.exceptions import MCTPError
from mctp.execution.paper import SpotPaperExecutor

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)

# ─── Order model tests ──────────────────────────────────────────────────────

def test_order_rejects_quote_quantity_for_sell():
    with pytest.raises(MCTPError):
        Order(
            symbol=BTCUSDT,
            side=Side.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            quantity_source=QuantitySource.QUOTE,
        )

def test_order_accepts_quote_quantity_for_buy():
    o = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
        quantity_source=QuantitySource.QUOTE,
        quote_quantity=Decimal("1000"),
    )
    assert o.quantity_source == QuantitySource.QUOTE

def test_order_rejects_float_quantity():
    with pytest.raises(AssertionError):
        Order(
            symbol=BTCUSDT,
            side=Side.BUY,
            order_type=OrderType.MARKET,
            quantity=0.1,
        )

def test_order_has_uuid_client_order_id():
    o1 = Order(symbol=BTCUSDT, side=Side.BUY,
               order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    o2 = Order(symbol=BTCUSDT, side=Side.BUY,
               order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    assert o1.client_order_id != o2.client_order_id
    assert len(o1.client_order_id) == 36  # UUID4 длина

# ─── Fill model tests ────────────────────────────────────────────────────────

def test_fill_net_base_received_bnb_commission():
    f = Fill(
        order_id="test",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("4000"),
        fill_price=Decimal("40000"),
        commission=Decimal("0.0001"),
        commission_asset=CommissionAsset.BNB,
    )
    # BNB комиссия не вычитается из базового актива
    assert f.net_base_received() == Decimal("0.1")

def test_fill_net_base_received_base_commission():
    f = Fill(
        order_id="test",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("4000"),
        fill_price=Decimal("40000"),
        commission=Decimal("0.0001"),
        commission_asset=CommissionAsset.BASE,
    )
    # BASE комиссия вычитается из базового актива
    assert f.net_base_received() == Decimal("0.0999")

def test_fill_net_quote_spent_quote_commission():
    f = Fill(
        order_id="test",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("4000"),
        fill_price=Decimal("40000"),
        commission=Decimal("4"),
        commission_asset=CommissionAsset.QUOTE,
    )
    # QUOTE комиссия добавляется к потраченному
    assert f.net_quote_spent() == Decimal("4004")

def test_fill_rejects_float():
    with pytest.raises(AssertionError):
        Fill(
            order_id="test",
            symbol=BTCUSDT,
            side=Side.BUY,
            base_qty_filled=0.1,  # float — должен упасть
            quote_qty_filled=Decimal("4000"),
            fill_price=Decimal("40000"),
            commission=Decimal("4"),
            commission_asset=CommissionAsset.QUOTE,
        )

# ─── SpotPaperExecutor tests ─────────────────────────────────────────────────

@pytest.fixture
def executor():
    ex = SpotPaperExecutor({
        "USDT": Decimal("10000"),
        "BTC": Decimal("0"),
    })
    ex.set_price(BTCUSDT, Decimal("40000"))
    return ex

@pytest.mark.asyncio
async def test_executor_implements_interface():
    assert issubclass(SpotPaperExecutor, ExecutionInterface)

@pytest.mark.asyncio
async def test_market_buy_fills_immediately(executor):
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
    )
    result = await executor.submit_order(order)
    assert result == ExecutionResult.FILLED

@pytest.mark.asyncio
async def test_market_buy_updates_balances(executor):
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("0.1"),
    )
    await executor.submit_order(order)
    balances = await executor.get_balances()
    assert balances["BTC"] == Decimal("0.1")
    # 0.1 BTC × 40000 = 4000 USDT + 4 USDT комиссия = 4004
    assert balances["USDT"] == Decimal("10000") - Decimal("4000") - Decimal("4")

@pytest.mark.asyncio
async def test_market_sell_updates_balances(executor):
    # Сначала купим
    buy = Order(symbol=BTCUSDT, side=Side.BUY,
                order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    await executor.submit_order(buy)
    btc_after_buy = (await executor.get_balances())["BTC"]
    # Теперь продадим
    sell = Order(symbol=BTCUSDT, side=Side.SELL,
                 order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    result = await executor.submit_order(sell)
    assert result == ExecutionResult.FILLED
    balances = await executor.get_balances()
    assert balances["BTC"] == Decimal("0")

@pytest.mark.asyncio
async def test_limit_order_returns_accepted(executor):
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        price=Decimal("39000"),
    )
    result = await executor.submit_order(order)
    assert result == ExecutionResult.ACCEPTED

@pytest.mark.asyncio
async def test_cancel_limit_order(executor):
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        price=Decimal("39000"),
    )
    await executor.submit_order(order)
    result = await executor.cancel_order(order.client_order_id)
    assert result == ExecutionResult.CANCELLED

@pytest.mark.asyncio
async def test_cancel_nonexistent_order_returns_rejected(executor):
    result = await executor.cancel_order("nonexistent-id")
    assert result == ExecutionResult.REJECTED

@pytest.mark.asyncio
async def test_get_balances_returns_decimal(executor):
    balances = await executor.get_balances()
    for asset, balance in balances.items():
        assert isinstance(balance, Decimal), f"{asset} balance is not Decimal"

@pytest.mark.asyncio
async def test_locked_balance_not_available(executor):
    # После LIMIT ордера баланс должен быть заблокирован
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        price=Decimal("40000"),
    )
    await executor.submit_order(order)
    balances = await executor.get_balances()
    # 0.1 × 40000 = 4000 USDT заблокировано
    assert balances["USDT"] == Decimal("10000") - Decimal("4000")

@pytest.mark.asyncio
async def test_cancel_restores_locked_balance(executor):
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        price=Decimal("40000"),
    )
    await executor.submit_order(order)
    await executor.cancel_order(order.client_order_id)
    balances = await executor.get_balances()
    assert balances["USDT"] == Decimal("10000")

@pytest.mark.asyncio
async def test_market_order_without_price_rejected(executor):
    # Если нет цены — ордер отклоняется
    ex = SpotPaperExecutor({"USDT": Decimal("1000"), "BTC": Decimal("0")})
    # Нет цены для символа
    order = Order(symbol=BTCUSDT, side=Side.BUY,
                  order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    result = await ex.submit_order(order)
    assert result == ExecutionResult.REJECTED

@pytest.mark.asyncio
async def test_executor_initial_balance_rejects_float():
    with pytest.raises(AssertionError):
        SpotPaperExecutor({"USDT": 1000.0})  # float — должен упасть
