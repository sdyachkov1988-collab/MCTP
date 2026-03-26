"""
Тесты v0.4: OrderRecord, OCOOrder, CostBasisUpdater, PnLCalculator,
             SpotPaperExecutor (расширенный).
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone

from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import Market, Side, OrderType, CommissionAsset, ExecutionResult
from mctp.core.order import Order, Fill
from mctp.core.constants import SL_EXECUTION_BUFFER

from mctp.execution.lifecycle import OrderRecord, OrderState, TERMINAL_STATES
from mctp.execution.oco import OCOOrder, OCOStatus, TERMINAL_OCO_STATUSES
from mctp.execution.paper import SpotPaperExecutor

from mctp.portfolio.updater import CostBasisUpdater
from mctp.portfolio.pnl import PnLCalculator, PnLResult

# ─── Fixtures / helpers ───────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)


def _fill(
    side: Side = Side.BUY,
    base_qty: Decimal = Decimal("0.1"),
    quote_qty: Decimal = Decimal("4000"),
    price: Decimal = Decimal("40000"),
    commission: Decimal = Decimal("4"),
    commission_asset: CommissionAsset = CommissionAsset.QUOTE,
    is_partial: bool = False,
) -> Fill:
    return Fill(
        order_id="test",
        symbol=BTCUSDT,
        side=side,
        base_qty_filled=base_qty,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=commission_asset,
        is_partial=is_partial,
    )


def _snapshot(
    held_qty: Decimal = Decimal("0"),
    avg_cost_basis: Decimal = Decimal("0"),
    free_quote: Decimal = Decimal("10000"),
    is_in_position: bool = False,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=held_qty,
        avg_cost_basis=avg_cost_basis,
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=is_in_position,
        meaningful_position=is_in_position,
    )


def _oco(
    qty: Decimal = Decimal("0.1"),
    tp_price: Decimal = Decimal("42000"),
    sl_stop_price: Decimal = Decimal("38000"),
) -> OCOOrder:
    sl_limit = sl_stop_price * (Decimal("1") - SL_EXECUTION_BUFFER)
    return OCOOrder(
        symbol=BTCUSDT,
        tp_price=tp_price,
        sl_stop_price=sl_stop_price,
        sl_limit_price=sl_limit,
        quantity=qty,
    )


@pytest.fixture
def executor():
    ex = SpotPaperExecutor({"USDT": Decimal("10000"), "BTC": Decimal("0")})
    ex.set_price(BTCUSDT, Decimal("40000"))
    return ex


@pytest.fixture
def executor_with_btc():
    ex = SpotPaperExecutor({"USDT": Decimal("10000"), "BTC": Decimal("1")})
    ex.set_price(BTCUSDT, Decimal("40000"))
    return ex


# ════════════════════════════════════════════════════════════════════════════
# OrderRecord
# ════════════════════════════════════════════════════════════════════════════

def test_order_record_initial_state_is_pending():
    rec = OrderRecord(client_order_id="abc")
    assert rec.state == OrderState.PENDING
    assert rec.is_terminal is False
    assert rec.fill_count == 0


def test_order_record_apply_result_accepted():
    rec = OrderRecord(client_order_id="abc")
    rec.apply_result(ExecutionResult.ACCEPTED)
    assert rec.state == OrderState.ACCEPTED


def test_order_record_apply_fill_partial():
    rec = OrderRecord(client_order_id="abc")
    rec.apply_fill(_fill(is_partial=True))
    assert rec.state == OrderState.PARTIAL_FILL
    assert rec.is_terminal is False


def test_order_record_apply_fill_full():
    rec = OrderRecord(client_order_id="abc")
    rec.apply_fill(_fill(is_partial=False))
    assert rec.state == OrderState.FILLED
    assert rec.is_terminal is True


def test_order_record_terminal_blocks_transitions():
    rec = OrderRecord(client_order_id="abc")
    rec.apply_fill(_fill(is_partial=False))   # → FILLED (terminal)
    with pytest.raises(ValueError):
        rec.apply_fill(_fill())
    with pytest.raises(ValueError):
        rec.apply_result(ExecutionResult.ACCEPTED)
    with pytest.raises(ValueError):
        rec.mark_cancelled()


def test_order_record_aggregates_multiple_fills():
    rec = OrderRecord(client_order_id="abc")
    f1 = _fill(base_qty=Decimal("1"), quote_qty=Decimal("40000"),
               commission=Decimal("40"), is_partial=True)
    f2 = _fill(base_qty=Decimal("1"), quote_qty=Decimal("42000"),
               commission=Decimal("42"), is_partial=True)
    rec.apply_fill(f1)
    rec.apply_fill(f2)
    assert rec.fill_count == 2
    assert rec.total_filled_base_qty == Decimal("2")
    assert rec.total_quote_amount == Decimal("82000")
    assert rec.total_commission == Decimal("82")


def test_order_record_avg_fill_price_none_when_empty():
    rec = OrderRecord(client_order_id="abc")
    assert rec.avg_fill_price is None


def test_order_record_avg_fill_price_weighted():
    """avg = (40000 + 42000) / (1 + 1) = 41000"""
    rec = OrderRecord(client_order_id="abc")
    rec.apply_fill(_fill(base_qty=Decimal("1"), quote_qty=Decimal("40000"), is_partial=True))
    rec.apply_fill(_fill(base_qty=Decimal("1"), quote_qty=Decimal("42000"), is_partial=False))
    assert rec.avg_fill_price == Decimal("41000")


# ════════════════════════════════════════════════════════════════════════════
# OCOOrder
# ════════════════════════════════════════════════════════════════════════════

def test_oco_validates_sl_limit_lt_sl_stop():
    with pytest.raises(ValueError):
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("42000"),
            sl_stop_price=Decimal("38000"),
            sl_limit_price=Decimal("38000"),   # equal → invalid
            quantity=Decimal("0.1"),
        )
    with pytest.raises(ValueError):
        OCOOrder(
            symbol=BTCUSDT,
            tp_price=Decimal("42000"),
            sl_stop_price=Decimal("38000"),
            sl_limit_price=Decimal("39000"),   # greater → invalid
            quantity=Decimal("0.1"),
        )


def test_oco_remaining_qty_correct():
    oco = _oco(qty=Decimal("1.0"))
    fill = _fill(side=Side.SELL, base_qty=Decimal("0.3"))
    oco.tp_fills.append(fill)
    assert oco.tp_filled_qty == Decimal("0.3")
    assert oco.remaining_qty == Decimal("0.7")


def test_oco_remaining_qty_never_negative():
    oco = _oco(qty=Decimal("0.1"))
    # Добавляем чрезмерный fill
    oco.tp_fills.append(_fill(side=Side.SELL, base_qty=Decimal("0.2")))
    assert oco.remaining_qty == Decimal("0")


def test_oco_check_status_returns_current():
    oco = _oco()
    assert oco.check_status() == OCOStatus.ACTIVE
    oco.status = OCOStatus.TP_PARTIAL
    assert oco.check_status() == OCOStatus.TP_PARTIAL


def test_oco_terminal_statuses():
    terminal = {OCOStatus.TP_FILLED, OCOStatus.SL_TRIGGERED,
                OCOStatus.PARTIAL_TP_THEN_SL, OCOStatus.CANCELLED}
    assert TERMINAL_OCO_STATUSES == terminal
    for s in terminal:
        oco = _oco()
        oco.status = s
        assert oco.is_terminal is True
    active = _oco()
    assert active.is_terminal is False


# ════════════════════════════════════════════════════════════════════════════
# CostBasisUpdater
# ════════════════════════════════════════════════════════════════════════════

def test_updater_buy_first_fill_basis_equals_price():
    snap = _snapshot()
    fill = _fill(side=Side.BUY, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4000"), price=Decimal("40000"),
                 commission=Decimal("4"), commission_asset=CommissionAsset.QUOTE)
    new_snap = CostBasisUpdater.apply_buy_fill(snap, fill)
    # net_base = 0.1 (QUOTE commission), new_held = 0.1
    # basis = (0 * 0 + 0.1 * 40000) / 0.1 = 40000
    assert new_snap.avg_cost_basis == Decimal("40040")
    assert new_snap.held_qty == Decimal("0.1")
    assert new_snap.is_in_position is True


def test_updater_buy_scale_in_weighted_average():
    """
    Initial: held=0.1 @ 40000 → basis=40000
    Buy 0.1 @ 42000 → new_held=0.2
    new_basis = (0.1×40000 + 0.1×42000) / 0.2 = 41000
    """
    snap = _snapshot(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"),
                     is_in_position=True)
    fill = _fill(side=Side.BUY, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4200"), price=Decimal("42000"),
                 commission=Decimal("4.2"), commission_asset=CommissionAsset.QUOTE)
    new_snap = CostBasisUpdater.apply_buy_fill(snap, fill)
    assert new_snap.avg_cost_basis == Decimal("41021")
    assert new_snap.held_qty == Decimal("0.2")


def test_updater_sell_partial_preserves_basis():
    snap = _snapshot(held_qty=Decimal("0.2"), avg_cost_basis=Decimal("41000"),
                     is_in_position=True)
    fill = _fill(side=Side.SELL, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4500"), price=Decimal("45000"),
                 commission=Decimal("4.5"))
    new_snap = CostBasisUpdater.apply_sell_fill(snap, fill)
    assert new_snap.avg_cost_basis == Decimal("41000")   # сохранён
    assert new_snap.held_qty == Decimal("0.1")
    assert new_snap.is_in_position is True


def test_updater_sell_full_zeroes_basis():
    snap = _snapshot(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"),
                     is_in_position=True)
    fill = _fill(side=Side.SELL, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4500"), price=Decimal("45000"),
                 commission=Decimal("4.5"))
    new_snap = CostBasisUpdater.apply_sell_fill(snap, fill)
    assert new_snap.avg_cost_basis == Decimal("0")
    assert new_snap.held_qty == Decimal("0")
    assert new_snap.is_in_position is False


def test_updater_buy_base_commission_reduces_held():
    """BASE commission: net_base = 0.1 - 0.0001 = 0.0999"""
    snap = _snapshot()
    fill = _fill(side=Side.BUY, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4000"), price=Decimal("40000"),
                 commission=Decimal("0.0001"), commission_asset=CommissionAsset.BASE)
    new_snap = CostBasisUpdater.apply_buy_fill(snap, fill)
    assert new_snap.held_qty == Decimal("0.0999")
    # basis = (0 + 0.0999 * 40000) / 0.0999 = 40000
    assert new_snap.avg_cost_basis == Decimal("40040.04004004004004004004004")


def test_updater_does_not_mutate_original_snapshot():
    snap = _snapshot(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"),
                     is_in_position=True)
    original_held  = snap.held_qty
    original_basis = snap.avg_cost_basis
    fill = _fill(side=Side.SELL, base_qty=Decimal("0.1"),
                 quote_qty=Decimal("4500"), price=Decimal("45000"),
                 commission=Decimal("4.5"))
    _ = CostBasisUpdater.apply_sell_fill(snap, fill)
    assert snap.held_qty == original_held
    assert snap.avg_cost_basis == original_basis


# ════════════════════════════════════════════════════════════════════════════
# PnLCalculator — контракт 23 (все три случая)
# ════════════════════════════════════════════════════════════════════════════

def test_pnl_quote_commission_profitable():
    """SELL 0.1 @ 45000. Basis=40000. gross=500, comm=4.5, net=495.5"""
    f = _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4500"),
              price=Decimal("45000"), commission=Decimal("4.5"),
              commission_asset=CommissionAsset.QUOTE)
    r = PnLCalculator.realized_pnl(f, Decimal("40000"))
    assert r.gross_pnl        == Decimal("500.0")
    assert r.commission_quote == Decimal("4.5")
    assert r.net_pnl          == Decimal("495.5")


def test_pnl_quote_commission_loss():
    """SELL 0.1 @ 35000. Basis=40000. gross=-500, comm=3.5, net=-503.5"""
    f = _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("3500"),
              price=Decimal("35000"), commission=Decimal("3.5"),
              commission_asset=CommissionAsset.QUOTE)
    r = PnLCalculator.realized_pnl(f, Decimal("40000"))
    assert r.gross_pnl == Decimal("-500.0")
    assert r.net_pnl   == Decimal("-503.5")


def test_pnl_base_commission():
    """
    SELL 0.1 @ 45000, BASE commission=0.0001
    net_sold = 0.0999
    net_pnl  = 0.0999 × 45000 − 0.1 × 40000 = 4495.5 − 4000 = 495.5
    comm_q   = 0.0001 × 45000 = 4.5
    """
    f = _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4500"),
              price=Decimal("45000"), commission=Decimal("0.0001"),
              commission_asset=CommissionAsset.BASE)
    r = PnLCalculator.realized_pnl(f, Decimal("40000"))
    assert r.net_pnl          == Decimal("495.5")
    assert r.commission_quote == Decimal("4.5")


def test_pnl_bnb_commission():
    """
    SELL 0.1 @ 45000, BNB comm=0.005, bnb_price=300
    gross = 500, comm_q = 1.5, net = 498.5
    """
    f = _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4500"),
              price=Decimal("45000"), commission=Decimal("0.005"),
              commission_asset=CommissionAsset.BNB)
    r = PnLCalculator.realized_pnl(f, Decimal("40000"), bnb_price=Decimal("300"))
    assert r.commission_quote == Decimal("1.5")
    assert r.net_pnl          == Decimal("498.5")
    assert r.bnb_rate_used    == Decimal("300")


def test_pnl_bnb_requires_price():
    f = _fill(side=Side.SELL, commission_asset=CommissionAsset.BNB,
              commission=Decimal("0.005"))
    with pytest.raises(ValueError):
        PnLCalculator.realized_pnl(f, Decimal("40000"), bnb_price=None)


def test_pnl_buy_fill_raises():
    f = _fill(side=Side.BUY)
    with pytest.raises(ValueError):
        PnLCalculator.realized_pnl(f, Decimal("40000"))


def test_pnl_per_lot_two_fills():
    fills = [
        _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4500"),
              price=Decimal("45000"), commission=Decimal("4.5")),
        _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4600"),
              price=Decimal("46000"), commission=Decimal("4.6")),
    ]
    results = PnLCalculator.pnl_per_lot(fills, Decimal("40000"))
    assert len(results) == 2
    assert results[0].net_pnl == Decimal("495.5")
    assert results[1].net_pnl == Decimal("595.4")


def test_pnl_per_lot_skips_buy_fills():
    fills = [
        _fill(side=Side.BUY),
        _fill(side=Side.SELL, base_qty=Decimal("0.1"), quote_qty=Decimal("4500"),
              price=Decimal("45000"), commission=Decimal("4.5")),
    ]
    results = PnLCalculator.pnl_per_lot(fills, Decimal("40000"))
    assert len(results) == 1   # BUY пропущен


def test_pnl_total_net_pnl():
    pnl_list = [
        PnLResult(gross_pnl=Decimal("500"), commission_quote=Decimal("4.5"),
                  net_pnl=Decimal("495.5"), commission_asset=CommissionAsset.QUOTE),
        PnLResult(gross_pnl=Decimal("-200"), commission_quote=Decimal("3"),
                  net_pnl=Decimal("-203"), commission_asset=CommissionAsset.QUOTE),
    ]
    assert PnLCalculator.total_net_pnl(pnl_list) == Decimal("292.5")


# ════════════════════════════════════════════════════════════════════════════
# SpotPaperExecutor v0.4
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_paper_market_creates_filled_record(executor):
    order = Order(symbol=BTCUSDT, side=Side.BUY,
                  order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    await executor.submit_order(order)
    rec = executor._order_records[order.client_order_id]
    assert rec.state == OrderState.FILLED
    assert rec.fill_count == 1


@pytest.mark.asyncio
async def test_paper_limit_creates_accepted_record(executor):
    order = Order(symbol=BTCUSDT, side=Side.BUY, order_type=OrderType.LIMIT,
                  quantity=Decimal("0.1"), price=Decimal("39000"))
    await executor.submit_order(order)
    rec = executor._order_records[order.client_order_id]
    assert rec.state == OrderState.ACCEPTED


@pytest.mark.asyncio
async def test_paper_cancel_creates_cancelled_record(executor):
    order = Order(symbol=BTCUSDT, side=Side.BUY, order_type=OrderType.LIMIT,
                  quantity=Decimal("0.1"), price=Decimal("39000"))
    await executor.submit_order(order)
    await executor.cancel_order(order.client_order_id)
    rec = executor._order_records[order.client_order_id]
    assert rec.state == OrderState.CANCELLED


@pytest.mark.asyncio
async def test_paper_get_fills_returns_fills(executor):
    order = Order(symbol=BTCUSDT, side=Side.BUY,
                  order_type=OrderType.MARKET, quantity=Decimal("0.1"))
    await executor.submit_order(order)
    fills = await executor.get_fills(order.client_order_id)
    assert len(fills) == 1
    assert fills[0].base_qty_filled == Decimal("0.1")


@pytest.mark.asyncio
async def test_paper_get_fills_empty_for_unknown(executor):
    fills = await executor.get_fills("nonexistent-id")
    assert fills == []


@pytest.mark.asyncio
async def test_paper_set_price_triggers_limit_order(executor):
    """LIMIT BUY at 39000 triggers when price drops to 38000."""
    order = Order(symbol=BTCUSDT, side=Side.BUY, order_type=OrderType.LIMIT,
                  quantity=Decimal("0.1"), price=Decimal("39000"))
    await executor.submit_order(order)
    executor.set_price(BTCUSDT, Decimal("38000"))   # 38000 <= 39000 → fills
    rec = executor._order_records[order.client_order_id]
    assert rec.state == OrderState.FILLED
    fills = await executor.get_fills(order.client_order_id)
    assert len(fills) == 1


@pytest.mark.asyncio
async def test_paper_oco_tp_triggered(executor_with_btc):
    oco = _oco(qty=Decimal("0.1"))
    lid = executor_with_btc.submit_oco(oco)
    executor_with_btc.set_price(BTCUSDT, Decimal("43000"))   # >= tp=42000
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.TP_FILLED


@pytest.mark.asyncio
async def test_paper_oco_sl_triggered(executor_with_btc):
    oco = _oco(qty=Decimal("0.1"))
    lid = executor_with_btc.submit_oco(oco)
    executor_with_btc.set_price(BTCUSDT, Decimal("37000"))   # <= sl_stop=38000
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.SL_TRIGGERED


@pytest.mark.asyncio
async def test_paper_oco_sl_fill_uses_limit_price_not_stop(executor_with_btc):
    """SL-филл исполняется по sl_limit_price, не sl_stop_price."""
    oco = _oco(qty=Decimal("0.1"))
    expected_sl_limit = Decimal("38000") * (Decimal("1") - SL_EXECUTION_BUFFER)
    lid = executor_with_btc.submit_oco(oco)
    results = executor_with_btc.set_price(BTCUSDT, Decimal("37000"))
    assert len(results) == 1
    fill = results[0].new_fills[0]
    assert fill.fill_price == expected_sl_limit
    assert fill.fill_price != Decimal("38000")


@pytest.mark.asyncio
async def test_paper_oco_partial_tp_then_sl(executor_with_btc):
    """
    simulate_partial_tp_fill → TP_PARTIAL
    set_price below SL → PARTIAL_TP_THEN_SL
    """
    oco = _oco(qty=Decimal("1.0"))
    lid = executor_with_btc.submit_oco(oco)
    fill = executor_with_btc.simulate_partial_tp_fill(lid, Decimal("0.5"))
    assert fill is not None
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.TP_PARTIAL
    executor_with_btc.set_price(BTCUSDT, Decimal("37000"))
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.PARTIAL_TP_THEN_SL


@pytest.mark.asyncio
async def test_paper_oco_check_status_before_cancel(executor_with_btc):
    oco = _oco()
    lid = executor_with_btc.submit_oco(oco)
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.ACTIVE
    await executor_with_btc.cancel_oco(lid)
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.CANCELLED


@pytest.mark.asyncio
async def test_paper_oco_cancel_terminal_is_noop(executor_with_btc):
    """Отмена уже завершённого OCO возвращает CANCELLED, статус не меняется."""
    oco = _oco(qty=Decimal("0.1"))
    lid = executor_with_btc.submit_oco(oco)
    executor_with_btc.set_price(BTCUSDT, Decimal("43000"))   # TP_FILLED
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.TP_FILLED
    result = await executor_with_btc.cancel_oco(lid)
    assert result == ExecutionResult.CANCELLED
    assert executor_with_btc.check_oco_status(lid) == OCOStatus.TP_FILLED  # не изменился


@pytest.mark.asyncio
async def test_paper_get_oco_visible_after_completion(executor_with_btc):
    oco = _oco(qty=Decimal("0.1"))
    lid = executor_with_btc.submit_oco(oco)
    executor_with_btc.set_price(BTCUSDT, Decimal("43000"))   # TP_FILLED
    retrieved = executor_with_btc.get_oco(lid)
    assert retrieved is not None
    assert retrieved.status == OCOStatus.TP_FILLED


@pytest.mark.asyncio
async def test_paper_balances_updated_after_oco_fill(executor_with_btc):
    """
    Initial: BTC=1, USDT=10000
    OCO SELL 0.1 BTC, TP=42000
    Fill: 0.1 × 42000 = 4200, comm = 4.2
    Expected: BTC=0.9, USDT=10000 + 4200 - 4.2 = 14195.8
    """
    oco = _oco(qty=Decimal("0.1"), tp_price=Decimal("42000"))
    executor_with_btc.submit_oco(oco)
    executor_with_btc.set_price(BTCUSDT, Decimal("43000"))
    balances = await executor_with_btc.get_balances()
    assert balances["BTC"]  == Decimal("0.9")
    assert balances["USDT"] == Decimal("14195.8")
