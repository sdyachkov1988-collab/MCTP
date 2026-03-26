"""
Тесты v0.7: should_record (регрессия), OrderStore, OrderTracker,
             WsEventDeduplicator, SpotPaperExecutor + OrderStore интеграция.
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import Market, Side, OrderType, TimeInForce, CommissionAsset
from mctp.core.order import Order, Fill
from mctp.core.constants import N_SNAP, SL_EXECUTION_BUFFER

from mctp.portfolio.equity import EquityTracker

from mctp.storage.order_store import OrderStore
from mctp.storage.exceptions import StorageCorruptedError

from mctp.execution.order_tracker import OrderTracker
from mctp.execution.paper import SpotPaperExecutor
from mctp.execution.oco import OCOOrder

from mctp.streams.dedup import WsEventDeduplicator

# ─── Helpers ──────────────────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
_NOW    = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)


def _make_limit_order(
    price: Decimal = Decimal("40000"),
    qty: Decimal = Decimal("0.1"),
    side: Side = Side.BUY,
) -> Order:
    return Order(
        symbol=BTCUSDT,
        side=side,
        order_type=OrderType.LIMIT,
        quantity=qty,
        price=price,
    )


def _make_oco(
    tp: Decimal = Decimal("45000"),
    sl_stop: Decimal = Decimal("38000"),
    qty: Decimal = Decimal("0.1"),
) -> OCOOrder:
    sl_limit = sl_stop * (Decimal("1") - SL_EXECUTION_BUFFER)
    return OCOOrder(
        symbol=BTCUSDT,
        tp_price=tp,
        sl_stop_price=sl_stop,
        sl_limit_price=sl_limit,
        quantity=qty,
    )


def _make_fill(price: Decimal = Decimal("45000"), qty: Decimal = Decimal("0.1")) -> Fill:
    quote_qty  = qty * price
    commission = quote_qty * Decimal("0.001")
    return Fill(
        order_id="fill-test",
        symbol=BTCUSDT,
        side=Side.SELL,
        base_qty_filled=qty,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=CommissionAsset.QUOTE,
    )


# ════════════════════════════════════════════════════════════════════════════
# should_record — регрессия минора v0.6
# ════════════════════════════════════════════════════════════════════════════

def test_should_record_true_when_history_empty():
    tracker = EquityTracker(Decimal("10000"))
    assert tracker.should_record(_NOW) is True


def test_should_record_false_before_n_snap_minutes():
    """Через 1 минуту после записи — False (< N_SNAP минут)."""
    tracker = EquityTracker(Decimal("10000"))
    snap = EquityTracker.make_snapshot(Decimal("10000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap)
    one_minute_later = _NOW + timedelta(minutes=1)
    assert tracker.should_record(one_minute_later) is False


def test_should_record_true_after_n_snap_minutes():
    """Через N_SNAP + 1 минуту — True."""
    tracker = EquityTracker(Decimal("10000"))
    snap = EquityTracker.make_snapshot(Decimal("10000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap)
    later = _NOW + timedelta(minutes=N_SNAP + 1)
    assert tracker.should_record(later) is True


def test_should_record_true_exactly_at_n_snap():
    """Ровно N_SNAP минут спустя — True (>=)."""
    tracker = EquityTracker(Decimal("10000"))
    snap = EquityTracker.make_snapshot(Decimal("10000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap)
    exactly = _NOW + timedelta(minutes=N_SNAP)
    assert tracker.should_record(exactly) is True


# ════════════════════════════════════════════════════════════════════════════
# OrderStore
# ════════════════════════════════════════════════════════════════════════════

def test_order_store_save_and_load_order(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    order = _make_limit_order()
    store.save_order(order)
    orders, ocos = store.load()
    assert order.client_order_id in orders
    loaded = orders[order.client_order_id]
    assert loaded.symbol.base      == "BTC"
    assert loaded.quantity         == order.quantity
    assert loaded.price            == order.price
    assert loaded.order_type       == OrderType.LIMIT
    assert loaded.client_order_id  == order.client_order_id


def test_order_store_save_and_load_oco(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    oco   = _make_oco()
    store.save_oco(oco)
    _, ocos = store.load()
    assert oco.list_order_id in ocos
    loaded = ocos[oco.list_order_id]
    assert loaded.tp_price      == oco.tp_price
    assert loaded.sl_stop_price == oco.sl_stop_price
    assert loaded.sl_limit_price == oco.sl_limit_price
    assert loaded.quantity      == oco.quantity


def test_order_store_remove_order(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    order = _make_limit_order()
    store.save_order(order)
    store.remove_order(order.client_order_id)
    orders, _ = store.load()
    assert order.client_order_id not in orders


def test_order_store_remove_oco(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    oco   = _make_oco()
    store.save_oco(oco)
    store.remove_oco(oco.list_order_id)
    _, ocos = store.load()
    assert oco.list_order_id not in ocos


def test_order_store_remove_nonexistent_no_error(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    store.remove_order("does-not-exist")  # должен молча игнорировать
    store.remove_oco("does-not-exist")


def test_order_store_empty_load_returns_empty_dicts(tmp_path):
    store = OrderStore(str(tmp_path / "missing.json"))
    orders, ocos = store.load()
    assert orders == {}
    assert ocos   == {}


def test_order_store_corrupted_raises(tmp_path):
    path = str(tmp_path / "orders.json")
    with open(path, "w") as f:
        f.write("{bad json{{")
    store = OrderStore(path)
    with pytest.raises(StorageCorruptedError):
        store.load()


def test_order_store_atomic_write(tmp_path):
    """После save нет tmp-файла."""
    path  = str(tmp_path / "orders.json")
    store = OrderStore(path)
    store.save_order(_make_limit_order())
    import os
    assert not os.path.exists(path + ".tmp")


def test_order_store_decimal_precision_preserved(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    order = _make_limit_order(price=Decimal("39999.99999"))
    store.save_order(order)
    orders, _ = store.load()
    assert orders[order.client_order_id].price == Decimal("39999.99999")


def test_order_store_oco_fills_preserved(tmp_path):
    store = OrderStore(str(tmp_path / "orders.json"))
    oco   = _make_oco()
    fill  = _make_fill()
    oco.tp_fills.append(fill)
    store.save_oco(oco)
    _, ocos = store.load()
    loaded = ocos[oco.list_order_id]
    assert len(loaded.tp_fills) == 1
    assert loaded.tp_fills[0].fill_price     == fill.fill_price
    assert loaded.tp_fills[0].base_qty_filled == fill.base_qty_filled
    assert loaded.tp_fills[0].commission     == fill.commission


def test_order_store_multiple_orders(tmp_path):
    store  = OrderStore(str(tmp_path / "orders.json"))
    order1 = _make_limit_order(price=Decimal("40000"))
    order2 = _make_limit_order(price=Decimal("39500"))
    order3 = _make_limit_order(price=Decimal("39000"))
    for o in [order1, order2, order3]:
        store.save_order(o)
    orders, _ = store.load()
    assert len(orders) == 3
    assert order1.client_order_id in orders
    assert order2.client_order_id in orders
    assert order3.client_order_id in orders


# ════════════════════════════════════════════════════════════════════════════
# OrderTracker
# ════════════════════════════════════════════════════════════════════════════

def test_order_tracker_register_and_get():
    tracker = OrderTracker()
    order   = _make_limit_order()
    tracker.register(order)
    assert tracker.get_order(order.client_order_id) is order


def test_order_tracker_unregister():
    tracker = OrderTracker()
    order   = _make_limit_order()
    tracker.register(order)
    tracker.unregister(order.client_order_id)
    assert tracker.get_order(order.client_order_id) is None


def test_order_tracker_scale_in_separate():
    """is_scale_in=True → только в active_scale_ins(), не в active_orders()."""
    tracker   = OrderTracker()
    regular   = _make_limit_order(price=Decimal("40000"))
    scale_in  = _make_limit_order(price=Decimal("39000"))
    tracker.register(regular)
    tracker.register(scale_in, is_scale_in=True)

    reg_ids = [o.client_order_id for o in tracker.active_orders()]
    si_ids  = [o.client_order_id for o in tracker.active_scale_ins()]

    assert regular.client_order_id   in reg_ids
    assert scale_in.client_order_id  not in reg_ids
    assert scale_in.client_order_id  in si_ids
    assert regular.client_order_id   not in si_ids


def test_order_tracker_is_scale_in_false_for_regular():
    tracker = OrderTracker()
    order   = _make_limit_order()
    tracker.register(order)
    assert tracker.is_scale_in(order.client_order_id) is False


def test_order_tracker_all_active_includes_both():
    tracker  = OrderTracker()
    regular  = _make_limit_order(price=Decimal("40000"))
    scale_in = _make_limit_order(price=Decimal("39000"))
    tracker.register(regular)
    tracker.register(scale_in, is_scale_in=True)
    all_ids = [o.client_order_id for o in tracker.all_active()]
    assert regular.client_order_id  in all_ids
    assert scale_in.client_order_id in all_ids


def test_order_tracker_count_correct():
    tracker = OrderTracker()
    tracker.register(_make_limit_order(price=Decimal("40000")))
    tracker.register(_make_limit_order(price=Decimal("39000")), is_scale_in=True)
    assert tracker.count() == 2


def test_order_tracker_unregister_nonexistent_no_error():
    tracker = OrderTracker()
    tracker.unregister("not-registered")  # молча


def test_order_tracker_get_nonexistent_returns_none():
    tracker = OrderTracker()
    assert tracker.get_order("missing") is None


# ════════════════════════════════════════════════════════════════════════════
# WsEventDeduplicator
# ════════════════════════════════════════════════════════════════════════════

def test_dedup_new_event_not_duplicate():
    dedup = WsEventDeduplicator()
    assert dedup.is_duplicate("evt-001") is False


def test_dedup_same_event_is_duplicate():
    dedup = WsEventDeduplicator()
    dedup.is_duplicate("evt-001")
    assert dedup.is_duplicate("evt-001") is True


def test_dedup_different_events_not_duplicate():
    dedup = WsEventDeduplicator()
    dedup.is_duplicate("evt-001")
    assert dedup.is_duplicate("evt-002") is False


def test_dedup_buffer_limit_1000():
    """После 1001-го уникального события первое больше не помнит."""
    dedup = WsEventDeduplicator()
    first = "evt-0"
    dedup.is_duplicate(first)
    for i in range(1, 1001):
        dedup.is_duplicate(f"evt-{i}")
    # first должен быть вытеснен
    assert dedup.is_duplicate(first) is False


def test_dedup_reset_clears_buffer():
    dedup = WsEventDeduplicator()
    dedup.is_duplicate("evt-001")
    assert dedup.is_duplicate("evt-001") is True
    dedup.reset()
    assert dedup.is_duplicate("evt-001") is False


# ════════════════════════════════════════════════════════════════════════════
# SpotPaperExecutor + OrderStore интеграция
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_paper_limit_order_persisted_to_store(tmp_path):
    store    = OrderStore(str(tmp_path / "orders.json"))
    executor = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0")},
        order_store=store,
    )
    order = _make_limit_order()
    await executor.submit_order(order)
    orders, _ = store.load()
    assert order.client_order_id in orders


@pytest.mark.asyncio
async def test_paper_cancel_order_removed_from_store(tmp_path):
    store    = OrderStore(str(tmp_path / "orders.json"))
    executor = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0")},
        order_store=store,
    )
    order = _make_limit_order()
    await executor.submit_order(order)
    await executor.cancel_order(order.client_order_id)
    orders, _ = store.load()
    assert order.client_order_id not in orders


@pytest.mark.asyncio
async def test_paper_oco_persisted_to_store(tmp_path):
    store    = OrderStore(str(tmp_path / "orders.json"))
    executor = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0.5")},
        order_store=store,
    )
    oco = _make_oco()
    executor.submit_oco(oco)
    _, ocos = store.load()
    assert oco.list_order_id in ocos


@pytest.mark.asyncio
async def test_paper_oco_cancelled_removed_from_store(tmp_path):
    store    = OrderStore(str(tmp_path / "orders.json"))
    executor = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0.5")},
        order_store=store,
    )
    oco = _make_oco()
    executor.submit_oco(oco)
    await executor.cancel_oco(oco.list_order_id)
    _, ocos = store.load()
    assert oco.list_order_id not in ocos


@pytest.mark.asyncio
async def test_paper_oco_triggered_removed_from_store(tmp_path):
    """OCO удаляется из store при срабатывании TP."""
    store    = OrderStore(str(tmp_path / "orders.json"))
    executor = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0.5")},
        order_store=store,
    )
    oco = _make_oco(tp=Decimal("45000"), sl_stop=Decimal("38000"))
    executor.submit_oco(oco)
    # триггер TP
    executor.set_price(BTCUSDT, Decimal("46000"))
    _, ocos = store.load()
    assert oco.list_order_id not in ocos
