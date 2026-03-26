"""
Тесты v0.6: EquitySnapshot, EquityTracker, PortfolioTracker, AdaptiveRiskController.
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta, date

from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import Market, Side, OrderType, CommissionAsset, OperationalMode
from mctp.core.order import Fill
from mctp.core.constants import N_SNAP, DAILY_LOSS_LIMIT_PCT, CONSECUTIVE_LOSSES_REDUCE, CONSECUTIVE_LOSSES_STOP

from mctp.portfolio.equity import EquitySnapshot, EquityTracker
from mctp.portfolio.tracker import PortfolioTracker
from mctp.storage.snapshot_store import SnapshotStore
from mctp.risk.adaptive import AdaptiveRiskController

# ─── Helpers ──────────────────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
_NOW = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)


def _snap(
    held_qty: Decimal = Decimal("0"),
    avg_cost_basis: Decimal = Decimal("0"),
    free_quote: Decimal = Decimal("10000"),
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=held_qty,
        avg_cost_basis=avg_cost_basis,
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=held_qty > Decimal("0"),
        meaningful_position=held_qty > Decimal("0"),
    )


def _buy_fill(
    qty: Decimal = Decimal("0.1"),
    price: Decimal = Decimal("40000"),
) -> Fill:
    quote_qty = qty * price
    commission = quote_qty * Decimal("0.001")
    return Fill(
        order_id="test-buy",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=qty,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=CommissionAsset.QUOTE,
    )


def _sell_fill(
    qty: Decimal = Decimal("0.1"),
    price: Decimal = Decimal("45000"),
) -> Fill:
    quote_qty = qty * price
    commission = quote_qty * Decimal("0.001")
    return Fill(
        order_id="test-sell",
        symbol=BTCUSDT,
        side=Side.SELL,
        base_qty_filled=qty,
        quote_qty_filled=quote_qty,
        fill_price=price,
        commission=commission,
        commission_asset=CommissionAsset.QUOTE,
    )


# ════════════════════════════════════════════════════════════════════════════
# EquitySnapshot
# ════════════════════════════════════════════════════════════════════════════

def test_equity_snapshot_formula():
    """total_equity = free_quote + held_qty × current_price."""
    snap = EquityTracker.make_snapshot(
        free_quote=Decimal("5000"),
        held_qty=Decimal("0.1"),
        current_price=Decimal("50000"),
        is_in_position=True,
        now=_NOW,
    )
    assert snap.held_value   == Decimal("5000")
    assert snap.total_equity == Decimal("10000")
    assert snap.timestamp.tzinfo is not None


def test_equity_snapshot_zero_held():
    snap = EquityTracker.make_snapshot(
        free_quote=Decimal("10000"),
        held_qty=Decimal("0"),
        current_price=Decimal("40000"),
        is_in_position=False,
        now=_NOW,
    )
    assert snap.total_equity == Decimal("10000")
    assert snap.held_value   == Decimal("0")
    assert snap.is_in_position is False


def test_equity_snapshot_rejects_naive_timestamp():
    with pytest.raises(ValueError):
        EquitySnapshot(
            timestamp=datetime(2026, 3, 22, 12, 0, 0),  # naive
            total_equity=Decimal("10000"),
            free_quote=Decimal("10000"),
            held_qty=Decimal("0"),
            held_value=Decimal("0"),
            current_price=Decimal("40000"),
            is_in_position=False,
        )


# ════════════════════════════════════════════════════════════════════════════
# EquityTracker
# ════════════════════════════════════════════════════════════════════════════

def test_equity_tracker_peak_updates():
    tracker = EquityTracker(Decimal("10000"))
    assert tracker.peak_equity == Decimal("10000")

    snap1 = EquityTracker.make_snapshot(Decimal("11000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap1)
    assert tracker.peak_equity == Decimal("11000")

    snap2 = EquityTracker.make_snapshot(Decimal("9000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap2)
    assert tracker.peak_equity == Decimal("11000")  # peak не уменьшается


def test_equity_tracker_drawdown_zero_at_peak():
    tracker = EquityTracker(Decimal("10000"))
    snap = EquityTracker.make_snapshot(Decimal("10000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap)
    assert tracker.current_drawdown_pct == Decimal("0")


def test_equity_tracker_drawdown_pct():
    tracker = EquityTracker(Decimal("10000"))
    snap_peak = EquityTracker.make_snapshot(Decimal("10000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap_peak)
    snap_down = EquityTracker.make_snapshot(Decimal("9000"), Decimal("0"), Decimal("40000"), False, _NOW)
    tracker.record(snap_down)
    # (10000 - 9000) / 10000 = 0.1
    assert tracker.current_drawdown_pct == Decimal("0.1")


def test_equity_tracker_history_not_capped():
    """История не ограничена по количеству — можно хранить больше N_SNAP записей."""
    tracker = EquityTracker(Decimal("10000"))
    for i in range(N_SNAP + 5):
        snap = EquityTracker.make_snapshot(
            Decimal(str(10000 + i)),
            Decimal("0"),
            Decimal("40000"),
            False,
            _NOW,
        )
        tracker.record(snap)
    assert len(tracker.history) == N_SNAP + 5


def test_equity_tracker_drawdown_zero_no_history():
    tracker = EquityTracker(Decimal("10000"))
    assert tracker.current_drawdown_pct == Decimal("0")


# ════════════════════════════════════════════════════════════════════════════
# PortfolioTracker
# ════════════════════════════════════════════════════════════════════════════

def test_portfolio_tracker_on_buy_fill():
    initial = _snap(free_quote=Decimal("10000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")))
    fill = _buy_fill(Decimal("0.1"), Decimal("40000"))
    new_snap = tracker.on_fill(fill)
    assert new_snap.held_qty == Decimal("0.1")
    assert new_snap.avg_cost_basis == Decimal("40040")
    assert new_snap.is_in_position is True


def test_portfolio_tracker_on_sell_fill():
    initial = _snap(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"), free_quote=Decimal("5000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("9000")))
    fill = _sell_fill(Decimal("0.1"), Decimal("45000"))
    new_snap = tracker.on_fill(fill)
    assert new_snap.held_qty == Decimal("0")
    assert new_snap.is_in_position is False


def test_portfolio_tracker_snapshot_property():
    initial = _snap()
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")))
    assert tracker.snapshot is initial


def test_portfolio_tracker_record_equity():
    initial = _snap(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"), free_quote=Decimal("5600"))
    eq_tracker = EquityTracker(Decimal("9600"))
    tracker = PortfolioTracker(initial, eq_tracker)
    eq_snap = tracker.record_equity(Decimal("40000"), now=_NOW)
    assert eq_snap is not None
    assert eq_snap.total_equity == Decimal("5600") + Decimal("0.1") * Decimal("40000")
    assert len(eq_tracker.history) == 1


def test_portfolio_tracker_realized_pnl():
    initial = _snap(held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000"), free_quote=Decimal("5000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("9000")))
    fill = _sell_fill(Decimal("0.1"), Decimal("45000"))
    result = tracker.realized_pnl(fill)
    # gross = 4500 - 4000 = 500; commission = 4500 * 0.001 = 4.5; net = 495.5
    assert result.gross_pnl == Decimal("500")
    assert result.net_pnl   == Decimal("495.5")


def test_portfolio_tracker_detect_external_balance_change_true():
    initial = _snap(free_quote=Decimal("10000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")))
    # биржа показывает 10100 — разница 100 > 0.01
    assert tracker.detect_external_balance_change(Decimal("10100")) is True


def test_portfolio_tracker_detect_external_balance_change_false():
    initial = _snap(free_quote=Decimal("10000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")))
    # разница 0.005 < 0.01
    assert tracker.detect_external_balance_change(Decimal("10000.005")) is False


def test_portfolio_tracker_saves_snapshot_on_fill(tmp_path):
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    initial = _snap(free_quote=Decimal("10000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")), snapshot_store=store)
    tracker.on_fill(_buy_fill())
    loaded = store.load()
    assert loaded is not None
    assert loaded.held_qty == Decimal("0.1")


# ════════════════════════════════════════════════════════════════════════════
# AdaptiveRiskController
# ════════════════════════════════════════════════════════════════════════════

def test_adaptive_risk_initial_state():
    ctrl = AdaptiveRiskController(Decimal("10000"))
    assert ctrl.get_loss_mult()     == Decimal("1")
    assert ctrl.operational_mode    == OperationalMode.RUN
    assert ctrl.consecutive_losses  == 0
    assert ctrl.daily_loss          == Decimal("0")


def test_adaptive_risk_loss_mult_reduces_after_consecutive_losses():
    ctrl = AdaptiveRiskController(Decimal("10000"))
    for _ in range(CONSECUTIVE_LOSSES_REDUCE):
        ctrl.on_trade_result(Decimal("-100"), Decimal("9700"))
    assert ctrl.get_loss_mult() == Decimal("0.5")


def test_adaptive_risk_win_resets_consecutive():
    ctrl = AdaptiveRiskController(Decimal("10000"))
    for _ in range(CONSECUTIVE_LOSSES_REDUCE):
        ctrl.on_trade_result(Decimal("-100"), Decimal("9700"))
    assert ctrl.get_loss_mult() == Decimal("0.5")
    ctrl.on_trade_result(Decimal("200"), Decimal("9900"))
    assert ctrl.consecutive_losses == 0
    assert ctrl.get_loss_mult()    == Decimal("1")


def test_adaptive_risk_daily_limit_pauses_live():
    """Дневной лимит убытка → PAUSE_NEW_ENTRIES если is_live=True."""
    ctrl = AdaptiveRiskController(Decimal("10000"), is_live=True)
    # 3% от 10000 = 300; внесём 310
    ctrl.on_trade_result(Decimal("-310"), Decimal("9690"))
    assert ctrl.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES


def test_adaptive_risk_daily_limit_no_pause_when_not_live():
    """is_live=False: режим всегда RUN даже при превышении лимита."""
    ctrl = AdaptiveRiskController(Decimal("10000"), is_live=False)
    ctrl.on_trade_result(Decimal("-500"), Decimal("9500"))
    assert ctrl.operational_mode == OperationalMode.RUN


def test_adaptive_risk_consecutive_stop_pauses_live():
    """consecutive_losses >= CONSECUTIVE_LOSSES_STOP → PAUSE если is_live."""
    ctrl = AdaptiveRiskController(Decimal("10000"), is_live=True)
    for _ in range(CONSECUTIVE_LOSSES_STOP):
        ctrl.on_trade_result(Decimal("-10"), Decimal("9950"))
    assert ctrl.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES


def test_adaptive_risk_consecutive_stop_no_pause_not_live():
    ctrl = AdaptiveRiskController(Decimal("10000"), is_live=False)
    for _ in range(CONSECUTIVE_LOSSES_STOP):
        ctrl.on_trade_result(Decimal("-10"), Decimal("9950"))
    assert ctrl.operational_mode == OperationalMode.RUN


def test_adaptive_risk_reset_daily():
    ctrl = AdaptiveRiskController(Decimal("10000"), is_live=True)
    ctrl.on_trade_result(Decimal("-400"), Decimal("9600"))
    assert ctrl.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES

    ctrl.reset_daily(equity=Decimal("9600"))
    assert ctrl.daily_loss          == Decimal("0")
    assert ctrl.operational_mode    == OperationalMode.RUN
    assert ctrl.daily_start_equity  == Decimal("9600")
    # consecutive_losses НЕ сбрасывается при reset_daily
    assert ctrl.consecutive_losses  > 0


def test_adaptive_risk_should_reset_daily():
    ctrl = AdaptiveRiskController(Decimal("10000"))
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    ctrl.reset_daily(now=yesterday)
    assert ctrl.should_reset_daily() is True


def test_adaptive_risk_should_not_reset_same_day():
    ctrl = AdaptiveRiskController(Decimal("10000"))
    now = datetime.now(timezone.utc)
    ctrl.reset_daily(now=now)
    assert ctrl.should_reset_daily(now=now) is False

# ════════════════════════════════════════════════════════════════════════════
# Contract 54: AdaptiveRiskController — все 4 множителя
# ════════════════════════════════════════════════════════════════════════════

def test_adaptive_risk_returns_all_four_multipliers():
    from mctp.risk.adaptive import AdaptiveRiskController
    from mctp.sizing.models import RiskMultipliers
    ctrl = AdaptiveRiskController(initial_equity=Decimal("10000"))
    mults = ctrl.get_risk_multipliers()
    assert isinstance(mults, RiskMultipliers)
    assert mults.loss_mult == Decimal("1.0")
    assert mults.atr_mult == Decimal("1.0")
    assert mults.regime_mult == Decimal("1.0")
    assert mults.anomaly_mult == Decimal("1.0")


def test_adaptive_risk_atr_mult_is_stub():
    """atr_mult всегда 1.0 до v0.11"""
    ctrl = AdaptiveRiskController(Decimal("10000"))
    assert ctrl.atr_mult == Decimal("1.0")


def test_adaptive_risk_regime_mult_is_stub():
    """regime_mult всегда 1.0 до v3.2"""
    ctrl = AdaptiveRiskController(Decimal("10000"))
    assert ctrl.regime_mult == Decimal("1.0")
