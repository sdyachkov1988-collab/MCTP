from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.core.enums import CommissionAsset, Market, Side
from mctp.core.order import Fill
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.accounting import AccountingLedger
from mctp.portfolio.equity import EquitySnapshot, EquityTracker
from mctp.portfolio.tracker import PortfolioTracker
from mctp.risk.adaptive import AdaptiveRiskController
from mctp.streams.base import StreamStaleFlags, StreamState, StreamType, refresh_stale_flags


BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
NOW = datetime(2026, 3, 22, 12, 0, 0, tzinfo=timezone.utc)


def _snapshot(
    held_qty: Decimal = Decimal("0"),
    avg_cost_basis: Decimal = Decimal("0"),
    free_quote: Decimal = Decimal("10000"),
    is_in_position: bool = False,
    meaningful_position: bool = False,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=held_qty,
        avg_cost_basis=avg_cost_basis,
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=is_in_position,
        meaningful_position=meaningful_position,
        timestamp=NOW,
    )


def _fill(
    *,
    side: Side = Side.BUY,
    price: Decimal = Decimal("40000"),
    qty: Decimal = Decimal("0.1"),
    commission: Decimal = Decimal("4"),
    commission_asset: CommissionAsset = CommissionAsset.QUOTE,
    order_id: str = "order-1",
    trade_id: str = "trade-1",
) -> Fill:
    return Fill(
        order_id=order_id,
        symbol=BTCUSDT,
        side=side,
        base_qty_filled=qty,
        quote_qty_filled=qty * price,
        fill_price=price,
        commission=commission,
        commission_asset=commission_asset,
        trade_id=trade_id,
        filled_at=NOW,
    )


def test_equity_snapshot_keeps_meaningful_position_separate():
    snapshot = EquitySnapshot(
        timestamp=NOW,
        total_equity=Decimal("10000"),
        free_quote=Decimal("9950"),
        held_qty=Decimal("0.0001"),
        held_value=Decimal("50"),
        current_price=Decimal("500000"),
        is_in_position=True,
        meaningful_position=False,
    )
    assert snapshot.is_in_position is True
    assert snapshot.meaningful_position is False


def test_portfolio_record_equity_propagates_meaningful_position():
    tracker = PortfolioTracker(
        _snapshot(
            held_qty=Decimal("0.0001"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("9990"),
            is_in_position=True,
            meaningful_position=False,
        ),
        EquityTracker(Decimal("10000")),
    )
    equity_snapshot = tracker.record_equity(Decimal("50000"), now=NOW)
    assert equity_snapshot is not None
    assert equity_snapshot.is_in_position is True
    assert equity_snapshot.meaningful_position is False


def test_accounting_history_records_bnb_rate_per_processed_fill():
    ledger = AccountingLedger()
    tracker = PortfolioTracker(
        _snapshot(),
        EquityTracker(Decimal("10000")),
        bnb_price_provider=lambda: Decimal("310"),
        accounting_ledger=ledger,
    )
    tracker.on_fill(
        _fill(
            commission=Decimal("0.005"),
            commission_asset=CommissionAsset.BNB,
            trade_id="trade-bnb",
        )
    )
    assert len(tracker.accounting.fill_history) == 1
    record = tracker.accounting.fill_history[0]
    assert record.trade_id == "trade-bnb"
    assert record.bnb_rate_at_fill == Decimal("310")
    assert record.fee_drag_quote == Decimal("1.550")


def test_bnb_fee_without_rate_is_hard_error():
    tracker = PortfolioTracker(
        _snapshot(),
        EquityTracker(Decimal("10000")),
        bnb_price_provider=lambda: None,
    )
    with pytest.raises(ValueError):
        tracker.on_fill(_fill(commission=Decimal("0.005"), commission_asset=CommissionAsset.BNB))


def test_non_bnb_fill_records_optional_missing_bnb_rate_explicitly():
    tracker = PortfolioTracker(
        _snapshot(),
        EquityTracker(Decimal("10000")),
        bnb_price_provider=lambda: None,
    )
    tracker.on_fill(_fill(commission=Decimal("4"), commission_asset=CommissionAsset.QUOTE))
    record = tracker.accounting.fill_history[0]
    assert record.bnb_rate_at_fill is None
    assert record.fee_drag_quote == Decimal("4")


def test_fee_drag_quote_total_accumulates_for_quote_base_and_bnb():
    tracker = PortfolioTracker(
        _snapshot(),
        EquityTracker(Decimal("10000")),
        bnb_price_provider=lambda: Decimal("300"),
    )
    tracker.on_fill(
        _fill(
            commission=Decimal("4"),
            commission_asset=CommissionAsset.QUOTE,
            order_id="quote-order",
            trade_id="quote-trade",
        )
    )
    tracker.on_fill(
        _fill(
            commission=Decimal("0.0001"),
            commission_asset=CommissionAsset.BASE,
            order_id="base-order",
            trade_id="base-trade",
        )
    )
    tracker.on_fill(
        _fill(
            commission=Decimal("0.005"),
            commission_asset=CommissionAsset.BNB,
            order_id="bnb-order",
            trade_id="bnb-trade",
        )
    )
    assert tracker.accounting.fee_drag_quote_total == Decimal("9.5")


def test_stale_flags_cover_all_four_streams():
    states = {
        StreamType.KLINE: StreamState(StreamType.KLINE, True, NOW, False),
        StreamType.BOOK_TICKER: StreamState(
            StreamType.BOOK_TICKER,
            True,
            datetime(2026, 3, 22, 11, 59, 50, tzinfo=timezone.utc),
            False,
        ),
        StreamType.BNB_TICKER: StreamState(StreamType.BNB_TICKER, True, None, False),
        StreamType.USER_DATA: StreamState(
            StreamType.USER_DATA,
            True,
            datetime(2026, 3, 22, 11, 59, 59, tzinfo=timezone.utc),
            False,
        ),
    }
    thresholds = {
        StreamType.KLINE: 60,
        StreamType.BOOK_TICKER: 5,
        StreamType.BNB_TICKER: 30,
        StreamType.USER_DATA: 5,
    }
    flags = refresh_stale_flags(states, thresholds, NOW)
    assert isinstance(flags, StreamStaleFlags)
    assert flags.kline is False
    assert flags.book_ticker is True
    assert flags.bnb_ticker is True
    assert flags.user_data is False
    assert states[StreamType.KLINE].is_stale is False
    assert states[StreamType.BOOK_TICKER].is_stale is True
    assert states[StreamType.BNB_TICKER].is_stale is True
    assert states[StreamType.USER_DATA].is_stale is False


def test_consecutive_losses_behavior_not_regressed():
    controller = AdaptiveRiskController(Decimal("10000"))
    controller.on_trade_result(Decimal("-100"), Decimal("9900"))
    controller.on_trade_result(Decimal("-100"), Decimal("9800"))
    controller.on_trade_result(Decimal("-100"), Decimal("9700"))
    assert controller.consecutive_losses == 3
    assert controller.loss_mult == Decimal("0.5")
    controller.on_trade_result(Decimal("50"), Decimal("9750"))
    assert controller.consecutive_losses == 0
    assert controller.loss_mult == Decimal("1")
