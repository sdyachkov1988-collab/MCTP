"""
Тесты v0.2: RiskLayer — 7 guards.
Каждый guard имеет минимум 2 теста (approve + reject).
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone

from mctp.core.types import Symbol, PortfolioSnapshot, Intent
from mctp.core.enums import Market, IntentType, QuantityMode, RejectionReason
from mctp.risk.layer import RiskLayer, RiskResult
from mctp.risk.config import RiskConfig

# ─── Fixtures ────────────────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
BNBUSDT = Symbol("BNB", "USDT", Market.SPOT)


def _snapshot(
    *,
    symbol: Symbol = None,
    held_qty: Decimal = Decimal("0"),
    avg_cost_basis: Decimal = Decimal("0"),
    free_quote: Decimal = Decimal("10000"),
    is_in_position: bool = False,
    meaningful_position: bool = False,
    scale_in_count: int = 0,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=symbol or BTCUSDT,
        held_qty=held_qty,
        avg_cost_basis=avg_cost_basis,
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=is_in_position,
        meaningful_position=meaningful_position,
        scale_in_count=scale_in_count,
    )


def _intent(
    intent_type: IntentType = IntentType.BUY,
    symbol: Symbol = None,
    quantity_mode: QuantityMode = None,
    partial_fraction: Decimal = None,
) -> Intent:
    return Intent(
        type=intent_type,
        symbol=symbol or BTCUSDT,
        quantity_mode=quantity_mode,
        partial_fraction=partial_fraction,
    )


def _config(**kwargs) -> RiskConfig:
    return RiskConfig(**kwargs)


# ─── Guard 1: позиция ─────────────────────────────────────────────────────────

def test_guard1_buy_in_position_scale_in_false_rejected():
    layer = RiskLayer(_config(scale_in_allowed=False))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(is_in_position=True, meaningful_position=True,
                  held_qty=Decimal("0.1"), avg_cost_basis=Decimal("40000")),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.ALREADY_IN_POSITION


def test_guard1_buy_not_in_position_approved():
    layer = RiskLayer(_config(scale_in_allowed=False))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(is_in_position=False, free_quote=Decimal("10000")),
        {},
    )
    assert result.approved is True
    assert result.rejection_reason is None


# ─── Guard 2: нулевая позиция ─────────────────────────────────────────────────

def test_guard2_sell_zero_held_qty_rejected():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.SELL),
        _snapshot(held_qty=Decimal("0"), is_in_position=False),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.NO_POSITION_TO_SELL


def test_guard2_sell_nonzero_held_qty_approved():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.SELL, quantity_mode=QuantityMode.FULL),
        _snapshot(
            held_qty=Decimal("0.1"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("5000"),
            is_in_position=True,
            meaningful_position=True,
        ),
        {},
    )
    assert result.approved is True


# ─── Guard 3: недостаточно котируемого актива ─────────────────────────────────

def test_guard3_buy_insufficient_quote_rejected():
    layer = RiskLayer(_config(min_order_value=Decimal("10")))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(is_in_position=False, free_quote=Decimal("5")),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.INSUFFICIENT_QUOTE


def test_guard3_buy_sufficient_quote_approved():
    layer = RiskLayer(_config(min_order_value=Decimal("10")))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(is_in_position=False, free_quote=Decimal("10000")),
        {},
    )
    assert result.approved is True


def test_guard3_respects_instrument_info_min_notional():
    layer = RiskLayer(_config(min_order_value=Decimal("10")))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(is_in_position=False, free_quote=Decimal("50")),
        {"min_notional": Decimal("100")},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.INSUFFICIENT_QUOTE


# ─── Guard 4: BNB символ запрещён ─────────────────────────────────────────────

def test_guard4_buy_bnbusdt_bnb_active_rejected():
    layer = RiskLayer(_config(bnb_discount_active=True))
    result = layer.check(
        _intent(IntentType.BUY, symbol=BNBUSDT),
        _snapshot(symbol=BNBUSDT, is_in_position=False, free_quote=Decimal("10000")),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.BNB_SYMBOL_FORBIDDEN


def test_guard4_buy_btcusdt_bnb_active_approved():
    layer = RiskLayer(_config(bnb_discount_active=True))
    result = layer.check(
        _intent(IntentType.BUY, symbol=BTCUSDT),
        _snapshot(symbol=BTCUSDT, is_in_position=False, free_quote=Decimal("10000")),
        {},
    )
    assert result.approved is True


def test_guard4_sell_bnbusdt_bnb_active_rejected():
    layer = RiskLayer(_config(bnb_discount_active=True))
    result = layer.check(
        _intent(IntentType.SELL, symbol=BNBUSDT, quantity_mode=QuantityMode.FULL),
        _snapshot(
            symbol=BNBUSDT,
            held_qty=Decimal("10"),
            avg_cost_basis=Decimal("300"),
            is_in_position=True,
            meaningful_position=True,
            free_quote=Decimal("1000"),
        ),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.BNB_SYMBOL_FORBIDDEN


# ─── Guard 5: quote qty запрещён для SELL ─────────────────────────────────────

def test_guard5_sell_quote_mode_rejected():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.SELL, quantity_mode=QuantityMode.QUOTE),
        _snapshot(
            held_qty=Decimal("0.1"),
            avg_cost_basis=Decimal("40000"),
            is_in_position=True,
            meaningful_position=True,
            free_quote=Decimal("5000"),
        ),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.QUOTE_QTY_SELL_FORBIDDEN


def test_guard5_sell_full_mode_approved():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.SELL, quantity_mode=QuantityMode.FULL),
        _snapshot(
            held_qty=Decimal("0.1"),
            avg_cost_basis=Decimal("40000"),
            is_in_position=True,
            meaningful_position=True,
            free_quote=Decimal("5000"),
        ),
        {},
    )
    assert result.approved is True


# ─── Guard 6: лимит scale-in ──────────────────────────────────────────────────

def test_guard6_scale_in_count_at_max_rejected():
    layer = RiskLayer(_config(scale_in_allowed=True, max_scale_in_count=3))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(
            held_qty=Decimal("0.3"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("10000"),
            is_in_position=True,
            meaningful_position=True,
            scale_in_count=3,
        ),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.MAX_SCALE_IN_REACHED


def test_guard6_scale_in_count_below_max_approved():
    layer = RiskLayer(_config(scale_in_allowed=True, max_scale_in_count=3))
    result = layer.check(
        _intent(IntentType.BUY),
        _snapshot(
            held_qty=Decimal("0.1"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("10000"),
            is_in_position=True,
            meaningful_position=True,  # meaningful → Guard 7 не срабатывает
            scale_in_count=1,
        ),
        {},
    )
    assert result.approved is True


# ─── Guard 7: восстановление суб-значимой позиции ────────────────────────────

def test_guard7_sub_meaningful_small_buy_rejected():
    """
    Guard 3 должен пройти: free_quote=10 >= min_order=10.
    held_qty=0.0001 BTC × avg=40000 → current_notional=4
    partial_fraction=0.1 × free_quote=10 → buy_notional=1
    total=5; min_notional=10; buffer=1.2 → 5 ≤ 12 → REJECTED by Guard 7
    """
    layer = RiskLayer(_config(scale_in_allowed=True, min_order_value=Decimal("10")))
    result = layer.check(
        _intent(IntentType.BUY, partial_fraction=Decimal("0.1")),
        _snapshot(
            held_qty=Decimal("0.0001"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("10"),
            is_in_position=True,
            meaningful_position=False,
            scale_in_count=0,
        ),
        {},
    )
    assert result.approved is False
    assert result.rejection_reason == RejectionReason.RESTORE_BELOW_MIN_NOTIONAL


def test_guard7_sub_meaningful_sufficient_buy_approved():
    """
    held_qty=0.0001 BTC × avg=40000 → current_notional=4
    partial_fraction=None (FULL) × free_quote=1000 → buy_notional=1000
    total=1004; min_notional=10; buffer=1.2 → 1004 > 12 → APPROVED
    """
    layer = RiskLayer(_config(scale_in_allowed=True, min_order_value=Decimal("10")))
    result = layer.check(
        _intent(IntentType.BUY, partial_fraction=None),
        _snapshot(
            held_qty=Decimal("0.0001"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("1000"),
            is_in_position=True,
            meaningful_position=False,
            scale_in_count=0,
        ),
        {},
    )
    assert result.approved is True


# ─── HOLD всегда проходит ─────────────────────────────────────────────────────

def test_hold_passes_with_no_position():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.HOLD),
        _snapshot(is_in_position=False, free_quote=Decimal("0")),
        {},
    )
    assert result.approved is True


def test_hold_passes_with_empty_quote():
    """HOLD проходит даже если денег нет и позиции нет."""
    layer = RiskLayer(_config(bnb_discount_active=True))
    result = layer.check(
        _intent(IntentType.HOLD, symbol=BNBUSDT),
        _snapshot(symbol=BNBUSDT, free_quote=Decimal("0")),
        {},
    )
    assert result.approved is True


# ─── RiskResult контракты ─────────────────────────────────────────────────────

def test_risk_result_checked_at_is_utc():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.HOLD),
        _snapshot(),
        {},
    )
    assert result.checked_at.tzinfo is not None


def test_risk_result_approved_has_no_rejection_reason():
    layer = RiskLayer(_config())
    result = layer.check(
        _intent(IntentType.HOLD),
        _snapshot(),
        {},
    )
    assert result.approved is True
    assert result.rejection_reason is None


def test_risk_layer_does_not_mutate_snapshot():
    """RiskLayer не должен модифицировать PortfolioSnapshot."""
    layer = RiskLayer(_config(scale_in_allowed=True, max_scale_in_count=3))
    snap = _snapshot(
        held_qty=Decimal("0.1"),
        avg_cost_basis=Decimal("40000"),
        free_quote=Decimal("5000"),
        is_in_position=True,
        meaningful_position=True,
        scale_in_count=1,
    )
    original_scale_in = snap.scale_in_count
    original_held = snap.held_qty
    original_quote = snap.free_quote

    layer.check(_intent(IntentType.BUY), snap, {})

    assert snap.scale_in_count == original_scale_in
    assert snap.held_qty == original_held
    assert snap.free_quote == original_quote
