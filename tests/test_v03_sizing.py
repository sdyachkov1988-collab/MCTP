"""
Тесты v0.3: PositionSizer, RiskMultipliers, SizerConfig, StreamState.
Контракт 54: Fixed Fractional Risk.
"""
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import Market
from mctp.core.constants import MAX_RISK_PCT_EARLY, MAX_RISK_PCT_FULL
from mctp.sizing.models import RiskMultipliers, SizerResult
from mctp.sizing.config import SizerConfig
from mctp.sizing.sizer import PositionSizer
from mctp.streams.base import StreamType, StreamState

# ─── Helpers ─────────────────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)


def _snapshot(free_quote: Decimal = Decimal("10000")) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=Decimal("0"),
        avg_cost_basis=Decimal("0"),
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=False,
        meaningful_position=False,
    )


def _instrument(
    lot_size: Decimal = Decimal("0.001"),
    min_qty: Decimal = Decimal("0.001"),
    max_qty: Decimal = Decimal("1000"),
    min_notional: Decimal = Decimal("10"),
) -> dict:
    return {
        "lot_size": lot_size,
        "min_qty": min_qty,
        "max_qty": max_qty,
        "min_notional": min_notional,
    }


# ─── SizerConfig tests ────────────────────────────────────────────────────────

def test_sizer_config_early_cap_applies():
    """risk_pct > MAX_RISK_PCT_EARLY, early cap → effective = MAX_RISK_PCT_EARLY"""
    cfg = SizerConfig(risk_pct=MAX_RISK_PCT_FULL, use_early_risk_cap=True)
    assert cfg.effective_risk_pct() == MAX_RISK_PCT_EARLY


def test_sizer_config_full_cap_applies():
    """risk_pct = MAX_RISK_PCT_FULL, no early cap → effective = MAX_RISK_PCT_FULL"""
    cfg = SizerConfig(risk_pct=MAX_RISK_PCT_FULL, use_early_risk_cap=False)
    assert cfg.effective_risk_pct() == MAX_RISK_PCT_FULL


def test_sizer_config_below_cap_unchanged():
    """risk_pct < both caps → returned as-is"""
    small = Decimal("0.005")
    cfg = SizerConfig(risk_pct=small, use_early_risk_cap=True)
    assert cfg.effective_risk_pct() == small


def test_sizer_config_rejects_float():
    with pytest.raises(AssertionError):
        SizerConfig(risk_pct=0.01)


# ─── RiskMultipliers tests ────────────────────────────────────────────────────

def test_risk_multipliers_combined_default_is_one():
    m = RiskMultipliers()
    assert m.combined() == Decimal("1.0")


def test_risk_multipliers_combined_normal():
    """loss=0.8, atr=0.9, regime=1.0, anomaly=1.0 → 0.72"""
    m = RiskMultipliers(
        loss_mult=Decimal("0.8"),
        atr_mult=Decimal("0.9"),
        regime_mult=Decimal("1.0"),
        anomaly_mult=Decimal("1.0"),
    )
    assert m.combined() == Decimal("0.72")


def test_risk_multipliers_combined_regime_zero_gives_zero():
    """Если regime_mult=0 → итог=0 независимо от других"""
    m = RiskMultipliers(
        loss_mult=Decimal("0.5"),
        atr_mult=Decimal("1.5"),
        regime_mult=Decimal("0"),
        anomaly_mult=Decimal("2.0"),
    )
    assert m.combined() == Decimal("0")


def test_risk_multipliers_rejects_float():
    with pytest.raises(AssertionError):
        RiskMultipliers(loss_mult=0.8)


# ─── PositionSizer: формула ───────────────────────────────────────────────────

def test_sizer_base_formula():
    """
    deposit=10000, risk=0.01, price=40000, stop=0.02
    raw = 10000 × 0.01 / (40000 × 0.02) = 100 / 800 = 0.125
    lot_size=0.001 → floor(0.125/0.001)=125 → qty=0.125
    """
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.02"),
        instrument_info=_instrument(),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is True
    assert result.quantity == Decimal("0.125")
    assert result.notional == Decimal("5000.000")


def test_sizer_formula_risk_used():
    """risk_used = deposit × effective_risk_pct × combined"""
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.02"),
        instrument_info=_instrument(),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    # base_risk = 10000 × 0.01 = 100, combined = 1.0 → risk_used = 100
    assert result.risk_used == Decimal("100.00")


# ─── PositionSizer: квантование вниз ─────────────────────────────────────────

def test_sizer_quantizes_floor():
    """
    raw = 10000 × 0.01 / (40000 × 0.03) = 100/1200 = 0.08333...
    lot_size=0.001 → floor(83.33) = 83 → qty=0.083 (не 0.084)
    """
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.030"),
        instrument_info=_instrument(lot_size=Decimal("0.001"), min_qty=Decimal("0.001")),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is True
    assert result.quantity == Decimal("0.083")


def test_sizer_quantizes_respects_larger_lot_size():
    """
    raw = 0.125, lot_size = 0.01 → floor(12.5) = 12 → qty = 0.12
    """
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.02"),
        instrument_info=_instrument(lot_size=Decimal("0.01"), min_qty=Decimal("0.01")),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is True
    assert result.quantity == Decimal("0.12")


# ─── PositionSizer: rejection cases ──────────────────────────────────────────

def test_sizer_rejects_below_min_qty():
    """
    raw=0.083, min_qty=0.1 → rejected
    """
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.030"),
        instrument_info=_instrument(
            lot_size=Decimal("0.001"),
            min_qty=Decimal("0.1"),   # выше расчётного qty
        ),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is False
    assert result.quantity is None
    assert result.notional is None
    assert "min_qty" in result.rejection_reason


def test_sizer_rejects_below_min_notional():
    """
    qty=0.083, price=40000, notional=3320; min_notional=5000 → rejected
    """
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0.030"),
        instrument_info=_instrument(
            lot_size=Decimal("0.001"),
            min_qty=Decimal("0.001"),
            min_notional=Decimal("5000"),  # выше расчётного notional
        ),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is False
    assert result.quantity is None
    assert "min_notional" in result.rejection_reason


def test_sizer_rejects_zero_stop_distance():
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    sizer = PositionSizer(cfg)
    result = sizer.calculate(
        _snapshot(Decimal("10000")),
        stop_distance_pct=Decimal("0"),
        instrument_info=_instrument(),
        current_price=Decimal("40000"),
        risk_multipliers=RiskMultipliers(),
    )
    assert result.approved is False


# ─── PositionSizer: early cap интеграция ─────────────────────────────────────

def test_sizer_early_cap_reduces_qty():
    """
    use_early_risk_cap=True → effective_risk = MAX_RISK_PCT_EARLY (0.010)
    use_early_risk_cap=False с тем же risk_pct=MAX_RISK_PCT_FULL (0.020) → вдвое больше qty
    """
    instr = _instrument()

    cfg_early = SizerConfig(risk_pct=MAX_RISK_PCT_FULL, use_early_risk_cap=True)
    cfg_full  = SizerConfig(risk_pct=MAX_RISK_PCT_FULL, use_early_risk_cap=False)

    r_early = PositionSizer(cfg_early).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"), instr, Decimal("40000"), RiskMultipliers()
    )
    r_full = PositionSizer(cfg_full).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"), instr, Decimal("40000"), RiskMultipliers()
    )
    assert r_early.approved and r_full.approved
    assert r_early.quantity < r_full.quantity


# ─── PositionSizer: RiskMultipliers применяются ───────────────────────────────

def test_sizer_risk_multiplier_reduces_qty():
    """loss_mult=0.5 → qty вдвое меньше чем при loss_mult=1.0"""
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    instr = _instrument()
    base = RiskMultipliers()
    half = RiskMultipliers(loss_mult=Decimal("0.5"))

    r_base = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"), instr, Decimal("40000"), base
    )
    r_half = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"), instr, Decimal("40000"), half
    )
    assert r_base.approved and r_half.approved
    assert r_half.quantity < r_base.quantity


def test_sizer_regime_zero_rejects():
    """regime_mult=0 → combined=0 → raw_qty=0 → rejected (below min_qty)"""
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    result = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")),
        Decimal("0.02"),
        _instrument(min_qty=Decimal("0.001")),
        Decimal("40000"),
        RiskMultipliers(regime_mult=Decimal("0")),
    )
    assert result.approved is False


# ─── SizerResult контракты ────────────────────────────────────────────────────

def test_sizer_result_calculated_at_is_utc():
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    result = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"),
        _instrument(), Decimal("40000"), RiskMultipliers(),
    )
    assert result.calculated_at.tzinfo is not None


def test_sizer_result_approved_has_no_rejection_reason():
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    result = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"),
        _instrument(), Decimal("40000"), RiskMultipliers(),
    )
    assert result.approved is True
    assert result.rejection_reason is None


def test_sizer_all_values_are_decimal():
    cfg = SizerConfig(risk_pct=Decimal("0.01"), use_early_risk_cap=False)
    result = PositionSizer(cfg).calculate(
        _snapshot(Decimal("10000")), Decimal("0.02"),
        _instrument(), Decimal("40000"), RiskMultipliers(),
    )
    assert isinstance(result.quantity, Decimal)
    assert isinstance(result.notional, Decimal)
    assert isinstance(result.risk_used, Decimal)


# ─── StreamState tests ────────────────────────────────────────────────────────

def test_stream_state_stale_check_no_update():
    """Поток без last_update_at → всегда stale"""
    state = StreamState(
        stream_type=StreamType.KLINE,
        is_connected=False,
        last_update_at=None,
        is_stale=True,
    )
    assert state.is_stale_check(threshold_seconds=5) is True


def test_stream_state_stale_check_recent():
    """Обновление только что → не stale"""
    state = StreamState(
        stream_type=StreamType.BOOK_TICKER,
        is_connected=True,
        last_update_at=datetime.now(timezone.utc),
        is_stale=False,
    )
    assert state.is_stale_check(threshold_seconds=60) is False


def test_stream_state_stale_check_old_update():
    """Обновление 2 часа назад при threshold=60s → stale"""
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    state = StreamState(
        stream_type=StreamType.USER_DATA,
        is_connected=True,
        last_update_at=old,
        is_stale=False,
    )
    assert state.is_stale_check(threshold_seconds=60) is True


def test_stream_type_all_four_exist():
    types = {s.value for s in StreamType}
    assert "KLINE"       in types
    assert "BOOK_TICKER" in types
    assert "BNB_TICKER"  in types
    assert "USER_DATA"   in types


def test_stream_state_rejects_naive_datetime():
    with pytest.raises(ValueError):
        StreamState(
            stream_type=StreamType.KLINE,
            is_connected=True,
            last_update_at=datetime.now(),  # naive — должен упасть
            is_stale=False,
        )
