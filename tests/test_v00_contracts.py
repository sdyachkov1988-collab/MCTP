import pytest
from decimal import Decimal
from datetime import datetime, timezone

from mctp.core.types import Symbol, PortfolioSnapshot, Intent
from mctp.core.enums import Market, IntentType, OperationalMode, RecoveryMode
from mctp.core.constants import (
    MAX_DRAWDOWN_STOP_PCT, MAX_RISK_PCT_FULL, MAX_RISK_PCT_EARLY,
    T_CANCEL, CONFIG_SCHEMA_VERSION, TRAILING_ATR_MULTIPLIER,
    MIN_NOTIONAL_BUFFER, MAX_PORTFOLIO_RISK_PCT
)
from mctp.config.schema import ConfigSchema
from mctp.core.exceptions import ConfigVersionError


def test_symbol_exchange_string():
    s = Symbol("BTC", "USDT", Market.SPOT)
    assert s.to_exchange_str() == "BTCUSDT"


def test_symbol_is_typed_not_string():
    s = Symbol("ETH", "USDT", Market.SPOT)
    assert isinstance(s, Symbol)
    assert s.base == "ETH"
    assert s.quote == "USDT"


def test_portfolio_snapshot_rejects_float_held_qty():
    with pytest.raises(AssertionError):
        PortfolioSnapshot(
            symbol=Symbol("BTC", "USDT", Market.SPOT),
            held_qty=0.1,
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("1000"),
            quote_asset="USDT",
            is_in_position=True,
            meaningful_position=True,
        )


def test_portfolio_snapshot_rejects_naive_datetime():
    with pytest.raises(ValueError):
        PortfolioSnapshot(
            symbol=Symbol("BTC", "USDT", Market.SPOT),
            held_qty=Decimal("0.1"),
            avg_cost_basis=Decimal("40000"),
            free_quote=Decimal("1000"),
            quote_asset="USDT",
            is_in_position=True,
            meaningful_position=True,
            timestamp=datetime.now(),
        )


def test_portfolio_snapshot_accepts_utc():
    snap = PortfolioSnapshot(
        symbol=Symbol("BTC", "USDT", Market.SPOT),
        held_qty=Decimal("0.1"),
        avg_cost_basis=Decimal("40000"),
        free_quote=Decimal("1000"),
        quote_asset="USDT",
        is_in_position=True,
        meaningful_position=True,
    )
    assert snap.timestamp.tzinfo is not None


def test_config_version_mismatch_raises():
    cfg = ConfigSchema(schema_version="0.0.1")
    with pytest.raises(ConfigVersionError):
        cfg.validate()


def test_config_version_match_passes():
    cfg = ConfigSchema(schema_version=CONFIG_SCHEMA_VERSION)
    cfg.validate()


def test_drawdown_stop_is_terminal_at_20_percent():
    assert MAX_DRAWDOWN_STOP_PCT == Decimal("0.20")


def test_early_max_risk_lower_than_full():
    assert MAX_RISK_PCT_EARLY < MAX_RISK_PCT_FULL
    assert MAX_RISK_PCT_EARLY == Decimal("0.010")


def test_all_financial_constants_are_decimal():
    from mctp.core import constants
    import inspect
    for name, value in inspect.getmembers(constants):
        if name.startswith("_"):
            continue
        if isinstance(value, float):
            pytest.fail(f"Constant {name} is float — must be Decimal")


def test_t_cancel_is_int_seconds():
    assert isinstance(T_CANCEL, int)
    assert T_CANCEL == 10


def test_recovery_mode_terminal_exists():
    assert RecoveryMode.TERMINAL in RecoveryMode


def test_operational_modes_complete():
    modes = {m.value for m in OperationalMode}
    assert "RUN" in modes
    assert "PAUSE_NEW_ENTRIES" in modes
    assert "CLOSE_ONLY" in modes
    assert "STOP" in modes


def test_min_notional_buffer():
    assert MIN_NOTIONAL_BUFFER == Decimal("1.2")


def test_trailing_atr_multiplier_is_configurable():
    assert TRAILING_ATR_MULTIPLIER == Decimal("2.0")
    assert isinstance(TRAILING_ATR_MULTIPLIER, Decimal)
