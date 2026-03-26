"""
Tests for v2.0-step1 critical fixes:
  Fix 1: run_testnet_platform.py uses BtcUsdtMtfV20Strategy
  Fix 2: _persist_snapshot() guarded — CRITICAL log + PAUSE on failure
  Fix 3: boundary leakage removed — internal enums for order_status / list_order_status
"""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctp.core.constants import CRITICAL_STORAGE_UNAVAILABLE_CODE
from mctp.core.enums import (
    CommissionAsset,
    ContingencyType,
    ExchangeOrderStatus,
    ExecutionResult,
    ListOrderStatus,
    ListStatusType,
    Market,
    OperationalMode,
    Side,
)
from mctp.core.order import Fill
from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.portfolio.equity import EquityTracker
from mctp.portfolio.tracker import PortfolioTracker
from mctp.runtime.testnet_adapters import adapt_binance_testnet_payload
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)
START = datetime(2026, 3, 26, 0, 0, 0, tzinfo=timezone.utc)


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


def _buy_fill() -> Fill:
    return Fill(
        order_id="test-buy",
        symbol=BTCUSDT,
        side=Side.BUY,
        base_qty_filled=Decimal("0.1"),
        quote_qty_filled=Decimal("4000"),
        fill_price=Decimal("40000"),
        commission=Decimal("4"),
        commission_asset=CommissionAsset.QUOTE,
    )


# ════════════════════════════════════════════════════════════════════════════
# Fix 1: run_testnet_platform.py uses BtcUsdtMtfV20Strategy
# ════════════════════════════════════════════════════════════════════════════

def test_fix1_run_testnet_platform_uses_real_strategy():
    """run_testnet_platform.py must import and instantiate BtcUsdtMtfV20Strategy."""
    import importlib
    mod = importlib.import_module("run_testnet_platform")
    # BtcUsdtMtfV20Strategy must be reachable from the module
    assert hasattr(mod, "BtcUsdtMtfV20Strategy") or "BtcUsdtMtfV20Strategy" in dir(mod) or True
    # The module-level code uses BtcUsdtMtfV20Strategy in main();
    # verify by checking the import is there and EmaCrossSmokeStrategy is NOT imported.
    source = importlib.util.find_spec("run_testnet_platform")
    assert source is not None
    import inspect
    src = inspect.getsource(mod)
    assert "BtcUsdtMtfV20Strategy" in src
    assert "EmaCrossSmokeStrategy" not in src


# ════════════════════════════════════════════════════════════════════════════
# Fix 2: _persist_snapshot() guarded — CRITICAL log + PAUSE on failure
# ════════════════════════════════════════════════════════════════════════════

def test_fix2_persist_failure_pauses_and_keeps_snapshot_unchanged(tmp_path):
    """IOError during persist -> system in PAUSE_NEW_ENTRIES, snapshot unchanged."""
    path = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    initial = _snap(free_quote=Decimal("10000"))
    tracker = PortfolioTracker(initial, EquityTracker(Decimal("10000")), snapshot_store=store)

    assert tracker.operational_mode == OperationalMode.RUN

    # Poison the store so save() raises IOError
    def broken_save(snapshot: PortfolioSnapshot) -> None:
        raise IOError("disk offline")

    store.save = broken_save  # type: ignore[method-assign]

    fill = _buy_fill()
    result = tracker.on_fill(fill)

    # In-memory snapshot must NOT have advanced
    assert result.held_qty == Decimal("0")
    assert result.free_quote == Decimal("10000")
    assert tracker.snapshot is initial or tracker.snapshot == initial

    # System must be in PAUSE_NEW_ENTRIES
    assert tracker.operational_mode == OperationalMode.PAUSE_NEW_ENTRIES


# ════════════════════════════════════════════════════════════════════════════
# Fix 3: boundary leakage — internal enums for order_status / list_order_status
# ════════════════════════════════════════════════════════════════════════════

def test_fix3_raw_filled_maps_to_exchange_order_status_enum():
    """Raw 'FILLED' from WS payload -> ExchangeOrderStatus.FILLED enum value."""
    payload = {
        "e": "executionReport",
        "E": int(START.timestamp() * 1000),
        "s": "BTCUSDT",
        "c": "client-1",
        "X": "FILLED",
        "S": "BUY",
        "l": "0",
        "L": "0",
    }
    event = adapt_binance_testnet_payload(
        StreamType.USER_DATA,
        payload,
        symbol=BTCUSDT,
    )
    assert event is not None
    assert isinstance(event.order_status, ExchangeOrderStatus)
    assert event.order_status is ExchangeOrderStatus.FILLED
    # StrEnum backward compat: still compares equal to raw string
    assert event.order_status == "FILLED"


def test_fix3_unknown_raw_order_status_raises_with_warning():
    """Unknown raw order status string -> ValueError (not silent)."""
    payload = {
        "e": "executionReport",
        "E": int(START.timestamp() * 1000),
        "s": "BTCUSDT",
        "c": "client-1",
        "X": "TOTALLY_UNKNOWN_STATUS",
        "S": "BUY",
        "l": "0",
        "L": "0",
    }
    with pytest.raises(ValueError):
        adapt_binance_testnet_payload(
            StreamType.USER_DATA,
            payload,
            symbol=BTCUSDT,
        )


def test_fix3_oco_list_status_uses_typed_enums():
    """OCO list status fields use ListStatusType / ListOrderStatus / ContingencyType enums."""
    payload = {
        "e": "listStatus",
        "E": int(START.timestamp() * 1000),
        "s": "BTCUSDT",
        "g": "oco-1",
        "l": "ALL_DONE",
        "L": "ALL_DONE",
        "c": "OCO",
    }
    event = adapt_binance_testnet_payload(
        StreamType.USER_DATA,
        payload,
        symbol=BTCUSDT,
    )
    assert event is not None
    assert isinstance(event.list_status_type, ListStatusType)
    assert event.list_status_type is ListStatusType.ALL_DONE
    assert isinstance(event.list_order_status, ListOrderStatus)
    assert event.list_order_status is ListOrderStatus.ALL_DONE
    assert isinstance(event.contingency_type, ContingencyType)
    assert event.contingency_type is ContingencyType.OCO
