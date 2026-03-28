"""
Тесты v0.5: SnapshotStore, BalanceCacheStore, schema validation,
             SpotPaperExecutor persistence, регрессия Минор 2.
"""
import json
import os
import pytest
from decimal import Decimal
from datetime import datetime, timezone, timedelta

from mctp.core.types import Symbol, PortfolioSnapshot
from mctp.core.enums import Market, Side, OrderType, ExecutionResult
from mctp.core.order import Order
from mctp.core.constants import CONFIG_SCHEMA_VERSION, BALANCE_CACHE_TTL

from mctp.storage.snapshot_store import SnapshotStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.exceptions import StorageCorruptedError, StorageSchemaMismatchError

from mctp.config.schema import validate_schema_version

from mctp.execution.paper import SpotPaperExecutor
from mctp.execution.lifecycle import OrderState

# ─── Helpers ─────────────────────────────────────────────────────────────────

BTCUSDT = Symbol("BTC", "USDT", Market.SPOT)


def _snap(
    held_qty: Decimal = Decimal("0.12345678"),
    avg_cost_basis: Decimal = Decimal("40000.99"),
    free_quote: Decimal = Decimal("9876.54"),
    scale_in_count: int = 0,
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        symbol=BTCUSDT,
        held_qty=held_qty,
        avg_cost_basis=avg_cost_basis,
        free_quote=free_quote,
        quote_asset="USDT",
        is_in_position=held_qty > Decimal("0"),
        meaningful_position=held_qty > Decimal("0"),
        scale_in_count=scale_in_count,
    )


# ════════════════════════════════════════════════════════════════════════════
# SnapshotStore
# ════════════════════════════════════════════════════════════════════════════

def test_snapshot_store_save_and_load(tmp_path):
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    snap  = _snap()

    store.save(snap)
    loaded = store.load()

    assert loaded is not None
    assert loaded.symbol       == snap.symbol
    assert loaded.held_qty     == snap.held_qty
    assert loaded.avg_cost_basis == snap.avg_cost_basis
    assert loaded.free_quote   == snap.free_quote
    assert loaded.quote_asset  == snap.quote_asset
    assert loaded.is_in_position      == snap.is_in_position
    assert loaded.meaningful_position == snap.meaningful_position
    assert loaded.scale_in_count      == snap.scale_in_count
    # timestamp — UTC-aware
    assert loaded.timestamp.tzinfo is not None


def test_snapshot_store_returns_none_if_no_file(tmp_path):
    store = SnapshotStore(str(tmp_path / "missing.json"))
    assert store.load() is None


def test_snapshot_store_raises_on_corrupted_file(tmp_path):
    path = str(tmp_path / "snap.json")
    with open(path, "w") as f:
        f.write("not valid json {{{")
    store = SnapshotStore(path)
    with pytest.raises(StorageCorruptedError):
        store.load()


def test_snapshot_store_atomic_write(tmp_path):
    """tmp-файл не должен оставаться после успешного save."""
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    store.save(_snap())
    assert not os.path.exists(path + ".tmp")


def test_snapshot_store_overwrites_previous(tmp_path):
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    store.save(_snap(held_qty=Decimal("0.1")))
    store.save(_snap(held_qty=Decimal("0.2")))  # overwrite
    loaded = store.load()
    assert loaded.held_qty == Decimal("0.2")


def test_snapshot_store_preserves_scale_in_count(tmp_path):
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    store.save(_snap(scale_in_count=3))
    loaded = store.load()
    assert loaded.scale_in_count == 3


def test_snapshot_store_decimal_precision(tmp_path):
    """Decimal с длинной дробью не теряет точность при round-trip."""
    path  = str(tmp_path / "snap.json")
    store = SnapshotStore(path)
    precise = Decimal("0.00012345")
    store.save(_snap(held_qty=precise))
    loaded = store.load()
    assert loaded.held_qty == precise


# ════════════════════════════════════════════════════════════════════════════
# BalanceCacheStore
# ════════════════════════════════════════════════════════════════════════════

def test_balance_cache_save_and_load(tmp_path):
    path    = str(tmp_path / "cache.json")
    store   = BalanceCacheStore(path)
    bals    = {"BTC": Decimal("0.5"), "USDT": Decimal("1234.56")}
    now_utc = datetime.now(timezone.utc)

    store.save(bals, now_utc)
    result = store.load()

    assert result is not None
    loaded_bals, loaded_ts = result
    assert loaded_bals["BTC"]  == Decimal("0.5")
    assert loaded_bals["USDT"] == Decimal("1234.56")
    assert loaded_ts.tzinfo is not None


def test_balance_cache_returns_none_if_no_file(tmp_path):
    store = BalanceCacheStore(str(tmp_path / "missing.json"))
    assert store.load() is None


def test_balance_cache_raises_on_corrupted(tmp_path):
    path = str(tmp_path / "cache.json")
    with open(path, "w") as f:
        f.write("{{broken")
    store = BalanceCacheStore(path)
    with pytest.raises(StorageCorruptedError):
        store.load()


def test_balance_cache_is_stale_when_no_file(tmp_path):
    store = BalanceCacheStore(str(tmp_path / "missing.json"))
    assert store.is_stale(ttl_seconds=30) is True


def test_balance_cache_is_stale_when_expired(tmp_path):
    path  = str(tmp_path / "cache.json")
    store = BalanceCacheStore(path)
    old   = datetime.now(timezone.utc) - timedelta(seconds=60)
    store.save({"USDT": Decimal("100")}, old)
    assert store.is_stale(ttl_seconds=30) is True


def test_balance_cache_not_stale_when_fresh(tmp_path):
    path  = str(tmp_path / "cache.json")
    store = BalanceCacheStore(path)
    fresh = datetime.now(timezone.utc) - timedelta(seconds=5)
    store.save({"USDT": Decimal("100")}, fresh)
    assert store.is_stale(ttl_seconds=30) is False


def test_balance_cache_updated_at_utc_required(tmp_path):
    store = BalanceCacheStore(str(tmp_path / "cache.json"))
    with pytest.raises(ValueError):
        store.save({"USDT": Decimal("100")}, datetime.now())  # naive


def test_balance_cache_saves_schema_version(tmp_path):
    store = BalanceCacheStore(str(tmp_path / "cache.json"))
    store.save({"USDT": Decimal("100")}, datetime.now(timezone.utc))
    raw = json.loads((tmp_path / "cache.json").read_text())
    assert raw["schema_version"] == CONFIG_SCHEMA_VERSION


def test_balance_cache_rejects_wrong_schema_version(tmp_path):
    path = tmp_path / "cache.json"
    path.write_text(json.dumps({
        "schema_version": "0.0.0",
        "balances": {"USDT": "100"},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }))
    with pytest.raises(StorageSchemaMismatchError):
        BalanceCacheStore(str(path)).load()


def test_balance_cache_rejects_missing_schema_version(tmp_path):
    path = tmp_path / "cache.json"
    path.write_text(json.dumps({
        "balances": {"USDT": "100"},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }))
    with pytest.raises(StorageSchemaMismatchError):
        BalanceCacheStore(str(path)).load()


# ════════════════════════════════════════════════════════════════════════════
# Schema validation
# ════════════════════════════════════════════════════════════════════════════

def test_schema_version_valid():
    validate_schema_version({"schema_version": CONFIG_SCHEMA_VERSION})


def test_schema_version_mismatch():
    with pytest.raises(StorageSchemaMismatchError):
        validate_schema_version({"schema_version": "0.0.0"})


def test_schema_version_missing_key():
    with pytest.raises(StorageSchemaMismatchError):
        validate_schema_version({"other_key": "value"})


# ════════════════════════════════════════════════════════════════════════════
# SpotPaperExecutor persistence
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_paper_persist_fill_state_saves_snapshot(tmp_path):
    snap_path = str(tmp_path / "snap.json")
    store     = SnapshotStore(snap_path)
    executor  = SpotPaperExecutor(
        {"USDT": Decimal("10000"), "BTC": Decimal("0")},
        snapshot_store=store,
    )
    snap = _snap()
    await executor.persist_fill_state(snap)
    loaded = store.load()
    assert loaded is not None
    assert loaded.held_qty == snap.held_qty
    assert loaded.avg_cost_basis == snap.avg_cost_basis


@pytest.mark.asyncio
async def test_paper_persist_fill_state_saves_balances(tmp_path):
    cache_path = str(tmp_path / "cache.json")
    cache      = BalanceCacheStore(cache_path)
    executor   = SpotPaperExecutor(
        {"USDT": Decimal("5000"), "BTC": Decimal("0.1")},
        balance_cache_store=cache,
    )
    await executor.persist_fill_state(_snap())
    result = cache.load()
    assert result is not None
    balances, _ = result
    assert balances["USDT"] == Decimal("5000")
    assert balances["BTC"]  == Decimal("0.1")


@pytest.mark.asyncio
async def test_paper_persist_fill_state_no_store_no_error():
    """Executor без store: persist_fill_state не должен бросать."""
    executor = SpotPaperExecutor({"USDT": Decimal("1000"), "BTC": Decimal("0")})
    await executor.persist_fill_state(_snap())  # no exception


@pytest.mark.asyncio
async def test_paper_restore_from_storage_returns_snapshot(tmp_path):
    snap_path = str(tmp_path / "snap.json")
    store     = SnapshotStore(snap_path)
    snap      = _snap(held_qty=Decimal("0.5"), scale_in_count=2)
    store.save(snap)

    executor = SpotPaperExecutor(
        {"USDT": Decimal("1000"), "BTC": Decimal("0")},
        snapshot_store=store,
    )
    restored = executor.restore_from_storage()
    assert restored is not None
    assert restored.held_qty      == Decimal("0.5")
    assert restored.scale_in_count == 2


@pytest.mark.asyncio
async def test_paper_restore_from_storage_returns_none_without_store():
    executor = SpotPaperExecutor({"USDT": Decimal("1000"), "BTC": Decimal("0")})
    assert executor.restore_from_storage() is None


# ════════════════════════════════════════════════════════════════════════════
# Регрессия Минор 2: LIMIT исполняется по цене ордера, не триггерной
# ════════════════════════════════════════════════════════════════════════════

@pytest.mark.asyncio
async def test_limit_order_fills_at_order_price_not_market_price():
    """
    LIMIT BUY @ 39000.
    Триггерная цена 38000 (ниже).
    Контракт: fill_price == 39000, не 38000.
    """
    executor = SpotPaperExecutor({"USDT": Decimal("10000"), "BTC": Decimal("0")})
    order = Order(
        symbol=BTCUSDT,
        side=Side.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("0.1"),
        price=Decimal("39000"),
    )
    await executor.submit_order(order)
    executor.set_price(BTCUSDT, Decimal("38000"))   # trigger: 38000 <= 39000

    fills = await executor.get_fills(order.client_order_id)
    assert len(fills) == 1
    assert fills[0].fill_price == Decimal("39000")   # order price, NOT 38000

# ════════════════════════════════════════════════════════════════════════════
# Contract 55: schema_version в SnapshotStore
# ════════════════════════════════════════════════════════════════════════════

def test_snapshot_store_saves_schema_version(tmp_path):
    """Contract 55: schema_version обязан быть в каждом снапшоте"""
    store = SnapshotStore(str(tmp_path / "snap.json"))
    store.save(_snap())
    raw = json.loads((tmp_path / "snap.json").read_text())
    assert "schema_version" in raw
    assert raw["schema_version"] == CONFIG_SCHEMA_VERSION


def test_snapshot_store_rejects_wrong_schema_version(tmp_path):
    """Contract 55: несовпадение schema_version → ошибка при загрузке"""
    path = tmp_path / "snap.json"
    snap = _snap()
    data = {
        "schema_version": "0.0.0",
        "symbol": {"base": snap.symbol.base, "quote": snap.symbol.quote, "market": snap.symbol.market.value},
        "held_qty": str(snap.held_qty),
        "avg_cost_basis": str(snap.avg_cost_basis),
        "free_quote": str(snap.free_quote),
        "quote_asset": snap.quote_asset,
        "is_in_position": snap.is_in_position,
        "meaningful_position": snap.meaningful_position,
        "scale_in_count": snap.scale_in_count,
        "timestamp": snap.timestamp.isoformat(),
    }
    path.write_text(json.dumps(data))
    store = SnapshotStore(str(path))
    with pytest.raises(StorageSchemaMismatchError):
        store.load()


def test_snapshot_store_rejects_missing_schema_version(tmp_path):
    """Contract 55: отсутствие schema_version → ошибка при загрузке"""
    path = tmp_path / "snap.json"
    snap = _snap()
    data = {
        "symbol": {"base": snap.symbol.base, "quote": snap.symbol.quote, "market": snap.symbol.market.value},
        "held_qty": str(snap.held_qty),
        "avg_cost_basis": str(snap.avg_cost_basis),
        "free_quote": str(snap.free_quote),
        "quote_asset": snap.quote_asset,
        "is_in_position": snap.is_in_position,
        "meaningful_position": snap.meaningful_position,
        "scale_in_count": snap.scale_in_count,
        "timestamp": snap.timestamp.isoformat(),
    }
    path.write_text(json.dumps(data))
    store = SnapshotStore(str(path))
    with pytest.raises(StorageSchemaMismatchError):
        store.load()
