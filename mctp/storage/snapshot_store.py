"""
SnapshotStore — персистенция PortfolioSnapshot.
Формат: JSON, атомарная запись через tmp-файл + os.replace().
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from mctp.core.types import PortfolioSnapshot, Symbol
from mctp.core.enums import Market
from mctp.core.constants import CONFIG_SCHEMA_VERSION
from mctp.storage.exceptions import StorageCorruptedError, StorageSchemaMismatchError


class SnapshotStore:
    """
    Сохраняет и загружает PortfolioSnapshot.
    Одна запись = последний снапшот (перезапись при каждом save).
    """

    def __init__(self, path: str):
        self._path = path
        self._tmp  = path + ".tmp"

    # ── Public API ────────────────────────────────────────────────────────────

    def save(self, snapshot: PortfolioSnapshot) -> None:
        """Атомарно сохранить snapshot: write → tmp → os.replace."""
        data = {
            "schema_version":      CONFIG_SCHEMA_VERSION,
            "symbol": {
                "base":   snapshot.symbol.base,
                "quote":  snapshot.symbol.quote,
                "market": snapshot.symbol.market.value,
            },
            "held_qty":            str(snapshot.held_qty),
            "avg_cost_basis":      str(snapshot.avg_cost_basis),
            "free_quote":          str(snapshot.free_quote),
            "quote_asset":         snapshot.quote_asset,
            "is_in_position":      snapshot.is_in_position,
            "meaningful_position": snapshot.meaningful_position,
            "scale_in_count":      snapshot.scale_in_count,
            "timestamp":           snapshot.timestamp.isoformat(),
        }
        with open(self._tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(self._tmp, self._path)

    def load(self) -> Optional[PortfolioSnapshot]:
        """Загрузить snapshot или None если файл не существует."""
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if "schema_version" not in data:
                raise StorageSchemaMismatchError(
                    f"SnapshotStore: schema_version key missing in {self._path}"
                )
            if data["schema_version"] != CONFIG_SCHEMA_VERSION:
                raise StorageSchemaMismatchError(
                    f"SnapshotStore: schema version mismatch: "
                    f"expected {CONFIG_SCHEMA_VERSION}, got {data['schema_version']}"
                )
            symbol = Symbol(
                base=data["symbol"]["base"],
                quote=data["symbol"]["quote"],
                market=Market(data["symbol"]["market"]),
            )
            ts = datetime.fromisoformat(data["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return PortfolioSnapshot(
                symbol=symbol,
                held_qty=Decimal(data["held_qty"]),
                avg_cost_basis=Decimal(data["avg_cost_basis"]),
                free_quote=Decimal(data["free_quote"]),
                quote_asset=data["quote_asset"],
                is_in_position=data["is_in_position"],
                meaningful_position=data["meaningful_position"],
                scale_in_count=data["scale_in_count"],
                timestamp=ts,
            )
        except StorageSchemaMismatchError:
            raise
        except (json.JSONDecodeError, KeyError) as exc:
            raise StorageCorruptedError(
                f"SnapshotStore file is corrupted: {self._path}"
            ) from exc

    def exists(self) -> bool:
        """True если файл существует и не пустой."""
        return os.path.exists(self._path) and os.path.getsize(self._path) > 0
