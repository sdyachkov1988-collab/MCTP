"""
BalanceCacheStore — персистенция WS кеша балансов.
Позволяет при рестарте paper-режима не вызывать REST API.
Формат: JSON, атомарная запись через tmp-файл + os.replace().
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from mctp.storage.exceptions import StorageCorruptedError


class BalanceCacheStore:
    """
    Сохраняет и загружает кеш балансов dict[str, Decimal].
    """

    def __init__(self, path: str):
        self._path = path
        self._tmp  = path + ".tmp"

    # ── Public API ────────────────────────────────────────────────────────────

    def save(self, balances: dict[str, Decimal], updated_at: datetime) -> None:
        """Атомарно сохранить балансы. updated_at обязан быть UTC-aware."""
        if updated_at.tzinfo is None:
            raise ValueError("BalanceCacheStore.save: updated_at must be UTC-aware")
        data = {
            "balances":   {asset: str(amount) for asset, amount in balances.items()},
            "updated_at": updated_at.isoformat(),
        }
        with open(self._tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(self._tmp, self._path)

    def load(self) -> Optional[tuple[dict[str, Decimal], datetime]]:
        """Вернуть (balances, updated_at) или None если файл не существует."""
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            balances = {
                asset: Decimal(amount_str)
                for asset, amount_str in data["balances"].items()
            }
            ts = datetime.fromisoformat(data["updated_at"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return balances, ts
        except (json.JSONDecodeError, KeyError) as exc:
            raise StorageCorruptedError(
                f"BalanceCacheStore file is corrupted: {self._path}"
            ) from exc

    def is_stale(self, ttl_seconds: int) -> bool:
        """
        True если файл не существует ИЛИ данные устарели.
        Устаревшие = (now - updated_at).total_seconds() > ttl_seconds.
        """
        result = self.load()
        if result is None:
            return True
        _, updated_at = result
        delta = (datetime.now(timezone.utc) - updated_at).total_seconds()
        return delta > ttl_seconds

    def exists(self) -> bool:
        """True если файл существует и не пустой."""
        return os.path.exists(self._path) and os.path.getsize(self._path) > 0
