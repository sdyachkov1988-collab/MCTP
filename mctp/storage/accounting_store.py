import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from mctp.core.enums import CommissionAsset, Market
from mctp.core.types import Symbol
from mctp.portfolio.accounting import AccountingFillRecord
from mctp.storage.exceptions import StorageCorruptedError


class AccountingStore:
    def __init__(self, path: str):
        self._path = path
        self._tmp = path + ".tmp"

    def save(self, records: list[AccountingFillRecord]) -> None:
        data = [
            {
                "trade_id": record.trade_id,
                "order_id": record.order_id,
                "symbol": {
                    "base": record.symbol.base,
                    "quote": record.symbol.quote,
                    "market": record.symbol.market.value,
                },
                "filled_at": record.filled_at.isoformat(),
                "fill_price": str(record.fill_price),
                "commission": str(record.commission),
                "commission_asset": record.commission_asset.value,
                "fee_drag_quote": str(record.fee_drag_quote),
                "bnb_rate_at_fill": None if record.bnb_rate_at_fill is None else str(record.bnb_rate_at_fill),
            }
            for record in records
        ]
        with open(self._tmp, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False)
        os.replace(self._tmp, self._path)

    def load(self) -> Optional[list[AccountingFillRecord]]:
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            result: list[AccountingFillRecord] = []
            for item in data:
                filled_at = datetime.fromisoformat(item["filled_at"])
                if filled_at.tzinfo is None:
                    filled_at = filled_at.replace(tzinfo=timezone.utc)
                result.append(
                    AccountingFillRecord(
                        trade_id=item["trade_id"],
                        order_id=item["order_id"],
                        symbol=Symbol(
                            base=item["symbol"]["base"],
                            quote=item["symbol"]["quote"],
                            market=Market(item["symbol"]["market"]),
                        ),
                        filled_at=filled_at,
                        fill_price=Decimal(item["fill_price"]),
                        commission=Decimal(item["commission"]),
                        commission_asset=CommissionAsset(item["commission_asset"]),
                        fee_drag_quote=Decimal(item["fee_drag_quote"]),
                        bnb_rate_at_fill=(
                            None if item["bnb_rate_at_fill"] is None else Decimal(item["bnb_rate_at_fill"])
                        ),
                    )
                )
            return result
        except (json.JSONDecodeError, KeyError) as exc:
            raise StorageCorruptedError(f"AccountingStore file is corrupted: {self._path}") from exc

    def exists(self) -> bool:
        return os.path.exists(self._path) and os.path.getsize(self._path) > 0
