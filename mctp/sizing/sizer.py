"""
PositionSizer — Контракт 54 Уровень 1: Fixed Fractional Risk.
Формула: size = deposit × risk_pct / (price × stop_distance_pct)
"""
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone

from mctp.core.types import PortfolioSnapshot
from .config import SizerConfig
from .models import RiskMultipliers, SizerResult


class PositionSizer:
    def __init__(self, config: SizerConfig):
        self._config = config

    def calculate(
        self,
        snapshot: PortfolioSnapshot,
        stop_distance_pct: Decimal,   # расстояние до стопа в % от цены входа
        instrument_info: dict,         # lot_size, min_qty, max_qty, min_notional
        current_price: Decimal,
        risk_multipliers: RiskMultipliers,
    ) -> SizerResult:
        """
        Рассчитывает размер позиции по формуле Fixed Fractional Risk.
        Не модифицирует snapshot.
        """

        def _reject(reason: str) -> SizerResult:
            return SizerResult(
                approved=False,
                quantity=None,
                notional=None,
                rejection_reason=reason,
                risk_used=Decimal("0"),
                calculated_at=datetime.now(timezone.utc),
            )

        def _to_decimal(val) -> Decimal:
            return val if isinstance(val, Decimal) else Decimal(str(val))

        # ── Извлечь ограничения инструмента ─────────────────────────────────
        lot_size    = _to_decimal(instrument_info.get("lot_size",    Decimal("0.00000001")))
        min_qty     = _to_decimal(instrument_info.get("min_qty",     Decimal("0")))
        max_qty     = _to_decimal(instrument_info.get("max_qty",     Decimal("9999999")))
        min_notional = _to_decimal(instrument_info.get("min_notional", Decimal("10")))

        # ── Guard: нулевые делители ──────────────────────────────────────────
        if current_price <= Decimal("0"):
            return _reject("current_price must be > 0")
        if stop_distance_pct <= Decimal("0"):
            return _reject("stop_distance_pct must be > 0")

        # ── Формула Fixed Fractional Risk ────────────────────────────────────
        # base_risk = deposit × effective_risk_pct
        effective_risk = self._config.effective_risk_pct()
        base_risk = snapshot.free_quote * effective_risk

        # adjusted_risk = base_risk × combined_multiplier
        combined = risk_multipliers.combined()
        adjusted_risk = base_risk * combined

        # raw_qty = adjusted_risk / (price × stop_distance_pct)
        raw_qty = adjusted_risk / (current_price * stop_distance_pct)

        # ── Квантование вниз по lot_size ─────────────────────────────────────
        if lot_size > Decimal("0"):
            qty = (raw_qty // lot_size) * lot_size
        else:
            qty = raw_qty

        # ── Проверки ограничений ─────────────────────────────────────────────
        if qty < min_qty:
            return _reject(f"qty {qty} < min_qty {min_qty}")

        if qty > max_qty:
            qty = max_qty

        notional = qty * current_price
        if notional < min_notional:
            return _reject(f"notional {notional} < min_notional {min_notional}")

        return SizerResult(
            approved=True,
            quantity=qty,
            notional=notional,
            rejection_reason=None,
            risk_used=adjusted_risk,
            calculated_at=datetime.now(timezone.utc),
        )
