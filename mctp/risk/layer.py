"""
RiskLayer — 7 защитных guards между Intent и Execution.
Контракты: Guard 1-7.
RiskLayer является READ-ONLY: никогда не модифицирует PortfolioSnapshot.
"""
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional

from mctp.core.types import Intent, PortfolioSnapshot
from mctp.core.enums import IntentType, QuantityMode, RejectionReason
from mctp.core.constants import ASSET_BNB, ASSET_USDT, MIN_NOTIONAL_BUFFER
from .config import RiskConfig


@dataclass
class RiskResult:
    approved: bool
    rejection_reason: Optional[RejectionReason]  # None если approved
    intent: Intent
    checked_at: datetime  # UTC

    def __post_init__(self):
        if self.checked_at.tzinfo is None:
            raise ValueError("RiskResult.checked_at must be UTC-aware")


class RiskLayer:
    def __init__(self, config: RiskConfig):
        self._config = config

    def check(
        self,
        intent: Intent,
        snapshot: PortfolioSnapshot,
        instrument_info: dict,
    ) -> RiskResult:
        """
        Проверяет Intent против PortfolioSnapshot в строгом порядке Guards 1-7.
        Не модифицирует snapshot.
        """

        def _approve() -> RiskResult:
            return RiskResult(
                approved=True,
                rejection_reason=None,
                intent=intent,
                checked_at=datetime.now(timezone.utc),
            )

        def _reject(reason: RejectionReason) -> RiskResult:
            return RiskResult(
                approved=False,
                rejection_reason=reason,
                intent=intent,
                checked_at=datetime.now(timezone.utc),
            )

        def _get_min_notional() -> Decimal:
            val = instrument_info.get("min_notional", self._config.min_order_value)
            return val if isinstance(val, Decimal) else Decimal(str(val))

        # HOLD всегда проходит — стратегия говорит «ничего не делать»
        if intent.type == IntentType.HOLD:
            return _approve()

        # ── Guard 1: позиция ────────────────────────────────────────────────
        # BUY при уже открытой позиции: разрешено только если scale_in=True
        if intent.type == IntentType.BUY and snapshot.is_in_position:
            if not self._config.scale_in_allowed:
                return _reject(RejectionReason.ALREADY_IN_POSITION)

        # ── Guard 2: нулевая позиция ─────────────────────────────────────────
        if intent.type == IntentType.SELL and snapshot.held_qty == Decimal("0"):
            return _reject(RejectionReason.NO_POSITION_TO_SELL)

        # ── Guard 3: недостаточно котируемого актива ─────────────────────────
        if intent.type == IntentType.BUY:
            min_val = _get_min_notional()
            if snapshot.free_quote < min_val:
                return _reject(RejectionReason.INSUFFICIENT_QUOTE)

        # ── Guard 4: BNB символ запрещён ─────────────────────────────────────
        # Применяется к BUY и SELL (HOLD уже вышел раньше)
        if self._config.bnb_discount_active:
            if intent.symbol.base == ASSET_BNB and intent.symbol.quote == ASSET_USDT:
                return _reject(RejectionReason.BNB_SYMBOL_FORBIDDEN)

        # ── Guard 5: quote qty запрещён для SELL ────────────────────────────
        if intent.type == IntentType.SELL and intent.quantity_mode == QuantityMode.QUOTE:
            return _reject(RejectionReason.QUOTE_QTY_SELL_FORBIDDEN)

        # ── Guard 6: лимит scale-in ──────────────────────────────────────────
        if intent.type == IntentType.BUY and snapshot.is_in_position:
            if snapshot.scale_in_count >= self._config.max_scale_in_count:
                return _reject(RejectionReason.MAX_SCALE_IN_REACHED)

        # ── Guard 7: восстановление суб-значимой позиции ─────────────────────
        # BUY + is_in_position + meaningful_position=False + scale_in=True
        # → это восстановление, не scale-in; проверяем минимальный нотионал
        if (
            intent.type == IntentType.BUY
            and snapshot.is_in_position
            and not snapshot.meaningful_position
            and self._config.scale_in_allowed
        ):
            buy_notional = (
                intent.partial_fraction if intent.partial_fraction is not None
                else Decimal("1")
            ) * snapshot.free_quote
            current_notional = snapshot.held_qty * snapshot.avg_cost_basis
            total_notional = current_notional + buy_notional
            min_notional = _get_min_notional()
            if total_notional <= MIN_NOTIONAL_BUFFER * min_notional:
                return _reject(RejectionReason.RESTORE_BELOW_MIN_NOTIONAL)

        return _approve()
