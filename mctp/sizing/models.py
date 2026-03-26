"""
Модели для расчёта размера позиции. Контракт 54.
RiskMultipliers — адаптивные коэффициенты риска.
SizerResult    — результат расчёта.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional


@dataclass
class RiskMultipliers:
    loss_mult: Decimal = Decimal("1.0")
    atr_mult: Decimal = Decimal("1.0")       # заглушка до v0.11
    regime_mult: Decimal = Decimal("1.0")    # заглушка до v3.2
    anomaly_mult: Decimal = Decimal("1.0")   # заглушка до v3.0

    def __post_init__(self):
        for name, val in [
            ("loss_mult", self.loss_mult),
            ("atr_mult", self.atr_mult),
            ("regime_mult", self.regime_mult),
            ("anomaly_mult", self.anomaly_mult),
        ]:
            if not isinstance(val, Decimal):
                raise AssertionError(f"RiskMultipliers.{name} must be Decimal")

    def combined(self) -> Decimal:
        """
        Перемножает все коэффициенты.
        Если regime_mult == 0 → результат = 0 независимо от остальных.
        """
        return self.loss_mult * self.atr_mult * self.regime_mult * self.anomaly_mult


@dataclass
class SizerResult:
    approved: bool
    quantity: Optional[Decimal]      # None если rejected
    notional: Optional[Decimal]      # quantity × price; None если rejected
    rejection_reason: Optional[str]  # None если approved
    risk_used: Decimal               # использованный риск в котируемом активе
    calculated_at: datetime          # UTC

    def __post_init__(self):
        if self.calculated_at.tzinfo is None:
            raise ValueError("SizerResult.calculated_at must be UTC-aware")
        if not isinstance(self.risk_used, Decimal):
            raise AssertionError("SizerResult.risk_used must be Decimal")
