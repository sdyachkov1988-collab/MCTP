"""
Конфигурация PositionSizer. Контракт 54.
"""
from dataclasses import dataclass
from decimal import Decimal

from mctp.core.constants import MAX_RISK_PCT_EARLY, MAX_RISK_PCT_FULL


@dataclass
class SizerConfig:
    risk_pct: Decimal            # текущий активный риск
    use_early_risk_cap: bool = True  # True до v0.11

    def __post_init__(self):
        if not isinstance(self.risk_pct, Decimal):
            raise AssertionError("SizerConfig.risk_pct must be Decimal")

    def effective_risk_pct(self) -> Decimal:
        """
        Возвращает реальный процент риска с учётом ограничения фазы.
        До v0.11 (use_early_risk_cap=True): cap = MAX_RISK_PCT_EARLY (1.0%)
        После  (use_early_risk_cap=False):  cap = MAX_RISK_PCT_FULL  (2.0%)
        """
        cap = MAX_RISK_PCT_EARLY if self.use_early_risk_cap else MAX_RISK_PCT_FULL
        return min(self.risk_pct, cap)
