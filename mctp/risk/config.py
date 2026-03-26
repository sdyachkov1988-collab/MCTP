from dataclasses import dataclass
from decimal import Decimal

from mctp.core.constants import DEFAULT_MIN_ORDER_VALUE


@dataclass
class RiskConfig:
    scale_in_allowed: bool = False
    max_scale_in_count: int = 3
    bnb_discount_active: bool = False
    min_order_value: Decimal = DEFAULT_MIN_ORDER_VALUE  # fallback если нет в instrument_info
    max_positions: int = 1                    # single-symbol для v0.0-v2.2

    def __post_init__(self):
        if not isinstance(self.min_order_value, Decimal):
            raise AssertionError("RiskConfig.min_order_value must be Decimal")
