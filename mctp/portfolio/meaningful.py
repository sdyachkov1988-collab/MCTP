from decimal import Decimal
from typing import Optional

from mctp.core.constants import MIN_MEANINGFUL_POSITION_LOT_MULTIPLIER


def is_meaningful_position(
    held_qty: Decimal,
    lot_size: Optional[Decimal] = None,
) -> bool:
    if held_qty <= Decimal("0"):
        return False
    if lot_size is None or lot_size <= Decimal("0"):
        return True
    threshold = lot_size * Decimal(MIN_MEANINGFUL_POSITION_LOT_MULTIPLIER)
    return held_qty >= threshold
