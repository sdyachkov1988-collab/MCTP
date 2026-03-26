from collections.abc import Mapping

from mctp.core.enums import Timeframe

from .models import WarmupRequirement


def compute_warmup_requirements(indicator_periods: Mapping[Timeframe, int]) -> tuple[WarmupRequirement, ...]:
    requirements = [
        WarmupRequirement(timeframe=timeframe, bars_required=bars_required)
        for timeframe, bars_required in indicator_periods.items()
    ]
    return tuple(sorted(requirements, key=lambda item: item.bars_required))


def validate_warmup_coverage(requirements: tuple[WarmupRequirement, ...], available_bars: Mapping[Timeframe, int]) -> bool:
    for requirement in requirements:
        if available_bars.get(requirement.timeframe, 0) < requirement.bars_required:
            return False
    return True
