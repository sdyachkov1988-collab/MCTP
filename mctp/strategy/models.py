from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Optional

from mctp.core.enums import Timeframe
from mctp.core.types import Intent, PortfolioSnapshot
from mctp.indicators.models import Candle


@dataclass(frozen=True)
class OnChainData:
    payload: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@dataclass(frozen=True)
class StrategyInput:
    snapshot: PortfolioSnapshot
    indicators: dict[str, object]
    candles: dict[Timeframe, list[Candle]]
    onchain: Optional[OnChainData] = None

    def __post_init__(self) -> None:
        immutable_indicators = MappingProxyType(dict(self.indicators))
        immutable_candles = MappingProxyType(
            {timeframe: tuple(candles) for timeframe, candles in self.candles.items()}
        )
        object.__setattr__(self, "indicators", immutable_indicators)
        object.__setattr__(self, "candles", immutable_candles)
