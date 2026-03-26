from dataclasses import dataclass, field
from decimal import Decimal

from mctp.core.constants import (
    ASSET_USDT,
    BASE_RISK_PCT,
    DEFAULT_FEE_RATE,
    STRATEGY_ID_LEGACY_EMA_CROSS,
    T_CANCEL,
)
from mctp.core.enums import CommissionAsset, Market, OrderType, Timeframe
from mctp.core.types import Symbol
from mctp.risk.config import RiskConfig
from mctp.sizing.config import SizerConfig


@dataclass
class BacktestConfig:
    symbol: Symbol
    initial_quote: Decimal
    warmup_bars: int
    ema_period: int
    atr_period: int
    instrument_info: dict
    initial_base: Decimal = Decimal("0")
    quote_asset: str = ASSET_USDT
    spread_bps: Decimal = Decimal("0")
    fee_rate: Decimal = DEFAULT_FEE_RATE
    commission_asset: CommissionAsset = CommissionAsset.QUOTE
    tp_atr_multiplier: Decimal = Decimal("2.0")
    sl_atr_multiplier: Decimal = Decimal("1.0")
    entry_order_type: OrderType = OrderType.MARKET
    entry_limit_discount_pct: Decimal = Decimal("0")
    timeframe: Timeframe = Timeframe.M15
    strategy_id: str = STRATEGY_ID_LEGACY_EMA_CROSS
    risk_config: RiskConfig = field(default_factory=RiskConfig)
    sizer_config: SizerConfig = field(
        default_factory=lambda: SizerConfig(risk_pct=BASE_RISK_PCT, use_early_risk_cap=True)
    )
    cancel_after_seconds: int = T_CANCEL

    def __post_init__(self) -> None:
        if self.symbol.market != Market.SPOT:
            raise AssertionError("BacktestConfig.symbol must be SPOT")
        for name, value in [
            ("initial_quote", self.initial_quote),
            ("initial_base", self.initial_base),
            ("spread_bps", self.spread_bps),
            ("fee_rate", self.fee_rate),
            ("tp_atr_multiplier", self.tp_atr_multiplier),
            ("sl_atr_multiplier", self.sl_atr_multiplier),
            ("entry_limit_discount_pct", self.entry_limit_discount_pct),
        ]:
            if not isinstance(value, Decimal):
                raise AssertionError(f"BacktestConfig.{name} must be Decimal")
        if self.cancel_after_seconds <= 0:
            raise AssertionError("cancel_after_seconds must be > 0")
        for required_key in ("lot_size", "min_qty", "max_qty", "min_notional"):
            if required_key not in self.instrument_info:
                raise AssertionError(f"instrument_info missing {required_key}")

    @property
    def required_warmup_bars(self) -> int:
        return max(self.warmup_bars, self.ema_period, self.atr_period + 1)
