"""
PnL расчёт. Контракт 23: все три случая комиссии.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from mctp.core.order import Fill
from mctp.core.enums import Side, CommissionAsset


@dataclass
class PnLResult:
    gross_pnl: Decimal
    commission_quote: Decimal
    net_pnl: Decimal
    commission_asset: CommissionAsset
    bnb_rate_used: Optional[Decimal] = None

    def __post_init__(self):
        for name, val in [
            ("gross_pnl",        self.gross_pnl),
            ("commission_quote", self.commission_quote),
            ("net_pnl",          self.net_pnl),
        ]:
            if not isinstance(val, Decimal):
                raise AssertionError(f"PnLResult.{name} must be Decimal")


class PnLCalculator:

    @staticmethod
    def realized_pnl(
        sell_fill: Fill,
        avg_cost_basis: Decimal,
        bnb_price: Optional[Decimal] = None,
    ) -> PnLResult:
        """
        Контракт 23: три случая комиссии.
        Принимает только SELL филлы.
        """
        if sell_fill.side != Side.SELL:
            raise ValueError(f"realized_pnl requires SELL fill, got {sell_fill.side}")

        base_qty         = sell_fill.base_qty_filled
        quote_qty_filled = sell_fill.quote_qty_filled
        cost_basis_total = base_qty * avg_cost_basis
        gross_pnl        = quote_qty_filled - cost_basis_total

        ca = sell_fill.commission_asset

        if ca == CommissionAsset.QUOTE:
            # Комиссия уже в USDT — вычесть из прибыли
            commission_quote = sell_fill.commission
            net_pnl          = gross_pnl - commission_quote

        elif ca == CommissionAsset.BASE:
            # Продано net_sold BTC (меньше на комиссию)
            net_sold         = base_qty - sell_fill.commission
            net_pnl          = net_sold * sell_fill.fill_price - cost_basis_total
            commission_quote = sell_fill.commission * sell_fill.fill_price

        elif ca == CommissionAsset.BNB:
            if bnb_price is None or bnb_price <= Decimal("0"):
                raise ValueError("bnb_price required and must be > 0 for BNB commission")
            commission_quote = sell_fill.commission * bnb_price
            net_pnl          = gross_pnl - commission_quote

        else:
            raise ValueError(f"Unknown CommissionAsset: {ca}")

        return PnLResult(
            gross_pnl=gross_pnl,
            commission_quote=commission_quote,
            net_pnl=net_pnl,
            commission_asset=ca,
            bnb_rate_used=bnb_price if ca == CommissionAsset.BNB else None,
        )

    @staticmethod
    def pnl_per_lot(
        sell_fills: list,
        avg_cost_basis: Decimal,
        bnb_price: Optional[Decimal] = None,
    ) -> list:
        """Каждый SELL-филл обрабатывается независимо. Не-SELL пропускаются."""
        results = []
        for fill in sell_fills:
            if fill.side != Side.SELL:
                continue
            results.append(PnLCalculator.realized_pnl(fill, avg_cost_basis, bnb_price))
        return results

    @staticmethod
    def total_net_pnl(pnl_results: list) -> Decimal:
        return sum((r.net_pnl for r in pnl_results), Decimal("0"))
