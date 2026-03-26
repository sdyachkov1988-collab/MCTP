"""
CostBasisUpdater — обновление avg_cost_basis ТОЛЬКО из филлов.
Контракт: никогда не модифицирует переданный snapshot.
"""
from decimal import Decimal
from datetime import datetime, timezone

from mctp.core.types import PortfolioSnapshot
from mctp.core.order import Fill
from mctp.core.enums import Side, CommissionAsset
from mctp.portfolio.meaningful import is_meaningful_position


class CostBasisUpdater:
    """Чистые статические методы. Не модифицирует переданный snapshot."""

    @staticmethod
    def apply_buy_fill(
        snapshot: PortfolioSnapshot,
        fill: Fill,
        bnb_rate_at_fill: Decimal | None = None,
        lot_size: Decimal | None = None,
    ) -> PortfolioSnapshot:
        """
        Применить BUY филл.
        new_basis = (prev_held × prev_basis + net_base × fill_price) / new_held
        """
        if fill.side != Side.BUY:
            raise ValueError(f"apply_buy_fill requires BUY fill, got {fill.side}")

        net_base      = fill.net_base_received()   # base qty after BASE commission
        net_quote_out = fill.net_quote_spent()     # quote spent incl. QUOTE commission

        new_held = snapshot.held_qty + net_base
        entry_fee_quote = CostBasisUpdater._entry_fee_quote(fill, bnb_rate_at_fill)
        total_entry_cost_quote = fill.quote_qty_filled + entry_fee_quote

        if new_held > Decimal("0"):
            new_basis = (
                snapshot.held_qty * snapshot.avg_cost_basis
                + total_entry_cost_quote
            ) / new_held
        else:
            new_basis = Decimal("0")

        new_free_quote = snapshot.free_quote - net_quote_out
        is_in_position = new_held > Decimal("0")
        meaningful = is_meaningful_position(new_held, lot_size)

        return PortfolioSnapshot(
            symbol=snapshot.symbol,
            held_qty=new_held,
            avg_cost_basis=new_basis,
            free_quote=new_free_quote,
            quote_asset=snapshot.quote_asset,
            is_in_position=is_in_position,
            meaningful_position=meaningful,
            scale_in_count=snapshot.scale_in_count,
            timestamp=fill.filled_at,
        )

    @staticmethod
    def apply_sell_fill(
        snapshot: PortfolioSnapshot,
        fill: Fill,
        lot_size: Decimal | None = None,
    ) -> PortfolioSnapshot:
        """
        Применить SELL филл.
        avg_cost_basis сохраняется при частичной продаже, обнуляется при полной.
        """
        if fill.side != Side.SELL:
            raise ValueError(f"apply_sell_fill requires SELL fill, got {fill.side}")

        new_held = max(Decimal("0"), snapshot.held_qty - fill.base_qty_filled)

        # Сколько quote получили (за вычетом комиссии если QUOTE)
        if fill.commission_asset == CommissionAsset.QUOTE:
            net_quote_received = fill.quote_qty_filled - fill.commission
        else:
            net_quote_received = fill.quote_qty_filled

        if new_held > Decimal("0"):
            new_basis = snapshot.avg_cost_basis  # сохранить
            is_in_position = True
            meaningful = is_meaningful_position(new_held, lot_size)
        else:
            new_basis = Decimal("0")
            is_in_position = False
            meaningful = False

        new_free_quote = snapshot.free_quote + net_quote_received

        return PortfolioSnapshot(
            symbol=snapshot.symbol,
            held_qty=new_held,
            avg_cost_basis=new_basis,
            free_quote=new_free_quote,
            quote_asset=snapshot.quote_asset,
            is_in_position=is_in_position,
            meaningful_position=meaningful,
            scale_in_count=snapshot.scale_in_count,
            timestamp=fill.filled_at,
        )

    @staticmethod
    def apply_fill(
        snapshot: PortfolioSnapshot,
        fill: Fill,
        bnb_rate_at_fill: Decimal | None = None,
        lot_size: Decimal | None = None,
    ) -> PortfolioSnapshot:
        """Диспетчер: BUY → apply_buy_fill, SELL → apply_sell_fill."""
        if fill.side == Side.BUY:
            return CostBasisUpdater.apply_buy_fill(snapshot, fill, bnb_rate_at_fill, lot_size)
        return CostBasisUpdater.apply_sell_fill(snapshot, fill, lot_size)

    @staticmethod
    def _entry_fee_quote(fill: Fill, bnb_rate_at_fill: Decimal | None) -> Decimal:
        if fill.commission_asset == CommissionAsset.QUOTE:
            return fill.commission
        if fill.commission_asset == CommissionAsset.BASE:
            return Decimal("0")
        if bnb_rate_at_fill is None or bnb_rate_at_fill <= Decimal("0"):
            raise ValueError("bnb_rate_at_fill required and must be > 0 for BNB commission fills")
        return fill.commission * bnb_rate_at_fill
