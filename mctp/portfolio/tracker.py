"""
PortfolioTracker is the single source of truth for portfolio state (v0.6).
State changes only through on_fill().
"""
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Optional

from mctp.core.enums import CommissionAsset
from mctp.core.order import Fill
from mctp.core.types import PortfolioSnapshot
from mctp.portfolio.accounting import AccountingLedger
from mctp.portfolio.equity import EquitySnapshot, EquityTracker
from mctp.portfolio.pnl import PnLCalculator, PnLResult
from mctp.portfolio.updater import CostBasisUpdater
from mctp.storage.snapshot_store import SnapshotStore


class PortfolioTracker:
    def __init__(
        self,
        initial_snapshot: PortfolioSnapshot,
        equity_tracker: EquityTracker,
        snapshot_store: Optional[SnapshotStore] = None,
        bnb_price_provider: Optional[Callable[[], Optional[Decimal]]] = None,
        lot_size_provider: Optional[Callable[[], Optional[Decimal]]] = None,
        accounting_ledger: Optional[AccountingLedger] = None,
    ) -> None:
        self._snapshot: PortfolioSnapshot = initial_snapshot
        self._equity_tracker: EquityTracker = equity_tracker
        self._snapshot_store: Optional[SnapshotStore] = snapshot_store
        self._bnb_price_provider: Optional[Callable[[], Optional[Decimal]]] = bnb_price_provider
        self._lot_size_provider: Optional[Callable[[], Optional[Decimal]]] = lot_size_provider
        self._accounting_ledger: AccountingLedger = accounting_ledger or AccountingLedger()

    @property
    def snapshot(self) -> PortfolioSnapshot:
        return self._snapshot

    @property
    def accounting(self) -> AccountingLedger:
        return self._accounting_ledger

    def on_fill(self, fill: Fill) -> PortfolioSnapshot:
        bnb_rate_at_fill = self._resolve_bnb_rate_at_fill(fill)
        lot_size = self._resolve_lot_size()
        self._accounting_ledger.record_fill(fill, bnb_rate_at_fill)
        self._snapshot = CostBasisUpdater.apply_fill(self._snapshot, fill, bnb_rate_at_fill, lot_size)
        self._persist_snapshot()
        return self._snapshot

    def replace_snapshot(self, **changes: object) -> PortfolioSnapshot:
        self._snapshot = replace(self._snapshot, **changes)
        self._persist_snapshot()
        return self._snapshot

    def restore_snapshot(self, snapshot: PortfolioSnapshot) -> PortfolioSnapshot:
        self._snapshot = snapshot
        self._persist_snapshot()
        return self._snapshot

    def record_equity(
        self,
        current_price: Decimal,
        now: Optional[datetime] = None,
    ) -> Optional[EquitySnapshot]:
        ts = now if now is not None else datetime.now(timezone.utc)
        if not self._equity_tracker.should_record(ts):
            return None
        snap = EquityTracker.make_snapshot(
            free_quote=self._snapshot.free_quote,
            held_qty=self._snapshot.held_qty,
            current_price=current_price,
            is_in_position=self._snapshot.is_in_position,
            meaningful_position=self._snapshot.meaningful_position,
            now=ts,
        )
        self._equity_tracker.record(snap)
        return snap

    def realized_pnl(self, sell_fill: Fill) -> PnLResult:
        bnb_price: Optional[Decimal] = None
        if self._bnb_price_provider is not None:
            bnb_price = self._bnb_price_provider()
        return PnLCalculator.realized_pnl(
            sell_fill=sell_fill,
            avg_cost_basis=self._snapshot.avg_cost_basis,
            bnb_price=bnb_price,
        )

    def detect_external_balance_change(
        self,
        exchange_free_quote: Decimal,
        tolerance: Decimal = Decimal("0.01"),
    ) -> bool:
        diff = abs(exchange_free_quote - self._snapshot.free_quote)
        return diff > tolerance

    def _resolve_bnb_rate_at_fill(self, fill: Fill) -> Optional[Decimal]:
        bnb_rate: Optional[Decimal] = None
        if self._bnb_price_provider is not None:
            bnb_rate = self._bnb_price_provider()
            if bnb_rate is not None and not isinstance(bnb_rate, Decimal):
                raise AssertionError("bnb_price_provider must return Decimal or None")
        if fill.commission_asset == CommissionAsset.BNB:
            if bnb_rate is None or bnb_rate <= Decimal("0"):
                raise ValueError("bnb_rate required and must be > 0 for BNB commission fills")
        return bnb_rate

    def _resolve_lot_size(self) -> Optional[Decimal]:
        if self._lot_size_provider is None:
            return None
        lot_size = self._lot_size_provider()
        if lot_size is not None and not isinstance(lot_size, Decimal):
            raise AssertionError("lot_size_provider must return Decimal or None")
        return lot_size

    def _persist_snapshot(self) -> None:
        if self._snapshot_store is not None:
            self._snapshot_store.save(self._snapshot)
