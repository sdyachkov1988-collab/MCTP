import asyncio
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from tempfile import TemporaryDirectory

from mctp.core.enums import Market, Timeframe
from mctp.core.types import Symbol
from mctp.indicators.models import Candle
from mctp.runtime import (
    BnbTickerEvent,
    BookTickerEvent,
    EmaCrossSmokeStrategy,
    KlineEvent,
    PaperRuntime,
    PaperRuntimeConfig,
)
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore
from mctp.streams.base import StreamType


async def run_local_demo() -> dict[str, object]:
    symbol = Symbol("BTC", "USDT", Market.SPOT)
    start = datetime(2026, 3, 22, 0, 0, 0, tzinfo=timezone.utc)
    with TemporaryDirectory() as temp_dir:
        runtime = PaperRuntime(
            config=PaperRuntimeConfig(
                symbol=symbol,
                timeframe=Timeframe.M15,
                instrument_info={
                    "lot_size": Decimal("0.001"),
                    "min_qty": Decimal("0.001"),
                    "max_qty": Decimal("1000"),
                    "min_notional": Decimal("10"),
                },
                initial_balances={"BTC": Decimal("0"), "USDT": Decimal("1000")},
            ),
            strategy=EmaCrossSmokeStrategy(),
            snapshot_store=SnapshotStore(f"{temp_dir}/snapshot.json"),
            balance_cache_store=BalanceCacheStore(f"{temp_dir}/balances.json"),
            accounting_store=AccountingStore(f"{temp_dir}/accounting.json"),
        )
        await runtime.start()
        await runtime.ping_all(start)
        candle_closes = [
            *[Decimal("100") + Decimal(index) for index in range(24)],
            Decimal("122"),
            Decimal("119"),
            Decimal("116"),
            Decimal("112"),
            Decimal("108"),
            Decimal("104"),
            Decimal("100"),
            Decimal("96"),
            Decimal("92"),
            Decimal("88"),
            Decimal("84"),
            Decimal("80"),
        ]
        for index, close in enumerate(candle_closes):
            book_time = start + timedelta(minutes=index)
            await runtime.channels[StreamType.BOOK_TICKER].publish(
                BookTickerEvent(timestamp=book_time, bid=close - Decimal("0.5"), ask=close + Decimal("0.5"))
            )
            await runtime.channels[StreamType.BNB_TICKER].publish(
                BnbTickerEvent(timestamp=book_time, price=Decimal("300"))
            )
            candle = Candle(
                timestamp=book_time,
                open=close,
                high=close + Decimal("1"),
                low=close - Decimal("1"),
                close=close,
                volume=Decimal("10"),
            )
            await runtime.channels[StreamType.KLINE].publish(KlineEvent(timeframe=Timeframe.M15, candle=candle))
            await runtime.process_all_available()
        summary = {
            "mode": "local",
            "runtime_status_before_shutdown": runtime.status.value,
            "strategy_calls": runtime.strategy_call_count,
            "last_intent": None if runtime.last_intent is None else runtime.last_intent.type.value,
            "fill_count": len(runtime.portfolio.accounting.fill_history),
            "handled_fill_count": len(runtime.handled_fills),
            "submitted_order_count": len(runtime.submitted_order_quantities),
            "snapshot_free_quote": runtime.portfolio.snapshot.free_quote,
            "snapshot_held_qty": runtime.portfolio.snapshot.held_qty,
            "kline_stale": runtime.channels[StreamType.KLINE].state.is_stale,
            "book_ticker_stale": runtime.channels[StreamType.BOOK_TICKER].state.is_stale,
        }
        await runtime.shutdown()
        summary["runtime_status_after_shutdown"] = runtime.status.value
        return summary


async def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"
    if mode != "local":
        raise SystemExit(
            "Only local demo mode is supported in run_paper_runtime.py; websocket transport remains library/runtime support, not a demo entrypoint."
        )
    summary = await run_local_demo()
    for key, value in summary.items():
        print(f"{key}={value}")


if __name__ == "__main__":
    asyncio.run(main())
