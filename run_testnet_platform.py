import asyncio
import os
from decimal import Decimal

from mctp.adapters import BinanceCredentials, BinanceSpotTestnetAdapterV1, BinanceSpotTestnetConfigV1
from mctp.core.constants import TESTNET_SMOKE_GUARD_ENV
from mctp.core.enums import Market, Timeframe
from mctp.core.types import Symbol
from mctp.runtime.strategy_smoke import EmaCrossSmokeStrategy
from mctp.runtime.testnet import TestnetRuntime, TestnetRuntimeConfig
from mctp.storage.accounting_store import AccountingStore
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.snapshot_store import SnapshotStore


def _require_smoke_guard() -> None:
    if os.getenv(TESTNET_SMOKE_GUARD_ENV) == "1":
        return
    raise SystemExit(
        "run_testnet_platform.py is a smoke-only Binance Spot Testnet check, not a long-running live/runtime launcher. "
        f"Set {TESTNET_SMOKE_GUARD_ENV}=1 to run this explicit smoke check."
    )


async def main() -> None:
    _require_smoke_guard()
    api_key = os.getenv("BINANCE_TESTNET_API_KEY")
    api_secret = os.getenv("BINANCE_TESTNET_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit("BINANCE_TESTNET_API_KEY and BINANCE_TESTNET_API_SECRET are required")

    symbol = Symbol("BTC", "USDT", Market.SPOT)
    adapter = BinanceSpotTestnetAdapterV1(
        BinanceSpotTestnetConfigV1(BinanceCredentials(api_key=api_key, api_secret=api_secret))
    )
    runtime = TestnetRuntime(
        config=TestnetRuntimeConfig(
            symbol=symbol,
            timeframe=Timeframe.M15,
            instrument_info={
                "lot_size": Decimal("0.001"),
                "min_qty": Decimal("0.001"),
                "max_qty": Decimal("1000"),
                "min_notional": Decimal("10"),
            },
            initial_balances={"BTC": Decimal("0"), "USDT": Decimal("0")},
        ),
        strategy=EmaCrossSmokeStrategy(),
        executor=adapter,
        snapshot_store=SnapshotStore("testnet_snapshot.json"),
        balance_cache_store=BalanceCacheStore("testnet_balances.json"),
        accounting_store=AccountingStore("testnet_accounting.json"),
    )
    await runtime.start()
    await runtime.ping_all()
    print("testnet_smoke_check_started=True")
    print("testnet_note=Binance Spot Testnet smoke check only; this script is not a production-like long-running launcher")
    await runtime.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
