from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from mctp.core.constants import (
    ASSET_BNB,
    EXCHANGE_USER_DATA_EVENT_ACCOUNT_POSITION,
    EXCHANGE_USER_DATA_EVENT_EXECUTION_REPORT,
    EXCHANGE_USER_DATA_EVENT_LIST_STATUS,
    RAW_COMMISSION_ASSET_BASE,
    RAW_COMMISSION_ASSET_QUOTE,
)
from mctp.core.enums import CommissionAsset, Side, Timeframe
from mctp.core.order import Fill
from mctp.core.types import Symbol
from mctp.runtime.adapters import adapt_binance_payload
from mctp.runtime.testnet_exchange_boundary import execution_result_from_exchange_status, parse_exchange_spot_symbol
from mctp.runtime.events import BnbTickerEvent, BookTickerEvent, ExecutionReportEvent, KlineEvent, OCOListStatusEvent, OutboundAccountPositionEvent
from mctp.streams.base import StreamType


def adapt_binance_testnet_payload(
    stream_type: StreamType,
    payload: Any,
    *,
    timeframe: Optional[Timeframe] = None,
    symbol: Optional[Symbol] = None,
) -> KlineEvent | BookTickerEvent | BnbTickerEvent | ExecutionReportEvent | OCOListStatusEvent | OutboundAccountPositionEvent | None:
    if stream_type in {StreamType.KLINE, StreamType.BOOK_TICKER, StreamType.BNB_TICKER}:
        return adapt_binance_payload(stream_type, payload, timeframe)
    if stream_type != StreamType.USER_DATA:
        return None
    event_type = payload.get("e")
    if event_type == EXCHANGE_USER_DATA_EVENT_ACCOUNT_POSITION:
        balances = {item["a"]: Decimal(str(item["f"])) for item in payload.get("B", [])}
        locked_balances = {item["a"]: Decimal(str(item.get("l", "0"))) for item in payload.get("B", [])}
        return OutboundAccountPositionEvent(
            timestamp=_from_millis(payload.get("E")),
            balances=balances,
            locked_balances=locked_balances,
        )
    if event_type == EXCHANGE_USER_DATA_EVENT_EXECUTION_REPORT:
        resolved_symbol = symbol or parse_exchange_spot_symbol(str(payload.get("s", "")))
        fill = None
        last_fill_qty = Decimal(str(payload.get("l", "0")))
        if last_fill_qty > Decimal("0"):
            last_fill_price = Decimal(str(payload.get("L", "0")))
            fill = Fill(
                order_id=str(payload.get("i", payload.get("c", ""))),
                symbol=resolved_symbol,
                side=Side(str(payload.get("S", "BUY"))),
                base_qty_filled=last_fill_qty,
                quote_qty_filled=last_fill_qty * last_fill_price,
                fill_price=last_fill_price,
                commission=Decimal(str(payload.get("n", "0"))),
                commission_asset=_commission_asset(payload.get("N")),
                trade_id=str(payload.get("t", payload.get("c", ""))),
                filled_at=_from_millis(payload.get("T") or payload.get("E")),
            )
        return ExecutionReportEvent(
            timestamp=_from_millis(payload.get("E")),
            symbol=resolved_symbol,
            client_order_id=str(payload.get("c", "")),
            execution_result=execution_result_from_exchange_status(str(payload.get("X", ""))),
            order_status=str(payload.get("X", "")),
            fill=fill,
        )
    if event_type == EXCHANGE_USER_DATA_EVENT_LIST_STATUS:
        resolved_symbol = symbol or parse_exchange_spot_symbol(str(payload.get("s", "")))
        return OCOListStatusEvent(
            timestamp=_from_millis(payload.get("E")),
            symbol=resolved_symbol,
            list_order_id=str(payload.get("g", payload.get("orderListId", ""))),
            list_status_type=str(payload.get("l", payload.get("listStatusType", ""))),
            list_order_status=str(payload.get("L", payload.get("listOrderStatus", ""))),
            contingency_type=str(payload.get("c", payload.get("contingencyType", ""))),
        )
    return None


def _from_millis(raw_value: Any) -> datetime:
    return datetime.fromtimestamp(int(raw_value) / 1000, tz=timezone.utc)


def _commission_asset(raw_asset: Any) -> CommissionAsset:
    normalized = str(raw_asset or RAW_COMMISSION_ASSET_QUOTE)
    if normalized == ASSET_BNB:
        return CommissionAsset.BNB
    if normalized == RAW_COMMISSION_ASSET_BASE:
        return CommissionAsset.BASE
    return CommissionAsset.QUOTE
