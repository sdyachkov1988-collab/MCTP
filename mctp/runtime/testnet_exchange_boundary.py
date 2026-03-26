from __future__ import annotations

from mctp.core.constants import (
    EXCHANGE_ACTIVE_ORDER_STATUSES,
    EXCHANGE_FILLED_OR_PARTIAL_ORDER_STATUSES,
    EXCHANGE_LIST_STATUS_ALL_DONE,
    EXCHANGE_OPEN_SUBMIT_ORDER_STATUSES,
    EXCHANGE_ORDER_STATUS_CANCELED,
    EXCHANGE_ORDER_STATUS_CANCELLED,
    EXCHANGE_ORDER_STATUS_FILLED,
    EXCHANGE_ORDER_STATUS_RANKS,
    EXCHANGE_STATUS_SOURCE_WEBSOCKET,
    EXCHANGE_STATUS_SOURCE_REST,
    EXCHANGE_TERMINAL_ORDER_STATUSES,
    SUPPORTED_EXCHANGE_SPOT_QUOTE_ASSETS,
)
from mctp.core.enums import ExecutionResult, Market
from mctp.core.types import Symbol


def execution_result_from_exchange_status(status: str) -> ExecutionResult:
    if status == EXCHANGE_ORDER_STATUS_FILLED:
        return ExecutionResult.FILLED
    if status in EXCHANGE_FILLED_OR_PARTIAL_ORDER_STATUSES[1:]:
        return ExecutionResult.PARTIAL_FILL
    if status in EXCHANGE_OPEN_SUBMIT_ORDER_STATUSES:
        return ExecutionResult.ACCEPTED
    if status in {EXCHANGE_ORDER_STATUS_CANCELED, EXCHANGE_ORDER_STATUS_CANCELLED}:
        return ExecutionResult.CANCELLED
    return ExecutionResult.REJECTED


def is_active_exchange_order_status(status: str) -> bool:
    return status in EXCHANGE_ACTIVE_ORDER_STATUSES


def is_terminal_exchange_order_status(status: str) -> bool:
    return status in EXCHANGE_TERMINAL_ORDER_STATUSES


def exchange_order_status_rank(status: str) -> int:
    return EXCHANGE_ORDER_STATUS_RANKS.get(status, 0)


def should_replace_exchange_order_status(
    current: str,
    current_source: str | None,
    incoming: str,
    incoming_source: str,
) -> bool:
    if current == incoming:
        return incoming_source == EXCHANGE_STATUS_SOURCE_WEBSOCKET and current_source != EXCHANGE_STATUS_SOURCE_WEBSOCKET
    if current == EXCHANGE_ORDER_STATUS_FILLED and incoming != EXCHANGE_ORDER_STATUS_FILLED:
        return False
    current_rank = exchange_order_status_rank(current)
    incoming_rank = exchange_order_status_rank(incoming)
    if incoming_rank < current_rank:
        return False
    if (
        current_source == EXCHANGE_STATUS_SOURCE_WEBSOCKET
        and incoming_source == EXCHANGE_STATUS_SOURCE_REST
        and incoming_rank <= current_rank
    ):
        return False
    return True


def is_external_oco_cancellation(list_status_type: str, list_order_status: str) -> bool:
    return (
        list_status_type == EXCHANGE_LIST_STATUS_ALL_DONE
        and list_order_status == EXCHANGE_LIST_STATUS_ALL_DONE
    )


def parse_exchange_spot_symbol(raw_symbol: str) -> Symbol:
    normalized = raw_symbol.upper()
    for quote_asset in SUPPORTED_EXCHANGE_SPOT_QUOTE_ASSETS:
        if normalized.endswith(quote_asset) and len(normalized) > len(quote_asset):
            return Symbol(normalized[: -len(quote_asset)], quote_asset, Market.SPOT)
    raise ValueError(f"Unsupported exchange symbol: {raw_symbol}")
