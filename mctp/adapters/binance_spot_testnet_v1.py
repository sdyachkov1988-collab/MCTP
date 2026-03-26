import asyncio
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from mctp.core.constants import (
    ACCOUNT_SNAPSHOT_TTL_SECONDS,
    BINANCE_ANNOUNCEMENTS_RSS_URL,
    BINANCE_SPOT_TESTNET_REST_BASE_URL,
    EXECUTION_STATE_RETENTION_SECONDS,
    EXCHANGE_LIST_STATUS_ALL_DONE,
    EXCHANGE_ORDER_STATUS_CANCELED,
    EXCHANGE_ORDER_STATUS_RANKS,
    EXCHANGE_ORDER_STATUS_REJECTED,
    EXCHANGE_STATUS_SOURCE_REST,
    EXCHANGE_STATUS_SOURCE_WEBSOCKET,
    EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE,
    EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE_SIGNATURE,
    EXCHANGE_WS_API_METHOD_USER_DATA_UNSUBSCRIBE,
    MAX_EXECUTION_STATE_TERMINAL_ENTRIES,
    T_CANCEL,
)
from mctp.core.enums import CommissionAsset, ExecutionResult, OrderType, Side
from mctp.core.exceptions import MCTPError
from mctp.core.interfaces import ExecutionInterface
from mctp.core.order import Fill, Order
from mctp.core.types import Symbol
from mctp.execution.oco import OCOOrder
from mctp.runtime.events import DelistingSignalEvent, ExecutionReportEvent, OutboundAccountPositionEvent
from mctp.runtime.testnet_exchange_boundary import (
    execution_result_from_exchange_status,
    is_active_exchange_order_status,
    is_terminal_exchange_order_status,
    should_replace_exchange_order_status,
)
from mctp.storage.balance_cache import BalanceCacheStore
from mctp.storage.order_store import OrderStore


@dataclass(frozen=True)
class BinanceCredentials:
    api_key: str
    api_secret: str


@dataclass
class BinanceSpotTestnetConfigV1:
    credentials: BinanceCredentials
    rest_base_url: str = BINANCE_SPOT_TESTNET_REST_BASE_URL
    account_snapshot_ttl_seconds: int = ACCOUNT_SNAPSHOT_TTL_SECONDS
    announcements_rss_url: str = BINANCE_ANNOUNCEMENTS_RSS_URL


class BinanceSpotRestClientV1:
    def __init__(self, config: BinanceSpotTestnetConfigV1) -> None:
        self._config = config

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        return await asyncio.to_thread(self._request_json_sync, method, path, params or {}, signed)

    def _request_json_sync(
        self,
        method: str,
        path: str,
        params: dict[str, Any],
        signed: bool,
    ) -> Any:
        request_params = dict(params)
        headers = {"X-MBX-APIKEY": self._config.credentials.api_key}
        if signed:
            request_params["timestamp"] = str(int(datetime.now(timezone.utc).timestamp() * 1000))
            query = urlencode(request_params)
            signature = hmac.new(
                self._config.credentials.api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            request_params["signature"] = signature
        query_string = urlencode(request_params)
        url = f"{self._config.rest_base_url}{path}"
        body = None
        if method.upper() == "GET":
            if query_string:
                url = f"{url}?{query_string}"
        else:
            body = query_string.encode("utf-8")
        request = Request(url, data=body, method=method.upper(), headers=headers)
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))


@dataclass(frozen=True)
class AccountSnapshotRecord:
    balances: dict[str, Decimal]
    locked_balances: dict[str, Decimal]
    fetched_at: datetime
    reason: str


@dataclass(frozen=True)
class ExchangeOrderSnapshot:
    client_order_id: str
    status: str
    side: Side
    executed_qty: Decimal
    cumulative_quote_qty: Decimal
    price: Decimal
    updated_at: datetime


class BinanceSpotTestnetAdapterV1(ExecutionInterface):
    def __init__(
        self,
        config: BinanceSpotTestnetConfigV1,
        rest_client: Optional[BinanceSpotRestClientV1] = None,
        balance_cache_store: Optional[BalanceCacheStore] = None,
        order_store: Optional[OrderStore] = None,
    ) -> None:
        self._config = config
        self._rest_client = rest_client or BinanceSpotRestClientV1(config)
        self._balance_cache_store = balance_cache_store
        self._order_store = order_store
        self._fills_by_client_order_id: dict[str, list] = {}
        self._seen_fill_keys_by_client_order_id: dict[str, set[str]] = {}
        self._order_statuses: dict[str, str] = {}
        self._order_status_sources: dict[str, str] = {}
        self._order_status_updated_at: dict[str, datetime] = {}
        self._balances: dict[str, Decimal] = {}
        self._locked_balances: dict[str, Decimal] = {}
        self._last_account_snapshot: Optional[AccountSnapshotRecord] = None

    async def submit_order(self, order: Order) -> ExecutionResult:
        payload = {
            "symbol": self._exchange_symbol(order.symbol),
            "side": order.side.value,
            "type": order.order_type.value,
            "quantity": str(order.quantity),
            "newClientOrderId": order.client_order_id,
        }
        if order.price is not None:
            payload["price"] = str(order.price)
        if order.order_type == OrderType.LIMIT:
            payload["timeInForce"] = order.time_in_force.value
        response = await self._rest_client.request_json("POST", "/api/v3/order", params=payload, signed=True)
        status = str(response.get("status", EXCHANGE_ORDER_STATUS_REJECTED))
        self._update_order_status(order.client_order_id, status, source=EXCHANGE_STATUS_SOURCE_REST)
        if self._order_store is not None and self._is_active_order_status(status):
            self._order_store.save_order(order)
        elif self._order_store is not None:
            self._order_store.remove_order(order.client_order_id)
        self.prune_execution_state()
        return self._map_execution_status(status)

    async def cancel_order(self, client_order_id: str) -> ExecutionResult:
        response = await self._rest_client.request_json(
            "DELETE",
            "/api/v3/order",
            params={"origClientOrderId": client_order_id},
            signed=True,
        )
        status = str(response.get("status", EXCHANGE_ORDER_STATUS_CANCELED))
        self._update_order_status(client_order_id, status, source=EXCHANGE_STATUS_SOURCE_REST)
        if self._order_store is not None:
            self._order_store.remove_order(client_order_id)
        self.prune_execution_state()
        return self._map_execution_status(status)

    async def get_balances(self) -> dict[str, Decimal]:
        return dict(self._balances)

    async def get_instrument_info(self, symbol: Symbol) -> dict:
        response = await self._rest_client.request_json(
            "GET",
            "/api/v3/exchangeInfo",
            params={"symbol": self._exchange_symbol(symbol)},
            signed=False,
        )
        symbols = response.get("symbols", [])
        if not symbols:
            return {}
        symbol_info = symbols[0]
        filters = {item["filterType"]: item for item in symbol_info.get("filters", [])}
        lot_filter = filters.get("LOT_SIZE", {})
        min_notional_filter = filters.get("MIN_NOTIONAL", {})
        return {
            "lot_size": Decimal(str(lot_filter.get("stepSize", "0"))),
            "min_qty": Decimal(str(lot_filter.get("minQty", "0"))),
            "max_qty": Decimal(str(lot_filter.get("maxQty", "0"))),
            "min_notional": Decimal(str(min_notional_filter.get("minNotional", "0"))),
        }

    async def get_fills(self, client_order_id: str) -> list:
        return list(self._fills_by_client_order_id.get(client_order_id, []))

    async def get_order_status(self, client_order_id: str) -> Optional[str]:
        return self._order_statuses.get(client_order_id)

    async def get_open_oco_order_ids(self, symbol: Symbol) -> list[str]:
        response = await self._rest_client.request_json(
            "GET",
            "/api/v3/openOrderList",
            signed=True,
        )
        if not isinstance(response, list):
            return []
        exchange_symbol = self._exchange_symbol(symbol)
        order_list_ids: list[str] = []
        for item in response:
            if not isinstance(item, dict):
                continue
            item_symbol = item.get("symbol")
            if item_symbol is not None and str(item_symbol) != exchange_symbol:
                continue
            order_list_id = item.get("orderListId")
            if order_list_id is None:
                continue
            order_list_ids.append(str(order_list_id))
        return order_list_ids

    async def get_open_order_snapshots(self, symbol: Symbol) -> list[ExchangeOrderSnapshot]:
        response = await self._rest_client.request_json(
            "GET",
            "/api/v3/openOrders",
            params={"symbol": self._exchange_symbol(symbol)},
            signed=True,
        )
        if not isinstance(response, list):
            return []
        snapshots: list[ExchangeOrderSnapshot] = []
        for item in response:
            if not isinstance(item, dict):
                continue
            client_order_id = item.get("clientOrderId")
            side = item.get("side")
            if client_order_id is None or side is None:
                continue
            updated_at_ms = int(item.get("updateTime", 0) or 0)
            updated_at = (
                datetime.fromtimestamp(updated_at_ms / 1000, tz=timezone.utc)
                if updated_at_ms > 0
                else datetime.now(timezone.utc)
            )
            snapshots.append(
                ExchangeOrderSnapshot(
                    client_order_id=str(client_order_id),
                    status=str(item.get("status", "")),
                    side=Side(str(side)),
                    executed_qty=Decimal(str(item.get("executedQty", "0"))),
                    cumulative_quote_qty=Decimal(str(item.get("cummulativeQuoteQty", "0"))),
                    price=Decimal(str(item.get("price", "0"))),
                    updated_at=updated_at,
                )
            )
        return snapshots

    async def submit_oco(self, oco: OCOOrder) -> str:
        payload = {
            "symbol": self._exchange_symbol(oco.symbol),
            "side": Side.SELL.value,
            "quantity": str(oco.quantity),
            "aboveType": "LIMIT",
            "abovePrice": str(oco.tp_price),
            "belowType": "STOP_LOSS_LIMIT",
            "belowStopPrice": str(oco.sl_stop_price),
            "belowPrice": str(oco.sl_limit_price),
            "belowTimeInForce": "GTC",
        }
        response = await self._rest_client.request_json("POST", "/api/v3/orderList/oco", params=payload, signed=True)
        order_list_id = str(response.get("orderListId", oco.list_order_id))
        orders = response.get("orders", [])
        tp_client_order_id = oco.tp_client_order_id
        sl_client_order_id = oco.sl_client_order_id
        if isinstance(orders, list) and len(orders) >= 2:
            tp_client_order_id = str(orders[0].get("clientOrderId", tp_client_order_id or ""))
            sl_client_order_id = str(orders[1].get("clientOrderId", sl_client_order_id or ""))
        if self._order_store is not None:
            self._order_store.save_oco(
                OCOOrder(
                    symbol=oco.symbol,
                    tp_price=oco.tp_price,
                    sl_stop_price=oco.sl_stop_price,
                    sl_limit_price=oco.sl_limit_price,
                    quantity=oco.quantity,
                    tp_client_order_id=tp_client_order_id or None,
                    sl_client_order_id=sl_client_order_id or None,
                    list_order_id=order_list_id,
                    status=oco.status,
                    created_at=oco.created_at,
                    updated_at=oco.updated_at,
                    tp_fills=list(oco.tp_fills),
                    sl_fills=list(oco.sl_fills),
                )
            )
        return order_list_id

    async def cancel_oco(self, list_order_id: str) -> ExecutionResult:
        response = await self._rest_client.request_json(
            "DELETE",
            "/api/v3/orderList",
            params={"orderListId": list_order_id},
            signed=True,
        )
        list_status_type = str(response.get("listStatusType", EXCHANGE_LIST_STATUS_ALL_DONE))
        if self._order_store is not None and list_status_type == EXCHANGE_LIST_STATUS_ALL_DONE:
            self._order_store.remove_oco(list_order_id)
        return ExecutionResult.CANCELLED if list_status_type == EXCHANGE_LIST_STATUS_ALL_DONE else ExecutionResult.REJECTED

    def build_user_data_ws_subscribe_signature_request(
        self,
        request_id: str,
        *,
        recv_window: Optional[int] = None,
    ) -> dict[str, Any]:
        timestamp = str(int(datetime.now(timezone.utc).timestamp() * 1000))
        params = {
            "apiKey": self._config.credentials.api_key,
            "timestamp": timestamp,
        }
        if recv_window is not None:
            params["recvWindow"] = str(recv_window)
        query = urlencode(params)
        signature = hmac.new(
            self._config.credentials.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return {
            "id": request_id,
            "method": EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE_SIGNATURE,
            "params": {
                **params,
                "signature": signature,
            },
        }

    @staticmethod
    def build_user_data_ws_subscribe_request(request_id: str) -> dict[str, Any]:
        return {
            "id": request_id,
            "method": EXCHANGE_WS_API_METHOD_USER_DATA_SUBSCRIBE,
        }

    @staticmethod
    def build_user_data_ws_unsubscribe_request(request_id: str, subscription_id: Any) -> dict[str, Any]:
        return {
            "id": request_id,
            "method": EXCHANGE_WS_API_METHOD_USER_DATA_UNSUBSCRIBE,
            "params": {"subscriptionId": subscription_id},
        }

    @staticmethod
    def is_user_data_ws_success_response(payload: Any, request_id: str) -> bool:
        return (
            isinstance(payload, dict)
            and str(payload.get("id", "")) == request_id
            and int(payload.get("status", 0) or 0) == 200
        )

    @staticmethod
    def extract_user_data_ws_subscription_id(payload: Any) -> Optional[Any]:
        if not isinstance(payload, dict):
            return None
        result = payload.get("result")
        if isinstance(result, dict):
            if "subscriptionId" in result:
                return result["subscriptionId"]
            if "subscription_id" in result:
                return result["subscription_id"]
            if "id" in result:
                return result["id"]
        if result is not None:
            return result
        return payload.get("subscriptionId")

    @staticmethod
    def unwrap_user_data_ws_payload(payload: Any) -> Optional[dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        if "event" in payload and isinstance(payload["event"], dict):
            return payload["event"]
        if payload.get("e") is not None:
            return payload
        return None

    async def refresh_account_snapshot(self, reason: str) -> AccountSnapshotRecord:
        if reason not in {"startup", "reconnect", "ttl"}:
            raise ValueError("account snapshot refresh reason must be startup, reconnect, or ttl")
        response = await self._rest_client.request_json("GET", "/api/v3/account", signed=True)
        balances = {
            item["asset"]: Decimal(str(item["free"]))
            for item in response.get("balances", [])
        }
        locked_balances = {
            item["asset"]: Decimal(str(item.get("locked", "0")))
            for item in response.get("balances", [])
        }
        fetched_at = datetime.now(timezone.utc)
        self._balances = balances
        self._locked_balances = locked_balances
        record = AccountSnapshotRecord(
            balances=balances,
            locked_balances=locked_balances,
            fetched_at=fetched_at,
            reason=reason,
        )
        self._last_account_snapshot = record
        if self._balance_cache_store is not None:
            self._balance_cache_store.save(balances, fetched_at)
        return record

    async def refresh_account_snapshot_if_due(self) -> Optional[AccountSnapshotRecord]:
        if self._balance_cache_store is None:
            return None
        if self._balance_cache_store.is_stale(self._config.account_snapshot_ttl_seconds):
            return await self.refresh_account_snapshot("ttl")
        return None

    async def get_exchange_order_snapshot(self, symbol: Symbol, client_order_id: str) -> Optional[ExchangeOrderSnapshot]:
        response = await self._rest_client.request_json(
            "GET",
            "/api/v3/order",
            params={"symbol": self._exchange_symbol(symbol), "origClientOrderId": client_order_id},
            signed=True,
        )
        if not isinstance(response, dict) or not response or "status" not in response or "side" not in response:
            return None
        updated_at_ms = int(response.get("updateTime", 0) or 0)
        updated_at = (
            datetime.fromtimestamp(updated_at_ms / 1000, tz=timezone.utc)
            if updated_at_ms > 0
            else datetime.now(timezone.utc)
        )
        return ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            status=str(response.get("status", "")),
            side=Side(str(response.get("side", Side.SELL.value))),
            executed_qty=Decimal(str(response.get("executedQty", "0"))),
            cumulative_quote_qty=Decimal(str(response.get("cummulativeQuoteQty", "0"))),
            price=Decimal(str(response.get("price", "0"))),
            updated_at=updated_at,
        )

    async def get_exchange_fill_for_order(self, symbol: Symbol, client_order_id: str) -> Optional[Fill]:
        fills = await self.get_exchange_fills_for_order(symbol, client_order_id)
        if not fills:
            return None
        return fills[-1]

    async def get_exchange_fills_for_order(self, symbol: Symbol, client_order_id: str) -> list[Fill]:
        order_snapshot = await self.get_exchange_order_snapshot(symbol, client_order_id)
        if order_snapshot is None or order_snapshot.executed_qty <= Decimal("0"):
            return []
        trades = await self._rest_client.request_json(
            "GET",
            "/api/v3/myTrades",
            params={"symbol": self._exchange_symbol(symbol), "origClientOrderId": client_order_id},
            signed=True,
        )
        if not isinstance(trades, list) or not trades:
            return []
        sorted_trades = sorted(
            (trade for trade in trades if isinstance(trade, dict)),
            key=lambda trade: (
                int(trade.get("time", 0) or 0),
                str(trade.get("id", "")),
            ),
        )
        fills: list[Fill] = []
        for trade in sorted_trades:
            trade_time_ms = int(trade.get("time", 0) or 0)
            filled_at = (
                datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc)
                if trade_time_ms > 0
                else order_snapshot.updated_at
            )
            fills.append(
                Fill(
                    order_id=client_order_id,
                    symbol=symbol,
                    side=order_snapshot.side,
                    base_qty_filled=Decimal(str(trade.get("qty", "0"))),
                    quote_qty_filled=Decimal(str(trade.get("quoteQty", "0"))),
                    fill_price=Decimal(str(trade.get("price", order_snapshot.price))),
                    commission=Decimal(str(trade.get("commission", "0"))),
                    commission_asset=CommissionAsset(str(trade.get("commissionAsset", CommissionAsset.QUOTE.value))),
                    trade_id=str(trade.get("id", client_order_id)),
                    filled_at=filled_at,
                )
            )
        return fills

    async def cancel_order_with_code(self, client_order_id: str, cancel_code: int = T_CANCEL) -> ExecutionResult:
        _ = cancel_code
        return await self.cancel_order(client_order_id)

    def load_local_active_ocos(self) -> dict[str, OCOOrder]:
        if self._order_store is None:
            return {}
        _, ocos = self._order_store.load()
        return ocos

    def load_local_active_orders(self) -> dict[str, Order]:
        if self._order_store is None:
            return {}
        orders, _ = self._order_store.load()
        return orders

    def remove_local_order(self, client_order_id: str) -> None:
        if self._order_store is not None:
            self._order_store.remove_order(client_order_id)

    def remove_local_oco(self, list_order_id: str) -> None:
        if self._order_store is not None:
            self._order_store.remove_oco(list_order_id)

    def get_cached_balance_state(self) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        return dict(self._balances), dict(self._locked_balances)

    def remember_exchange_fills(self, client_order_id: str, fills: list[Fill]) -> None:
        if not fills:
            return
        for fill in fills:
            self._remember_fill(client_order_id, fill)

    def handle_user_data_event(self, event: ExecutionReportEvent | OutboundAccountPositionEvent) -> Optional[Fill]:
        if isinstance(event, OutboundAccountPositionEvent):
            self._merge_account_position_event(event)
            if self._balance_cache_store is not None:
                self._balance_cache_store.save(dict(self._balances), event.timestamp)
            return None
        self._update_order_status(
            event.client_order_id,
            event.order_status,
            source=EXCHANGE_STATUS_SOURCE_WEBSOCKET,
            observed_at=event.timestamp,
        )
        if self._order_store is not None:
            if self._is_active_order_status(event.order_status):
                existing_orders, _ = self._order_store.load()
                order = existing_orders.get(event.client_order_id)
                if order is not None:
                    self._order_store.save_order(order)
            else:
                self._order_store.remove_order(event.client_order_id)
        accepted_fill = None
        if event.fill is not None:
            accepted_fill = self._remember_fill(event.client_order_id, event.fill)
        self.prune_execution_state(now=event.timestamp)
        return accepted_fill

    def prune_execution_state(self, now: Optional[datetime] = None) -> None:
        observed_at = now or datetime.now(timezone.utc)
        active_orders: set[str] = set()
        if self._order_store is not None:
            stored_orders, _ = self._order_store.load()
            active_orders = set(stored_orders)
        removable: list[str] = []
        terminal_items: list[tuple[datetime, str]] = []
        for client_order_id, status in self._order_statuses.items():
            if client_order_id in active_orders:
                continue
            if not self._is_terminal_order_status(status):
                continue
            updated_at = self._order_status_updated_at.get(client_order_id, observed_at)
            if (observed_at - updated_at).total_seconds() >= EXECUTION_STATE_RETENTION_SECONDS:
                removable.append(client_order_id)
            else:
                terminal_items.append((updated_at, client_order_id))
        if len(terminal_items) > MAX_EXECUTION_STATE_TERMINAL_ENTRIES:
            overflow = len(terminal_items) - MAX_EXECUTION_STATE_TERMINAL_ENTRIES
            terminal_items.sort(key=lambda item: item[0])
            removable.extend(client_order_id for _, client_order_id in terminal_items[:overflow])
        for client_order_id in dict.fromkeys(removable):
            self._order_statuses.pop(client_order_id, None)
            self._order_status_sources.pop(client_order_id, None)
            self._order_status_updated_at.pop(client_order_id, None)
            self._fills_by_client_order_id.pop(client_order_id, None)
            self._seen_fill_keys_by_client_order_id.pop(client_order_id, None)

    @staticmethod
    def _map_execution_status(status: str) -> ExecutionResult:
        return execution_result_from_exchange_status(status)

    @staticmethod
    def _exchange_symbol(symbol: Symbol) -> str:
        return symbol.to_exchange_str()

    @staticmethod
    def _is_active_order_status(status: str) -> bool:
        return is_active_exchange_order_status(status)

    @staticmethod
    def _is_terminal_order_status(status: str) -> bool:
        return is_terminal_exchange_order_status(status)

    @staticmethod
    def _order_status_rank(status: str) -> int:
        return EXCHANGE_ORDER_STATUS_RANKS.get(status, 0)

    def _update_order_status(
        self,
        client_order_id: str,
        status: str,
        *,
        source: str,
        observed_at: Optional[datetime] = None,
    ) -> None:
        current = self._order_statuses.get(client_order_id)
        current_source = self._order_status_sources.get(client_order_id)
        if current is not None and not self._should_replace_order_status(current, current_source, status, source):
            return
        self._order_statuses[client_order_id] = status
        self._order_status_sources[client_order_id] = source
        self._order_status_updated_at[client_order_id] = observed_at or datetime.now(timezone.utc)

    def _should_replace_order_status(
        self,
        current: str,
        current_source: Optional[str],
        incoming: str,
        incoming_source: str,
    ) -> bool:
        return should_replace_exchange_order_status(current, current_source, incoming, incoming_source)

    def _merge_account_position_event(self, event: OutboundAccountPositionEvent) -> None:
        merged_balances = dict(self._balances)
        merged_locked_balances = dict(self._locked_balances)
        for asset, balance in event.balances.items():
            merged_balances[asset] = balance
        for asset, locked_balance in event.locked_balances.items():
            merged_locked_balances[asset] = locked_balance
        self._balances = merged_balances
        self._locked_balances = merged_locked_balances

    def _remember_fill(self, client_order_id: str, fill: Fill) -> Optional[Fill]:
        fill_key = self._fill_key(fill)
        seen = self._seen_fill_keys_by_client_order_id.setdefault(client_order_id, set())
        if fill_key in seen:
            return None
        seen.add(fill_key)
        self._fills_by_client_order_id.setdefault(client_order_id, []).append(fill)
        return fill

    @staticmethod
    def _fill_key(fill: Fill) -> str:
        return "|".join(
            (
                fill.trade_id,
                fill.filled_at.isoformat(),
                fill.side.value,
                str(fill.base_qty_filled),
                str(fill.quote_qty_filled),
                str(fill.fill_price),
                str(fill.commission),
                fill.commission_asset.value,
            )
        )


class BinanceDelistingDetectorV1:
    def __init__(
        self,
        config: BinanceSpotTestnetConfigV1,
        rest_client: Optional[BinanceSpotRestClientV1] = None,
        rss_fetcher: Optional[Any] = None,
    ) -> None:
        self._config = config
        self._rest_client = rest_client or BinanceSpotRestClientV1(config)
        self._rss_fetcher = rss_fetcher or self._fetch_rss_sync

    async def check_symbol(self, symbol: Symbol) -> DelistingSignalEvent:
        exchange_info = await self._rest_client.request_json(
            "GET",
            "/api/v3/exchangeInfo",
            params={"symbol": symbol.to_exchange_str()},
            signed=False,
        )
        listed = True
        details = "symbol listed"
        symbols = exchange_info.get("symbols", [])
        if not symbols or symbols[0].get("status") != "TRADING":
            listed = False
            details = "exchangeInfo status is not TRADING"
        rss_text = await asyncio.to_thread(self._rss_fetcher)
        if symbol.to_exchange_str() in rss_text and "delist" in rss_text.lower():
            listed = False
            details = "announcement feed mentions delisting"
        return DelistingSignalEvent(
            symbol=symbol,
            listed=listed,
            source="binance_testnet_v1",
            details=details,
            days_until_delisting=None,
        )

    def _fetch_rss_sync(self) -> str:
        request = Request(self._config.announcements_rss_url, method="GET")
        with urlopen(request) as response:
            payload = response.read().decode("utf-8")
        root = ET.fromstring(payload)
        return "".join(item.text or "" for item in root.iter())
