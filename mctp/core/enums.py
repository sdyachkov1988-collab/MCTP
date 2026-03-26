from enum import Enum, StrEnum


class Market(Enum):
    SPOT = "SPOT"
    FUTURES = "FUTURES"


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LOSS_LIMIT = "STOP_LOSS_LIMIT"


class Timeframe(Enum):
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"


class TimeInForce(Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"


class QuantitySource(Enum):
    BASE = "BASE"
    QUOTE = "QUOTE"


class IntentType(Enum):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class QuantityMode(Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"
    QUOTE = "QUOTE"   # quote-denominated quantity; forbidden for SELL (Guard 5)


class ExecutionResult(Enum):
    ACCEPTED = "ACCEPTED"
    FILLED = "FILLED"
    PARTIAL_FILL = "PARTIAL_FILL"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class CommissionAsset(Enum):
    BNB = "BNB"
    BASE = "BASE"
    QUOTE = "QUOTE"


class OperationalMode(Enum):
    RUN = "RUN"
    PAUSE_NEW_ENTRIES = "PAUSE_NEW_ENTRIES"
    CLOSE_ONLY = "CLOSE_ONLY"
    STOP = "STOP"


class RejectionReason(Enum):
    ALREADY_IN_POSITION = "already_in_position"
    NO_POSITION_TO_SELL = "no_position_to_sell"
    INSUFFICIENT_QUOTE = "insufficient_quote"
    BNB_SYMBOL_FORBIDDEN = "bnb_symbol_forbidden"
    QUOTE_QTY_SELL_FORBIDDEN = "quote_qty_sell_forbidden"
    MAX_SCALE_IN_REACHED = "max_scale_in_reached"
    RESTORE_BELOW_MIN_NOTIONAL = "restore_below_min_notional"
    OCO_PRICE_INVALID = "oco_price_invalid"
    SOFTWARE_STOP_CONFLICT = "software_stop_conflict"


class RecoveryMode(Enum):
    NORMAL = "NORMAL"
    LIGHT = "LIGHT"
    MODERATE = "MODERATE"
    TERMINAL = "TERMINAL"


class BookTickerStatus(Enum):
    CURRENT = "CURRENT"
    STALE = "STALE"


class ApiMode(Enum):
    REST_WEBSOCKET = "REST_WEBSOCKET"
    WEBSOCKET_API = "WEBSOCKET_API"


class ProtectionMode(Enum):
    NONE = "NONE"
    EXCHANGE_OCO = "EXCHANGE_OCO"
    SOFTWARE_STOP = "SOFTWARE_STOP"


class AlertSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class SymbolChangeStage(Enum):
    IDLE = "IDLE"
    AWAITING_ZERO = "AWAITING_ZERO"
    AWAITING_CONFIG_UPDATE = "AWAITING_CONFIG_UPDATE"
    RESTART_REQUIRED = "RESTART_REQUIRED"


class BasisRecoveryState(Enum):
    NONE = "NONE"
    MISSING = "MISSING"
    MANUAL = "MANUAL"
    ZERO_DECLARED = "ZERO_DECLARED"
    CLOSE_PENDING = "CLOSE_PENDING"


class ExchangeOrderStatus(StrEnum):
    """Internal typed representation of exchange order status.

    Uses StrEnum so that comparisons with raw string constants
    in existing code remain backward-compatible.
    """
    NEW = "NEW"
    PENDING_NEW = "PENDING_NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class ListStatusType(StrEnum):
    """Internal typed representation of OCO list status type."""
    RESPONSE = "RESPONSE"
    EXEC_STARTED = "EXEC_STARTED"
    ALL_DONE = "ALL_DONE"


class ListOrderStatus(StrEnum):
    """Internal typed representation of OCO list order status."""
    EXECUTING = "EXECUTING"
    ALL_DONE = "ALL_DONE"
    REJECT = "REJECT"


class ContingencyType(StrEnum):
    """Internal typed representation of OCO contingency type."""
    OCO = "OCO"
