import hashlib
import json
import tracemalloc
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from mctp.core.constants import SPM_CONSECUTIVE_STOP, SPM_PF_WARNING, SPM_WINRATE_WARNING, SPM_WINRATE_WINDOW


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def to_jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("observability timestamps must be UTC-aware")
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return to_jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


class StructuredJsonLogger:
    def __init__(self, path: Optional[str] = None) -> None:
        self._path = Path(path) if path is not None else None
        self.records: list[dict[str, Any]] = []

    def emit(self, record: dict[str, Any]) -> dict[str, Any]:
        if "timestamp" not in record:
            raise ValueError("structured log record requires timestamp")
        jsonable = to_jsonable(record)
        assert isinstance(jsonable, dict)
        self.records.append(jsonable)
        if self._path is not None:
            _ensure_parent(self._path)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(jsonable, ensure_ascii=False, sort_keys=True) + "\n")
        return jsonable


class HashChainAuditLogger:
    def __init__(self, path: Optional[str] = None) -> None:
        self._path = Path(path) if path is not None else None
        self.records: list[dict[str, Any]] = []

    def append(self, payload: dict[str, Any]) -> dict[str, Any]:
        jsonable_payload = to_jsonable(payload)
        assert isinstance(jsonable_payload, dict)
        prev_hash = self.records[-1]["record_hash"] if self.records else "GENESIS"
        content = {"prev_hash": prev_hash, "payload": jsonable_payload}
        record_hash = hashlib.sha256(
            json.dumps(content, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        record = {
            "timestamp": jsonable_payload["timestamp"],
            "prev_hash": prev_hash,
            "record_hash": record_hash,
            "payload": jsonable_payload,
        }
        self.records.append(record)
        if self._path is not None:
            _ensure_parent(self._path)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        return record

    def verify_chain(self) -> bool:
        prev_hash = "GENESIS"
        for record in self.records:
            content = {"prev_hash": prev_hash, "payload": record["payload"]}
            expected = hashlib.sha256(
                json.dumps(content, ensure_ascii=False, sort_keys=True).encode("utf-8")
            ).hexdigest()
            if record["prev_hash"] != prev_hash:
                return False
            if record["record_hash"] != expected:
                return False
            prev_hash = record["record_hash"]
        return True


@dataclass
class StrategyPerformanceSnapshot:
    mode: str
    smoke_only: bool
    observed_trade_count: int
    warning: bool
    stop_strategy: bool
    win_rate: Optional[Decimal]
    profit_factor: Optional[Decimal]
    consecutive_losses: int
    warning_reasons: list[str]


class StrategyPerformanceMonitor:
    def __init__(self, mode: str = "testnet_smoke") -> None:
        self.mode = mode
        self.smoke_only = mode == "testnet_smoke"
        self._trade_results: list[Decimal] = []

    def observe_trade(self, net_pnl_quote: Decimal) -> None:
        self._trade_results.append(net_pnl_quote)

    def snapshot(self) -> StrategyPerformanceSnapshot:
        warning_reasons: list[str] = []
        stop_strategy = self._consecutive_losses() >= SPM_CONSECUTIVE_STOP
        win_rate = self._win_rate()
        profit_factor = self._profit_factor()
        if win_rate is not None and len(self._trade_results) >= SPM_WINRATE_WINDOW and win_rate < SPM_WINRATE_WARNING:
            warning_reasons.append("win_rate")
        if profit_factor is not None and profit_factor < SPM_PF_WARNING:
            warning_reasons.append("profit_factor")
        return StrategyPerformanceSnapshot(
            mode=self.mode,
            smoke_only=self.smoke_only,
            observed_trade_count=len(self._trade_results),
            warning=bool(warning_reasons),
            stop_strategy=stop_strategy,
            win_rate=win_rate,
            profit_factor=profit_factor,
            consecutive_losses=self._consecutive_losses(),
            warning_reasons=warning_reasons,
        )

    def _win_rate(self) -> Optional[Decimal]:
        if not self._trade_results:
            return None
        window = self._trade_results[-SPM_WINRATE_WINDOW:]
        wins = sum(1 for value in window if value > Decimal("0"))
        return Decimal(wins) / Decimal(len(window))

    def _profit_factor(self) -> Optional[Decimal]:
        if not self._trade_results:
            return None
        gross_profit = sum((value for value in self._trade_results if value > Decimal("0")), Decimal("0"))
        gross_loss = sum((-value for value in self._trade_results if value < Decimal("0")), Decimal("0"))
        if gross_loss == Decimal("0"):
            return None
        return gross_profit / gross_loss

    def _consecutive_losses(self) -> int:
        count = 0
        for result in reversed(self._trade_results):
            if result >= Decimal("0"):
                break
            count += 1
        return count


class ObservabilityHub:
    def __init__(
        self,
        *,
        structured_log_path: Optional[str] = None,
        audit_log_path: Optional[str] = None,
        performance_monitor: Optional[StrategyPerformanceMonitor] = None,
    ) -> None:
        self.structured_logger = StructuredJsonLogger(structured_log_path)
        self.audit_logger = HashChainAuditLogger(audit_log_path)
        self.performance_monitor = performance_monitor or StrategyPerformanceMonitor()
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        self.heartbeat_count = 0
        self.last_latency_us: dict[str, int] = {}
        self.last_memory_snapshot: dict[str, int] = {"current_bytes": 0, "peak_bytes": 0}

    def emit(self, record: dict[str, Any], *, audit: bool = False) -> None:
        emitted = self.structured_logger.emit(record)
        if audit:
            self.audit_logger.append(emitted)

    def record_heartbeat(self, timestamp: datetime, symbol: str, status: str, stale_flags: dict[str, bool]) -> None:
        self.heartbeat_count += 1
        self.emit(
            {
                "timestamp": timestamp,
                "event_type": "heartbeat",
                "symbol": symbol,
                "intent": None,
                "risk_result": None,
                "sizer_result": None,
                "execution_result": None,
                "status": status,
                "stale_flags": stale_flags,
                "heartbeat_count": self.heartbeat_count,
            }
        )

    def record_latency(self, timestamp: datetime, symbol: str, metric: str, latency_us: int) -> None:
        self.last_latency_us[metric] = latency_us
        self.emit(
            {
                "timestamp": timestamp,
                "event_type": "latency_metric",
                "symbol": symbol,
                "intent": None,
                "risk_result": None,
                "sizer_result": None,
                "execution_result": None,
                "metric": metric,
                "latency_us": latency_us,
            }
        )

    def record_memory(self, timestamp: datetime, symbol: str) -> None:
        current_bytes, peak_bytes = tracemalloc.get_traced_memory()
        self.last_memory_snapshot = {"current_bytes": current_bytes, "peak_bytes": peak_bytes}
        self.emit(
            {
                "timestamp": timestamp,
                "event_type": "memory_metric",
                "symbol": symbol,
                "intent": None,
                "risk_result": None,
                "sizer_result": None,
                "execution_result": None,
                "current_bytes": current_bytes,
                "peak_bytes": peak_bytes,
            }
        )
