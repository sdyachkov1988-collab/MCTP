import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Protocol

from mctp.core.enums import AlertSeverity
from mctp.runtime.observability import to_jsonable


@dataclass(frozen=True)
class AlertRecord:
    timestamp: datetime
    severity: AlertSeverity
    code: str
    message: str
    symbol: str
    context: dict[str, Any] = field(default_factory=dict)
    delivered_via: tuple[str, ...] = ()


class AlertChannel(Protocol):
    name: str

    def deliver(self, alert: AlertRecord) -> None:
        ...


class JsonFileAlertChannel:
    def __init__(self, path: str, name: str) -> None:
        self._path = Path(path)
        self.name = name

    def deliver(self, alert: AlertRecord) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = to_jsonable(
            {
                "timestamp": alert.timestamp,
                "severity": alert.severity,
                "code": alert.code,
                "message": alert.message,
                "symbol": alert.symbol,
                "context": alert.context,
                "delivered_via": list(alert.delivered_via),
            }
        )
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


class MemoryAlertChannel:
    def __init__(self, name: str) -> None:
        self.name = name
        self.records: list[AlertRecord] = []

    def deliver(self, alert: AlertRecord) -> None:
        self.records.append(alert)


class AlertDispatcher:
    def __init__(self, primary: AlertChannel, backup: AlertChannel) -> None:
        self.primary = primary
        self.backup = backup
        self.delivered: list[AlertRecord] = []

    def dispatch(
        self,
        timestamp: datetime,
        severity: AlertSeverity,
        code: str,
        message: str,
        symbol: str,
        context: Optional[dict[str, Any]] = None,
    ) -> AlertRecord:
        context = context or {}
        channels: list[str] = []
        last_error: Optional[Exception] = None
        base_alert = AlertRecord(
            timestamp=timestamp,
            severity=severity,
            code=code,
            message=message,
            symbol=symbol,
            context=context,
            delivered_via=(),
        )
        try:
            primary_alert = AlertRecord(**{**base_alert.__dict__, "delivered_via": (self.primary.name,)})
            self.primary.deliver(primary_alert)
            channels.append(self.primary.name)
            alert = primary_alert
        except Exception as exc:
            last_error = exc
            backup_alert = AlertRecord(**{**base_alert.__dict__, "delivered_via": (self.backup.name,)})
            self.backup.deliver(backup_alert)
            channels.append(self.backup.name)
            alert = backup_alert
        if not channels and last_error is not None:
            raise last_error
        self.delivered.append(alert)
        return alert
