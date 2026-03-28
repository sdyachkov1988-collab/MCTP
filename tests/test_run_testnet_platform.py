import pytest

import run_testnet_platform
from mctp.core.constants import (
    TESTNET_ENABLE_DEFAULT_FILE_LOGS_ENV,
    TESTNET_SMOKE_GUARD_ENV,
    TESTNET_STRUCTURED_LOG_PATH_ENV,
)


def test_run_testnet_platform_requires_explicit_smoke_guard(monkeypatch):
    monkeypatch.delenv(TESTNET_SMOKE_GUARD_ENV, raising=False)

    with pytest.raises(SystemExit) as exc_info:
        run_testnet_platform._require_smoke_guard()

    message = str(exc_info.value)
    assert "smoke-only" in message
    assert TESTNET_SMOKE_GUARD_ENV in message


def test_run_testnet_platform_smoke_guard_allows_explicit_execution(monkeypatch):
    monkeypatch.setenv(TESTNET_SMOKE_GUARD_ENV, "1")
    run_testnet_platform._require_smoke_guard()


@pytest.mark.asyncio
async def test_run_testnet_platform_main_advances_without_legacy_listen_key_stage(monkeypatch):
    lifecycle: list[str] = []

    class FakeRuntime:
        def __init__(self, **kwargs):
            lifecycle.append("runtime_init")

        async def start(self):
            lifecycle.append("start")

        async def ping_all(self):
            lifecycle.append("ping_all")

        async def shutdown(self):
            lifecycle.append("shutdown")

    class FakeAdapter:
        def __init__(self, *args, **kwargs):
            lifecycle.append("adapter_init")

    monkeypatch.setenv(TESTNET_SMOKE_GUARD_ENV, "1")
    monkeypatch.setenv("BINANCE_TESTNET_API_KEY", "k")
    monkeypatch.setenv("BINANCE_TESTNET_API_SECRET", "s")
    monkeypatch.setattr(run_testnet_platform, "BinanceSpotTestnetAdapterV1", FakeAdapter)
    monkeypatch.setattr(run_testnet_platform, "TestnetRuntime", FakeRuntime)

    await run_testnet_platform.main()

    assert lifecycle == ["adapter_init", "runtime_init", "start", "ping_all", "shutdown"]


def test_testnet_optional_file_logging_paths_are_disabled_by_default(monkeypatch):
    monkeypatch.delenv(TESTNET_ENABLE_DEFAULT_FILE_LOGS_ENV, raising=False)
    monkeypatch.delenv(TESTNET_STRUCTURED_LOG_PATH_ENV, raising=False)
    paths = run_testnet_platform._optional_testnet_file_logging_paths()
    assert paths == {
        "structured_log_path": None,
        "audit_log_path": None,
        "primary_alert_path": None,
        "backup_alert_path": None,
    }


def test_testnet_optional_file_logging_paths_use_defaults_when_enabled(monkeypatch):
    monkeypatch.setenv(TESTNET_ENABLE_DEFAULT_FILE_LOGS_ENV, "1")
    paths = run_testnet_platform._optional_testnet_file_logging_paths()
    assert paths["structured_log_path"] == "testnet_structured.log.jsonl"
    assert paths["audit_log_path"] == "testnet_audit.log.jsonl"
    assert paths["primary_alert_path"] == "testnet_alerts_primary.log.jsonl"
    assert paths["backup_alert_path"] == "testnet_alerts_backup.log.jsonl"
