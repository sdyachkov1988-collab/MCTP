# MCTP — Modular Crypto Trading Platform

## Текущая подтверждённая стадия
- подтверждённая стадия репозитория: `v2.0-step2-fix` (accepted baseline)
- 503 теста зелёные
- текущий рабочий фокус: freeze/acceptance baseline зафиксирован; новый corridor ещё не зафиксирован

## Подтверждённые стадии
- `v0.0`-`v1.7` полностью
- `v2.0-step1`: стратегия + MTF агрегатор + backtest/paper wiring
- `v2.0-patch1`: три CRITICAL фикса — run_testnet_platform использует BtcUsdtMtfV20Strategy, _persist_snapshot() защищён, boundary leakage устранён, ExchangeOrderStatus/ListOrderStatus enums добавлены
- `v2.0-step2`: testnet wiring — LiveMtfAggregator, MtfKlineManager, 4 независимых kline канала M15/H1/H4/D1, REST priming, startup gate
- `v2.0-step2-fix`: 5 audit fixes поверх `v2.0-step2`; текущий tag/HEAD
- принятый `v2.0 backtest wiring`: `--strategy` в `run_backtest_csv.py`, backward-compatible legacy default, согласованный protective OCO flow в `_run_v20_btcusdt_mtf`

## Архитектурные инварианты (нарушение недопустимо)
- все финансовые значения — только `Decimal`
- все временные метки — только UTC-aware `datetime`
- `Symbol` — только типизированный объект, не plain string
- весь async-код — через `asyncio`
- константы — только из `mctp/core/constants.py`
- strategy layer остаётся read-only
- backtest использует только закрытые свечи
- exchange-specific логика остаётся внутри adapter/runtime boundary
- paper mode и testnet mode остаются разделёнными

## Что реально реализовано
- core domain models, risk/sizing, portfolio/accounting, order lifecycle, OCO/software-stop
- deterministic backtest, analytics, CSV backtest input, trade export
- paper runtime
- Binance Spot TESTNET adapter v1
- startup sync / restart reconciliation through `v1.3`
- observability through `v1.4`
- alerting through `v1.5`
- `v1.6` safety controls и последующий reliability hardening
- `v1.7` scenario matrix, chaos/integration, operator artifacts, transition gate
- `v2.0-step1` BtcUsdtMtfV20Strategy, MTF агрегатор, backtest/paper wiring
- `v2.0-patch1` три CRITICAL фикса (boundary leakage, persist_snapshot, run_testnet_platform)
- `v2.0-step2` testnet wiring (LiveMtfAggregator, MtfKlineManager, REST priming, startup gate)

## Известные проблемы которые нужно исправить (в порядке приоритета)

Текущий подтверждённый stabilization batch закрыт:
- `schema_version` добавлен в `BalanceCacheStore` и `OrderStore`
- M15 gap / dropped bucket path в `mctp/strategy/mtf.py` теперь пишет warning
- `Timeframe.MONTHLY` добавлен
- `float(T_CANCEL)` убран
- EMA использует явный SMA seed
- CCI scaling constant вынесен в `mctp/core/constants.py`

Новый feature corridor после freeze всё ещё не зафиксирован.

## Следующая задача для агента
Acceptance/freeze завершён для baseline `v2.0-step2-fix`. Следующий feature corridor ещё не зафиксирован.

## Anti-scope правила
- не добавлять multi-pair scope
- не добавлять futures
- не добавлять regime/anomaly/on-chain/ML
- не добавлять allocation engine
- не переписывать архитектуру без прямой необходимости
- не трогать принятые v1.7 operator artifacts без явного указания

## Рабочий процесс
- `audit -> minimal patch -> tests -> audit -> context sync`
- после каждого изменения обновить MCTP_context.md и AGENTS.md
- коммитить только зелёные тесты
