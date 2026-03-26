# MCTP — Modular Crypto Trading Platform

## Текущая подтверждённая стадия
- подтверждённая стадия репозитория: `v2.0-step2-fix` (pending acceptance)
- 478 тестов зелёные
- следующий рабочий коридор: v2.0 backtest wiring — OCO в `_run_v20_btcusdt_mtf`, `--strategy` флаг в `run_backtest_csv.py`

## Подтверждённые стадии
- `v0.0`-`v1.7` полностью
- `v2.0-step1`: стратегия + MTF агрегатор + backtest/paper wiring
- `v2.0-patch1`: три CRITICAL фикса — run_testnet_platform использует BtcUsdtMtfV20Strategy, _persist_snapshot() защищён, boundary leakage устранён, ExchangeOrderStatus/ListOrderStatus enums добавлены
- `v2.0-step2`: testnet wiring — LiveMtfAggregator, MtfKlineManager, 4 независимых kline канала M15/H1/H4/D1, REST priming, startup gate

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

### MAJOR
4. `mctp/strategy/mtf.py` — нет warning при молчаливом отбрасывании bucket при gap в данных
5. `mctp/backtest/config.py:22` — `fee_rate` не из `constants.py`
6. `mctp/storage/order_store.py` и `balance_cache.py` — нет проверки schema_version
7. `mctp/core/enums.py` — нет `Timeframe.MONTHLY`
8. `mctp/indicators/engine.py` — cold-start EMA без seed

### MEDIUM
10. `mctp/execution/paper.py:123` — `float(T_CANCEL)` отклонение от Decimal дисциплины
11. `mctp/indicators/engine.py` — magic number для CCI не из constants.py

## Следующая задача для агента
v2.0 backtest wiring:
1. OCO wiring в `_run_v20_btcusdt_mtf` flow
2. `--strategy` флаг в `run_backtest_csv.py`
3. Прогнать полный тест-сьют
4. Провести аудит изменений

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
