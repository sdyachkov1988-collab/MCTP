# MCTP — Modular Crypto Trading Platform

## Текущая подтверждённая стадия
- подтверждённая стадия репозитория: `v1.7 + v2.0-step1`
- `v2.0-step1` принят: `BtcUsdtMtfV20Strategy` + MTF агрегатор реализованы и протестированы
- 458 тестов зелёные
- следующий рабочий коридор: v2.0 testnet wiring — подключение реальной стратегии к testnet runtime

## Подтверждённые стадии
- `v0.0`-`v1.7` полностью
- `v2.0-step1`: стратегия + MTF агрегатор + backtest/paper wiring

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

## Известные проблемы которые нужно исправить (в порядке приоритета)

### CRITICAL
1. `run_testnet_platform.py` — использует EmaCrossSmokeStrategy вместо BtcUsdtMtfV20Strategy
2. `mctp/portfolio/tracker.py` — `_persist_snapshot()` без try/catch и алерта
3. `mctp/runtime/events.py` — raw exchange статусы протекают за пределы adapter boundary

### MAJOR
4. `mctp/strategy/mtf.py` — нет warning при молчаливом отбрасывании bucket при gap в данных
5. `mctp/backtest/config.py:22` — `fee_rate` не из `constants.py`
6. `mctp/storage/order_store.py` и `balance_cache.py` — нет проверки schema_version
7. `docs/v1_7_to_v2_0_readiness_gate.md` — устарел, нужно обновить

### MEDIUM
8. `mctp/execution/paper.py:123` — `float(T_CANCEL)` отклонение от Decimal дисциплины
9. `mctp/indicators/engine.py` — magic number для CCI не из constants.py
10. Warmup 19200 свечей (200 дней M15) нигде не задокументирован

## Следующая задача для агента
Исправить три CRITICAL проблемы выше.
Затем: подключить `BtcUsdtMtfV20Strategy` к testnet runtime.

Порядок работы:
1. Fix `_persist_snapshot()` — try/catch + alert dispatch
2. Fix `events.py` — убрать raw exchange поля за boundary
3. Update `run_testnet_platform.py` — заменить smoke на v2.0 стратегию
4. Прогнать полный тест-сьют — должно быть 458+ зелёных
5. Провести аудит изменений

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
