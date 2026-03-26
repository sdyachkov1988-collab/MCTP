# MCTP — Modular Crypto Trading Platform

## Текущая подтвержденная стадия
- подтвержденная стадия репозитория: `v1.7`
- разрешенный рабочий коридор сейчас включает принятые post-`v1.6` hardening steps, `v1.7` verification/artifact work и узкий transition-gate polish перед `v2.0`
- работа вне прямого задания не должна добавлять `v2.0+` scope

## Подтвержденные стадии
- `v0.0`-`v1.7`
- принятые verification steps внутри `v1.7`: scenario matrix, chaos/integration, independent WS stream verification, operator-readiness artifacts

## Архитектурные инварианты
- все финансовые значения — только `Decimal`
- все временные метки — только UTC-aware `datetime`
- `Symbol` — только типизированный объект, не plain string
- весь async-код — через `asyncio`
- константы — только из `mctp/core/constants.py`
- strategy layer остается read-only
- backtest использует только закрытые свечи
- exchange-specific логика остается внутри adapter/runtime boundary
- paper mode и testnet mode остаются разделенными

## Что реально реализовано
- core domain models, risk/sizing, portfolio/accounting, order lifecycle, OCO/software-stop
- deterministic backtest, analytics, CSV backtest input, trade export
- paper runtime
- Binance Spot TESTNET adapter v1
- startup sync / restart reconciliation through `v1.3`
- observability through `v1.4`
- alerting through `v1.5`
- `v1.6` safety controls и последующий reliability hardening

## Anti-scope правила
- не добавлять `v2.0+` live-trading / automation scope
- не добавлять production live trading readiness
- не добавлять multi-pair, futures, regime/anomaly/research features
- не переписывать архитектуру без прямой необходимости текущей задачи

## Рабочий процесс
- `audit -> minimal patch -> tests -> audit -> context sync`
