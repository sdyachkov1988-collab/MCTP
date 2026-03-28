# MCTP — Статус архитектурных контрактов

Версия матрицы: `v2.0-step2-fix`
Последняя сверка: по фактическому коду, тестам и roadmap `v4.0`

Статусы:
- `Точно` — реализован строго по контракту на текущей стадии
- `Приблизительно` — реализован по сути, но есть небольшие отклонения или неполная формализация
- `Частично` — часть реализована, но есть реальный недостающий кусок
- `Планово отсутствует` — контракт относится к будущей фазе roadmap, отсутствие сейчас не считается багом
- `Нарушен` — должен быть уже сейчас, но в коде отсутствует или противоречит инварианту

---

## Контракты 01–44 — SpotFirst Layer

| # | Кратко | Статус | Что в коде / что не хватает |
|---|--------|--------|------------------------------|
| 01 | `asyncio` event loop | Точно | Весь async-код идёт через `asyncio` |
| 02 | Хранилище и миграция схемы | Частично | `SnapshotStore` проверяет `schema_version`, но `OrderStore` и `BalanceCacheStore` нет |
| 03 | `N_SNAP` как именованная константа | Приблизительно | Слой snapshot/equity есть, константная дисциплина в целом соблюдается |
| 04 | 4 независимых WS потока | Точно | Реализованы kline, book ticker symbol, BNBUSDT, user data; есть testnet/chaos coverage |
| 05 | WS-кеш баланса + REST только по правилам | Приблизительно | Логика кеша и refresh есть, но persistence без `schema_version` |
| 06 | BNB-to-quote из кеша ticker | Приблизительно | BNB rate cache и связанные потоки реализованы |
| 07 | `StrategyInput` с candle map до 7 TF | Частично | Есть `M15/H1/H4/D1`, но отсутствует `Timeframe.MONTHLY`; `5M` и `1W` не интегрированы в текущую v2.0 strategy |
| 08 | Мультисимвольный `PortfolioSnapshot` | Планово отсутствует | Single-symbol ядро соответствует текущему scope; мультисимволы только с `v2.3` |
| 09 | Все константы именованы и вынесены | Частично | `fee_rate` уже исправлен через `DEFAULT_FEE_RATE`, но magic number `0.015` для CCI ещё в коде |
| 10 | Только закрытые свечи | Точно | Enforced в backtest/paper/testnet |
| 11 | UTC alignment закрытия свечей | Приблизительно | UTC-aware datetime соблюдаются, MTF aggregation идёт по UTC-точкам |
| 12 | Валидация качества свечей | Приблизительно | Базовая загрузка/валидация есть, но формализация всех quality checks не выглядит полной |
| 13 | `Symbol` типизированный, BNB guard | Приблизительно | `Symbol` типизирован, BNB guard и boundary discipline есть |
| 14 | `min_meaningful_position_size` | Приблизительно | Meaningful position логика реализована |
| 15 | SELL quantity policy | Приблизительно | SELL quote quantity запрещён, quantity mode поддержан по смыслу |
| 16 | Scale-in policy | Приблизительно | Базовая поддержка позиции и scale-in semantics есть, но не как полностью отдельный policy contract |
| 17 | Protective mode: software stop XOR OCO | Приблизительно | OCO/software-stop exclusivity реализована и тестировалась |
| 18 | OCO pre-validation | Точно | Есть runtime validation по рынку и stale checks |
| 19 | Gap-risk при рестарте | Приблизительно | Startup gap-risk path реализован |
| 20 | Средняя стоимость, avg_cost_basis | Приблизительно | Cost basis логика есть и тестируется |
| 21 | Потеря базиса = HALT | Приблизительно | Missing basis handling реализован в startup/recovery |
| 22 | `quoteOrderQty` basis только из fills | Приблизительно | Fill-driven accounting соблюдается |
| 23 | Реализованный PnL: BNB/BASE/QUOTE | Точно | Поддержано и покрыто тестами |
| 24 | BNB списания исключены из external-change detection | Приблизительно | Защитная логика и balance truth разделены |
| 25 | Полная стоимость портфеля в quote | Приблизительно | Equity/portfolio valuation реализованы |
| 26 | Unrealized PnL с stale fallback | Приблизительно | Основная логика mark-to-market есть, но контракт не выглядит формально закрытым слово-в-слово |
| 27 | Equity snapshots каждые `N_SNAP` | Приблизительно | Equity/history слой реализован |
| 28 | Процедура смены символа | Приблизительно | Guardrails и symbol-change procedure описаны и частично поддержаны |
| 29 | Delisting detection: API + external monitoring | Приблизительно | API plumbing есть; внешний мониторинг не выглядит полноценно закрытым |
| 30 | Политика делистинга | Приблизительно | Forced exit / no-reentry window есть |
| 31 | Maker/taker fee model | Приблизительно | Fee/accounting model реализован |
| 32 | Изоляция окружений paper/testnet/live | Точно | Paper и testnet разделены; production live ещё не реализован |
| 33 | Версионирование конфигов / схем | Частично | `SnapshotStore` versioned, но не все хранилища versioned |
| 34 | Изоляция капитала / slice | Приблизительно | Sizing/risk pipeline есть |
| 35 | Trade viability как optional risk policy | Частично | По смыслу есть risk gating, но отдельная формализация optional policy не завершена |
| 36 | Только `Decimal`, без `float` | Частично | Финансовые значения на `Decimal`, но остаётся `float(T_CANCEL)` в `paper.py` |
| 37 | Только UTC-aware timestamps | Точно | Соблюдается по моделям и runtime |
| 38 | `clientOrderId` UUID4 и persistence | Приблизительно | ID discipline и persistence есть, но не весь контракт формализован в одном месте |
| 39 | Post-only rejection policy | Приблизительно | Execution boundary и rejection handling есть |
| 40 | Пиннинг зависимостей | Нарушен | Roadmap требует pinned versions/`requirements.txt`, а проект живёт через непинованный `pyproject.toml` |
| 41 | Rate limit ownership | Приблизительно | Бюджет/guardrails учтены на adapter/runtime уровне |
| 42 | Резервирование алертов | Приблизительно | Primary/backup alerting реализованы |
| 43 | Тирирование checklist | Приблизительно | Operator artifacts и readiness docs есть |
| 44 | Критерии перехода на фьючерсы | Планово отсутствует | По roadmap оценивается при `v2.2`, не должен быть закрыт сейчас |

---

## Контракты 45–67 — Master Edition Layer

| # | Кратко | Фаза roadmap | Статус | Что в коде / что не хватает |
|---|--------|--------------|--------|------------------------------|
| 45 | Multi-pair | `v2.3–v2.5` | Планово отсутствует | Вне текущего scope |
| 46 | Indicator Engine расширенный | `v3.x` | Частично | Indicator engine v1 есть; полный расширенный набор roadmap ещё впереди |
| 47 | Strategy Allocation Engine | `v2.3+` | Планово отсутствует | Вне текущего scope |
| 48 | Regime Engine | `v3.x` | Планово отсутствует | Отдельного regime layer нет |
| 49 | On-chain DataProvider | `v3.x` | Планово отсутствует | Вне текущего scope |
| 50 | ML strategies | `v5.0+` | Планово отсутствует | Вне текущего scope |
| 51 | Portfolio optimization | `v5.0+` | Планово отсутствует | Вне текущего scope |
| 52 | Research Layer | `v3.4–v3.6` | Планово отсутствует | Walk-forward/Monte Carlo в ядре платформы не интегрированы |
| 53 | Anomaly Detection Engine | `v3.0+` | Планово отсутствует | Отдельного anomaly layer нет |
| 54 | Adaptive Risk Management (10 уровней) | `v0.3–v4.0` | Частично | Risk stack есть, но не весь 10-level contract формализован и доведён до roadmap depth |
| 55 | Data & Persistence Layer | `v0.x+` | Частично | JSON + atomic write реализованы; `schema_version` непоследовательно применён |
| 56 | Strategy Interface | `v0.11+` | Точно | `StrategyBase`, `StrategyInput`, read-only strategy layer реализованы |
| 57 | Logging & Audit Trail | `v1.4+` | Точно | Structured logs + hash-chain audit реализованы |
| 58 | `TradingCore` architecture | `v2.2` | Планово отсутствует | Папки `mctp/trading_core` ещё нет; переход только намечен roadmap |
| 59 | Restart Protection Gate | `v1.2+ / v2.2` | Приблизительно | Startup/recovery gate и protection paths есть, но не как выделенный `TradingCore` слой |
| 60 | ExecutionAdapter interface | `v0.x+ / v4.0` | Приблизительно | Adapter boundary и сменные executors есть; futures executor ещё отсутствует |
| 61 | Product rules / exchange boundary | `v2.2+` | Приблизительно | Boundary discipline сильная, но полного `TradingCore` product-rules слоя ещё нет |
| 62 | StrategyDispatcher per-symbol | `v2.3+` | Планово отсутствует | Multi-pair ещё не начат |
| 63 | MultiplierProvider | `v3.x+` | Планово отсутствует | Regime/anomaly multiplier stack ещё не начат |
| 64 | Incident classes / operational maturity | `v2.2+` | Приблизительно | Runbooks, alerts, operator artifacts есть, но formal incident-class system ещё не завершён |
| 65 | Migration contract `v2.1 -> v2.2` | `v2.2` | Планово отсутствует | `TradingCore` migration ещё впереди |
| 66 | Secrets Management | `v1.x+` | Частично | Secrets берутся из env vars, smoke guard и runtime discipline есть; полноценный secrets management contract ещё не закрыт |
| 67 | Criteria paper -> spot live | `v2.2` | Планово отсутствует | Критерии описаны в roadmap, но автоматического gate/enforcement в коде пока нет |

---

## Реальные пробелы, которые уже должны быть закрыты

Это не будущий roadmap scope, а текущие недостающие части относительно уже заявленной архитектуры:

| Приоритет | Контракт | Проблема | Файл |
|-----------|----------|----------|------|
| MAJOR | 07 | Нет `Timeframe.MONTHLY` | `mctp/core/enums.py` |
| MAJOR | 02 / 33 / 55 | Нет `schema_version` check в `OrderStore` | `mctp/storage/order_store.py` |
| MAJOR | 02 / 33 / 55 | Нет `schema_version` check в `BalanceCacheStore` | `mctp/storage/balance_cache.py` |
| MAJOR | 09 | CCI magic number не из `constants.py` | `mctp/indicators/engine.py` |
| MAJOR | 46 | EMA cold-start без seed | `mctp/indicators/engine.py` |
| MAJOR | MTF audit gap | Нет warning при silent bucket drop на gap | `mctp/strategy/mtf.py` |
| MEDIUM | 36 | `float(T_CANCEL)` | `mctp/execution/paper.py` |
| MEDIUM | 40 | Нет pinned dependency policy как в roadmap | `pyproject.toml` / отсутствует `requirements.txt` |

---

## Что уже исправлено по сравнению с предыдущей матрицей

- `fee_rate` больше не нарушает контракт 09: теперь он берётся из `DEFAULT_FEE_RATE`
- `run_testnet_platform.py` уже использует `BtcUsdtMtfV20Strategy`
- `_persist_snapshot()` уже защищён
- boundary leakage по exchange status enums уже устранён
- `v2.0-step2` testnet MTF wiring уже реализован и подтверждён тестами

---

## Итог по фазам

| Фаза | Версии | Статус |
|------|--------|--------|
| Фаза 0 — Контракты | `v0.0` | Завершена |
| Фаза 1 — Базовая архитектура | `v0.1–v0.3` | Завершена |
| Фаза 2 — Fill-driven база | `v0.4–v0.6` | Завершена |
| Фаза 3 — Симуляция | `v0.7–v0.12` | Завершена |
| Фаза 4 — Биржа | `v1.0–v1.3` | Завершена |
| Фаза 5 — Безопасность | `v1.4–v1.7` | Завершена |
| Фаза 6 — Первый Live / `v2.0` corridor | `v2.0–v2.2` | В работе; текущая подтверждённая точка — `v2.0-step2-fix` |
| Фаза 7 — Multi-pair | `v2.3–v2.5` | Не начата |
| Фаза 8 — Intelligence | `v3.0–v3.3` | Не начата |
| Фаза 9 — Research | `v3.4–v3.6` | Не начата |
| Фаза 10 — Futures | `v4.0–v4.2` | Не начата |
| Фаза 11 — ML | `v5.0+` | Не начата |
