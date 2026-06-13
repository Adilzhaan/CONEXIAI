# Промт для Claude Code — Scoring & Damage Engine (Conexi)

> Вставь это в Claude Code как задачу. Бэкенд — **FastAPI**. Сначала прочитай существующие модели/роутеры, не дублируй.

---

## Контекст

Conexi — система раннего обнаружения корпоративных рисков. Сейчас в проекте/документации **три разные, несшитые системы формул**, и их надо свести в один движок:

1. **Risk Score** — `(Severity × Probability × Velocity) + Signal Density`, шкала 0–100. **БАГ:** без нормировки даёт сотни, а не 0–100 (пример из доки S=7,P=8,V=6,D=5 «≈61» не сходится с `(7×8×6)+5=341`).
2. **Формулы ущерба** (деньги в ₸): Op/Reg/Rep/Market/Stakeholder Loss.
3. **5 доменных формул** (быстрая оценка ожидаемого ущерба): вероятность × влияние.

**Задача:** построить единый модуль `risk_engine`, который считает риск согласованно на трёх слоях и устраняет противоречия.

---

## Целевая архитектура (3 слоя)

```
Слой 1: Signal Features   — из сигнала вытаскиваем severity, probability, velocity,
                            signal_density, reach, sentiment, audience_weight (все 0–10)
Слой 2: Risk Score (0–100)— "насколько горит", нормированный, по доменам PR/HR/GR/OR/MR/IM/CI
Слой 3: Expected Loss (₸) — "сколько денег": быстрая оценка (5 формул) + детальная (Том 4)
Триггер: Risk Score + динамика → создаёт кризис
```

Связь слоёв: **Слой 2 говорит, опасно ли это (0–100); Слой 3 переводит в деньги.** Быстрая оценка (5 формул) — для рисков-прогнозов; детальная (Том 4) — когда инцидент/кризис реальный.

---

## СЛОЙ 2 — Risk Score (с нормировкой, конфигурируемый)

Все входы в шкале **0–10**. НЕ копируй сломанную формулу из доки буквально. Реализуй нормированную:

```python
# рекомендуемый дефолт — мультипликативное ядро (все три фактора должны быть высоки)
core   = (severity/10) * (probability/10) * (velocity/10)   # 0..1
signal = signal_density / 10                                # 0..1

risk_score = clamp(
    (core * w_core + signal * w_signal) / (w_core + w_signal) * 100,
    0, 100
)
```

- Веса `w_core`, `w_signal` — из конфига отдела (см. блок «Конфигурируемость»), дефолт 1.0 / 1.0.
- Результат всегда `clamp(0, 100)`.
- Шкала: `0–20` низкий · `21–50` средний · `51–75` высокий · `76–100` критический.

> Важно: пример «61» из документа был иллюстративным и не сходится с его же формулой. НЕ подгоняй под 61 — реализуй корректную нормированную формулу и зафиксируй её тестами.

---

## СЛОЙ 3 — Expected Loss и Damage (₸)

### 3a. Быстрая оценка ожидаемого ущерба (5 доменных формул)

```python
financial_risk   = probability * loss
operational_risk = probability * impact * duration
reputational_risk= reach * sentiment * audience_weight
regulatory_risk  = inspection_probability * potential_fine
supplier_risk    = failure_probability * downtime_loss
```

### 3b. Детальный ущерб (Том 4) — для инцидентов/кризисов

```python
total_loss = op_loss + reg_loss + rep_loss + market_loss + stakeholder_loss

op_loss          = (p_per_hour * t_downtime) + (demurrage * metal_price)
                   # p_per_hour = revenue_per_hour / 0.7
reg_loss         = fine_adm + fine_environmental + license_suspension_penalty
rep_loss         = media_impressions * cpm_kz * sentiment_factor
market_loss      = (delta_marketcap_pct * enterprise_value) + delta_cost_of_debt
stakeholder_loss = supplier_fines + employee_churn_cost + community_compensation
                   # employee_churn_cost = engineers_left * 6 * avg_salary
```

**Коэффициенты — в settings/таблицу конфигов, НЕ хардкод:**
- `MRP_2026 = 3690` (₸)
- штрафы: сокрытие травматизма до `2000*MRP`; промбезопасность `1000–3000*MRP`
- `cpm_kz`: 500–1500 ₸ / 1000 показов
- `sentiment_factor`: негатив `1.5`, нейтрал `0.5`, позитив `0`
- `delta_marketcap_pct`: типично −3%…−15%

---

## Триггер кризиса

```python
is_crisis    = risk_score > trigger_threshold or sharp_rise_24_48h(history)
crisis_mode  = risk_score > crisis_threshold
# дефолты: trigger_threshold = 70, crisis_threshold = 90 (конфигурируемы)
```

`sharp_rise_24_48h` — рост score выше заданной дельты за окно 24–48ч.

---

## Конфигурируемость по отделам (важно)

Веса, пороги и коэффициенты **настраиваются per отдел** с защитой:

```
risk_configs(id, company_id, scope[company|department], department,
             formula_weights jsonb, thresholds jsonb, coefficients jsonb,
             is_active, version, created_by, created_at)
```

- Resolution: конфиг отдела → иначе дефолт компании.
- Каждый параметр имеет min/max; изменение в пределах — сразу; вне пределов или смена порога триггера — апрув admin.
- Версионирование + audit log. В каждой записи риска пишем `config_version`.
- История задним числом НЕ пересчитывается.

**(Опционально, если делаем редактируемую формулу):** хранить выражение как строку, считать через безопасный эвалюатор (`asteval` с whitelist переменных/функций — НЕ `eval`), обязательный бэктест на истории + апрув перед активацией, фолбэк на дефолт при ошибке.

---

## Модель данных (новое)

```
risk_scores(id, risk_id, domain, severity, probability, velocity, signal_density,
            risk_score, config_version, computed_at)

loss_estimates(id, risk_id, kind[quick|detailed], domain,
               op_loss, reg_loss, rep_loss, market_loss, stakeholder_loss,
               total_loss, currency, computed_at)

scoring_coefficients(id, company_id, key, value, min, max)   # MRP, cpm, sentiment_factor...
```

---

## Эндпоинты (FastAPI)

```
POST /risk/score          — вход: features; выход: risk_score + уровень + config_version
POST /risk/loss/quick     — 5 доменных формул → expected loss
POST /risk/loss/detailed  — Том 4 → total_loss с разбивкой
POST /risk/trigger/check  — проверка триггера по score + истории
GET  /configs/{scope}     — текущий конфиг (company|department)
PUT  /configs/{scope}     — изменить (с валидацией min/max + апрув-флоу)
```

---

## Acceptance Criteria

1. Все формулы — отдельные **чистые функции** в `services/`, покрыты юнит-тестами.
2. `risk_score` **всегда** в диапазоне 0–100 (property-тест на случайных входах 0–10).
3. Веса/пороги/коэффициенты читаются из конфига, не хардкод; дефолты заданы.
4. Resolution отдел→компания работает; в записи риска сохраняется `config_version`.
5. Детальный ущерб воспроизводит пример постмортема из документа (Op 45 + Reg 8 + Rep 12 ≈ 65 млн ₸) на соответствующих входах.
6. Триггер создаёт кризис при score>70 и при резком росте; Crisis Mode при >90.
7. Изменение порога триггера или выход за min/max требует апрува (тест на блокировку).
8. Alembic-миграции применяются чисто.

## Чего НЕ делать

- НЕ использовать сломанную ненормированную формулу из документа буквально.
- НЕ хардкодить МРП, CPM, sentiment_factor, пороги.
- НЕ использовать `eval()`/`exec()` для редактируемых формул — только sandbox с whitelist.
- НЕ пересчитывать историю при смене конфига.

---

## Порядок реализации

1. Чистые функции формул + тесты (`services/scoring.py`, `services/damage.py`).
2. Таблицы + миграции + конфиг-резолвер.
3. Эндпоинты.
4. Триггер-логика + история.
5. Конфигурируемость по отделам + апрув-флоу.
6. (Опц.) редактируемая формула через safe-эвалюатор.
