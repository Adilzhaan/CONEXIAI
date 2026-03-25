import json
import logging
from typing import Any

import anthropic

logger = logging.getLogger("conexiai")

_client: anthropic.AsyncAnthropic | None = None


def get_client(api_key: str) -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


async def close() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


def _build_indexed(items: list[dict], prefix: str, text_key: str, url_key: str, label_fn) -> tuple[list[dict], str]:
    indexed = []
    for i, item in enumerate(items):
        indexed.append({
            "id": f"{prefix}{i+1}",
            "label": label_fn(item),
            "url": item.get(url_key, ""),
        })
    text = "\n".join(f"[{n['id']}] {n['label']} | {n['url']}" for n in indexed) or "Нет данных."
    return indexed, text


async def analyze_company_risks(
    company_name: str,
    employees: list[dict[str, Any]],
    news: list[dict[str, Any]],
    threads_posts: list[dict[str, Any]],
    vacancies: list[dict[str, Any]],
    regulatory_news: list[dict[str, Any]],
    market_news: list[dict[str, Any]],
    hr_emails: list[dict[str, Any]],
    pr_emails: list[dict[str, Any]],
    gr_emails: list[dict[str, Any]],
    api_key: str,
) -> dict[str, Any]:
    client = get_client(api_key)

    # Index all sources
    news_idx, news_text = _build_indexed(
        news, "N", "title", "link",
        lambda x: f"{x.get('title','')} — {x.get('source','')} ({x.get('pub_date','')})"
    )
    threads_idx, threads_text = _build_indexed(
        threads_posts, "T", "captionText", "postUrl",
        lambda x: f"@{x.get('username','')}: {x.get('captionText','')[:250]}"
    )
    vac_idx, vac_text = _build_indexed(
        vacancies, "H", "title", "url",
        lambda x: f"{x.get('title','')} | {x.get('area','')} | {x.get('salary') or 'зарплата не указана'}"
    )
    reg_idx, reg_text = _build_indexed(
        regulatory_news, "G", "title", "link",
        lambda x: f"{x.get('title','')} — {x.get('source','')} ({x.get('pub_date','')})"
    )
    mkt_idx, mkt_text = _build_indexed(
        market_news, "M", "title", "link",
        lambda x: f"{x.get('title','')} — {x.get('source','')} ({x.get('pub_date','')})"
    )

    # Emails by type
    def _email_text(emails: list[dict], prefix: str) -> tuple[list[dict], str]:
        idx = []
        for i, e in enumerate(emails):
            idx.append({"id": f"{prefix}{i+1}", "label": f"[{e.get('position','')}] {e.get('text','')[:300]}", "url": ""})
        return idx, "\n".join(f"[{n['id']}] {n['label']}" for n in idx) or "Сообщений нет."

    hr_idx, hr_email_text = _email_text(hr_emails, "HR")
    pr_idx, pr_email_text = _email_text(pr_emails, "PR")
    gr_idx, gr_email_text = _email_text(gr_emails, "GR")

    # Source lookup for resolving IDs → title + url
    source_lookup: dict[str, dict] = {}
    for n in news_idx:
        source_lookup[n["id"]] = {"title": n["label"][:80], "url": n["url"], "type": "news"}
    for t in threads_idx:
        source_lookup[t["id"]] = {"title": t["label"][:80], "url": t["url"], "type": "threads"}
    for v in vac_idx:
        source_lookup[v["id"]] = {"title": v["label"][:80], "url": v["url"], "type": "hh"}
    for g in reg_idx:
        source_lookup[g["id"]] = {"title": g["label"][:80], "url": g["url"], "type": "regulatory"}
    for m in mkt_idx:
        source_lookup[m["id"]] = {"title": m["label"][:80], "url": m["url"], "type": "market"}
    for h in hr_idx:
        source_lookup[h["id"]] = {"title": h["label"][:80], "url": "", "type": "hr_email"}
    for p in pr_idx:
        source_lookup[p["id"]] = {"title": p["label"][:80], "url": "", "type": "pr_email"}
    for g in gr_idx:
        source_lookup[g["id"]] = {"title": g["label"][:80], "url": "", "type": "gr_email"}

    employee_count = len(employees)
    departments = list({e.get("department") for e in employees if e.get("department")})
    dept_text = ", ".join(departments) if departments else "не указаны"

    prompt = f"""Ты — эксперт по корпоративным рискам. Проанализируй данные о компании «{company_name}» и составь структурированный отчёт по 5 категориям риска.

## Компания
Название: {company_name} | Сотрудников: {employee_count} | Отделы: {dept_text}

## Источники данных

### [N] Google News (медиа)
{news_text}

### [T] Threads (соцсеть)
{threads_text}

### [H] HH.ru (вакансии)
{vac_text}

### [G] Регуляторные новости
{reg_text}

### [M] Рынок и отрасль
{mkt_text}

### [HR] Внутренние сообщения (HR-сигналы)
{hr_email_text}

### [PR] Внутренние сообщения (PR/медиа-сигналы)
{pr_email_text}

### [GR] Внутренние сообщения (GR/юридические сигналы)
{gr_email_text}

## Задание

Оцени риски по 5 категориям (0–100). Для каждой категории дай 2–5 конкретных рисков со ссылками на источники.
ВАЖНО: внутренние сообщения ([HR], [PR], [GR]) — это реальные сигналы от сотрудников компании. Они имеют высокий приоритет. Если в сообщении нет явного риска — так и укажи, но всё равно включи его в анализ как нейтральный сигнал.

Категории:
- media: медиа-репутация (используй [N], [T])
- hr: кадровый риск (используй [H], [HR]; повышай балл если есть жалобы или увольнения)
- gr: регуляторный/GR риск (используй [G], [GR]; повышай балл при упоминании проверок, судов)
- pr: PR и местное давление (используй [T], [PR]; повышай балл при негативных упоминаниях)
- market: рыночный и отраслевой риск (используй [M], [H])

Ответь строго в JSON:
{{
  "overall_score": <0-100>,
  "advice": "<2-3 предложения совета руководству>",
  "categories": {{
    "media":  {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["N1"]}}]}},
    "hr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["H1", "HR1"]}}]}},
    "gr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["G1"]}}]}},
    "pr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["T1"]}}]}},
    "market": {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["M1"]}}]}}
  }}
}}

Только JSON, без пояснений."""

    def _resolve_risks(raw_risks: list) -> list[dict]:
        result = []
        for r in raw_risks:
            if isinstance(r, dict):
                text_val = str(r.get("text", r))
                ids = r.get("source_ids", [])
                sources = [
                    source_lookup[sid]
                    for sid in ids
                    if sid in source_lookup and source_lookup[sid].get("url")
                ]
                result.append({"text": text_val, "sources": sources})
            else:
                result.append({"text": str(r), "sources": []})
        return result

    try:
        response = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            thinking={"type": "adaptive"},
            messages=[{"role": "user", "content": prompt}],
        )

        text = ""
        for block in response.content:
            if block.type == "text":
                text = block.text
                break

        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text

        result = json.loads(text)
        cats_raw = result.get("categories", {})

        categories = {}
        for key in ("media", "hr", "gr", "pr", "market"):
            cat = cats_raw.get(key, {})
            categories[key] = {
                "score": int(cat.get("score", 50)),
                "risks": _resolve_risks(cat.get("risks", [])),
            }

        overall = int(result.get("overall_score", sum(c["score"] for c in categories.values()) // 5))

        return {
            "score": overall,
            "advice": str(result.get("advice", "")),
            "risks": _resolve_risks(result.get("risks", [])),
            "categories": categories,
        }

    except Exception as e:
        logger.exception("AI analysis failed for company %s", company_name)
        fallback_cat = {"score": 50, "risks": [{"text": "Анализ недоступен: " + str(e), "sources": []}]}
        return {
            "score": 50,
            "advice": "Анализ временно недоступен. Проверьте API-ключ Anthropic.",
            "risks": [{"text": "Не удалось выполнить AI-анализ: " + str(e), "sources": []}],
            "categories": {k: fallback_cat for k in ("media", "hr", "gr", "pr", "market")},
        }
