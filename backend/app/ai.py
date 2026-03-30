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


async def analyze_market_position(
    company_name: str,
    stock: dict[str, Any] | None,
    market_index: dict[str, Any] | None,
    top_stocks: list[dict[str, Any]],
    news: list[dict[str, Any]],
    api_key: str,
) -> dict[str, Any]:
    client = get_client(api_key)

    if stock:
        _chg = stock.get("lasttoprevprice") or stock.get("change")
        _chg_str = f"{_chg:+.2f}%" if _chg is not None else "н/д"
        stock_text = (
            f"Тикер: {stock.get('ticker')} ({stock.get('source')}) | "
            f"Цена: {stock.get('last')} | "
            f"Изменение: {_chg_str} | "
            f"Объём торгов: {stock.get('voltoday') or 'н/д'}"
        )
    else:
        stock_text = "Компания не торгуется на открытом рынке."

    idx = market_index or {}
    idx_val = idx.get("currentvalue") or idx.get("lastvalue") or "н/д"
    idx_chg = idx.get("lasttoprevprice")
    index_text = (
        f"IMOEX: {idx_val} | Изменение: {f'{idx_chg:+.2f}%' if idx_chg else 'н/д'}"
    ) if market_index else "Индекс недоступен."

    def fmt_stock(s: dict) -> str:
        chg = s.get("change")
        vol = s.get("volume_rub") or 0
        return (
            f"{s['ticker']:8s} {s.get('name','')[:20]:20s} | "
            f"Цена: {s.get('last') or '—':>10} | "
            f"Изм: {f'{chg:+.2f}%' if chg is not None else '—':>8} | "
            f"Объём: {vol:>16,.0f} ₽"
        )

    leaders_text = "\n".join(fmt_stock(s) for s in top_stocks) or "Данные недоступны."
    news_text = "\n".join(
        f"- {n.get('title', '')} ({n.get('pub_date', '')})" for n in news[:12]
    ) or "Новостей нет."

    prompt = f"""Ты — инвестиционный аналитик. Проведи глубокий анализ рыночной позиции компании «{company_name}».

## Биржевые данные компании
{stock_text}

## Состояние рынка (MOEX)
{index_text}

## Топ ликвидных акций рынка (рыночный контекст и конкуренты)
{leaders_text}

## Новости по компании и отрасли
{news_text}

## Задание
Дай детальный инвестиционный анализ в JSON:
{{
  "market_position": "<3-4 предложения о позиции компании на рынке, её доле и месте среди конкурентов>",
  "competitive_analysis": "<3-4 предложения о конкурентах: кто угрожает, как сравнивается компания по масштабу и динамике>",
  "market_risks": [
    {{"title": "<название риска>", "description": "<1-2 предложения>", "severity": "high|medium|low"}},
    {{"title": "...", "description": "...", "severity": "..."}},
    {{"title": "...", "description": "...", "severity": "..."}}
  ],
  "market_opportunities": [
    {{"title": "<возможность>", "description": "<1-2 предложения>"}},
    {{"title": "...", "description": "..."}}
  ],
  "risk_score": <0-100>,
  "trend": "bullish|bearish|neutral",
  "summary": "<2-3 предложения итогового вывода для инвесторов и руководства>"
}}

Только JSON."""

    try:
        response = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=2048,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text
                break
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        return json.loads(text)
    except Exception as e:
        logger.exception("Market AI analysis failed for %s", company_name)
        return {
            "market_position": "Анализ временно недоступен.",
            "competitive_analysis": "Анализ временно недоступен.",
            "market_risks": [{"title": "Ошибка", "description": str(e), "severity": "medium"}],
            "market_opportunities": [],
            "risk_score": 50,
            "trend": "neutral",
            "summary": "Не удалось выполнить анализ. Проверьте API-ключ.",
        }


async def analyze_company_risks(
    company_name: str,
    employees: list[dict[str, Any]],
    news: list[dict[str, Any]],
    yandex_news: list[dict[str, Any]],
    threads_posts: list[dict[str, Any]],
    social: dict[str, list[dict[str, Any]]],
    reviews: list[dict[str, Any]],
    vacancies: list[dict[str, Any]],
    regulatory_news: list[dict[str, Any]],
    market_news: list[dict[str, Any]],
    finance: dict[str, Any],
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
    yn_idx, yn_text = _build_indexed(
        yandex_news, "YN", "title", "link",
        lambda x: f"{x.get('title','')} ({x.get('pub_date','')})"
    )
    threads_idx, threads_text = _build_indexed(
        threads_posts, "T", "captionText", "postUrl",
        lambda x: f"@{x.get('username','')}: {x.get('captionText','')[:250]}"
    )
    # Social media
    ig_posts = social.get("instagram", [])
    tk_posts = social.get("tiktok", [])
    yt_posts = social.get("youtube", [])
    tw_posts = social.get("twitter", [])
    fb_posts = social.get("facebook", [])

    ig_idx, ig_text = _build_indexed(ig_posts, "IG", "text", "url",
        lambda x: f"@{x.get('author','')}: {x.get('text','')[:250]}")
    tk_idx, tk_text = _build_indexed(tk_posts, "TK", "text", "url",
        lambda x: f"@{x.get('author','')}: {x.get('text','')[:250]}")
    yt_idx, yt_text = _build_indexed(yt_posts, "YT", "text", "url",
        lambda x: f"{x.get('author','')}: {x.get('text','')[:250]}")
    tw_idx, tw_text = _build_indexed(tw_posts, "X", "text", "url",
        lambda x: f"@{x.get('author','')}: {x.get('text','')[:250]}")
    fb_idx, fb_text = _build_indexed(fb_posts, "FB", "text", "url",
        lambda x: f"{x.get('author','')}: {x.get('text','')[:250]}")

    # Reviews
    rev_idx, rev_text = _build_indexed(
        reviews, "R", "text", "url",
        lambda x: f"[{x.get('source','')} {'★'*int(x.get('rating') or 0)}] {x.get('text','')[:250]}"
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

    # Finance / investment data
    stock = finance.get("stock")
    if stock:
        change = stock.get("lasttoprevprice") or stock.get("change")
        fin_text = (
            f"Тикер: {stock.get('ticker')} ({stock.get('source')}) | "
            f"Цена: {stock.get('last')} | "
            f"Изменение: {f'{change:+.2f}%' if change else 'н/д'} | "
            f"Объём: {stock.get('voltoday') or 'н/д'}"
        )
    else:
        fin_text = "Биржевые данные не найдены (компания не торгуется публично или данные недоступны)."

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
    _type_map = [
        (news_idx, "news"), (yn_idx, "yandex_news"), (threads_idx, "threads"),
        (ig_idx, "instagram"), (tk_idx, "tiktok"), (yt_idx, "youtube"),
        (tw_idx, "twitter"), (fb_idx, "facebook"), (rev_idx, "review"),
        (vac_idx, "hh"), (reg_idx, "regulatory"), (mkt_idx, "market"),
        (hr_idx, "hr_email"), (pr_idx, "pr_email"), (gr_idx, "gr_email"),
    ]
    for idx_list, src_type in _type_map:
        for item in idx_list:
            source_lookup[item["id"]] = {"title": item["label"][:80], "url": item["url"], "type": src_type}

    employee_count = len(employees)
    departments = list({e.get("department") for e in employees if e.get("department")})
    dept_text = ", ".join(departments) if departments else "не указаны"

    prompt = f"""Ты — эксперт по корпоративным рискам. Проанализируй данные о компании «{company_name}» и составь структурированный отчёт по 5 категориям риска.

## Компания
Название: {company_name} | Сотрудников: {employee_count} | Отделы: {dept_text}

## Источники данных

### [N] Google News
{news_text}

### [YN] Yandex News
{yn_text}

### [T] Threads
{threads_text}

### [IG] Instagram
{ig_text}

### [TK] TikTok
{tk_text}

### [YT] YouTube
{yt_text}

### [X] Twitter / X
{tw_text}

### [FB] Facebook
{fb_text}

### [R] Отзывы (2GIS, Google Maps)
{rev_text}

### [H] HH.ru (вакансии)
{vac_text}

### [G] Регуляторные новости
{reg_text}

### [M] Рынок и отрасль
{mkt_text}

### [FIN] Биржевые / инвестиционные данные
{fin_text}

### [HR] Внутренние сообщения — HR
{hr_email_text}

### [PR] Внутренние сообщения — PR/медиа
{pr_email_text}

### [GR] Внутренние сообщения — GR/юридика
{gr_email_text}

## Задание

Оцени риски по 5 категориям (0–100). Для каждой категории дай 2–5 конкретных рисков со ссылками на источники.

Правила:
- Внутренние сообщения ([HR], [PR], [GR]) — реальные сигналы от сотрудников, высокий приоритет.
- Отзывы ([R]) с низким рейтингом (1–2 звезды) сигнализируют о PR и HR рисках.
- Соцсети ([IG], [TK], [YT], [X], [FB], [T]) — репутационные сигналы от широкой аудитории.
- [FIN] — если цена акции падает или объём аномальный, повышай балл market.
- Если данных по источнику нет — не выдумывай риски, напиши "Данных недостаточно".

Категории:
- media: медиа-репутация (используй [N], [YN], [T], [IG], [TK], [YT], [X], [FB])
- hr: кадровый риск (используй [H], [HR], [R])
- gr: регуляторный/GR риск (используй [G], [GR])
- pr: PR и репутационное давление (используй [T], [IG], [X], [FB], [R], [PR])
- market: рыночный и инвестиционный риск (используй [M], [FIN], [H])

Ответь строго в JSON:
{{
  "overall_score": <0-100>,
  "advice": "<2-3 предложения совета руководству>",
  "categories": {{
    "media":  {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["N1", "IG1"]}}]}},
    "hr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["H1", "HR1"]}}]}},
    "gr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["G1"]}}]}},
    "pr":     {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["X1", "R1"]}}]}},
    "market": {{"score": <0-100>, "risks": [{{"text": "...", "source_ids": ["M1", "FIN"]}}]}}
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
