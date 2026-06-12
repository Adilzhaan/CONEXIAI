import json
import logging
import re
from typing import Any

import anthropic

logger = logging.getLogger("conexiai")

_client: anthropic.AsyncAnthropic | None = None


def _repair_json(text: str) -> str:
    """Fix common issues in Claude's JSON output before parsing."""
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        text = text.strip()
    # Extract first {...} block
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        text = text[start:end + 1]
    # Fix Python literals
    text = re.sub(r"\bTrue\b",  "true",  text)
    text = re.sub(r"\bFalse\b", "false", text)
    text = re.sub(r"\bNone\b",  "null",  text)
    # Remove JS-style comments
    text = re.sub(r"//[^\n]*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    # Remove trailing commas before } or ]
    text = re.sub(r",\s*([}\]])", r"\1", text)
    # Fix unescaped literal newlines/tabs inside JSON string values.
    # Walk char-by-char so we only touch characters that are inside strings.
    result = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
        elif ch == "\\" and in_string:
            result.append(ch)
            escape_next = True
        elif ch == '"':
            result.append(ch)
            in_string = not in_string
        elif in_string and ch == "\n":
            result.append("\\n")
        elif in_string and ch == "\r":
            result.append("\\r")
        elif in_string and ch == "\t":
            result.append("\\t")
        else:
            result.append(ch)
    fixed = "".join(result)

    # After the walk, all in-string newlines are escaped to \n, so any literal
    # newline remaining is structural. Fix missing commas in all common cases.

    # Missing comma between adjacent objects/arrays
    fixed = re.sub(r"}\s*{", "},{", fixed)
    fixed = re.sub(r"]\s*\[", "],[", fixed)

    # Missing comma between a string value and the next string (key or value).
    # Pattern: closing " then whitespace/newline then opening " — no comma between them.
    # This covers: "value"\n"next_key": and "item1"\n"item2" in arrays.
    fixed = re.sub(r'"(\s*\n\s*)"', r'",\1"', fixed)

    # Missing comma after a number/boolean/null before the next element
    fixed = re.sub(r'(\d)(\s*\n\s*)"', r'\1,\2"', fixed)
    fixed = re.sub(r'(true|false|null)(\s*\n\s*)"', r'\1,\2"', fixed)

    return fixed


def _complete_json(text: str) -> str:
    """Close any unclosed strings, arrays and objects (handles truncated responses)."""
    stack = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            escape_next = False
        elif ch == "\\" and in_string:
            escape_next = True
        elif ch == '"':
            in_string = not in_string
        elif not in_string:
            if ch in "{[":
                stack.append("}" if ch == "{" else "]")
            elif ch in "}]" and stack and stack[-1] == ch:
                stack.pop()
    closing = ('\"' if in_string else "") + "".join(reversed(stack))
    return text + closing


def _parse_json(text: str) -> Any:
    repaired = _repair_json(text)
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        # Fallback: response may be truncated — try completing it
        completed = _complete_json(repaired)
        try:
            return json.loads(completed)
        except json.JSONDecodeError as e:
            snippet = repaired[max(0, e.pos - 120): e.pos + 120]
            logger.error("JSON parse failed pos=%d line=%d col=%d\nNEAR: %s",
                         e.pos, e.lineno, e.colno, repr(snippet))
            raise


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


async def suggest_sources(
    company_name: str,
    industry: str,
    description: str,
    api_key: str,
) -> list[dict[str, Any]]:
    """Ask Haiku to recommend monitoring sources for a company."""
    import asyncio as _aio
    import anthropic as _a
    prompt = (
        f"Компания: «{company_name}»\n"
        f"Отрасль: {industry or 'неизвестна'}\n"
        f"Описание: {description or 'нет'}\n\n"
        f"Порекомендуй ровно 30 конкретных источников для мониторинга этой компании.\n"
        f"Включи разнообразные категории: финансовые порталы, отраслевые СМИ, регуляторные сайты, "
        f"новостные агентства (местные и международные), официальный сайт компании, "
        f"биржи (если публичная), соцсети компании (LinkedIn, Instagram, Facebook, YouTube), "
        f"форумы и сообщества, рейтинговые агентства, государственные реестры.\n"
        f"Для каждого источника укажи реальный рабочий URL.\n\n"
        f"Верни JSON:\n"
        f'{{"sources": [\n'
        f'  {{"name": "название", "url": "https://...", "category": "финансы|СМИ|регулятор|соцсети|официальный|биржа|отрасль|форум|рейтинг|госреестр", "description": "что здесь искать (1 строка)"}}\n'
        f']}}'
    )
    def _call():
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2500,
            system="JSON API. Respond ONLY with valid JSON. No markdown.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        return json.loads(raw).get("sources", [])
    try:
        return await _aio.to_thread(_call)
    except Exception as e:
        logger.warning("suggest_sources failed: %s", e)
        return []


async def analyze_source_findings(
    company_name: str,
    source_name: str,
    articles: list[dict[str, Any]],
    api_key: str,
) -> dict[str, Any]:
    """Haiku analyzes articles found on a specific source and extracts risks/signals."""
    import asyncio as _aio
    import anthropic as _a
    lines = "\n".join(
        f"- {a.get('title', '')} | {a.get('pub_date', '')} | {a.get('snippet', '')[:150]}"
        for a in articles[:10]
    )
    prompt = (
        f"Источник: {source_name}\n"
        f"Компания: «{company_name}»\n\n"
        f"Найденные материалы:\n{lines}\n\n"
        f"Проанализируй эти материалы и извлеки:\n"
        f"1. Ключевые риски или негативные сигналы\n"
        f"2. Позитивные сигналы или возможности\n"
        f"3. Общий вывод по источнику (1 предложение)\n\n"
        f"Верни JSON:\n"
        f'{{"risks": ["риск 1", "риск 2"], "positives": ["сигнал 1"], "verdict": "вывод", "risk_level": "high|medium|low|none"}}'
    )
    def _call():
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            system="JSON API. Respond ONLY with valid JSON. No markdown.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        return json.loads(raw)
    try:
        return await _aio.to_thread(_call)
    except Exception as e:
        logger.warning("analyze_source_findings failed: %s", e)
        return {"risks": [], "positives": [], "verdict": "Анализ недоступен", "risk_level": "none"}


async def analyze_market_position(
    company_name: str,
    stock: dict[str, Any] | None,
    market_indices: list[dict[str, Any]],
    top_stocks: list[dict[str, Any]],
    news: list[dict[str, Any]],
    api_key: str,
    fundamentals: dict[str, Any] | None = None,
) -> dict[str, Any]:
    client = get_client(api_key)

    if stock:
        _chg = stock.get("lasttoprevprice") or stock.get("change")
        _chg_str = f"{_chg:+.2f}%" if _chg is not None else "н/д"
        stock_text = (
            f"Тикер: {stock.get('ticker')} ({stock.get('exchange', stock.get('source'))}) | "
            f"Цена: {stock.get('last')} | "
            f"Изменение: {_chg_str} | "
            f"Объём торгов: {stock.get('volume') or 'н/д'}"
        )
    else:
        stock_text = "Компания не торгуется на открытом рынке."

    def fmt_index(idx: dict) -> str:
        chg = idx.get("change_pct")
        chg_str = f"{chg:+.2f}%" if chg is not None else "н/д"
        return f"{idx.get('name', idx.get('symbol', ''))}: {idx.get('last') or '—'} | Изм: {chg_str}"

    index_text = "\n".join(fmt_index(i) for i in market_indices) or "Индексы недоступны."

    def fmt_stock(s: dict) -> str:
        chg = s.get("change_pct") or s.get("change")
        return (
            f"{s.get('ticker',''):8s} {s.get('name','')[:20]:20s} | "
            f"Цена: {s.get('last') or '—':>10} | "
            f"Изм: {f'{chg:+.2f}%' if chg is not None else '—':>8}"
        )

    leaders_text = "\n".join(fmt_stock(s) for s in top_stocks) or "Данные недоступны."
    news_text = "\n".join(
        f"- {n.get('title', '')} ({n.get('pub_date', '')})" for n in news[:12]
    ) or "Новостей нет."

    fund = fundamentals or {}
    fund_lines = []
    if fund.get("sector"):       fund_lines.append(f"Сектор: {fund['sector']}")
    if fund.get("industry"):     fund_lines.append(f"Индустрия: {fund['industry']}")
    if fund.get("country"):      fund_lines.append(f"Страна: {fund['country']}")
    if fund.get("market_cap_fmt"): fund_lines.append(f"Рыночная капитализация: {fund['market_cap_fmt']}")
    if fund.get("enterprise_value_fmt"): fund_lines.append(f"EV: {fund['enterprise_value_fmt']}")
    if fund.get("trailing_pe"):  fund_lines.append(f"P/E (TTM): {fund['trailing_pe']}")
    if fund.get("forward_pe"):   fund_lines.append(f"Forward P/E: {fund['forward_pe']}")
    if fund.get("price_to_book"): fund_lines.append(f"P/B: {fund['price_to_book']}")
    if fund.get("beta"):         fund_lines.append(f"Бета: {fund['beta']} ({'высокая волатильность' if fund['beta'] > 1.5 else 'умеренная волатильность' if fund['beta'] > 1 else 'низкая волатильность'})")
    if fund.get("dividend_yield"): fund_lines.append(f"Дивидендная доходность: {fund['dividend_yield']}")
    if fund.get("profit_margins"): fund_lines.append(f"Маржа чистой прибыли: {fund['profit_margins']}")
    if fund.get("revenue_growth"): fund_lines.append(f"Рост выручки (г/г): {fund['revenue_growth']}")
    if fund.get("earnings_growth"): fund_lines.append(f"Рост прибыли (г/г): {fund['earnings_growth']}")
    if fund.get("return_on_equity"): fund_lines.append(f"ROE: {fund['return_on_equity']}")
    if fund.get("debt_to_equity"): fund_lines.append(f"Долг/Капитал: {fund['debt_to_equity']}")
    if fund.get("full_time_employees"): fund_lines.append(f"Сотрудников: {fund['full_time_employees']:,}")
    if fund.get("fifty_day_avg"): fund_lines.append(f"Скользящая средняя 50 дней: {fund['fifty_day_avg']}")
    if fund.get("two_hundred_day_avg"): fund_lines.append(f"Скользящая средняя 200 дней: {fund['two_hundred_day_avg']}")
    fund_text = "\n".join(fund_lines) if fund_lines else "Фундаментальные данные недоступны."

    prompt = f"""Ты — инвестиционный аналитик. Проведи глубокий анализ рыночной позиции компании «{company_name}».

## Биржевые данные компании
{stock_text}

## Фундаментальные показатели (Yahoo Finance / Google Finance)
{fund_text}

## Состояние мировых рынков (KASE / AIX / NYSE / NASDAQ / LSE)
{index_text}

## Топ акций рынка (рыночный контекст)
{leaders_text}

## Новости по компании и отрасли
{news_text}

## Задание
Дай лаконичный инвестиционный анализ в JSON. Все тексты — максимально короткие (4-7 слов на пункт):
{{
  "summary": "<1 предложение: ключевой вывод для инвестора>",
  "market_position": "<1 предложение о позиции компании>",
  "market_position_points": ["<факт 5-7 слов>", "<факт 5-7 слов>", "<факт 5-7 слов>"],
  "competitors": [
    {{"name": "<конкурент>", "status": "stronger|weaker|similar", "note": "<4-5 слов>"}},
    {{"name": "...", "status": "...", "note": "..."}},
    {{"name": "...", "status": "...", "note": "..."}}
  ],
  "market_risks": [
    {{"title": "<риск 4-6 слов>", "severity": "high|medium|low"}},
    {{"title": "...", "severity": "..."}},
    {{"title": "...", "severity": "..."}}
  ],
  "market_opportunities": [
    {{"title": "<возможность 4-6 слов>"}},
    {{"title": "..."}},
    {{"title": "..."}}
  ],
  "swot": {{
    "strengths": ["<сила компании 4-6 слов>", "<сила>", "<сила>"],
    "weaknesses": ["<слабость 4-6 слов>", "<слабость>", "<слабость>"],
    "opportunities": ["<возможность 4-6 слов>", "<возможность>", "<возможность>"],
    "threats": ["<угроза 4-6 слов>", "<угроза>", "<угроза>"]
  }},
  "risk_score": <0-100>,
  "trend": "bullish|bearish|neutral"
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
        return _parse_json(text)
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
    lang: str = "ru",
) -> dict[str, Any]:
    client = get_client(api_key)

    # Index all sources — hard caps to keep prompt size manageable
    news_idx, news_text = _build_indexed(
        news[:60], "N", "title", "link",
        lambda x: f"{x.get('title','')} — {x.get('source','')} ({x.get('pub_date','')})"
    )
    yn_idx, yn_text = _build_indexed(
        yandex_news[:20], "YN", "title", "link",
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

    # Emails by type — include confidence score so Claude weighs them properly
    def _email_text(emails: list[dict], prefix: str) -> tuple[list[dict], str]:
        idx = []
        for i, e in enumerate(emails):
            conf = e.get("confidence_score")
            conf_tag = f" conf={conf}/100" if conf is not None else ""
            label = f"[{e.get('position','')}]{conf_tag} {e.get('text','')[:300]}"
            idx.append({"id": f"{prefix}{i+1}", "label": label, "url": ""})
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

    from datetime import date
    today_str = date.today().strftime("%d.%m.%Y")

    lang_instruction = (
        "IMPORTANT: Write the entire response in English. All risk descriptions, advice, scenarios, and labels must be in English."
        if lang == "en" else
        "Пиши весь ответ на русском языке."
    )

    prompt = f"""{lang_instruction}

Ты — эксперт по корпоративным рискам. Проанализируй данные о компании «{company_name}» и составь структурированный отчёт по 5 категориям риска.

## Компания
Название: {company_name} | Сотрудников: {employee_count} | Отделы: {dept_text}
Дата анализа: {today_str}

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

## Веса источников (применяй при выставлении балла)

ВЫСОКИЙ ПРИОРИТЕТ (вес ×3):
- [HR], [PR], [GR] с пометкой conf≥70 — подтверждённые сигналы от сотрудников
- [G] — официальные регуляторные события (суд, штраф, проверка, отзыв лицензии)

СРЕДНИЙ ПРИОРИТЕТ (вес ×2):
- [HR], [PR], [GR] с conf 40–69 — сигналы средней достоверности
- [N], [YN] — статьи крупных СМИ (Forbes, Reuters, КазИнформ, Коммерсант, RBC)
- [R] с рейтингом 1–2 звезды — явное недовольство клиентов/сотрудников
- [FIN] — биржевые данные

НИЗКИЙ ПРИОРИТЕТ (вес ×0.5, не поднимай балл выше 45 на основе только этих источников):
- [IG], [TK], [YT], [X], [FB], [T] — соцсети (высокий уровень шума, субъективность)
- [N], [YN] — региональные/малоизвестные СМИ без явного указания источника
- [H] — вакансии (косвенный сигнал)

ПРАВИЛА АГРЕГАЦИИ:
- Одно событие подтверждается 3+ независимыми источниками → повышай балл на 10–15
- Единственный источник — соцсеть или анонимный аккаунт → не повышай балл выше 35
- Новость старше 60 дней → снижай её вес вдвое (учитывай дату публикации в скобках)
- Новость старше 180 дней → не используй как активный риск, только как фон
- Если данных по источнику нет — не выдумывай риски, пиши "Данных недостаточно"

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
  }},
  "scenarios": [
    {{
      "id": "A",
      "label": "Мягкое реагирование",
      "level": "low",
      "trigger": "<при каком условии активировать, 1 предложение>",
      "steps": [
        {{"action": "<конкретное действие 6-10 слов>", "owner": "<HR|PR|GR|CEO|Legal>", "deadline": "<сегодня|48ч|неделя>"}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}}
      ]
    }},
    {{
      "id": "B",
      "label": "Активное управление",
      "level": "medium",
      "trigger": "<при каком условии активировать>",
      "steps": [
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}}
      ]
    }},
    {{
      "id": "C",
      "label": "Кризисный протокол",
      "level": "high",
      "trigger": "<при каком условии активировать>",
      "steps": [
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}},
        {{"action": "...", "owner": "...", "deadline": "..."}}
      ]
    }}
  ]
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

    import asyncio as _asyncio
    for _attempt in range(4):
        try:
            response = await client.messages.create(
                model="claude-opus-4-6",
                max_tokens=16000,
                messages=[{"role": "user", "content": prompt}],
            )
            break
        except Exception as _e:
            _emsg = str(_e)
            if "overloaded" in _emsg and _attempt < 3:
                _wait = 15 * (2 ** _attempt)  # 15s, 30s, 60s
                logger.warning("Claude overloaded, retry %d/3 in %ds", _attempt + 1, _wait)
                await _asyncio.sleep(_wait)
            else:
                raise

    try:
        text = ""
        for block in response.content:
            if block.type == "text":
                text = block.text
                break

        result = _parse_json(text)
        cats_raw = result.get("categories", {})

        categories = {}
        for key in ("media", "hr", "gr", "pr", "market"):
            cat = cats_raw.get(key, {})
            categories[key] = {
                "score": int(cat.get("score", 50)),
                "risks": _resolve_risks(cat.get("risks", [])),
            }

        overall = int(result.get("overall_score", sum(c["score"] for c in categories.values()) // 5))

        # Parse scenarios
        raw_scenarios = result.get("scenarios", [])
        scenarios = []
        for sc in raw_scenarios:
            if isinstance(sc, dict):
                scenarios.append({
                    "id": str(sc.get("id", "")),
                    "label": str(sc.get("label", "")),
                    "level": str(sc.get("level", "medium")),
                    "trigger": str(sc.get("trigger", "")),
                    "steps": [
                        {
                            "action": str(s.get("action", "")),
                            "owner": str(s.get("owner", "")),
                            "deadline": str(s.get("deadline", "")),
                        }
                        for s in sc.get("steps", []) if isinstance(s, dict)
                    ],
                })

        return {
            "score": overall,
            "advice": str(result.get("advice", "")),
            "risks": _resolve_risks(result.get("risks", [])),
            "categories": categories,
            "scenarios": scenarios,
        }

    except Exception as e:
        logger.exception("AI analysis failed for company %s", company_name)
        fallback_cat = {"score": 50, "risks": [{"text": "Анализ недоступен: " + str(e), "sources": []}]}
        return {
            "score": 50,
            "advice": "Анализ временно недоступен. Проверьте API-ключ Anthropic.",
            "risks": [{"text": "Не удалось выполнить AI-анализ: " + str(e), "sources": []}],
            "categories": {k: fallback_cat for k in ("media", "hr", "gr", "pr", "market")},
            "scenarios": [],
        }


async def analyze_company_risks_parallel(
    company_name: str,
    news: list[dict[str, Any]],
    yandex_news: list[dict[str, Any]],
    threads_posts: list[dict[str, Any]],
    social: dict[str, list[dict[str, Any]]],
    vacancies: list[dict[str, Any]],
    regulatory_news: list[dict[str, Any]],
    market_news: list[dict[str, Any]],
    finance: dict[str, Any],
    hr_emails: list[dict[str, Any]],
    pr_emails: list[dict[str, Any]],
    gr_emails: list[dict[str, Any]],
    employees: list[dict[str, Any]],
    api_key: str,
    lang: str = "ru",
    on_stage: Any = None,  # optional callable(stage_text: str)
    competitor_news: list[dict[str, Any]] | None = None,  # {competitor_name, title, source, pub_date}
) -> dict[str, Any]:
    """
    11-agent distributed pipeline:
      Stage 1 — 5 parallel Haiku filter agents (100 articles each → relevant only)
      Stage 2 — 5 parallel Haiku category agents (one per media/hr/gr/pr/market)
      Stage 3 — 1 Haiku aggregator (overall score + advice + scenarios)
    """
    import asyncio as _aio
    from datetime import date as _date

    _MODEL = "claude-haiku-4-5-20251001"
    today_str = _date.today().strftime("%d.%m.%Y")

    def _haiku(prompt: str, max_tokens: int = 1024) -> dict | list:
        import anthropic as _a, time as _time
        # max_retries=0 — we handle retries ourselves with proper backoff
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        attempt = 0
        while True:
            try:
                msg = c.messages.create(
                    model=_MODEL, max_tokens=max_tokens,
                    system="JSON API. Respond ONLY with a valid JSON object. No markdown.",
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text.strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:])
                    raw = raw.rsplit("```", 1)[0].strip()
                return json.loads(raw)
            except Exception as _e:
                if "overloaded" in str(_e):
                    # exponential backoff capped at 120s — retry forever
                    wait = min(10 * (2 ** attempt), 120)
                    attempt += 1
                    logger.info("Haiku overloaded, waiting %ds (attempt %d)…", wait, attempt)
                    _time.sleep(wait)
                else:
                    raise

    # ── helpers ──────────────────────────────────────────────────────────
    def _fmt_articles(articles: list) -> str:
        return "\n".join(
            f"[{i}] {a.get('title', '')} | {a.get('source', '')} | {a.get('pub_date', '')}"
            for i, a in enumerate(articles)
        ) or "Нет статей."

    def _fmt_list(items: list, key: str = "title", limit: int = 15) -> str:
        return "\n".join(
            f"[{i+1}] {it.get(key, '')[:140]}" for i, it in enumerate(items[:limit])
        ) or "Нет данных."

    # ── STAGE 1: filter agents ────────────────────────────────────────────
    all_articles = (news or []) + (yandex_news or []) + (regulatory_news or []) + (market_news or [])
    all_articles = all_articles[:500]

    def _filter_batch(batch: list, idx: int) -> list[dict]:
        if not batch:
            return []
        try:
            result = _haiku(
                f"""Строгий фильтр для «{company_name}».
ВКЛЮЧАЙ только если в заголовке ЯВНО упомянута именно «{company_name}» и статья о ней.
ОТКЛОНЯЙ: другие компании/команды с похожим названием, общеотраслевые новости, реклама.
Верни JSON: {{"relevant": [0, 2, 5, ...]}} — индексы (0-based).

{_fmt_articles(batch)}""",
                max_tokens=256,
            )
            indices = result.get("relevant", [])
            filtered = [batch[i] for i in indices if isinstance(i, int) and 0 <= i < len(batch)]
            logger.info("Filter agent %d: %d/%d kept", idx, len(filtered), len(batch))
            return filtered
        except Exception as e:
            logger.warning("Filter agent %d failed: %r — keeping batch", idx, e)
            return batch[:20]

    batches = [all_articles[i:i+100] for i in range(0, len(all_articles), 100)]
    while len(batches) < 5:
        batches.append([])

    _n_batches = sum(1 for b in batches if b)
    # Sequential — one API call at a time to avoid 529 floods
    filter_results = []
    for _i, _b in enumerate(batches):
        if _b and on_stage:
            on_stage(f" Фильтрация статей ({_i + 1}/{_n_batches})…")
        filter_results.append(await _aio.to_thread(_filter_batch, _b, _i))
        if _i < len(batches) - 1 and _b:
            await _aio.sleep(3)

    seen_k: set[str] = set()
    filtered: list[dict] = []
    for batch_out in filter_results:
        for a in batch_out:
            k = (a.get("title") or "")[:60].lower()
            if k and k not in seen_k:
                seen_k.add(k); filtered.append(a)

    logger.info("Stage 1 done: %d/%d articles after filtering", len(filtered), len(all_articles))

    # ── STAGE 2: single combined analysis call (all 5 categories + aggregator) ──
    news_txt = _fmt_articles(filtered[:50])
    thr_txt  = "\n".join(f"[{i+1}] @{p.get('username','')}: {(p.get('captionText') or '')[:100]}"
                         for i, p in enumerate(threads_posts[:8])) or "Нет."
    ig_txt   = _fmt_list(social.get("instagram", []), "text", limit=8)
    yt_txt   = _fmt_list(social.get("youtube",   []), "text", limit=5)
    vac_txt  = _fmt_list(vacancies, "title", limit=10)
    hr_e_txt = "\n".join(f"- [{e.get('position','')}] {e.get('text','')[:150]}" for e in hr_emails[:5]) or ""
    pr_e_txt = "\n".join(f"- [{e.get('position','')}] {e.get('text','')[:150]}" for e in pr_emails[:5]) or ""
    gr_e_txt = "\n".join(f"- [{e.get('position','')}] {e.get('text','')[:150]}" for e in gr_emails[:5]) or ""

    finance = finance or {}
    stock = finance.get("stock")
    # fundamentals: either nested under "fundamentals" key (full market data)
    # or the finance dict itself contains sector/industry (passed from _run_combined_job)
    _fund = finance.get("fundamentals") or (finance if finance.get("sector") or finance.get("trailing_pe") else {})
    _fin_parts = []
    if stock:
        _fin_parts.append(f"Тикер: {stock.get('ticker')} | Цена: {stock.get('last')}")
    else:
        _fin_parts.append("Не торгуется публично.")
    if _fund.get("sector"):         _fin_parts.append(f"Сектор: {_fund['sector']}")
    if _fund.get("industry"):       _fin_parts.append(f"Индустрия: {_fund['industry']}")
    if _fund.get("market_cap_fmt"): _fin_parts.append(f"Рыночная кап.: {_fund['market_cap_fmt']}")
    if _fund.get("trailing_pe"):    _fin_parts.append(f"P/E: {_fund['trailing_pe']}")
    if _fund.get("beta"):           _fin_parts.append(f"Бета: {_fund['beta']}")
    if _fund.get("revenue_growth"): _fin_parts.append(f"Рост выручки: {_fund['revenue_growth']}")
    if _fund.get("profit_margins"): _fin_parts.append(f"Маржа прибыли: {_fund['profit_margins']}")
    if _fund.get("debt_to_equity"): _fin_parts.append(f"Долг/Капитал: {_fund['debt_to_equity']}")
    fin_txt = " | ".join(_fin_parts)

    if on_stage:
        on_stage(" Анализ рисков по категориям…")

    comp_txt = ""
    if competitor_news:
        by_name: dict[str, list] = {}
        for cn in competitor_news:
            n = cn.get("competitor_name", "?")
            by_name.setdefault(n, []).append(cn)
        parts = []
        for cname, arts in list(by_name.items())[:5]:
            lines = "\n".join(
                f"  - {a.get('title', '')[:120]} [{a.get('source', '')}]"
                for a in arts[:10]
            )
            parts.append(f"{cname}:\n{lines}")
        comp_txt = "\n\n".join(parts)

    def _combined_analysis() -> dict:
        comp_section = f"\n\n=== КОНКУРЕНТЫ (учти их активность при оценке рисков нашей компании) ===\n{comp_txt}" if comp_txt else ""
        prompt = f"""Компания: «{company_name}». Дата: {today_str}.

=== НОВОСТИ ({len(filtered)} релевантных) ===
{news_txt}

=== СОЦИАЛЬНЫЕ СЕТИ ===
Threads: {thr_txt}
Instagram: {ig_txt}
YouTube: {yt_txt}

=== HR / ВАКАНСИИ ===
{vac_txt}
{hr_e_txt}

=== GR / РЕГУЛЯТОРИКА ===
{gr_e_txt}

=== PR СИГНАЛЫ ===
{pr_e_txt}

=== РЫНОК ===
{fin_txt}{comp_section}

Проанализируй ВСЕ 5 категорий риска и дай итоговую оценку. Если конкуренты активны в какой-то категории — это повышает риск нашей компании в той же категории (market, pr, hr и т.д.).

JSON (строго такая структура):
{{
  "categories": {{
    "media":  {{"score": 0-100, "risks": [{{"text": "риск", "severity": "high|medium|low"}}]}},
    "hr":     {{"score": 0-100, "risks": [{{"text": "риск", "severity": "high|medium|low"}}]}},
    "gr":     {{"score": 0-100, "risks": [{{"text": "риск", "severity": "high|medium|low"}}]}},
    "pr":     {{"score": 0-100, "risks": [{{"text": "риск", "severity": "high|medium|low"}}]}},
    "market": {{"score": 0-100, "risks": [{{"text": "риск", "severity": "high|medium|low"}}]}}
  }},
  "overall_score": 0-100,
  "advice": "2-3 предложения руководству",
  "scenarios": [
    {{"id":"A","label":"Мягкое реагирование","level":"low","trigger":"условие","steps":[{{"action":"действие","owner":"HR|PR|GR|CEO","deadline":"сегодня|48ч|неделя"}}]}},
    {{"id":"B","label":"Активное управление","level":"medium","trigger":"условие","steps":[{{"action":"...","owner":"...","deadline":"..."}}]}},
    {{"id":"C","label":"Кризисный протокол","level":"high","trigger":"условие","steps":[{{"action":"...","owner":"...","deadline":"..."}}]}}
  ]
}}"""
        try:
            return _haiku(prompt, max_tokens=4096)
        except Exception as e:
            logger.warning("Combined analysis failed: %r", e)
            return {}

    raw = await _aio.to_thread(_combined_analysis)

    # Parse categories
    raw_cats = raw.get("categories", {})
    categories: dict[str, Any] = {}
    for cat in ("media", "hr", "gr", "pr", "market"):
        cd = raw_cats.get(cat, {})
        score = max(0, min(100, int(cd.get("score", 50))))
        risks = [
            {"text": (r.get("text", str(r)) if isinstance(r, dict) else str(r))[:300],
             "severity": r.get("severity", "medium") if isinstance(r, dict) else "medium",
             "sources": []}
            for r in cd.get("risks", [])[:5]
        ]
        categories[cat] = {"score": score, "risks": risks}
        logger.info("Category %s: score=%d risks=%d", cat, score, len(risks))

    overall = max(0, min(100, int(raw.get("overall_score",
        sum(v["score"] for v in categories.values()) // 5))))

    logger.info("Stage 2 done: overall=%d", overall)
    return {
        "score":      overall,
        "advice":     str(raw.get("advice", "")),
        "risks":      [],
        "categories": categories,
        "scenarios":  raw.get("scenarios", []),
    }


def filter_articles(
    articles: list[dict],  # list of {title, source, pub_date}
    company_name: str,
    api_key: str,
    on_stage=None,         # optional callable(str) — called before each batch
) -> list[int]:
    """
    AI-powered relevance filter. Returns sorted list of selected indices.
    Synchronous — call via asyncio.to_thread().
    Retries forever on 529 overloaded with exponential backoff capped at 120s.
    """
    import anthropic as _a, time as _time

    c = _a.Anthropic(api_key=api_key, max_retries=0)

    def _call(prompt: str) -> dict:
        attempt = 0
        while True:
            try:
                msg = c.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=512,
                    system="JSON API. Respond ONLY with a valid JSON object. No markdown.",
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text.strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
                return json.loads(raw)
            except Exception as _e:
                if "overloaded" in str(_e):
                    wait = min(10 * (2 ** attempt), 120)
                    attempt += 1
                    logger.info("filter_articles overloaded, waiting %ds (attempt %d)…", wait, attempt)
                    _time.sleep(wait)
                else:
                    logger.warning("filter_articles call failed: %r — keeping first 20 of batch", _e)
                    return {"relevant": list(range(min(20, len(articles))))}

    batches = [articles[i:i + 100] for i in range(0, len(articles), 100)]
    n_real = sum(1 for b in batches if b)
    all_selected: list[int] = []

    for bi, batch in enumerate(batches):
        if not batch:
            continue
        if on_stage:
            on_stage(f" Проверка статей ({bi + 1}/{n_real})…")

        offset = bi * 100
        lines = "\n".join(
            f"[{i}] {a.get('title', '')} | {a.get('source', '')} | {a.get('pub_date', '')}"
            for i, a in enumerate(batch)
        )
        result = _call(
            f"Ты — строгий фильтр новостей для риск-мониторинга компании «{company_name}».\n\n"
            f"ВКЛЮЧАЙ статью ТОЛЬКО если выполнены ВСЕ условия:\n"
            f"1. В заголовке или источнике ЯВНО упомянута именно «{company_name}» (точное совпадение названия)\n"
            f"2. Статья о деятельности ЭТОЙ конкретной компании (не однофамильцев, не команд, не других организаций)\n\n"
            f"ОТКЛОНЯЙ если:\n"
            f"- Другая компания/организация/команда с похожим или таким же названием\n"
            f"- Название упомянуто вскользь или как контекст, а статья о другом\n"
            f"- Общеотраслевые новости без прямого упоминания «{company_name}»\n"
            f"- Реклама, вакансии, прайс-листы, технические справки\n\n"
            f"Верни JSON: {{\"relevant\": [0, 2, 5, ...]}} — только индексы (0-based) прошедших фильтр.\n\n"
            f"Материалов: {len(batch)}\n{lines}"
        )
        indices = result.get("relevant", [])
        kept = [offset + i for i in indices if isinstance(i, int) and 0 <= i < len(batch)]
        all_selected.extend(kept)
        logger.info("filter_articles batch %d: %d/%d kept", bi, len(kept), len(batch))

        if bi < len(batches) - 1:
            _time.sleep(3)

    return sorted(all_selected)


def filter_competitor_articles(
    articles: list[dict],  # list of {competitor_name, title, source, pub_date}
    our_company: str,
    api_key: str,
) -> list[int]:
    """
    AI agent that selects competitor articles relevant to OUR company's risk.
    Returns sorted list of selected indices (same interface as filter_articles).
    Synchronous — call via asyncio.to_thread().
    """
    import anthropic as _a, time as _time

    if not articles:
        return []

    c = _a.Anthropic(api_key=api_key, max_retries=0)

    def _call(prompt: str) -> dict:
        attempt = 0
        while True:
            try:
                msg = c.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=512,
                    system="JSON API. Respond ONLY with a valid JSON object. No markdown.",
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text.strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
                return json.loads(raw)
            except Exception as _e:
                if "overloaded" in str(_e):
                    wait = min(10 * (2 ** attempt), 120)
                    attempt += 1
                    _time.sleep(wait)
                else:
                    logger.warning("filter_competitor_articles failed: %r", _e)
                    return {"relevant": []}

    all_selected: list[int] = []
    batches = [articles[i:i + 100] for i in range(0, len(articles), 100)]

    for bi, batch in enumerate(batches):
        if not batch:
            continue
        lines = "\n".join(
            f"[{i}] [{a.get('competitor_name','')}] {a.get('title','')} | {a.get('source','')} | {a.get('pub_date','')}"
            for i, a in enumerate(batch)
        )
        result = _call(
            f"Ты — аналитик рисков компании «{our_company}».\n\n"
            f"Ниже список новостей о КОНКУРЕНТАХ. Отбери только те, которые могут создать РИСК для «{our_company}»:\n"
            f"- Конкурент активно нанимает сотрудников (HR-риск: переманивание кадров)\n"
            f"- Конкурент получил крупный контракт или выиграл тендер (market-риск)\n"
            f"- Конкурент запустил новый продукт/проект (market-риск)\n"
            f"- Конкурент получил господдержку, льготы, регуляторное преимущество (gr-риск)\n"
            f"- Скандал у конкурента, который может переключить внимание СМИ на всю отрасль (media/pr-риск)\n"
            f"- Банкротство/проблемы конкурента — клиенты ищут замену (market-шанс, но и риск волатильности)\n\n"
            f"ОТКЛОНЯЙ: общий фон, нерелевантные упоминания, рекламу.\n\n"
            f"Верни JSON: {{\"relevant\": [0, 3, 7, ...]}} — только индексы (0-based).\n\n"
            f"{lines}"
        )
        indices = result.get("relevant", [])
        offset = bi * 100
        kept = [offset + i for i in indices if isinstance(i, int) and 0 <= i < len(batch)]
        all_selected.extend(kept)
        logger.info("filter_competitor_articles batch %d: %d/%d kept", bi, len(kept), len(batch))

        if bi < len(batches) - 1:
            _time.sleep(2)

    return sorted(all_selected)


_COMM_PROMPTS = {
    "press": (
        "Пресс-релиз для СМИ",
        """Напиши официальный пресс-релиз от имени компании для публикации в СМИ.
Тон: профессиональный, уверенный, без паники. Структура: заголовок, лид (1 абзац), основная часть (2-3 абзаца), цитата руководителя, контакты пресс-службы (placeholder).
Объём: 250-350 слов. Язык: русский.""",
    ),
    "internal": (
        "Письмо сотрудникам",
        """Напиши внутреннее письмо от CEO сотрудникам компании.
Тон: спокойный, открытый, поддерживающий. Структура: приветствие, описание ситуации без деталей, что делает компания, что это значит для сотрудников, призыв к спокойствию и продуктивности.
Объём: 150-200 слов. Язык: русский.""",
    ),
    "regulatory": (
        "Письмо регулятору",
        """Напиши официальное письмо в регуляторный орган (без указания конкретного органа — оставь [Наименование органа]).
Тон: формальный, юридический, конструктивный. Структура: обращение, описание ситуации, принятые меры, запрос или уведомление, подпись (placeholder).
Объём: 200-250 слов. Язык: русский.""",
    ),
}


async def generate_communication(
    company_name: str,
    comm_type: str,
    scenario: dict,
    risk_summary: str,
    api_key: str,
) -> str:
    client = get_client(api_key)

    _, comm_instruction = _COMM_PROMPTS.get(
        comm_type, _COMM_PROMPTS["press"]
    )

    scenario_steps = "\n".join(
        f"  {i+1}. {s.get('action','')} [{s.get('owner','')} / {s.get('deadline','')}]"
        for i, s in enumerate(scenario.get("steps", []))
    )

    prompt = f"""Ты — PR-директор и кризисный коммуникатор компании «{company_name}».

## Контекст ситуации
{risk_summary}

## Активированный сценарий: {scenario.get('label', '')} (уровень: {scenario.get('level', '')})
Триггер: {scenario.get('trigger', '')}
Шаги:
{scenario_steps}

## Задание
{comm_instruction}

Напиши только текст коммуникации, без пояснений и комментариев."""

    response = await client.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    for block in response.content:
        if hasattr(block, "text"):
            return block.text.strip()
    return ""


def generate_loss_scenarios(
    company_name: str,
    risks: list[dict],
    industry: str,
    api_key: str,
) -> list[dict]:
    """For the top risks, quantify loss in 3 branches: worst / base / reacted.
    Returns list aligned to input risks: [{title, category, worst:{...}, base:{...}, reacted:{...}}].
    Sync — call via asyncio.to_thread()."""
    import anthropic as _a, json as _json
    top = [r for r in (risks or []) if (r.get("title") or r.get("text"))][:5]
    if not top or not api_key:
        return []

    lines = "\n".join(
        f"[{i}] ({r.get('category','')}) {r.get('title') or r.get('text','')}"
        for i, r in enumerate(top)
    )
    prompt = (
        f"Компания: «{company_name}» (отрасль: {industry or 'н/д'}).\n\n"
        f"Для каждого риска оцени ПОТЕРИ в 3 сценариях. Будь конкретным с цифрами "
        f"(деньги в USD, отток клиентов в %, удар по репутации −баллов из 100, срок в днях).\n\n"
        f"Риски:\n{lines}\n\n"
        f"Верни JSON: {{\"scenarios\": [\n"
        f"  {{\"i\": 0,\n"
        f"    \"worst\":   {{\"money\": \"$500K\", \"churn\": \"25%\", \"rep\": -30, \"days\": 7,  \"text\": \"что произойдёт без реакции\"}},\n"
        f"    \"base\":    {{\"money\": \"$200K\", \"churn\": \"10%\", \"rep\": -15, \"days\": 14, \"text\": \"наиболее вероятный исход\"}},\n"
        f"    \"reacted\": {{\"money\": \"$50K\",  \"churn\": \"3%\",  \"rep\": -5,  \"days\": 30, \"text\": \"если среагировать быстро\"}}\n"
        f"  }}\n"
        f"]}} — ровно {len(top)} элементов, по индексу i."
    )

    def _call():
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            system="JSON API. Respond ONLY with valid JSON. No markdown.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        return _parse_json(raw).get("scenarios", [])

    try:
        scen = _call()
    except Exception as e:
        logger.warning("generate_loss_scenarios failed: %s", e)
        return []

    by_i = {s.get("i"): s for s in scen if isinstance(s, dict)}
    out = []
    for idx, r in enumerate(top):
        s = by_i.get(idx, {})
        if not s:
            continue
        out.append({
            "title":    (r.get("title") or r.get("text") or "")[:100],
            "category": r.get("category", ""),
            "worst":    s.get("worst", {}),
            "base":     s.get("base", {}),
            "reacted":  s.get("reacted", {}),
        })
    return out


# ── Risk Score formula ────────────────────────────────────────────────────
# Risk Score = (Severity × Probability × Velocity) + Signal Density
# The AI rates each of the 4 fundamentals 0–10 per category; the score is then
# computed deterministically here (transparent + tunable). Default mapping to
# 0–100: the multiplicative core (S×P×V) contributes up to CORE_W points,
# Signal Density adds up to DENSITY_W points.
RISK_FORMULA_CORE_W = 70
RISK_FORMULA_DENSITY_W = 30


def compute_risk_score(severity, probability, velocity, signal_density,
                       core_w: float = RISK_FORMULA_CORE_W,
                       density_w: float = RISK_FORMULA_DENSITY_W) -> int:
    """4-factor risk score. Each factor expected on a 0–10 scale.

    Risk Score = (Severity × Probability × Velocity) + Signal Density,
    normalised to 0–100 via (core_w, density_w)."""
    def _c(v):
        try:
            return max(0.0, min(10.0, float(v)))
        except (TypeError, ValueError):
            return 5.0
    s, p, v, d = _c(severity), _c(probability), _c(velocity), _c(signal_density)
    core = (s / 10.0) * (p / 10.0) * (v / 10.0)          # 0..1
    return int(round(max(0, min(100, core * core_w + (d / 10.0) * density_w))))


def analyze_with_history(
    company_name: str,
    own_rows: list[dict],
    comp_rows: list[dict],
    prev_fps: list[dict],
    api_key: str,
    prev_articles: list[dict] | None = None,
    category_hints: dict | None = None,
    scoring_instruction: str = "",
) -> dict[str, Any]:
    """
    Risk analysis with previous-run history context.
    own_rows / comp_rows — already AI-filtered rows from analysis_articles.
    prev_fps — [{key, title, category, score, consecutive_runs}] from last risk_run.
    Returns {score, advice, categories, risks, scenarios}.
    Sync — call via asyncio.to_thread().
    """
    import anthropic as _a, time as _t
    from datetime import date as _date

    today = _date.today().strftime("%d.%m.%Y")

    def _fmt_own(rows: list[dict]) -> str:
        by_src: dict[str, list] = {}
        for r in rows[:60]:
            key = r.get("source_type") or r.get("source") or "news"
            by_src.setdefault(key, []).append(r)
        lines = []
        for src, items in by_src.items():
            lines.append(f"[{src.upper()}]")
            for it in items[:10]:
                lines.append(f"  - {it['title']} ({it.get('pub_date', '')})")
        return "\n".join(lines) or "Нет материалов."

    def _fmt_comp(rows: list[dict]) -> str:
        by_name: dict[str, list] = {}
        for r in rows:
            n = r.get("competitor_name") or r.get("entity_name", "?")
            by_name.setdefault(n, []).append(r)
        parts = []
        for name, items in list(by_name.items())[:6]:
            arts = "\n".join(f"  - {r['title']}" for r in items[:8])
            parts.append(f"{name}:\n{arts}")
        return "\n\n".join(parts) or "Нет данных о конкурентах."

    def _fmt_prev(fps: list[dict]) -> str:
        if not fps:
            return "Первый анализ — история рисков отсутствует."
        lines = [
            f"  [{fp.get('category','?')}] {fp.get('title','')} "
            f"(score={fp.get('score','?')}, повторялся {fp.get('consecutive_runs',1)} раз)"
            for fp in fps[:20]
        ]
        return "\n".join(lines)

    def _fmt_prev_articles(articles: list[dict]) -> str:
        if not articles:
            return "Нет данных о предыдущих статьях."
        lines = []
        for a in articles[:20]:
            note = f" | ИИ: {a['risk_note']}" if a.get("risk_note") else ""
            lines.append(f"  - «{a.get('title','')}» [{a.get('source','')} {a.get('pub_date','')}]{note}")
        return "\n".join(lines)

    prev_arts_section = ""
    if prev_articles:
        prev_arts_section = f"""
=== СТАТЬИ ИЗ ПРЕДЫДУЩЕГО ПРОГОНА (контекст, не анализировать повторно) ===
{_fmt_prev_articles(prev_articles)}
"""

    # Department preferences (preferred sources + watch-keywords per category)
    hints_section = ""
    _cat_ru = {"media": "InfoField & Media", "hr": "HR", "gr": "Gov. Relations",
               "pr": "PR", "market": "Market"}
    if category_hints:
        _hl = []
        for cat, h in category_hints.items():
            parts = []
            if h.get("sources"):
                parts.append("приоритетные источники: " + ", ".join(h["sources"]))
            if h.get("keywords"):
                parts.append("следить за словами: " + ", ".join(h["keywords"]))
            if parts:
                _hl.append(f"  [{_cat_ru.get(cat, cat)}] " + "; ".join(parts))
        if _hl:
            hints_section = (
                "\n=== НАСТРОЙКИ ОТДЕЛОВ (учитывай при оценке категорий) ===\n"
                + "\n".join(_hl)
                + "\nЕсли встречаешь указанные ключевые слова или материалы из приоритетных "
                  "источников в соответствующей категории — придай им больший вес.\n"
            )

    scoring_instruction_section = ""
    if scoring_instruction and scoring_instruction.strip():
        scoring_instruction_section = (
            "\n=== ДОПОЛНИТЕЛЬНАЯ ИНСТРУКЦИЯ ПО ОЦЕНКЕ (от пользователя) ===\n"
            + scoring_instruction.strip()[:1500] + "\n"
        )

    prompt = f"""Компания: «{company_name}». Дата: {today}.

=== МАТЕРИАЛЫ ПО НАШЕЙ КОМПАНИИ (прошли строгий AI-фильтр) ===
{_fmt_own(own_rows)}

=== КОНКУРЕНТЫ (прошли строгий AI-фильтр) ===
{_fmt_comp(comp_rows)}

=== РИСКИ ИЗ ПРЕДЫДУЩЕГО АНАЛИЗА ===
{_fmt_prev(prev_fps)}{prev_arts_section}{hints_section}

=== 4 ОСНОВЫ RISK SCORE (оценивай каждую категорию по этим факторам, шкала 0–10) ===
1. severity (серьёзность) — насколько это опасно для компании
2. probability (вероятность) — насколько вероятно развитие/реализация
3. velocity (скорость) — насколько быстро это развивается
4. signal_density (плотность сигналов) — сколько подтверждений есть (внутренних + внешних источников)
Итоговый score категории считается по формуле: Risk Score = (Severity × Probability × Velocity) + Signal Density.
НЕ вычисляй score сам — просто честно проставь 4 фактора (0–10), систему посчитает балл сама.
{scoring_instruction_section}
Задача:
1. Выяви риски по 5 категориям на основе новых материалов и оцени 4 фактора для каждой категории
2. Сравни с предыдущими рисками — повторяющийся риск важнее нового (выше probability/velocity)
3. Учти активность конкурентов при оценке market-рисков
4. Отдельно выдели ПОЛОЖИТЕЛЬНЫЕ сигналы — новости которые снижают риск компании
5. Для КАЖДОГО риска определи тип:
   - "incident" — событие УЖЕ ПРОИЗОШЛО, есть факт в материалах (иск подан, скандал опубликован,
     сокращения объявлены, штраф выписан, авария случилась). Указывай тип incident ТОЛЬКО при наличии
     свершившегося факта в источниках.
   - "risk" — прогноз/угроза, которая МОЖЕТ случиться (возможен отток, репутация под угрозой, риск проверки).
   Инцидент важнее риска — это уже реальность, а не вероятность.

Положительные сигналы: рост прибыли, выигранные суды, награды, расширение бизнеса,
позитивные отзывы, успешные партнёрства, рост найма, ESG-инициативы и т.д.

Верни JSON строго такой структуры:
{{
  "categories": {{
    "media":  {{"severity": 0-10, "probability": 0-10, "velocity": 0-10, "signal_density": 0-10, "risks": [{{"title": "...", "text": "...", "severity": "high|medium|low", "type": "risk|incident"}}]}},
    "hr":     {{"severity": 0-10, "probability": 0-10, "velocity": 0-10, "signal_density": 0-10, "risks": [{{"title": "...", "text": "...", "severity": "high|medium|low", "type": "risk|incident"}}]}},
    "gr":     {{"severity": 0-10, "probability": 0-10, "velocity": 0-10, "signal_density": 0-10, "risks": [{{"title": "...", "text": "...", "severity": "high|medium|low", "type": "risk|incident"}}]}},
    "pr":     {{"severity": 0-10, "probability": 0-10, "velocity": 0-10, "signal_density": 0-10, "risks": [{{"title": "...", "text": "...", "severity": "high|medium|low", "type": "risk|incident"}}]}},
    "market": {{"severity": 0-10, "probability": 0-10, "velocity": 0-10, "signal_density": 0-10, "risks": [{{"title": "...", "text": "...", "severity": "high|medium|low", "type": "risk|incident"}}]}}
  }},
  "overall_score": 0-100,
  "advice": "2-3 предложения руководству",
  "risks": [
    {{"category": "media|hr|gr|pr|market", "title": "краткое название", "text": "описание", "severity": "high|medium|low", "score": 0-100, "type": "risk|incident"}}
  ],
  "positive_signals": [
    {{"category": "media|hr|gr|pr|market", "title": "краткое название", "weight": "low|medium|high"}}
  ],
  "scenarios": [
    {{"id":"A","label":"Мягкое реагирование","level":"low","trigger":"условие","steps":[{{"action":"...","owner":"HR|PR|GR|CEO","deadline":"сегодня|48ч|неделя"}}]}},
    {{"id":"B","label":"Активное управление","level":"medium","trigger":"условие","steps":[]}},
    {{"id":"C","label":"Кризисный протокол","level":"high","trigger":"условие","steps":[]}}
  ]
}}"""

    def _call() -> dict:
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        attempt = 0
        while True:
            try:
                msg = c.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=4096,
                    system="JSON API. Respond ONLY with valid JSON. No markdown.",
                    messages=[{"role": "user", "content": prompt}],
                )
                raw = msg.content[0].text.strip()
                if raw.startswith("```"):
                    raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    from json_repair import repair_json
                    return json.loads(repair_json(raw))
            except Exception as e:
                if "overloaded" in str(e):
                    wait = min(10 * (2 ** attempt), 120)
                    attempt += 1
                    logger.info("analyze_with_history overloaded, waiting %ds", wait)
                    _t.sleep(wait)
                else:
                    raise

    try:
        raw = _call()
    except Exception as e:
        logger.warning("analyze_with_history failed: %s", e)
        raw = {}

    raw_cats = raw.get("categories", {})
    categories: dict[str, Any] = {}
    for cat in ("media", "hr", "gr", "pr", "market"):
        cd = raw_cats.get(cat, {})
        # 4-factor formula: (Severity × Probability × Velocity) + Signal Density.
        # Falls back to a direct "score" if the model didn't return the factors.
        if any(k in cd for k in ("severity", "probability", "velocity", "signal_density")):
            score = compute_risk_score(
                cd.get("severity", 5), cd.get("probability", 5),
                cd.get("velocity", 5), cd.get("signal_density", 5))
            factors = {
                "severity":       cd.get("severity", 5),
                "probability":    cd.get("probability", 5),
                "velocity":       cd.get("velocity", 5),
                "signal_density": cd.get("signal_density", 5),
            }
        else:
            score = max(0, min(100, int(cd.get("score", 50))))
            factors = None
        risks = [
            {
                "title":    (r.get("title") or r.get("text") or "")[:100],
                "text":     r.get("text", "")[:300],
                "severity": r.get("severity", "medium") if isinstance(r, dict) else "medium",
                "sources":  [],
                "type":     "incident" if isinstance(r, dict) and r.get("type") == "incident" else "risk",
            }
            for r in cd.get("risks", [])[:5]
        ]
        categories[cat] = {"score": score, "risks": risks}
        if factors:
            categories[cat]["factors"] = factors

    overall = max(0, min(100,
        round(sum(v["score"] for v in categories.values()) / len(categories))
        if categories else (raw.get("overall_score") or 50)
    ))

    # Flat risks list — deduplicate against categories
    seen_titles: set[str] = set()
    flat_risks: list[dict] = []
    for cat, cd in categories.items():
        for r in cd["risks"]:
            t = r.get("title", "")[:60].lower()
            if t and t in seen_titles:
                continue
            seen_titles.add(t)
            flat_risks.append({
                "category": cat,
                "title":    r.get("title", ""),
                "text":     r.get("text", ""),
                "severity": r.get("severity", "medium"),
                "score":    50,
                "sources":  [],
                "type":     "incident" if r.get("type") == "incident" else "risk",
            })
    for r in raw.get("risks", []):
        if not isinstance(r, dict):
            continue
        t = (r.get("title") or r.get("text") or "")[:60].lower()
        if t and t in seen_titles:
            continue
        seen_titles.add(t)
        flat_risks.append({
            "category": r.get("category", "media"),
            "title":    (r.get("title") or r.get("text") or "")[:100],
            "text":     r.get("text", "")[:300],
            "severity": r.get("severity", "medium"),
            "score":    r.get("score", 50),
            "sources":  [],
            "type":     "incident" if r.get("type") == "incident" else "risk",
        })

    # Validate and clean positive signals
    positive_signals = [
        {
            "category": s.get("category", "media"),
            "title":    (s.get("title") or "")[:100],
            "weight":   s.get("weight", "low") if s.get("weight") in ("low", "medium", "high") else "low",
        }
        for s in raw.get("positive_signals", [])
        if isinstance(s, dict)
    ]

    return {
        "score":            overall,
        "advice":           str(raw.get("advice", "")),
        "risks":            flat_risks,
        "categories":       categories,
        "scenarios":        raw.get("scenarios", []),
        "positive_signals": positive_signals,
    }
