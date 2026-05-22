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


async def analyze_market_position(
    company_name: str,
    stock: dict[str, Any] | None,
    market_indices: list[dict[str, Any]],
    top_stocks: list[dict[str, Any]],
    news: list[dict[str, Any]],
    api_key: str,
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

    prompt = f"""Ты — инвестиционный аналитик. Проведи глубокий анализ рыночной позиции компании «{company_name}».

## Биржевые данные компании
{stock_text}

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
            on_stage(f"🔍 Фильтрация статей ({_i + 1}/{_n_batches})…")
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

    stock = finance.get("stock") if finance else None
    fin_txt = (f"Тикер: {stock.get('ticker')} | Цена: {stock.get('last')}"
               if stock else "Не торгуется публично.")

    if on_stage:
        on_stage("🧠 Анализ рисков по категориям…")

    def _combined_analysis() -> dict:
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
{fin_txt}

Проанализируй ВСЕ 5 категорий риска и дай итоговую оценку.

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
            on_stage(f"🔍 Проверка статей ({bi + 1}/{n_real})…")

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
