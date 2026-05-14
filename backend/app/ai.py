import json
import logging
import re
from typing import Any

import anthropic

logger = logging.getLogger("conexiai")

_client: anthropic.AsyncAnthropic | None = None


def _parse_json(text: str) -> Any:
    """Extract and parse JSON from Claude's response, tolerating common formatting issues."""
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        text = text.strip()
    # Extract first {...} block in case there's surrounding prose
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        text = text[start:end + 1]
    # Remove trailing commas before } or ]  (invalid in JSON, common in Claude output)
    text = re.sub(r",\s*([}\]])", r"\1", text)
    return json.loads(text)


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

    prompt = f"""Ты — эксперт по корпоративным рискам. Проанализируй данные о компании «{company_name}» и составь структурированный отчёт по 5 категориям риска.

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
