"""
Competitive Intelligence (CI) module.
Collects news about competitors from Google News, Yandex, GDELT,
and optionally scrapes competitor websites. Then runs AI analysis.
"""
import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger("conexiai.ci")

_http = httpx.AsyncClient(timeout=10, follow_redirects=True)


async def close() -> None:
    await _http.aclose()


# ── News collection ──────────────────────────────────────────────────────────

async def _google_news(query: str, limit: int = 8) -> list[dict]:
    from urllib.parse import quote
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    try:
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=ru&gl=RU&ceid=RU:ru"
        r = await _http.get(url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall("./channel/item")[:limit]:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link",  "").strip()
            pub_raw = item.findtext("pubDate", "")
            pub_date = ""
            if pub_raw:
                try:
                    pub_date = parsedate_to_datetime(pub_raw).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pub_date = pub_raw[:16]
            src = item.find("source")
            source = src.text.strip() if src is not None and src.text else "Google News"
            if title and link:
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": source})
        return items
    except Exception:
        return []


async def _yandex_news(query: str, limit: int = 6) -> list[dict]:
    from urllib.parse import quote
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    try:
        url = f"https://news.yandex.ru/yandsearch?rss=1&text={quote(query)}&lang=ru"
        r = await _http.get(url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall("./channel/item")[:limit]:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link",  "").strip()
            pub_raw = item.findtext("pubDate", "")
            pub_date = ""
            if pub_raw:
                try:
                    pub_date = parsedate_to_datetime(pub_raw).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pub_date = pub_raw[:16]
            if title and link:
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": "Yandex News"})
        return items
    except Exception:
        return []


async def _gdelt_news(query: str, limit: int = 6) -> list[dict]:
    from urllib.parse import quote
    try:
        url = (
            f"https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={quote(query)}&mode=artlist&maxrecords={limit}"
            f"&format=json&sort=DateDesc"
        )
        r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        articles = r.json().get("articles") or []
        items = []
        for a in articles[:limit]:
            title = (a.get("title") or "").strip()
            url_  = (a.get("url")   or "").strip()
            if not title or not url_:
                continue
            seendate = a.get("seendate") or ""
            pub_date = ""
            if seendate:
                try:
                    from datetime import datetime
                    pub_date = datetime.strptime(seendate[:8], "%Y%m%d").strftime("%d.%m.%Y")
                except Exception:
                    pass
            items.append({"title": title, "link": url_, "pub_date": pub_date, "source": a.get("domain") or "GDELT"})
        return items
    except Exception:
        return []


async def _scrape_website(url: str) -> str:
    """Fetch competitor website and extract plain text (first 2000 chars)."""
    if not url:
        return ""
    try:
        if not url.startswith("http"):
            url = "https://" + url
        r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
        r.raise_for_status()
        import re
        text = re.sub(r"<[^>]+>", " ", r.text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:2000]
    except Exception:
        return ""


async def fetch_competitor_data(competitor: dict) -> dict[str, Any]:
    """Fetch all news + website data for a single competitor."""
    name    = competitor.get("name", "")
    website = competitor.get("website", "")

    google, yandex, gdelt, site_text = await asyncio.gather(
        _google_news(name, limit=8),
        _yandex_news(name, limit=6),
        _gdelt_news(name, limit=6),
        _scrape_website(website),
    )

    # Deduplicate by title
    seen: set[str] = set()
    news: list[dict] = []
    for item in google + yandex + gdelt:
        key = item["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            news.append(item)

    return {
        "id":        competitor.get("id"),
        "name":      name,
        "website":   website,
        "news":      news[:15],
        "site_text": site_text,
        "news_count": len(news),
    }


# ── AI Analysis ──────────────────────────────────────────────────────────────

async def analyze_ci(
    company_name: str,
    competitors_data: list[dict[str, Any]],
    api_key: str,
) -> dict[str, Any]:
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=api_key)

    comp_sections = []
    for c in competitors_data:
        news_lines = "\n".join(
            f"  - {n['title']} ({n.get('pub_date','')})" for n in c["news"][:8]
        ) or "  нет новостей"
        site = f"  Сайт: {c['site_text'][:500]}" if c.get("site_text") else ""
        comp_sections.append(
            f"### {c['name']}\n{news_lines}\n{site}"
        )

    prompt = f"""Ты — аналитик конкурентной разведки. Проанализируй ситуацию для компании «{company_name}».

## Данные о конкурентах
{chr(10).join(comp_sections)}

## Задание
Дай лаконичный анализ в JSON. Все тексты — короткие:
{{
  "summary": "<1 предложение: общий вывод о конкурентной среде>",
  "threat_level": "high|medium|low",
  "competitors": [
    {{
      "name": "<название конкурента>",
      "activity": "growing|stable|declining",
      "key_move": "<главный сигнал из новостей, 5-8 слов>",
      "threat": "high|medium|low",
      "impact": "<как это влияет на {company_name}, 1 предложение>"
    }}
  ],
  "opportunities": ["<возможность 5-7 слов>", "..."],
  "risks": ["<риск 5-7 слов>", "..."],
  "recommendations": ["<действие 5-7 слов>", "..."]
}}

Только JSON."""

    try:
        response = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = ""
        for block in response.content:
            if hasattr(block, "text"):
                text = block.text.strip()
                break
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
        import json
        return json.loads(text)
    except Exception as e:
        logger.exception("CI analysis failed for %s", company_name)
        return {
            "summary": "Анализ временно недоступен.",
            "threat_level": "medium",
            "competitors": [],
            "opportunities": [],
            "risks": [],
            "recommendations": [],
        }


async def score_competitor(name: str, news: list[dict], api_key: str) -> dict[str, Any]:
    """Lightweight risk score for a single competitor — uses Haiku for speed/cost."""
    import anthropic as _a, json as _json
    if not news:
        return {"score": None, "categories": {}, "summary": ""}
    news_lines = "\n".join(f"- {n['title']}" for n in news[:12])
    prompt = (
        f"Оцени риски компании «{name}» по новостям. Оценка 0–100 (0=нет рисков, 100=критические).\n\n"
        f"Новости:\n{news_lines}\n\n"
        f"Верни ТОЛЬКО JSON:\n"
        f'{{"score":<0-100>,"categories":{{"media":<0-100>,"hr":<0-100>,"gr":<0-100>,"pr":<0-100>,"market":<0-100>}},"summary":"<1 предложение>"}}'
    )
    try:
        client = _a.AsyncAnthropic(api_key=api_key)
        resp = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:-1])
        return _json.loads(text)
    except Exception as e:
        logger.warning("score_competitor %s: %r", name, e)
        return {"score": None, "categories": {}, "summary": ""}


async def run_ci(
    company_name: str,
    competitors: list[dict],
    api_key: str,
) -> dict[str, Any]:
    """Full CI pipeline: collect data → AI analysis + risk scores."""
    if not competitors:
        return {"summary": "Конкуренты не добавлены.", "threat_level": "low",
                "competitors": [], "opportunities": [], "risks": [], "recommendations": []}

    # Fetch all competitors in parallel
    competitors_data = await asyncio.gather(
        *[fetch_competitor_data(c) for c in competitors]
    )
    competitors_data = list(competitors_data)

    # Run CI analysis + individual risk scores in parallel
    risk_scores, ai = await asyncio.gather(
        asyncio.gather(*[
            score_competitor(c["name"], c["news"], api_key)
            for c in competitors_data
        ]),
        analyze_ci(company_name, competitors_data, api_key),
    )

    # Attach risk score to each competitor data entry
    for c, rs in zip(competitors_data, risk_scores):
        c["risk"] = rs

    # Merge news + risk into AI competitors list
    data_map = {c["name"]: c for c in competitors_data}
    for c in ai.get("competitors", []):
        cd = data_map.get(c["name"], {})
        c["news"] = cd.get("news", [])[:5]
        c["risk"] = cd.get("risk", {})

    return {"ai": ai, "competitors_data": competitors_data}
