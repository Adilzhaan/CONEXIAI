import xml.etree.ElementTree as ET
from urllib.parse import quote
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

_http = httpx.AsyncClient(timeout=8, follow_redirects=True)


async def close() -> None:
    await _http.aclose()


async def _fetch_by_query(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        url = f"https://news.google.com/rss/search?q={quote(query)}&hl=ru&gl=RU&ceid=RU:ru"
        r = await _http.get(url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items: list[dict[str, Any]] = []
        for item in root.findall("./channel/item")[:limit]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date_raw = item.findtext("pubDate", "")
            source_el = item.find("source")
            source = source_el.text.strip() if source_el is not None and source_el.text else ""
            pub_date = ""
            if pub_date_raw:
                try:
                    dt = parsedate_to_datetime(pub_date_raw)
                    pub_date = dt.strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pub_date = pub_date_raw[:16]
            if title and link:
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": source})
        return items
    except Exception:
        return []


async def _fetch_yandex(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        url = f"https://news.yandex.ru/yandsearch?rss=1&text={quote(query)}&lang=ru"
        r = await _http.get(url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items: list[dict[str, Any]] = []
        for item in root.findall("./channel/item")[:limit]:
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            pub_date_raw = item.findtext("pubDate", "")
            pub_date = ""
            if pub_date_raw:
                try:
                    pub_date = parsedate_to_datetime(pub_date_raw).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pub_date = pub_date_raw[:16]
            if title and link:
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": "Yandex News"})
        return items
    except Exception:
        return []


async def _fetch_yahoo(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={quote(query)}&newsCount={limit}&quotesCount=0"
        r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        news_items = r.json().get("news", [])
        items: list[dict[str, Any]] = []
        for n in news_items[:limit]:
            title = (n.get("title") or "").strip()
            link  = (n.get("link")  or "").strip()
            if not title or not link:
                continue
            pub_ts = n.get("providerPublishTime")
            pub_date = ""
            if pub_ts:
                from datetime import datetime, timezone
                pub_date = datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%d.%m.%Y %H:%M")
            items.append({
                "title":    title,
                "link":     link,
                "pub_date": pub_date,
                "source":   n.get("publisher") or "Yahoo News",
            })
        return items
    except Exception:
        return []


async def fetch_yahoo_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_yahoo(company_name, limit)


async def fetch_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    """Google News + Yahoo News combined, deduplicated by title."""
    import asyncio
    google, yahoo = await asyncio.gather(
        _fetch_by_query(company_name, limit),
        _fetch_yahoo(company_name, limit),
    )
    seen: set[str] = set()
    combined: list[dict[str, Any]] = []
    for item in google + yahoo:
        key = item["title"].lower()[:60]
        if key not in seen:
            seen.add(key)
            combined.append(item)
    return combined[:limit * 2]


async def fetch_yandex_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_yandex(company_name, limit)


async def fetch_regulatory_news(company_name: str, limit: int = 6) -> list[dict[str, Any]]:
    """GR Risk — regulatory/legal news."""
    query = f"{company_name} регулятор проверка суд штраф лицензия закон"
    return await _fetch_by_query(query, limit)


async def fetch_market_news(company_name: str, limit: int = 6) -> list[dict[str, Any]]:
    """Market & Industry — financial/sector news."""
    query = f"{company_name} рынок инвесторы отрасль финансы конкуренты"
    return await _fetch_by_query(query, limit)
