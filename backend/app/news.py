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


async def fetch_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_by_query(company_name, limit)


async def fetch_regulatory_news(company_name: str, limit: int = 6) -> list[dict[str, Any]]:
    """GR Risk — regulatory/legal news."""
    query = f"{company_name} регулятор проверка суд штраф лицензия закон"
    return await _fetch_by_query(query, limit)


async def fetch_market_news(company_name: str, limit: int = 6) -> list[dict[str, Any]]:
    """Market & Industry — financial/sector news."""
    query = f"{company_name} рынок инвесторы отрасль финансы конкуренты"
    return await _fetch_by_query(query, limit)
