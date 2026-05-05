import xml.etree.ElementTree as ET
from urllib.parse import quote
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from . import gr_sources as gr_client

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


async def _fetch_reddit(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    """Search Reddit via Atom RSS feed — no auth required."""
    query = company_name.replace('"', '').strip()
    _NS = "http://www.w3.org/2005/Atom"
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    async def _parse_feed(url: str) -> None:
        try:
            r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; CONEXIAI/1.0)"})
            if r.status_code != 200:
                return
            root = ET.fromstring(r.content)
            for entry in root.findall(f"{{{_NS}}}entry"):
                title = (entry.findtext(f"{{{_NS}}}title") or "").strip()
                if not title or title.lower()[:50] in seen:
                    continue
                seen.add(title.lower()[:50])
                # Link
                link_el = entry.find(f"{{{_NS}}}link")
                link = link_el.get("href", "") if link_el is not None else ""
                # Date
                updated = entry.findtext(f"{{{_NS}}}updated") or ""
                pub_date = updated[:10].replace("-", ".")[8:] + "." + updated[:10].replace("-", ".")[5:7] + "." + updated[:10].replace("-", ".")[0:4] if updated else ""
                # Subreddit from category
                cat = entry.find(f"{{{_NS}}}category")
                sub = cat.get("label", "reddit") if cat is not None else "reddit"
                sub = sub.replace("r/", "").strip()
                items.append({
                    "title":    title,
                    "link":     link,
                    "pub_date": pub_date,
                    "source":   f"Reddit r/{sub}",
                    "type":     "reddit",
                })
        except Exception:
            pass

    import asyncio
    tasks = [
        _parse_feed(f"https://www.reddit.com/search.rss?q={quote(query)}&sort=new&limit={limit}"),
        _parse_feed(f"https://www.reddit.com/search.rss?q={quote(query)}&sort=relevance&limit={limit}"),
    ]
    for sub in ["investing", "stocks", "business", "Kazakhstan"]:
        tasks.append(
            _parse_feed(f"https://www.reddit.com/r/{sub}/search.rss?q={quote(query)}&restrict_sr=1&sort=new&limit=5")
        )
    await asyncio.gather(*tasks)
    return items[:limit]


async def _fetch_gdelt(query: str, limit: int) -> list[dict[str, Any]]:
    """GDELT Doc 2.0 API — global news database, free, no key required."""
    try:
        from urllib.parse import quote as _quote
        url = (
            f"https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={_quote(query)}&mode=artlist&maxrecords={limit}"
            f"&format=json&sort=DateDesc"
        )
        r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        articles = r.json().get("articles") or []
        items: list[dict[str, Any]] = []
        for a in articles[:limit]:
            title = (a.get("title") or "").strip()
            url_  = (a.get("url")   or "").strip()
            if not title or not url_:
                continue
            seendateraw = a.get("seendate") or ""
            pub_date = ""
            if seendateraw:
                try:
                    from datetime import datetime
                    pub_date = datetime.strptime(seendateraw[:8], "%Y%m%d").strftime("%d.%m.%Y")
                except Exception:
                    pass
            items.append({
                "title":    title,
                "link":     url_,
                "pub_date": pub_date,
                "source":   a.get("domain") or "GDELT",
            })
        return items
    except Exception:
        return []


async def fetch_gdelt_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_gdelt(company_name, limit)


def _build_match_rules(company_name: str, ceo_name: str = "") -> dict:
    """
    Build match rules for a company name:
    - full_phrase: the whole name must appear as a substring (primary check)
    - all_words: all significant words (len >= 4) must appear (AND logic)
    - any_words: any significant word — only used as last-resort fallback
    - ceo_words: CEO name words for separate check
    """
    name_lower = company_name.lower().strip()
    words = [w for w in name_lower.split() if len(w) >= 4]

    rules: dict = {
        "full_phrase": name_lower,
        "all_words": words,
        "any_words": [w for w in name_lower.split() if len(w) >= 3],
        "ceo_words": [],
    }
    if ceo_name:
        ceo_words = [w.lower() for w in ceo_name.split() if len(w) >= 4]
        rules["ceo_words"] = ceo_words
    return rules


def _is_relevant(title: str, link: str, source: str, rules: dict, preferred_lower: list[str]) -> bool:
    t = title.lower()

    # Preferred source → always include
    if any(p in source or p in link.lower() for p in preferred_lower):
        return True

    # Full company name as substring (most reliable)
    if rules["full_phrase"] and rules["full_phrase"] in t:
        return True

    # All significant words present (AND logic) — catches "BI Group KZ" etc.
    all_words = rules["all_words"]
    if len(all_words) >= 2 and all(w in t for w in all_words):
        return True

    # Single-word company name → just check that one word (already covered by full_phrase)
    if len(all_words) == 1 and all_words[0] in t:
        return True

    # CEO name: all CEO words present
    ceo = rules["ceo_words"]
    if len(ceo) >= 2 and all(w in t for w in ceo):
        return True
    if len(ceo) == 1 and ceo[0] in t:
        return True

    return False


def _filter_relevant(
    items: list[dict[str, Any]],
    rules: dict,
    preferred_sources: list[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    preferred_lower = [s.lower() for s in preferred_sources]
    relevant, other = [], []
    for item in items:
        if _is_relevant(item["title"], item.get("link", ""), item.get("source", ""), rules, preferred_lower):
            relevant.append(item)
        else:
            other.append(item)
    return relevant, other


async def fetch_news(
    company_name: str,
    limit: int = 8,
    company: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch news with smart filtering based on company profile.
    Uses exact quoted query, CEO name, location for targeting.
    Filters results to keep only articles relevant to the company.
    """
    import asyncio

    co = company or {}
    ceo_name      = (co.get("ceo_name")      or "").strip()
    location      = (co.get("location")      or "").strip()
    news_sources  = co.get("news_sources")   or []

    # Build targeted queries
    exact_query = f'"{company_name}"'
    queries = [exact_query]

    if ceo_name:
        queries.append(f'"{ceo_name}"')

    if location:
        city = location.split(",")[0].strip()
        if city and city.lower() not in company_name.lower():
            queries.append(f'"{company_name}" {city}')

    # Parallel fetch across all queries + sources
    tasks = [_fetch_by_query(q, limit) for q in queries]
    tasks += [
        _fetch_yahoo(exact_query, limit),
        _fetch_gdelt(company_name, limit),
        _fetch_reddit(company_name, limit),
        gr_client.fetch_gr_news(company_name, categories=["media_kz", "media_global"], relevance_filter=True),
    ]
    results = await asyncio.gather(*tasks)

    # Merge with dedup
    seen: set[str] = set()
    all_items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                all_items.append(item)

    # Filter: relevant first, then fill with others if not enough
    rules = _build_match_rules(company_name, ceo_name)
    relevant, other = _filter_relevant(all_items, rules, list(news_sources))

    combined = relevant + other
    return combined[:limit * 2]


async def fetch_yandex_news(
    company_name: str,
    limit: int = 8,
    company: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    co = company or {}
    ceo_name = (co.get("ceo_name") or "").strip()
    news_sources = co.get("news_sources") or []

    import asyncio
    tasks = [_fetch_yandex(f'"{company_name}"', limit)]
    if ceo_name:
        tasks.append(_fetch_yandex(f'"{ceo_name}"', limit // 2))

    results = await asyncio.gather(*tasks)
    seen: set[str] = set()
    all_items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                all_items.append(item)

    rules = _build_match_rules(company_name, ceo_name)
    relevant, other = _filter_relevant(all_items, rules, list(news_sources))
    return (relevant + other)[:limit]


async def fetch_regulatory_news(
    company_name: str,
    limit: int = 6,
    company: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """GR Risk — regulatory/legal news filtered to company."""
    co = company or {}
    industry = (co.get("industry") or "").strip()
    location = (co.get("location") or "").strip()

    base = f'"{company_name}" регулятор проверка суд штраф лицензия закон'
    extra = f'"{company_name}" {industry} регулятор' if industry else ""

    import asyncio
    tasks = [_fetch_by_query(base, limit)]
    if extra:
        tasks.append(_fetch_by_query(extra, limit // 2))

    results = await asyncio.gather(*tasks)
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)

    rules = _build_match_rules(company_name, (co.get("ceo_name") or "").strip())
    relevant, other = _filter_relevant(items, rules, [])
    return (relevant + other)[:limit]


async def fetch_market_news(
    company_name: str,
    limit: int = 6,
    company: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Market & Industry — financial/sector news filtered to company."""
    co = company or {}
    industry = (co.get("industry") or "").strip()

    base = f'"{company_name}" рынок инвесторы финансы'
    extra = f'"{company_name}" {industry}' if industry else ""

    import asyncio
    tasks = [_fetch_by_query(base, limit)]
    if extra:
        tasks.append(_fetch_by_query(extra, limit // 2))

    results = await asyncio.gather(*tasks)
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)

    rules = _build_match_rules(company_name, (co.get("ceo_name") or "").strip())
    relevant, other = _filter_relevant(items, rules, [])
    return (relevant + other)[:limit]
