import logging
import xml.etree.ElementTree as ET
from urllib.parse import quote
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from . import gr_sources as gr_client

logger = logging.getLogger("conexiai.news")
_http = httpx.AsyncClient(timeout=15, follow_redirects=True)


async def close() -> None:
    await _http.aclose()


async def _fetch_by_query(query: str, limit: int, region: str = "RU") -> list[dict[str, Any]]:
    # region: "RU"=Россия/СНГ, "KZ"=Казахстан, "US"=США(en), "GB"=UK/EU(en), "DE"=Германия
    if region == "KZ":
        locale = "hl=ru&gl=KZ&ceid=KZ:ru"
    elif region == "US":
        locale = "hl=en-US&gl=US&ceid=US:en"
    elif region == "GB":
        locale = "hl=en-GB&gl=GB&ceid=GB:en"
    elif region == "DE":
        locale = "hl=de&gl=DE&ceid=DE:de"
    else:
        locale = "hl=ru&gl=RU&ceid=RU:ru"
    try:
        url = f"https://news.google.com/rss/search?q={quote(query)}&{locale}"
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


async def _fetch_yandex_lang(query: str, limit: int, lang: str) -> list[dict[str, Any]]:
    try:
        url = f"https://news.yandex.ru/yandsearch?rss=1&text={quote(query)}&lang={lang}"
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
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": f"Yandex News ({lang})"})
        logger.info("Yandex News (%s): %d items", lang, len(items))
        return items
    except Exception as e:
        logger.warning("Yandex News (%s) error: %r", lang, e)
        return []


async def _fetch_yandex(query: str, limit: int) -> list[dict[str, Any]]:
    import asyncio
    results = await asyncio.gather(
        _fetch_yandex_lang(query, limit, "ru"),
        _fetch_yandex_lang(query, limit, "en"),
        _fetch_yandex_lang(query, limit, "kk"),
    )
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)
    return items



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


async def _fetch_kase(company_name: str, limit: int = 15) -> list[dict[str, Any]]:
    """Fetch KASE-related news via Google News with exchange-specific queries."""
    import asyncio
    exact = f'"{company_name}"'
    queries = [
        f'{exact} site:kase.kz',          # direct KASE publications
        f'{exact} KASE биржа',             # exchange news in media
        f'{exact} KASE облигации акции',   # financial instruments
    ]
    tasks = [_fetch_by_query(q, limit, region="RU") for q in queries]
    results = await asyncio.gather(*tasks)

    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                item["source"] = f"KASE / {item.get('source', 'Google News')}"
                items.append(item)
    logger.info("KASE News: %d items for '%s'", len(items), company_name)
    return items[:limit]


async def _fetch_bing(query: str, limit: int) -> list[dict[str, Any]]:
    try:
        url = f"https://www.bing.com/news/search?q={quote(query)}&format=rss&count={limit}"
        r = await _http.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1)"})
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
                items.append({"title": title, "link": link, "pub_date": pub_date, "source": "Bing News"})
        logger.info("Bing News: %d items", len(items))
        return items
    except Exception as e:
        logger.warning("Bing News error: %r", e)
        return []


def _pub_date_to_dt(item: dict):
    from datetime import datetime, timezone
    pd = (item.get("pub_date") or "").strip()
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(pd, fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


async def fetch_google_news_only(company_name: str, limit: int = 300) -> list[dict[str, Any]]:
    """Fetch from Google News (multi-region + date slices) + Bing + KASE in parallel."""
    import asyncio
    from datetime import datetime, timezone, timedelta

    query = f'"{company_name}"'
    parts = company_name.lower().split()
    strong = [w for w in parts if len(w) >= 4 and w not in _GENERIC_WORDS]
    bare_queries = [query] if strong else [query, company_name]

    # Date slices — Google News returns different articles per window
    now = datetime.now(timezone.utc)
    slices = [
        (now - timedelta(days=7),  now),
        (now - timedelta(days=30), now - timedelta(days=7)),
        (now - timedelta(days=90), now - timedelta(days=30)),
    ]
    def _date_q(base: str, start: datetime, end: datetime) -> str:
        return f"{base} after:{start.strftime('%Y-%m-%d')} before:{end.strftime('%Y-%m-%d')}"

    # Build all Google News query variants
    google_tasks = []
    for base_q in bare_queries:
        for region in ("RU", "KZ", "US", "GB"):
            # Plain (no date filter)
            google_tasks.append(_fetch_by_query(base_q, 100, region=region))
            # Each date slice
            for start, end in slices:
                google_tasks.append(_fetch_by_query(_date_q(base_q, start, end), 100, region=region))

    other_tasks = [
        _fetch_bing(query, 100),
        _fetch_bing(company_name, 100),
        _fetch_kase(company_name, 30),
    ]

    all_results = await asyncio.gather(*google_tasks, *other_tasks)

    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in all_results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)

    items.sort(key=_pub_date_to_dt, reverse=True)

    # Drop articles older than 90 days
    _cutoff = now - timedelta(days=90)
    items = [
        it for it in items
        if _pub_date_to_dt(it) == datetime.min.replace(tzinfo=timezone.utc)
        or _pub_date_to_dt(it) >= _cutoff
    ]

    return items[:limit]


async def fetch_yahoo_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_yahoo(company_name, limit)


async def fetch_reddit_news(company_name: str, limit: int = 15) -> list[dict[str, Any]]:
    return await _fetch_reddit(company_name, limit)


async def _fetch_serpapi_query(query: str, api_key: str, gl: str = "kz", hl: str = "ru", limit: int = 10) -> list[dict[str, Any]]:
    """Single SerpAPI Google News request — returns structured news results."""
    try:
        url = (
            f"https://serpapi.com/search"
            f"?engine=google_news"
            f"&q={quote(query)}"
            f"&gl={gl}&hl={hl}"
            f"&api_key={api_key}"
            f"&num={limit}"
        )
        r = await _http.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        items: list[dict[str, Any]] = []
        for art in (data.get("news_results") or [])[:limit]:
            title = (art.get("title") or "").strip()
            link  = (art.get("link")  or "").strip()
            if not title or not link:
                continue
            source = ""
            src = art.get("source")
            if isinstance(src, dict):
                source = src.get("name", "")
            elif isinstance(src, str):
                source = src
            pub_date = art.get("date", "")
            snippet  = art.get("snippet", "")
            items.append({
                "title":    title,
                "link":     link,
                "pub_date": pub_date,
                "source":   f"SerpAPI/{source}" if source else "SerpAPI",
                "snippet":  snippet,
            })
        logger.info("SerpAPI gl=%s: %d items for '%s'", gl, len(items), query)
        return items
    except Exception as e:
        logger.warning("SerpAPI gl=%s error: %r", gl, e)
        return []


async def fetch_serpapi_news(company_name: str, api_key: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    Fetch news via SerpAPI Google News — 3 requests (KZ/RU/US).
    Keeps total requests low since API quota is limited.
    """
    import asyncio
    query = f'"{company_name}"'
    results = await asyncio.gather(
        _fetch_serpapi_query(query, api_key, gl="kz", hl="ru", limit=limit),
        _fetch_serpapi_query(query, api_key, gl="ru", hl="ru", limit=limit),
        _fetch_serpapi_query(query, api_key, gl="us", hl="en", limit=limit),
    )
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)
    return items


async def _fetch_reddit(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    """Search Reddit via Atom RSS feed — no auth required."""
    query = f'"{company_name.strip()}"'  # exact phrase search
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
        logger.info("GDELT: %d items", len(items))
        return items
    except Exception as e:
        logger.warning("GDELT error: %r", e)
        return []


async def fetch_gdelt_news(company_name: str, limit: int = 8) -> list[dict[str, Any]]:
    return await _fetch_gdelt(company_name, limit)


import re as _re

_GENERIC_WORDS = {"corp", "company", "limited", "holding", "казахстан", "kazakhstan", "llc", "ltd", "group", "груп"}


def _build_match_rules(company_name: str, ceo_name: str = "", industry: str = "", location: str = "") -> dict:
    name_lower = company_name.lower().strip()
    parts = name_lower.split()

    # Strong words: 4+ chars, not generic — used for AND-match
    strong_words = [w for w in parts if len(w) >= 4 and w not in _GENERIC_WORDS]

    # Context keywords from industry and location — used to disambiguate short names
    context_words: list[str] = []
    for field in (industry, location):
        for w in field.lower().split():
            cleaned = _re.sub(r"[^a-zа-яёa-z0-9]", "", w)
            if len(cleaned) >= 4:
                context_words.append(cleaned)

    # Name is "weak" if it has no strong unique words (e.g. "BI Group", "AI Corp")
    is_weak_name = len(strong_words) == 0

    rules: dict = {
        "full_phrase":    name_lower,
        "strong_words":   strong_words,
        "is_weak_name":   is_weak_name,
        "context_words":  list(dict.fromkeys(context_words)),  # deduplicated
        "ceo_words":      [],
    }
    if ceo_name:
        rules["ceo_words"] = [w.lower() for w in ceo_name.split() if len(w) >= 4]
    return rules


def _word_in_text(word: str, text: str) -> bool:
    """Check word appears as whole word (not substring of another word)."""
    return bool(_re.search(r'(?<![а-яёa-z])' + _re.escape(word) + r'(?![а-яёa-z])', text))


def _is_relevant(title: str, link: str, source: str, rules: dict, preferred_lower: list[str]) -> bool:
    t = title.lower()
    phrase_match = rules["full_phrase"] and rules["full_phrase"] in t

    # Weak name (e.g. "BI Group"): require exact phrase + at least one context word from industry/location
    if rules["is_weak_name"]:
        if not phrase_match:
            return False
        context = rules["context_words"]
        if context and not any(w in t for w in context):
            return False
        return True

    # Strong name: exact phrase match is sufficient
    if phrase_match:
        return True

    # All strong words present as whole words — AND logic
    strong = rules["strong_words"]
    if len(strong) >= 2 and all(_word_in_text(w, t) for w in strong):
        return True

    # Single strong word — whole-word check with length guard
    if len(strong) == 1 and _word_in_text(strong[0], t) and len(t) > 25:
        return True

    # CEO name: all words present as whole words
    ceo = rules["ceo_words"]
    if len(ceo) >= 2 and all(_word_in_text(w, t) for w in ceo):
        return True

    return False


def _filter_relevant(
    items: list[dict[str, Any]],
    rules: dict,
    preferred_sources: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return only relevant items — no fallback to irrelevant ones."""
    return [
        item for item in items
        if _is_relevant(item["title"], item.get("link", ""), item.get("source", ""), rules, preferred_sources or [])
    ]


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
    industry      = (co.get("industry")      or "").strip()
    news_sources  = co.get("news_sources")   or []

    exact_query = f'"{company_name}"'

    # For weak/ambiguous names (no unique words ≥4 chars), append context to query
    _generic = _GENERIC_WORDS
    parts = company_name.lower().split()
    strong = [w for w in parts if len(w) >= 4 and w not in _generic]
    if not strong and (industry or location):
        ctx = industry.split()[0] if industry else location.split(",")[0].strip()
        context_query = f'"{company_name}" {ctx}'
    else:
        context_query = exact_query

    queries = [context_query]
    if context_query != exact_query:
        queries.append(exact_query)   # also try bare phrase as fallback

    if ceo_name:
        queries.append(f'"{ceo_name}"')

    if location:
        city = location.split(",")[0].strip()
        if city and city.lower() not in company_name.lower():
            queries.append(f'"{company_name}" {city}')

    # Parallel fetch across all queries + sources (RU + KZ + US + GB regions)
    tasks = []
    for q in queries:
        tasks.append(_fetch_by_query(q, limit, region="RU"))
        tasks.append(_fetch_by_query(q, limit, region="KZ"))
        tasks.append(_fetch_by_query(q, limit, region="US"))
        tasks.append(_fetch_by_query(q, limit, region="GB"))
    tasks += [
        _fetch_yahoo(context_query, limit),
        _fetch_gdelt(context_query, limit),
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

    industry = (co.get("industry") or "").strip()
    rules = _build_match_rules(company_name, ceo_name, industry=industry, location=location)
    relevant = _filter_relevant(all_items, rules, list(news_sources))

    # Drop articles older than 90 days, sort newest first
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)

    def _parse_pub_date(item: dict):
        pd = (item.get("pub_date") or "").strip()
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
            try:
                return datetime.strptime(pd, fmt).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                pass
        return datetime.min.replace(tzinfo=timezone.utc)

    fresh = [item for item in relevant if _parse_pub_date(item) >= cutoff]
    fresh.sort(key=_parse_pub_date, reverse=True)
    return fresh[:limit * 2]


async def fetch_yandex_news(
    company_name: str,
    limit: int = 8,
    company: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    co = company or {}
    ceo_name     = (co.get("ceo_name")  or "").strip()
    industry     = (co.get("industry")  or "").strip()
    location     = (co.get("location")  or "").strip()
    news_sources = co.get("news_sources") or []

    import asyncio
    parts_y = company_name.lower().split()
    strong_y = [w for w in parts_y if len(w) >= 4 and w not in _GENERIC_WORDS]
    if not strong_y and (industry or location):
        ctx_y = industry.split()[0] if industry else location.split(",")[0].strip()
        yandex_query = f'"{company_name}" {ctx_y}'
    else:
        yandex_query = f'"{company_name}"'

    tasks = [_fetch_yandex(yandex_query, limit)]
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

    rules = _build_match_rules(company_name, ceo_name, industry=industry, location=location)
    relevant = _filter_relevant(all_items, rules, list(news_sources))
    return relevant[:limit]


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
    relevant = _filter_relevant(items, rules)
    return relevant[:limit]


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
    relevant = _filter_relevant(items, rules)
    return relevant[:limit]


# ── hh.kz ─────────────────────────────────────────────────────────────────────

def _parse_hh_items(raw: list[dict]) -> list[dict[str, Any]]:
    from datetime import datetime as _dt
    items: list[dict[str, Any]] = []
    for v in raw:
        title = (v.get("name") or "").strip()
        if not title:
            continue
        url_ = v.get("alternate_url") or ""
        pub_raw = (v.get("published_at") or "")[:10]
        pub_date = ""
        if pub_raw:
            try:
                pub_date = _dt.strptime(pub_raw, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                pass
        salary = v.get("salary") or {}
        sal_str = ""
        if salary:
            fr, to, cur = salary.get("from"), salary.get("to"), salary.get("currency", "KZT")
            if fr and to:
                sal_str = f"{fr}–{to} {cur}"
            elif fr:
                sal_str = f"от {fr} {cur}"
            elif to:
                sal_str = f"до {to} {cur}"
        snippet = v.get("snippet") or {}
        desc = (snippet.get("requirement") or snippet.get("responsibility") or "")[:300]
        items.append({
            "title":    title,
            "link":     url_,
            "pub_date": pub_date,
            "source":   "hh.kz",
            "text":     (f"{sal_str} | {desc}" if sal_str else desc),
            "type":     "hr",
        })
    return items


_HH_HEADERS = {
    "User-Agent": "CONEXIAI/1.0 (hello@conexiai.kz)",
    "HH-User-Agent": "CONEXIAI/1.0 (hello@conexiai.kz)",
}
_HH_API = "https://api.hh.ru"  # hh.ru = parent; hh.kz redirects here


async def fetch_hh_vacancies(company_name: str, limit: int = 20) -> list[dict[str, Any]]:
    """Fetch open vacancies from hh.kz for HR risk monitoring (mass hiring/layoffs signal)."""
    import asyncio

    async def _by_employer() -> list[dict]:
        try:
            er = await _http.get(
                f"{_HH_API}/employers?text={quote(company_name)}&per_page=5",
                headers=_HH_HEADERS,
            )
            er.raise_for_status()
            employers = er.json().get("items") or []
            emp_id = None
            for emp in employers:
                if company_name.lower() in (emp.get("name") or "").lower():
                    emp_id = emp["id"]
                    break
            if not emp_id and employers:
                emp_id = employers[0]["id"]
            if not emp_id:
                return []
            vr = await _http.get(
                f"{_HH_API}/vacancies?employer_id={emp_id}&per_page={limit}&order_by=publication_time",
                headers=_HH_HEADERS,
            )
            vr.raise_for_status()
            logger.info("hh.kz employer %s: %d vacancies", emp_id, len(vr.json().get("items") or []))
            return _parse_hh_items(vr.json().get("items") or [])
        except Exception as e:
            logger.warning("hh.kz employer search: %r", e)
            return []

    async def _by_text() -> list[dict]:
        try:
            tr = await _http.get(
                f"{_HH_API}/vacancies?text={quote(company_name)}&per_page={limit}&order_by=publication_time",
                headers=_HH_HEADERS,
            )
            tr.raise_for_status()
            raw = [
                v for v in (tr.json().get("items") or [])
                if company_name.lower() in (v.get("employer") or {}).get("name", "").lower()
            ]
            return _parse_hh_items(raw)
        except Exception as e:
            logger.warning("hh.kz text search: %r", e)
            return []

    results = await asyncio.gather(_by_employer(), _by_text())
    seen: set[str] = set()
    items: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                items.append(item)
    logger.info("hh.kz: %d vacancies total for '%s'", len(items), company_name)
    return items[:limit]


# ── krisha.kz ─────────────────────────────────────────────────────────────────

async def fetch_krisha_listings(
    company_name: str,
    limit: int = 10,
    serpapi_key: str = "",
) -> list[dict[str, Any]]:
    """Fetch krisha.kz real estate listings mentioning the company.

    Uses SerpAPI (engine=google, site:krisha.kz) when key provided,
    otherwise falls back to Google News RSS site: queries.
    """
    import asyncio

    if serpapi_key:
        # SerpAPI web search — finds actual listing pages indexed by Google
        items: list[dict[str, Any]] = []
        for query in (
            f'site:krisha.kz "{company_name}"',
            f'site:krisha.kz {company_name} аренда офис',
        ):
            try:
                url = (
                    f"https://serpapi.com/search"
                    f"?engine=google"
                    f"&q={quote(query)}"
                    f"&gl=kz&hl=ru"
                    f"&num={limit}"
                    f"&api_key={serpapi_key}"
                )
                r = await _http.get(url, timeout=15)
                r.raise_for_status()
                for res in (r.json().get("organic_results") or [])[:limit]:
                    title = (res.get("title") or "").strip()
                    link  = (res.get("link")  or "").strip()
                    if title and link and "krisha.kz" in link:
                        snippet = res.get("snippet", "")
                        items.append({
                            "title":    title,
                            "link":     link,
                            "pub_date": "",
                            "source":   "krisha.kz",
                            "text":     snippet[:300],
                        })
            except Exception as e:
                logger.warning("krisha.kz SerpAPI: %r", e)

        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for item in items:
            key = item["title"].lower()[:60]
            if key not in seen:
                seen.add(key)
                deduped.append(item)
        logger.info("krisha.kz: %d listings (SerpAPI) for '%s'", len(deduped), company_name)
        return deduped[:limit]

    # Fallback: Google News RSS with site: filter (may return 0 for classifieds)
    results = await asyncio.gather(
        _fetch_by_query(f'site:krisha.kz "{company_name}"', limit, region="RU"),
        _fetch_by_query(f'site:krisha.kz {company_name} аренда', limit, region="RU"),
    )
    seen2: set[str] = set()
    fb: list[dict[str, Any]] = []
    for batch in results:
        for item in batch:
            key = item["title"].lower()[:60]
            if key not in seen2:
                seen2.add(key)
                item["source"] = "krisha.kz"
                fb.append(item)
    logger.info("krisha.kz: %d listings (RSS fallback) for '%s'", len(fb), company_name)
    return fb[:limit]
