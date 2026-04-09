"""
GR Sources Monitor — scrapes government, regulatory, and industry sites.
Fetches RSS feeds where available, falls back to HTML scraping.
Filters results by company name relevance.
"""
import asyncio
import re
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote, urljoin

import httpx

_http = httpx.AsyncClient(
    timeout=12,
    follow_redirects=True,
    headers={"User-Agent": "Mozilla/5.0 (compatible; CONEXIAI/1.0)"},
)


async def close() -> None:
    await _http.aclose()


# ── Source registry ───────────────────────────────────────────────────────────

SOURCES: list[dict[str, Any]] = [
    # ── Kazakhstan: Gov & Regulators ──
    {"name": "Акорда",          "url": "https://www.akorda.kz",       "rss": "https://www.akorda.kz/ru/rss",               "category": "gov_kz"},
    {"name": "gov.kz",          "url": "https://www.gov.kz",          "rss": None,                                          "category": "gov_kz"},
    {"name": "Парламент РК",    "url": "https://www.parlam.kz",       "rss": "https://www.parlam.kz/ru/rss",               "category": "gov_kz"},
    {"name": "Адилет",          "url": "https://adilet.zan.kz",       "rss": None,                                          "category": "gov_kz"},
    {"name": "Открытые НПА",    "url": "https://legalacts.egov.kz",   "rss": None,                                          "category": "gov_kz"},

    # ── Finance / Market ──
    {"name": "Нацбанк РК",      "url": "https://www.nationalbank.kz", "rss": "https://nationalbank.kz/rss/news_ru.xml",    "category": "finance_kz"},
    {"name": "АРРФР",           "url": "https://www.gov.kz/memleket/entities/ardfm", "rss": None,                          "category": "finance_kz"},
    {"name": "KASE",            "url": "https://www.kase.kz",         "rss": "https://kase.kz/ru/rss/news",               "category": "finance_kz"},
    {"name": "МФЦА",            "url": "https://www.aifc.kz",         "rss": None,                                          "category": "finance_kz"},

    # ── Ministries ──
    {"name": "Минфин РК",       "url": "https://www.gov.kz/memleket/entities/minfin",   "rss": None,                      "category": "ministry_kz"},
    {"name": "Минэнерго РК",    "url": "https://www.gov.kz/memleket/entities/energo",   "rss": None,                      "category": "ministry_kz"},
    {"name": "МТИ РК",          "url": "https://www.gov.kz/memleket/entities/mti",      "rss": None,                      "category": "ministry_kz"},
    {"name": "МИИР РК",         "url": "https://www.gov.kz/memleket/entities/industry", "rss": None,                      "category": "ministry_kz"},
    {"name": "МСХ РК",          "url": "https://www.gov.kz/memleket/entities/agro",     "rss": None,                      "category": "ministry_kz"},

    # ── Procurement / Tax ──
    {"name": "Госзакупки",      "url": "https://goszakupki.gov.kz",  "rss": None,                                          "category": "procurement_kz"},
    {"name": "КГД (налоги)",    "url": "https://kgd.gov.kz",         "rss": "https://kgd.gov.kz/ru/rss.xml",             "category": "procurement_kz"},

    # ── Industry: Oil & Gas ──
    {"name": "KazEnergy",       "url": "https://www.kazenergy.com",   "rss": "https://kazenergy.com/ru/rss",              "category": "industry_kz"},
    {"name": "Petrocouncil",    "url": "https://petrocouncil.kz",     "rss": None,                                         "category": "industry_kz"},
    {"name": "KAP",             "url": "https://kap.kz",              "rss": None,                                         "category": "industry_kz"},

    # ── Industry: Construction ──
    {"name": "KN.kz",           "url": "https://kn.kz",               "rss": "https://kn.kz/rss/news",                    "category": "construction_kz"},
    {"name": "Homsters",        "url": "https://homsters.kz",          "rss": None,                                         "category": "construction_kz"},

    # ── Industry: Manufacturing ──
    {"name": "Атамекен",        "url": "https://atameken.kz",         "rss": "https://atameken.kz/rss",                   "category": "industry_kz"},

    # ── Global Regulators ──
    {"name": "SEC",             "url": "https://www.sec.gov",          "rss": "https://www.sec.gov/rss/news/pressreleases.xml",          "category": "global"},
    {"name": "ESMA",            "url": "https://www.esma.europa.eu",   "rss": "https://www.esma.europa.eu/sites/default/files/rss.xml",  "category": "global"},
    {"name": "IMF",             "url": "https://www.imf.org",          "rss": "https://www.imf.org/en/rss-feeds",                        "category": "global"},
    {"name": "World Bank",      "url": "https://www.worldbank.org",    "rss": "https://feeds.worldbank.org/worldbank/all/rss.xml",       "category": "global"},
    {"name": "OECD",            "url": "https://www.oecd.org",         "rss": "https://www.oecd.org/rss/oecdrss.xml",                    "category": "global"},
    {"name": "BIS",             "url": "https://www.bis.org",          "rss": "https://www.bis.org/rss/All.xml",                         "category": "global"},
    {"name": "Federal Reserve", "url": "https://www.federalreserve.gov", "rss": "https://www.federalreserve.gov/feeds/press_all.xml",   "category": "global"},
    {"name": "ECB",             "url": "https://www.ecb.europa.eu",    "rss": "https://www.ecb.europa.eu/rss/press.html",               "category": "global"},

    # ── Kazakhstan Media ──
    {"name": "Tengri News",     "url": "https://tengrinews.kz",        "rss": "https://tengrinews.kz/rss/",                             "category": "media_kz"},
    {"name": "Informburo",      "url": "https://informburo.kz",        "rss": "https://informburo.kz/rss/news",                         "category": "media_kz"},
    {"name": "Капитал",         "url": "https://kapital.kz",           "rss": "https://kapital.kz/rss.xml",                             "category": "media_kz"},
    {"name": "Forbes KZ",       "url": "https://forbes.kz",            "rss": "https://forbes.kz/rss/",                                 "category": "media_kz"},
    {"name": "Kursiv",          "url": "https://kursiv.media",         "rss": "https://kursiv.media/rss",                               "category": "media_kz"},
    {"name": "InBusiness",      "url": "https://inbusiness.kz",        "rss": "https://inbusiness.kz/rss",                              "category": "media_kz"},
    {"name": "Bluescreen",      "url": "https://bluescreen.kz",        "rss": "https://bluescreen.kz/rss",                              "category": "media_kz"},
    {"name": "The Steppe",      "url": "https://the-steppe.com",       "rss": "https://the-steppe.com/rss",                             "category": "media_kz"},
    {"name": "Digital Business","url": "https://digitalbusiness.kz",   "rss": "https://digitalbusiness.kz/rss",                         "category": "media_kz"},
    {"name": "Weproject",       "url": "https://weproject.media",      "rss": "https://weproject.media/rss",                            "category": "media_kz"},
    {"name": "01tech",          "url": "https://01tech.kz",            "rss": "https://01tech.kz/rss",                                  "category": "media_kz"},
    {"name": "er10.kz",         "url": "https://er10.kz",              "rss": "https://er10.kz/rss",                                    "category": "media_kz"},
    {"name": "Zona.kz",         "url": "https://zona.kz",              "rss": "https://zona.kz/rss",                                    "category": "media_kz"},
    {"name": "LSM.kz",          "url": "https://lsm.kz",               "rss": "https://lsm.kz/rss",                                     "category": "media_kz"},
    {"name": "Власть",          "url": "https://vlast.kz",             "rss": "https://vlast.kz/rss",                                   "category": "media_kz"},
    {"name": "Zakon.kz",        "url": "https://zakon.kz",             "rss": "https://zakon.kz/rss.xml",                               "category": "media_kz"},
    {"name": "Nur.kz",          "url": "https://nur.kz",               "rss": "https://nur.kz/rss/",                                    "category": "media_kz"},
    {"name": "Sputnik KZ",      "url": "https://sputnik.kz",           "rss": "https://sputnik.kz/export/rss2/index.xml",               "category": "media_kz"},
    {"name": "24.kz",           "url": "https://24.kz",                "rss": "https://24.kz/rss",                                      "category": "media_kz"},
    {"name": "Казправда",       "url": "https://kazpravda.kz",         "rss": "https://kazpravda.kz/rss",                               "category": "media_kz"},
    {"name": "Егемен",          "url": "https://egemen.kz",            "rss": "https://egemen.kz/rss",                                  "category": "media_kz"},
    {"name": "Baigeнews",       "url": "https://baigenews.kz",         "rss": "https://baigenews.kz/rss",                               "category": "media_kz"},

    # ── Global Media ──
    {"name": "Reuters",         "url": "https://www.reuters.com",      "rss": "https://feeds.reuters.com/reuters/businessNews",          "category": "media_global"},
    {"name": "Bloomberg",       "url": "https://www.bloomberg.com",    "rss": "https://feeds.bloomberg.com/politics/news.rss",           "category": "media_global"},
    {"name": "FT",              "url": "https://www.ft.com",           "rss": "https://www.ft.com/rss/home",                            "category": "media_global"},
    {"name": "WSJ",             "url": "https://www.wsj.com",          "rss": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",            "category": "media_global"},
    {"name": "TechCrunch",      "url": "https://techcrunch.com",       "rss": "https://techcrunch.com/feed/",                           "category": "media_global"},
    {"name": "The Verge",       "url": "https://theverge.com",         "rss": "https://www.theverge.com/rss/index.xml",                 "category": "media_global"},
    {"name": "CNBC",            "url": "https://www.cnbc.com",         "rss": "https://www.cnbc.com/id/100003114/device/rss/rss.html",  "category": "media_global"},
    {"name": "Yahoo Finance",   "url": "https://finance.yahoo.com",    "rss": "https://finance.yahoo.com/rss/",                         "category": "media_global"},
]


# ── Fetchers ─────────────────────────────────────────────────────────────────

async def _fetch_rss(source: dict) -> list[dict]:
    rss_url = source["rss"]
    try:
        r = await _http.get(rss_url)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall("./channel/item")[:15]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            desc  = re.sub(r"<[^>]+>", " ", item.findtext("description") or "").strip()
            pub_raw = item.findtext("pubDate") or ""
            pub_date = ""
            if pub_raw:
                try:
                    pub_date = parsedate_to_datetime(pub_raw).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pub_date = pub_raw[:16]
            if title and link:
                items.append({
                    "title":    title,
                    "link":     link,
                    "snippet":  desc[:200],
                    "pub_date": pub_date,
                    "source":   source["name"],
                    "category": source["category"],
                })
        return items
    except Exception:
        return []


async def _fetch_html(source: dict) -> list[dict]:
    """Scrape HTML page and extract links+text as news items."""
    try:
        r = await _http.get(source["url"])
        r.raise_for_status()
        html = r.text

        # Extract all anchor tags with text
        anchors = re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', html, re.IGNORECASE | re.DOTALL)
        items = []
        base = source["url"]
        seen: set[str] = set()
        for href, text in anchors:
            title = re.sub(r"<[^>]+>", "", text).strip()
            title = re.sub(r"\s+", " ", title).strip()
            if len(title) < 20:
                continue
            if not href.startswith("http"):
                href = urljoin(base, href)
            if href in seen:
                continue
            seen.add(href)
            items.append({
                "title":    title[:150],
                "link":     href,
                "snippet":  "",
                "pub_date": "",
                "source":   source["name"],
                "category": source["category"],
            })
            if len(items) >= 10:
                break
        return items
    except Exception:
        return []


async def _fetch_source(source: dict) -> list[dict]:
    if source.get("rss"):
        items = await _fetch_rss(source)
        if items:
            return items
    # Fallback to HTML scraping
    return await _fetch_html(source)


# ── Relevance filter ─────────────────────────────────────────────────────────

def _keywords(company_name: str) -> list[str]:
    words = re.split(r"[\s\-_.,/]+", company_name.lower())
    return [w for w in words if len(w) >= 3]


def _is_relevant(item: dict, keywords: list[str]) -> bool:
    text = f"{item.get('title','')} {item.get('snippet','')}".lower()
    return any(kw in text for kw in keywords)


# ── Public API ────────────────────────────────────────────────────────────────

async def fetch_gr_news(
    company_name: str,
    categories: list[str] | None = None,
    limit_per_source: int = 10,
    relevance_filter: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetch GR news from all registered sources.
    Returns items relevant to company_name (or all if relevance_filter=False).
    categories: filter by category (e.g. ['gov_kz', 'finance_kz']) — None = all
    """
    sources = SOURCES
    if categories:
        sources = [s for s in SOURCES if s["category"] in categories]

    results = await asyncio.gather(*[_fetch_source(s) for s in sources])

    keywords = _keywords(company_name)
    all_items: list[dict] = []
    seen: set[str] = set()

    for items in results:
        for item in items[:limit_per_source]:
            key = item["title"].lower()[:60]
            if key in seen:
                continue
            if relevance_filter and keywords and not _is_relevant(item, keywords):
                continue
            seen.add(key)
            all_items.append(item)

    return all_items


async def fetch_gr_news_all(company_name: str, limit: int = 30) -> list[dict[str, Any]]:
    """All GR sources, relevance-filtered, capped at limit."""
    items = await fetch_gr_news(company_name, relevance_filter=True)
    return items[:limit]


async def fetch_gr_news_by_category(company_name: str) -> dict[str, list[dict[str, Any]]]:
    """Returns dict keyed by category."""
    categories = list({s["category"] for s in SOURCES})
    results = await asyncio.gather(
        *[fetch_gr_news(company_name, categories=[cat], relevance_filter=True) for cat in categories]
    )
    return {cat: items for cat, items in zip(categories, results)}


def get_sources_by_category() -> dict[str, list[dict]]:
    """Returns all sources grouped by category (for UI display)."""
    out: dict[str, list] = {}
    for s in SOURCES:
        out.setdefault(s["category"], []).append({"name": s["name"], "url": s["url"]})
    return out
