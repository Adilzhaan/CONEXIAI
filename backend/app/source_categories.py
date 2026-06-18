"""
Source categories — WHERE a signal came from (the channel), a dimension that is
INDEPENDENT of the monitoring zone (Media/HR/GR/PR/Market/CI = which function the
signal affects). A news item from a media outlet (source = Медиа) can belong to
the PR zone.

Single source of truth for classifying any article/signal into one of the 8
document categories. Deterministic domain / name / scraper-id mapping with a safe
fallback (media for web news).
"""
from urllib.parse import urlparse

# ── 8 categories from the document: key → Russian label ──────────────────────
SOURCE_CATEGORIES = {
    "media":      "Медиа",            # СМИ, ТВ, радио, агентства
    "social":     "Соцсети",          # LinkedIn, X, FB, Instagram, TikTok, Telegram, YouTube
    "forums":     "Форумы",           # Reddit, отраслевые форумы, сообщества
    "reviews":    "Отзывы",           # Google/App/Play reviews, Glassdoor, Indeed
    "regulators": "Регуляторы",       # министерства, суды, биржи, антимонопольные, эко
    "industry":   "Отраслевые",       # ассоциации, тендеры, закупки, аналитика
    "corporate":  "Корпоративные",    # CRM, ERP, HR, Helpdesk, почта, документы
    "production": "Производственные",  # датчики, оборудование, логи, телеметрия
}

# UI colours per category
SOURCE_CATEGORY_COLORS = {
    "media": "#5b9dff", "social": "#e2729f", "forums": "#f5b544", "reviews": "#38d6b8",
    "regulators": "#f2766e", "industry": "#a78bfa", "corporate": "#8fbcff", "production": "#cbb26a",
}

# domain substring → category (checked against the article URL)
_DOMAIN_MAP = {
    # social
    "instagram.com": "social", "t.me": "social", "telegram": "social", "x.com": "social",
    "twitter.com": "social", "facebook.com": "social", "fb.com": "social", "tiktok.com": "social",
    "youtube.com": "social", "youtu.be": "social", "linkedin.com": "social",
    "threads.net": "social", "vk.com": "social", "ok.ru": "social",
    # forums
    "reddit.com": "forums", "forum": "forums", "4pda": "forums", "pikabu.ru": "forums",
    "yvision.kz": "forums",
    # reviews
    "glassdoor": "reviews", "indeed.com": "reviews", "play.google.com": "reviews",
    "apps.apple.com": "reviews", "otzovik": "reviews", "flamp": "reviews",
    "2gis": "reviews", "hh.kz": "reviews", "hh.ru": "reviews", "enbek.kz": "reviews",
    # regulators (KZ gov / exchanges / courts)
    "gov.kz": "regulators", "egov.kz": "regulators", "kase.kz": "regulators",
    "sud.gov.kz": "regulators", "adilet.zan.kz": "regulators", "legalacts.egov.kz": "regulators",
    "nationalbank.kz": "regulators", "aifc.kz": "regulators", "stat.gov.kz": "regulators",
    # industry (tenders / procurement / associations)
    "goszakup.gov.kz": "industry", "zakup": "industry", "tender": "industry",
}

# our scraper id (source_type) → category
_SOURCE_TYPE_MAP = {
    "instagram": "social", "threads": "social", "youtube": "social", "telegram": "social",
    "reddit": "forums",
    "hh": "reviews",
    "kase": "regulators", "regulatory": "regulators",
    "gdelt": "media", "google_news": "media", "yahoo_news": "media",
    "serpapi": "media", "news": "media", "yandex": "media",
    "production": "production", "sensor": "production", "scada": "production",
    "crm": "corporate", "erp": "corporate", "helpdesk": "corporate", "email": "corporate",
}

# free-text source-name keyword → category
_NAME_MAP = {
    "instagram": "social", "telegram": "social", "youtube": "social", "tiktok": "social",
    "facebook": "social", "twitter": "social", "linkedin": "social", "threads": "social", "вконтакте": "social",
    "reddit": "forums", "форум": "forums", "forum": "forums",
    "glassdoor": "reviews", "indeed": "reviews", "отзыв": "reviews", "review": "reviews", "2gis": "reviews", "hh": "reviews",
    "регулятор": "regulators", "министерств": "regulators", " суд": "regulators", "биржа": "regulators",
    "kase": "regulators", "антимоноп": "regulators", "эколог": "regulators", "egov": "regulators",
    "тендер": "industry", "закуп": "industry", "ассоциац": "industry", "отраслев": "industry",
    "crm": "corporate", "erp": "corporate", "helpdesk": "corporate", "почта": "corporate",
    "датчик": "production", "телеметри": "production", "scada": "production", "оборудование": "production",
}


def classify(source: str | None = None, url: str | None = None,
             source_type: str | None = None) -> str:
    """Return one of the 8 SOURCE_CATEGORIES keys. Never empty (fallback: media)."""
    u = (url or "").lower()
    if u:
        host = ""
        try:
            host = urlparse(u if "//" in u else "//" + u).netloc
        except Exception:
            host = u
        hay = host or u
        # longest (most specific) domain first: goszakup.gov.kz before gov.kz
        for dom in sorted(_DOMAIN_MAP, key=len, reverse=True):
            if dom in hay:
                return _DOMAIN_MAP[dom]

    st = (source_type or "").lower().strip()
    if st in _SOURCE_TYPE_MAP:
        return _SOURCE_TYPE_MAP[st]

    s = (source or "").lower()
    for kw, cat in _NAME_MAP.items():
        if kw in s:
            return cat
    for kw, cat in _NAME_MAP.items():   # some feeds put a domain in the source name
        if kw in u:
            return cat

    return "media"


def label(cat: str | None) -> str:
    return SOURCE_CATEGORIES.get(cat or "", SOURCE_CATEGORIES["media"])


def color(cat: str | None) -> str:
    return SOURCE_CATEGORY_COLORS.get(cat or "", SOURCE_CATEGORY_COLORS["media"])
