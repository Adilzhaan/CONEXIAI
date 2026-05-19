import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"), override=False)


def _get(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


class Settings:
    # Supabase
    SUPABASE_URL: str = _get("SUPABASE_URL")
    SUPABASE_ANON_KEY: str = _get("SUPABASE_ANON_KEY")
    # Service role key — bypasses RLS, used for public invite lookup
    SUPABASE_SERVICE_KEY: str = os.getenv("SUPABASE_SERVICE_KEY", "")

    # n8n
    N8N_FIND_RISKS_WEBHOOK_URL: str = _get("N8N_FIND_RISKS_WEBHOOK_URL")
    N8N_CEO_EMAIL_WEBHOOK_URL: str = _get("N8N_CEO_EMAIL_WEBHOOK_URL")
    # Optional — if set, n8n sends an invite email to new members
    N8N_INVITE_WEBHOOK_URL: str = os.getenv("N8N_INVITE_WEBHOOK_URL", "")

    # App
    SESSION_COOKIE_SECURE: bool = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"
    SESSION_ACCESS_COOKIE_NAME: str = os.getenv("SESSION_ACCESS_COOKIE_NAME", "sb_access_token")
    SESSION_REFRESH_COOKIE_NAME: str = os.getenv("SESSION_REFRESH_COOKIE_NAME", "sb_refresh_token")

    # Apify
    APIFY_TOKEN: str = os.getenv("APIFY_TOKEN", "")

    # Instagram (instagrapi)
    INSTAGRAM_USERNAME: str = os.getenv("INSTAGRAM_USERNAME", "")
    INSTAGRAM_PASSWORD: str = os.getenv("INSTAGRAM_PASSWORD", "")

    # Reddit (PRAW)
    REDDIT_CLIENT_ID: str = os.getenv("REDDIT_CLIENT_ID", "")
    REDDIT_CLIENT_SECRET: str = os.getenv("REDDIT_CLIENT_SECRET", "")

    # YouTube Data API v3
    YOUTUBE_API_KEY: str = os.getenv("YOUTUBE_API_KEY", "")

    # Telegram (Pyrogram)
    TELEGRAM_API_ID: int = int(os.getenv("TELEGRAM_API_ID") or "0")
    TELEGRAM_API_HASH: str = os.getenv("TELEGRAM_API_HASH", "")
    TWELVE_DATA_API_KEY: str = os.getenv("TWELVE_DATA_API_KEY", "")
    FINANCE_ENABLED: bool = os.getenv("FINANCE_ENABLED", "true").lower() == "true"

    # Anthropic
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")

    # Monitoring
    MONITORING_ENABLED: bool = os.getenv("MONITORING_ENABLED", "false").lower() == "true"
    MONITORING_INTERVAL_MINUTES: int = int(os.getenv("MONITORING_INTERVAL_MINUTES", "15"))
    MONITORING_NEW_ARTICLES_THRESHOLD: int = int(os.getenv("MONITORING_NEW_ARTICLES_THRESHOLD", "3"))

    # UI
    APP_NAME: str = os.getenv("APP_NAME", "CONEXIAI")
    SITE_URL: str = os.getenv("SITE_URL", "")


settings = Settings()

