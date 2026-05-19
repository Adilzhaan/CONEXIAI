"""
Telegram public channel search via Pyrogram.
Requires: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE (one-time auth).
Session saved to telegram_session.session after first login.
"""
import asyncio
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("conexiai.telegram")

SESSION_PATH = str(Path(__file__).parent.parent / "telegram_session")


async def fetch_telegram_posts(
    company_name: str,
    api_id: int,
    api_hash: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not api_id or not api_hash:
        return []

    try:
        from pyrogram import Client
        from pyrogram.errors import SessionPasswordNeeded

        app = Client(
            SESSION_PATH,
            api_id=api_id,
            api_hash=api_hash,
        )

        posts = []

        async with app:
            async for message in app.search_global(company_name, limit=limit):
                if not message.text and not message.caption:
                    continue
                text = (message.text or message.caption or "").strip()[:400]
                chat = message.chat
                chat_title = getattr(chat, "title", "") or getattr(chat, "username", "") or ""
                chat_username = getattr(chat, "username", "")
                url = f"https://t.me/{chat_username}/{message.id}" if chat_username else ""

                from datetime import timezone
                date = message.date.strftime("%d.%m.%Y") if message.date else ""

                posts.append({
                    "url":      url,
                    "text":     text,
                    "channel":  chat_title,
                    "views":    getattr(message, "views", 0) or 0,
                    "date":     date,
                    "platform": "telegram",
                })

        logger.info("Telegram: %d posts for '%s'", len(posts), company_name)
        return posts

    except Exception as e:
        logger.error("Telegram error: %s", e)
        return []
