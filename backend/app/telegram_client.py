"""Telegram public channel search via Telethon."""
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
        from telethon import TelegramClient
        from telethon.tl.functions.contacts import SearchRequest

        client = TelegramClient(SESSION_PATH, api_id, api_hash)
        await client.connect()

        if not await client.is_user_authorized():
            logger.error("Telegram: not authorized — run auth script first")
            await client.disconnect()
            return []

        posts = []

        result = await client(SearchRequest(q=company_name, limit=limit))

        for message in result.messages:
            if not hasattr(message, "message") or not message.message:
                continue

            text = (message.message or "").strip()[:400]
            peer_id = message.peer_id

            chat_title = ""
            chat_username = ""
            for chat in result.chats:
                if hasattr(peer_id, "channel_id") and getattr(chat, "id", None) == peer_id.channel_id:
                    chat_title = getattr(chat, "title", "")
                    chat_username = getattr(chat, "username", "")
                    break
                if hasattr(peer_id, "chat_id") and getattr(chat, "id", None) == peer_id.chat_id:
                    chat_title = getattr(chat, "title", "")
                    break

            url = f"https://t.me/{chat_username}/{message.id}" if chat_username else ""
            date = message.date.strftime("%d.%m.%Y") if message.date else ""

            posts.append({
                "url":      url,
                "text":     text,
                "channel":  chat_title,
                "views":    getattr(message, "views", 0) or 0,
                "date":     date,
                "platform": "telegram",
            })

        await client.disconnect()
        logger.info("Telegram: %d posts for '%s'", len(posts), company_name)
        return posts

    except Exception as e:
        logger.error("Telegram error: %s", e)
        return []
