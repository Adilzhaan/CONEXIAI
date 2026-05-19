"""
Instagram scraping via instagrapi.
Searches posts mentioning the company by hashtag and keyword.
Session is saved to disk to avoid re-login on every request.
"""
import asyncio
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("conexiai.instagram")

SESSION_FILE = Path(__file__).parent.parent / "instagram_session.json"

_client = None


def _get_client(username: str, password: str):
    global _client
    if _client is not None:
        return _client

    from instagrapi import Client
    cl = Client()
    cl.delay_range = [2, 5]  # random delay between requests to avoid ban

    if SESSION_FILE.exists():
        try:
            cl.load_settings(str(SESSION_FILE))
            cl.login(username, password)
            logger.info("Instagram: loaded session from file")
            _client = cl
            return _client
        except Exception as e:
            logger.warning("Instagram: session load failed (%s), re-logging in", e)
            _client = None

    cl.login(username, password)
    cl.dump_settings(str(SESSION_FILE))
    logger.info("Instagram: logged in and saved session")
    _client = cl
    return _client


async def fetch_instagram_posts(
    company_name: str,
    username: str,
    password: str,
    limit: int = 15,
) -> list[dict[str, Any]]:
    if not username or not password:
        return []

    def _sync_fetch():
        try:
            cl = _get_client(username, password)

            # Search by hashtag (remove spaces, lowercase)
            tag = company_name.lower().replace(" ", "")
            posts = []
            seen: set[str] = set()

            try:
                medias = cl.hashtag_medias_recent(tag, amount=limit)
                for m in medias:
                    key = str(m.pk)
                    if key in seen:
                        continue
                    seen.add(key)
                    text = (m.caption_text or "").strip()
                    posts.append({
                        "url":      f"https://www.instagram.com/p/{m.code}/",
                        "text":     text[:400],
                        "author":   m.user.username if m.user else "",
                        "likes":    m.like_count or 0,
                        "comments": m.comment_count or 0,
                        "date":     m.taken_at.strftime("%d.%m.%Y") if m.taken_at else "",
                        "platform": "instagram",
                    })
            except Exception as e:
                logger.warning("Instagram hashtag search failed for '%s': %s", tag, e)

            # Also search by keyword in top posts if hashtag gave nothing
            if not posts:
                try:
                    results = cl.search_hashtags(company_name)
                    for h in results[:3]:
                        medias2 = cl.hashtag_medias_top(h.name, amount=5)
                        for m in medias2:
                            key = str(m.pk)
                            if key in seen:
                                continue
                            seen.add(key)
                            text = (m.caption_text or "").strip()
                            if company_name.lower() not in text.lower():
                                continue
                            posts.append({
                                "url":      f"https://www.instagram.com/p/{m.code}/",
                                "text":     text[:400],
                                "author":   m.user.username if m.user else "",
                                "likes":    m.like_count or 0,
                                "comments": m.comment_count or 0,
                                "date":     m.taken_at.strftime("%d.%m.%Y") if m.taken_at else "",
                                "platform": "instagram",
                            })
                except Exception as e:
                    logger.warning("Instagram keyword search failed: %s", e)

            logger.info("Instagram: %d posts found for '%s'", len(posts), company_name)
            return posts[:limit]

        except Exception as e:
            logger.error("Instagram fetch error: %s", e)
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _sync_fetch)
