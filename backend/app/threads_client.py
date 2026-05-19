"""
Threads search via unofficial threads-api (Danie1).
No login required for public data — search by keyword.
"""
import asyncio
import logging
from typing import Any

logger = logging.getLogger("conexiai.threads")


async def fetch_threads_posts(company_name: str, limit: int = 20) -> list[dict[str, Any]]:
    try:
        from threads_api.src.threads_api import ThreadsAPI

        api = ThreadsAPI()
        posts: list[dict[str, Any]] = []
        seen: set[str] = set()

        # Search by keyword
        try:
            results = await api.search(company_name)
            threads_list = results if isinstance(results, list) else (results or {}).get("threads", [])
            for item in threads_list:
                _extract(item, posts, seen, limit)
        except Exception as e:
            logger.warning("Threads search failed: %s", e)

        logger.info("Threads: %d posts for '%s'", len(posts), company_name)
        return posts[:limit]

    except Exception as e:
        logger.error("Threads client error: %s", e)
        return []


def _extract(item: Any, posts: list, seen: set, limit: int) -> None:
    if len(posts) >= limit:
        return
    try:
        # item can be a thread object or dict
        if hasattr(item, "thread_items"):
            items = item.thread_items
        elif isinstance(item, dict):
            items = item.get("thread_items", [item])
        else:
            items = [item]

        for ti in (items or []):
            post = ti.post if hasattr(ti, "post") else (ti.get("post", ti) if isinstance(ti, dict) else ti)
            if not post:
                continue

            # Extract fields — handle both object and dict
            def _get(obj, *keys):
                for k in keys:
                    if hasattr(obj, k):
                        return getattr(obj, k)
                    if isinstance(obj, dict) and k in obj:
                        return obj[k]
                return None

            pk = str(_get(post, "pk", "id") or "")
            if pk in seen:
                continue
            seen.add(pk)

            text = (_get(post, "caption") or "")
            if hasattr(text, "text"):
                text = text.text
            elif isinstance(text, dict):
                text = text.get("text", "")
            text = str(text or "").strip()[:400]

            user = _get(post, "user") or {}
            username = (_get(user, "username") or "") if user else ""

            code = _get(post, "code") or pk
            url = f"https://www.threads.net/t/{code}" if code else ""

            likes = _get(post, "like_count") or 0
            taken_at = _get(post, "taken_at") or 0
            date = ""
            if taken_at:
                from datetime import datetime, timezone
                try:
                    date = datetime.fromtimestamp(int(taken_at), tz=timezone.utc).strftime("%d.%m.%Y")
                except Exception:
                    pass

            posts.append({
                "url":      url,
                "text":     text,
                "author":   str(username),
                "likes":    int(likes),
                "date":     date,
                "platform": "threads",
            })
    except Exception as e:
        logger.debug("Threads extract error: %s", e)
