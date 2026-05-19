"""VK search via official API (free)."""
import asyncio
import logging
import os
from typing import Any

logger = logging.getLogger("conexiai.vk")


async def fetch_vk_posts(
    company_name: str,
    access_token: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not access_token:
        return []

    def _sync():
        try:
            import vk_api
            # Support both token and login/password
            login    = os.environ.get("VK_LOGIN", "")
            password = os.environ.get("VK_PASSWORD", "")
            if login and password:
                vk_session = vk_api.VkApi(login=login, password=password)
                vk_session.auth(token_only=True)
            else:
                vk_session = vk_api.VkApi(token=access_token)
            vk = vk_session.get_api()

            response = vk.newsfeed.search(
                q=company_name,
                count=limit,
                extended=1,
            )

            posts = []
            items = response.get("items", [])
            profiles = {p["id"]: p for p in response.get("profiles", [])}
            groups = {-g["id"]: g for g in response.get("groups", [])}

            for item in items:
                owner_id = item.get("owner_id") or item.get("from_id", 0)
                author = ""
                if owner_id and owner_id > 0:
                    p = profiles.get(owner_id, {})
                    author = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
                elif owner_id and owner_id < 0:
                    g = groups.get(owner_id, {})
                    author = g.get("name", "")

                post_id = item.get("id", "")
                url = f"https://vk.com/wall{owner_id}_{post_id}" if owner_id and post_id else ""

                from datetime import datetime, timezone
                date = ""
                ts = item.get("date", 0)
                if ts:
                    date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y")

                text = (item.get("text") or "").strip()[:400]
                likes = item.get("likes", {}).get("count", 0)
                reposts = item.get("reposts", {}).get("count", 0)
                comments = item.get("comments", {}).get("count", 0)

                posts.append({
                    "url":      url,
                    "text":     text,
                    "author":   author,
                    "likes":    likes,
                    "reposts":  reposts,
                    "comments": comments,
                    "date":     date,
                    "platform": "vk",
                })

            logger.info("VK: %d posts for '%s'", len(posts), company_name)
            return posts
        except Exception as e:
            logger.error("VK error: %s", e)
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _sync)
