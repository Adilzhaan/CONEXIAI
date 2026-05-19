"""Reddit search via PRAW (official API, free)."""
import asyncio
import logging
from typing import Any

logger = logging.getLogger("conexiai.reddit")


async def fetch_reddit_posts(
    company_name: str,
    client_id: str,
    client_secret: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if not client_id or not client_secret:
        return []

    def _sync():
        try:
            import praw
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent="CONEXIAI/1.0",
            )
            posts = []
            seen: set[str] = set()
            for post in reddit.subreddit("all").search(company_name, sort="new", limit=limit):
                if post.id in seen:
                    continue
                seen.add(post.id)
                from datetime import datetime, timezone
                date = datetime.fromtimestamp(post.created_utc, tz=timezone.utc).strftime("%d.%m.%Y")
                posts.append({
                    "url":     f"https://reddit.com{post.permalink}",
                    "title":   post.title,
                    "text":    (post.selftext or "")[:300],
                    "author":  str(post.author) if post.author else "",
                    "score":   post.score,
                    "comments": post.num_comments,
                    "subreddit": str(post.subreddit),
                    "date":    date,
                    "platform": "reddit",
                })
            logger.info("Reddit: %d posts for '%s'", len(posts), company_name)
            return posts
        except Exception as e:
            logger.error("Reddit error: %s", e)
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _sync)
