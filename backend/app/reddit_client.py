"""Reddit search via public JSON API (no auth required)."""
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("conexiai.reddit")


_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.reddit.com/",
}


async def fetch_reddit_posts(
    company_name: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    url = "https://www.reddit.com/search.json"
    # combine original name + no-spaces version: e.g. "BI Group" OR "BIGROUP"
    compact = company_name.replace(" ", "")
    query = f'"{company_name}" OR "{compact}"' if compact != company_name else f'"{company_name}"'
    params = {"q": query, "sort": "new", "limit": limit, "type": "link", "t": "year"}

    try:
        async with httpx.AsyncClient(headers=_HEADERS, timeout=15) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        posts = []
        for child in data.get("data", {}).get("children", []):
            p = child.get("data", {})
            ts = p.get("created_utc", 0)
            date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y") if ts else ""
            posts.append({
                "url":       f"https://reddit.com{p.get('permalink', '')}",
                "title":     p.get("title", ""),
                "text":      (p.get("selftext") or "")[:300],
                "author":    p.get("author", ""),
                "score":     p.get("score", 0),
                "comments":  p.get("num_comments", 0),
                "subreddit": p.get("subreddit", ""),
                "date":      date,
                "platform":  "reddit",
            })

        # keep only posts that actually mention the company name
        name_variants = [company_name.lower(), company_name.replace(" ", "").lower()]
        filtered = [
            p for p in posts
            if any(v in (p["title"] + " " + p["text"]).lower() for v in name_variants)
        ]
        result = filtered if filtered else posts
        logger.info("Reddit: %d posts (%d after filter) for '%s'", len(posts), len(result), company_name)
        return result
    except Exception as e:
        logger.error("Reddit error: %s", e)
        return []
