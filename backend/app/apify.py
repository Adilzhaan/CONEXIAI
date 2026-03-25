import httpx
from typing import Any

_http = httpx.AsyncClient(timeout=120)

THREADS_ACTOR = "igview-owner~threads-search-scraper"


async def close() -> None:
    await _http.aclose()


async def fetch_threads_posts(
    company_name: str,
    token: str,
    max_posts: int = 20,
) -> list[dict[str, Any]]:
    """
    Запускает Apify Threads scraper и возвращает список постов с captionText.
    Каждый элемент: {"postId": ..., "postUrl": ..., "username": ..., "captionText": ...}
    """
    if not token:
        return []

    url = (
        f"https://api.apify.com/v2/acts/{THREADS_ACTOR}"
        f"/run-sync-get-dataset-items?token={token}"
    )
    payload = {
        "maxPosts": max_posts,
        "searchQuery": company_name,
        "sort": "top",
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict[str, Any]] = r.json()
        return [
            {
                "postId": item.get("postId", ""),
                "postUrl": item.get("postUrl", ""),
                "username": item.get("username", ""),
                "captionText": item.get("captionText", ""),
            }
            for item in items
            if item.get("captionText")
        ]
    except Exception:
        return []
