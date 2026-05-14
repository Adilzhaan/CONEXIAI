"""
Apify social media scrapers.
- Instagram  : apify~instagram-post-scraper (by account URL)
- TikTok     : clockworks~tiktok-scraper
- Threads    : igview-owner~threads-search-scraper
- YouTube    : streamers~youtube-scraper
"""
import asyncio
import logging
import re
from typing import Any

import httpx

logger = logging.getLogger("conexiai")

# Sync runs can take 1–3 min per actor
_http = httpx.AsyncClient(timeout=180)

APIFY_BASE = "https://api.apify.com/v2/acts"

ACTOR_INSTAGRAM = "apify~instagram-post-scraper"
ACTOR_TIKTOK    = "clockworks~tiktok-scraper"
ACTOR_THREADS   = "igview-owner~threads-search-scraper"
ACTOR_YOUTUBE   = "streamers~youtube-scraper"


async def close() -> None:
    await _http.aclose()


# ──────────────────────────────────────────────
# Relevance filter
# ──────────────────────────────────────────────

def _keywords(company_name: str) -> list[str]:
    """Extract meaningful keywords from company name (2+ chars, lowercase)."""
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{2,}", company_name)
    kw = [w.lower() for w in words]
    kw.append(company_name.lower())
    return list(dict.fromkeys(kw))  # deduplicate, preserve order


def _is_relevant(text: str, keywords: list[str]) -> bool:
    """Return True if any keyword appears in the text."""
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in keywords)


# ──────────────────────────────────────────────
# Instagram
# ──────────────────────────────────────────────

async def fetch_instagram_posts(
    company_name: str,
    token: str,
    limit: int = 24,
    instagram_url: str | None = None,
) -> list[dict[str, Any]]:
    """Scrape Instagram posts by account URL. Skipped if no URL provided."""
    if not token or not instagram_url:
        return []

    url = f"{APIFY_BASE}/{ACTOR_INSTAGRAM}/run-sync-get-dataset-items?token={token}"
    payload = {
        "username":        [instagram_url],
        "resultsLimit":    limit,
        "dataDetailLevel": "basicData",
        "skipPinnedPosts": False,
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        posts = []
        for item in items:
            caption = (item.get("caption") or item.get("text") or "").strip()
            posts.append({
                "url":      item.get("url") or f"https://www.instagram.com/p/{item.get('shortCode', '')}",
                "text":     caption[:500],
                "author":   item.get("ownerUsername") or item.get("username", ""),
                "likes":    item.get("likesCount") or item.get("likesNumber", 0),
                "comments": item.get("commentsCount", 0),
                "platform": "instagram",
            })
        logger.info("Instagram: %d posts from '%s'", len(posts), instagram_url)
        return posts
    except Exception as e:
        logger.warning("Instagram scraper failed: %s", e)
        return []


# ──────────────────────────────────────────────
# TikTok
# ──────────────────────────────────────────────

async def fetch_tiktok_posts(
    company_name: str,
    token: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    if not token:
        return []

    kws = _keywords(company_name)
    clean = re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9]", "", company_name).lower()
    url = f"{APIFY_BASE}/{ACTOR_TIKTOK}/run-sync-get-dataset-items?token={token}"
    payload = {
        "hashtags":                    [clean],
        "resultsPerPage":              limit,
        "commentsPerPost":             0,
        "excludePinnedPosts":          False,
        "maxFollowersPerProfile":      0,
        "maxFollowingPerProfile":      0,
        "maxRepliesPerComment":        0,
        "proxyCountryCode":            "None",
        "scrapeRelatedVideos":         False,
        "shouldDownloadAvatars":       False,
        "shouldDownloadCovers":        False,
        "shouldDownloadMusicCovers":   False,
        "shouldDownloadSlideshowImages": False,
        "shouldDownloadVideos":        False,
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        relevant = []
        for item in items:
            text = item.get("text") or item.get("desc") or ""
            if _is_relevant(text, kws):
                relevant.append({
                    "url":      item.get("webVideoUrl") or item.get("videoUrl", ""),
                    "text":     text[:500],
                    "author":   item.get("authorName") or item.get("author", {}).get("uniqueId", ""),
                    "likes":    item.get("likeCount") or item.get("diggCount", 0),
                    "views":    item.get("playCount", 0),
                    "platform": "tiktok",
                })
        logger.info("TikTok: %d total → %d relevant for '%s'", len(items), len(relevant), company_name)
        return relevant
    except Exception as e:
        logger.warning("TikTok scraper failed: %s", e)
        return []


# ──────────────────────────────────────────────
# Threads
# ──────────────────────────────────────────────

async def fetch_threads_posts(
    company_name: str,
    token: str,
    max_posts: int = 20,
) -> list[dict[str, Any]]:
    if not token:
        return []

    url = f"{APIFY_BASE}/{ACTOR_THREADS}/run-sync-get-dataset-items?token={token}"
    payload = {
        "searchQuery": company_name,
        "maxPosts":    max(max_posts, 10),  # minimum 10 required by actor
        "sort":        "top",
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        posts = []
        for item in items:
            text     = (item.get("captionText") or "").strip()
            url_post = item.get("postUrl") or ""
            author   = item.get("username") or ""
            date     = item.get("takenAtFormatted") or ""
            posts.append({
                "postUrl":  url_post,
                "username": author,
                "text":     text[:500],
                "likes":    item.get("likeCount", 0),
                "date":     date,
                "platform": "threads",
            })
        logger.info("Threads: %d posts for '%s'", len(posts), company_name)
        return posts
    except Exception as e:
        logger.warning("Threads scraper failed: %s", e)
        return []


# ──────────────────────────────────────────────
# YouTube
# ──────────────────────────────────────────────

async def fetch_youtube_videos(
    company_name: str,
    token: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    if not token:
        return []

    kws = _keywords(company_name)
    url = f"{APIFY_BASE}/{ACTOR_YOUTUBE}/run-sync-get-dataset-items?token={token}"
    payload = {
        "searchQueries":                [company_name],
        "maxResults":                   limit,
        "maxResultsShorts":             0,
        "maxResultStreams":              0,
        "downloadSubtitles":            True,
        "hasCC":                        False,
        "hasLocation":                  False,
        "hasSubtitles":                 False,
        "is360":                        False,
        "is3D":                         False,
        "is4K":                         False,
        "isBought":                     False,
        "isHD":                         False,
        "isHDR":                        False,
        "isLive":                       False,
        "isVR180":                      False,
        "preferAutoGeneratedSubtitles": False,
        "saveSubsToKVS":                False,
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        relevant = []
        for item in items:
            title = item.get("title") or ""
            desc  = item.get("description") or item.get("text") or ""
            combined = f"{title} {desc}"
            if _is_relevant(combined, kws):
                relevant.append({
                    "url":         item.get("url") or item.get("videoUrl", ""),
                    "title":       title[:200],
                    "text":        desc[:400],
                    "author":      item.get("channelName") or item.get("channel", ""),
                    "views":       item.get("viewCount") or item.get("views", 0),
                    "likes":       item.get("likeCount") or item.get("likes", 0),
                    "platform":    "youtube",
                })
        logger.info("YouTube: %d total → %d relevant for '%s'", len(items), len(relevant), company_name)
        return relevant
    except Exception as e:
        logger.warning("YouTube scraper failed: %s", e)
        return []


# ──────────────────────────────────────────────
# Combined runner
# ──────────────────────────────────────────────

async def fetch_all_social(
    company_name: str,
    token: str,
    limit: int = 20,
    instagram_url: str | None = None,
    tiktok_url: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Run scrapers in parallel. Instagram/TikTok only run if account URL provided."""
    instagram, tiktok, threads, youtube = await asyncio.gather(
        fetch_instagram_posts(company_name, token, limit, instagram_url=instagram_url),
        fetch_tiktok_posts(company_name, token, limit),
        fetch_threads_posts(company_name, token, limit),
        fetch_youtube_videos(company_name, token, limit),
    )
    return {
        "instagram": instagram,
        "tiktok":    tiktok,
        "threads":   threads,
        "youtube":   youtube,
    }
