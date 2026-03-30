"""
Apify social media scrapers.
- Instagram  : apify~instagram-hashtag-scraper
- TikTok     : clockworks~tiktok-scraper
- Threads    : watcher.data~search-threads-by-keywords
- YouTube    : streamers~youtube-scraper

All scrapers apply a relevance filter — only posts that mention
the company name (or its keywords) are returned.
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

ACTOR_INSTAGRAM = "apify~instagram-hashtag-scraper"
ACTOR_TIKTOK    = "clockworks~tiktok-scraper"
ACTOR_THREADS   = "watcher.data~search-threads-by-keywords"
ACTOR_YOUTUBE   = "streamers~youtube-scraper"


async def close() -> None:
    await _http.aclose()


# ──────────────────────────────────────────────
# Relevance filter
# ──────────────────────────────────────────────

def _keywords(company_name: str) -> list[str]:
    """Extract meaningful keywords from company name (3+ chars, lowercase)."""
    words = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", company_name)
    # Add the full name too for exact match
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
    limit: int = 30,
) -> list[dict[str, Any]]:
    if not token:
        return []

    kws = _keywords(company_name)
    clean = re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9]", "", company_name).lower()
    extra = [re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9]", "", w).lower() for w in kws if len(w) >= 4]
    hashtags = list(dict.fromkeys([clean] + extra))
    hashtags = [h for h in hashtags if h][:5]

    url = f"{APIFY_BASE}/{ACTOR_INSTAGRAM}/run-sync-get-dataset-items?token={token}"
    payload = {
        "hashtags":      hashtags,
        "keywordSearch": False,
        "resultsLimit":  limit,
        "resultsType":   "posts",
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        relevant = []
        for item in items:
            caption = item.get("caption") or item.get("text") or ""
            if _is_relevant(caption, kws):
                relevant.append({
                    "url":      item.get("url") or item.get("shortCode", ""),
                    "text":     caption[:500],
                    "author":   item.get("ownerUsername") or item.get("username", ""),
                    "likes":    item.get("likesCount") or item.get("likesNumber", 0),
                    "comments": item.get("commentsCount", 0),
                    "platform": "instagram",
                })
        logger.info("Instagram: %d total → %d relevant for '%s'", len(items), len(relevant), company_name)
        return relevant
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
    max_posts: int = 30,
) -> list[dict[str, Any]]:
    if not token:
        return []

    kws = _keywords(company_name)
    url = f"{APIFY_BASE}/{ACTOR_THREADS}/run-sync-get-dataset-items?token={token}"
    payload = {
        "keywords":           [company_name],
        "sortByRecent":       True,
        "proxyConfiguration": {"useApifyProxy": False},
    }

    try:
        r = await _http.post(url, json=payload)
        r.raise_for_status()
        items: list[dict] = r.json()
        relevant = []
        for item in items:
            text = (
                item.get("captionText") or item.get("text") or
                item.get("content") or item.get("body") or ""
            )
            if _is_relevant(text, kws):
                relevant.append({
                    "postId":      item.get("postId", ""),
                    "postUrl":     item.get("postUrl") or item.get("url", ""),
                    "username":    item.get("username") or item.get("author", ""),
                    "captionText": text[:500],
                    "platform":    "threads",
                })
        logger.info("Threads: %d total → %d relevant for '%s'", len(items), len(relevant), company_name)
        return relevant
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
    limit: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    """Run all 4 scrapers in parallel and return combined dict."""
    instagram, tiktok, threads, youtube = await asyncio.gather(
        fetch_instagram_posts(company_name, token, limit),
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
