"""YouTube search via official Data API v3 (free, 10k units/day)."""
import asyncio
import logging
from typing import Any

logger = logging.getLogger("conexiai.youtube")


async def fetch_youtube_videos(
    company_name: str,
    api_key: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if not api_key:
        return []

    def _sync():
        try:
            from googleapiclient.discovery import build
            youtube = build("youtube", "v3", developerKey=api_key)

            response = youtube.search().list(
                q=company_name,
                part="snippet",
                type="video",
                maxResults=limit,
                order="date",
                relevanceLanguage="ru",
            ).execute()

            videos = []
            for item in response.get("items", []):
                snippet = item.get("snippet", {})
                video_id = item.get("id", {}).get("videoId", "")
                published = snippet.get("publishedAt", "")
                date = published[:10].replace("-", ".")[::-1].replace(".", ".", 2) if published else ""
                # Convert YYYY.MM.DD → DD.MM.YYYY
                if published:
                    parts = published[:10].split("-")
                    date = f"{parts[2]}.{parts[1]}.{parts[0]}" if len(parts) == 3 else ""

                videos.append({
                    "url":         f"https://www.youtube.com/watch?v={video_id}",
                    "title":       snippet.get("title", ""),
                    "description": (snippet.get("description", "") or "")[:200],
                    "channel":     snippet.get("channelTitle", ""),
                    "date":        date,
                    "thumbnail":   snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                    "platform":    "youtube",
                })

            logger.info("YouTube API: %d videos for '%s'", len(videos), company_name)
            return videos
        except Exception as e:
            logger.error("YouTube API error: %s", e)
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _sync)
