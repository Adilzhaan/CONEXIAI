"""YouTube search via scrapetube (no API key required)."""
import asyncio
import logging
from typing import Any

logger = logging.getLogger("conexiai.youtube")


async def fetch_youtube_videos(
    company_name: str,
    limit: int = 10,
) -> list[dict[str, Any]]:

    def _sync():
        try:
            import scrapetube
            videos = []
            for item in scrapetube.get_search(company_name, limit=limit, sort_by="upload_date"):
                video_id = item.get("videoId", "")
                title = item.get("title", {}).get("runs", [{}])[0].get("text", "")
                channel = item.get("longBylineText", {}).get("runs", [{}])[0].get("text", "")
                published = item.get("publishedTimeText", {}).get("simpleText", "")
                views = item.get("viewCountText", {}).get("simpleText", "")
                duration = item.get("lengthText", {}).get("simpleText", "")
                thumbnails = item.get("thumbnail", {}).get("thumbnails", [{}])
                thumbnail = thumbnails[-1].get("url", "") if thumbnails else ""

                desc_runs = item.get("detailedMetadataSnippets", [{}])
                description = ""
                if desc_runs:
                    description = "".join(
                        r.get("text", "") for r in desc_runs[0].get("snippetText", {}).get("runs", [])
                    )[:200]

                videos.append({
                    "url":         f"https://www.youtube.com/watch?v={video_id}",
                    "title":       title,
                    "description": description,
                    "channel":     channel,
                    "date":        published,
                    "thumbnail":   thumbnail,
                    "views":       views,
                    "duration":    duration,
                    "platform":    "youtube",
                })

            logger.info("YouTube: %d videos for '%s'", len(videos), company_name)
            return videos
        except Exception as e:
            logger.error("YouTube error: %s", e)
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _sync)
