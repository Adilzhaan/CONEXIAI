"""
Smart News Monitor — runs every hour.
For each company:
  1. Fetch latest news headlines (free, no AI)
  2. Compare hashes with last snapshot in Supabase
  3. If changed → run full risk analysis and save result
"""
import asyncio
import hashlib
import logging
from typing import Any

logger = logging.getLogger("conexiai.monitor")


def _hash_titles(news: list[dict]) -> list[str]:
    """Return sorted list of short hashes for news titles."""
    return sorted(
        hashlib.md5(item["title"].encode()).hexdigest()[:12]
        for item in news if item.get("title")
    )


def _has_new_news(current: list[str], stored: list[str]) -> bool:
    stored_set = set(stored)
    return any(h not in stored_set for h in current)


async def _get_snapshot(company_id: str, service_key: str, supabase) -> dict[str, Any] | None:
    try:
        rows = await supabase.rest_select_service(
            table="monitoring_snapshots",
            service_key=service_key,
            query_params={"company_id": f"eq.{company_id}"},
        )
        return rows[0] if rows else None
    except Exception:
        return None


async def _upsert_snapshot(
    company_id: str,
    service_key: str,
    supabase,
    news_hashes: list[str],
    last_score: int | None = None,
    ran_analysis: bool = False,
) -> None:
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()

    row: dict[str, Any] = {
        "company_id": company_id,
        "news_hashes": news_hashes,
        "last_checked_at": now,
    }
    if ran_analysis:
        row["last_run_at"] = now
    if last_score is not None:
        row["last_score"] = last_score

    try:
        await supabase.rest_upsert_service(
            table="monitoring_snapshots",
            service_key=service_key,
            rows=[row],
            on_conflict="company_id",
        )
    except Exception as e:
        logger.warning("Failed to upsert snapshot for %s: %s", company_id, e)


async def _refresh_market_cache(
    company_id: str,
    company_name: str,
    service_key: str,
    supabase,
    finance_client,
    news_client,
    ai_client,
    settings,
) -> None:
    try:
        from datetime import datetime, timezone
        market_data, company_news = await asyncio.gather(
            finance_client.fetch_full_market_data(company_name, twelve_key=settings.TWELVE_DATA_API_KEY),
            news_client.fetch_news(company_name),
        )
        ai_analysis = None
        if settings.ANTHROPIC_API_KEY:
            ai_analysis = await ai_client.analyze_market_position(
                company_name=company_name,
                stock=market_data.get("stock"),
                market_index=market_data.get("market_index"),
                top_stocks=market_data.get("top_stocks", []),
                news=company_news,
                api_key=settings.ANTHROPIC_API_KEY,
            )
        await supabase.rest_upsert_service(
            table="market_cache",
            service_key=service_key,
            rows=[{
                "company_id": company_id,
                "finance": market_data,
                "ai_analysis": ai_analysis,
                "news": company_news,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }],
            on_conflict="company_id",
        )
        logger.info("[monitor] Market cache updated for '%s'", company_name)
    except Exception as e:
        logger.warning("[monitor] Market cache update failed for '%s': %s", company_name, e)


async def _refresh_ci(
    company_id: str,
    company_name: str,
    service_key: str,
    supabase,
    ci_client,
    settings,
) -> None:
    try:
        from datetime import datetime, timezone
        competitors = await supabase.rest_select_service(
            table="competitors",
            service_key=service_key,
            select="id,name,website",
            query_params={"company_id": f"eq.{company_id}"},
        )
        if not competitors:
            return
        result = await ci_client.run_ci(company_name, competitors, settings.ANTHROPIC_API_KEY)
        await supabase.rest_upsert_service(
            table="ci_reports",
            service_key=service_key,
            rows=[{
                "company_id": company_id,
                "report": result,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }],
            on_conflict="company_id",
        )
        logger.info("[monitor] CI report updated for '%s'", company_name)
    except Exception as e:
        logger.warning("[monitor] CI update failed for '%s': %s", company_name, e)


async def check_and_run(
    company_id: str,
    company_name: str,
    service_key: str,
    supabase,
    news_client,
    ai_client,
    apify_client,
    hh_client,
    finance_client,
    settings,
) -> None:
    from datetime import datetime, timezone
    t0 = datetime.now(timezone.utc)
    logger.info("[monitor] ▶ Checking '%s' at %s", company_name, t0.strftime("%Y-%m-%d %H:%M:%S UTC"))

    # 1. Fetch only news headlines — cheap, no AI
    try:
        t_news = datetime.now(timezone.utc)
        news = await news_client.fetch_news(company_name, limit=12)
        elapsed = (datetime.now(timezone.utc) - t_news).total_seconds()
        logger.info("[monitor] ✓ News fetched for '%s': %d articles in %.1fs", company_name, len(news), elapsed)
    except Exception as e:
        logger.warning("[monitor] ✗ News fetch failed for '%s': %s", company_name, e)
        return

    current_hashes = _hash_titles(news)

    # 2. Compare with snapshot
    snapshot = await _get_snapshot(company_id, service_key, supabase)
    stored_hashes = snapshot.get("news_hashes") or [] if snapshot else []
    last_checked = snapshot.get("last_checked_at") if snapshot else None
    last_run = snapshot.get("last_run_at") if snapshot else None

    if last_checked:
        logger.info("[monitor] Last checked: %s | Last analysis: %s", last_checked, last_run or "never")
    else:
        logger.info("[monitor] No previous snapshot for '%s' — first run", company_name)

    if not _has_new_news(current_hashes, stored_hashes):
        logger.info("[monitor] ✓ No new news for '%s' — skipping analysis (last run: %s)", company_name, last_run or "never")
        await _upsert_snapshot(company_id, service_key, supabase, current_hashes)
        return

    new_count = sum(1 for h in current_hashes if h not in set(stored_hashes))
    logger.info("[monitor] ⚡ %d new article(s) for '%s' — starting full analysis", new_count, company_name)

    # 3. Full analysis (same logic as risks_run)
    try:
        t_gather = datetime.now(timezone.utc)
        logger.info("[monitor] → Fetching social, Yandex, vacancies, regulatory, market, finance for '%s'…", company_name)
        (
            social_data, yandex_news, vacancies,
            regulatory_news, market_news, finance_data,
        ) = await asyncio.gather(
            apify_client.fetch_all_social(company_name=company_name, token=settings.APIFY_TOKEN, limit=10),
            news_client.fetch_yandex_news(company_name, limit=8),
            hh_client.fetch_vacancies(company_name, limit=10),
            news_client.fetch_regulatory_news(company_name, limit=6),
            news_client.fetch_market_news(company_name, limit=6),
            finance_client.fetch_market_data(company_name) if settings.FINANCE_ENABLED else asyncio.sleep(0, result={"found": False}),
        )

        gather_elapsed = (datetime.now(timezone.utc) - t_gather).total_seconds()
        logger.info("[monitor] ✓ Data gathered for '%s' in %.1fs", company_name, gather_elapsed)

        logger.info("[monitor] → Sending to Claude AI for risk analysis: '%s'…", company_name)
        t_ai = datetime.now(timezone.utc)
        analysis = await ai_client.analyze_company_risks(
            company_name=company_name,
            employees=[],
            news=news,
            yandex_news=yandex_news,
            threads_posts=social_data.get("threads", []),
            social={
                "instagram": social_data.get("instagram", []),
                "tiktok":    social_data.get("tiktok", []),
                "youtube":   social_data.get("youtube", []),
            },
            reviews=[],
            vacancies=vacancies,
            regulatory_news=regulatory_news,
            market_news=market_news,
            finance=finance_data,
            hr_emails=[],
            pr_emails=[],
            gr_emails=[],
            api_key=settings.ANTHROPIC_API_KEY,
        )

        ai_elapsed = (datetime.now(timezone.utc) - t_ai).total_seconds()
        logger.info("[monitor] ✓ AI analysis done for '%s' in %.1fs, score=%s", company_name, ai_elapsed, analysis.get("score"))

        # Save risk_run using service key (bypasses RLS)
        await supabase.rest_insert_service(
            table="risk_runs",
            service_key=service_key,
            rows=[{
                "company_id": company_id,
                "status": "done",
                "score": analysis["score"],
                "advice": analysis["advice"],
                "risks": analysis["risks"],
                "categories": analysis["categories"],
            }],
        )

        score = analysis.get("score")
        prev_score = snapshot.get("last_score") if snapshot else None
        if prev_score is not None and score is not None:
            delta = int(score) - int(prev_score)
            if delta >= 10:
                logger.warning(
                    "[monitor] Risk score jumped +%d for '%s': %d → %d",
                    delta, company_name, prev_score, score,
                )

        await _upsert_snapshot(
            company_id, service_key, supabase,
            current_hashes, last_score=score, ran_analysis=True,
        )
        total_elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info("[monitor] ✅ Analysis saved for '%s' | score=%s | total=%.1fs", company_name, score, total_elapsed)

        # Also refresh market cache in background
        asyncio.ensure_future(_refresh_market_cache(
            company_id, company_name, service_key, supabase, finance_client,
            news_client, ai_client, settings,
        ))

        # Refresh CI report if ci_client available
        if ci_client is not None:
            asyncio.ensure_future(_refresh_ci(
                company_id, company_name, service_key, supabase, ci_client, settings,
            ))

    except Exception as e:
        logger.exception("[monitor] Full analysis failed for '%s': %s", company_name, e)
        await _upsert_snapshot(company_id, service_key, supabase, current_hashes)


async def run_monitoring_cycle(
    supabase,
    news_client,
    ai_client,
    apify_client,
    hh_client,
    finance_client,
    settings,
    ci_client=None,
) -> None:
    if not settings.SUPABASE_SERVICE_KEY:
        logger.warning("[monitor] SUPABASE_SERVICE_KEY not set — monitoring disabled")
        return
    if not settings.ANTHROPIC_API_KEY:
        logger.warning("[monitor] ANTHROPIC_API_KEY not set — monitoring disabled")
        return

    logger.info("[monitor] Starting hourly monitoring cycle")

    try:
        companies = await supabase.rest_select_service(
            table="companies",
            service_key=settings.SUPABASE_SERVICE_KEY,
            select="id,name",
        )
    except Exception as e:
        logger.error("[monitor] Failed to fetch companies: %s", e)
        return

    logger.info("[monitor] Monitoring %d companies", len(companies))

    for company in companies:
        company_id = company.get("id")
        company_name = company.get("name", "")
        if not company_id or not company_name:
            continue
        try:
            await check_and_run(
                company_id=company_id,
                company_name=company_name,
                service_key=settings.SUPABASE_SERVICE_KEY,
                supabase=supabase,
                news_client=news_client,
                ai_client=ai_client,
                apify_client=apify_client,
                hh_client=hh_client,
                finance_client=finance_client,
                settings=settings,
            )
            # Small pause between companies to avoid rate limits
            await asyncio.sleep(2)
        except Exception as e:
            logger.exception("[monitor] Unexpected error for '%s': %s", company_name, e)

    logger.info("[monitor] Cycle complete")
