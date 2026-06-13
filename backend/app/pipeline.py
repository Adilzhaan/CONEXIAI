"""
Pipeline v2 — per-source scraping agents → date filter → AI semantic filter
→ analysis_articles staging → risk analysis with history → risk escalation
"""
import asyncio
import hashlib
import logging
import uuid as _uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

logger = logging.getLogger("conexiai.pipeline")

CUTOFF_DAYS = 30


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_pub_date(s: str) -> datetime | None:
    if not s:
        return None
    ds = s.strip().lower()
    if any(w in ds for w in ("ago", "назад", "hour", "час", "мин", "min",
                              "today", "сегодня", "вчера", "yesterday")):
        return datetime.now(timezone.utc) - timedelta(hours=12)
    for fmt in (
        "%d.%m.%Y %H:%M", "%d.%m.%Y",
        "%Y-%m-%dt%H:%M:%Sz", "%Y-%m-%dt%H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
        "%d/%m/%Y", "%Y.%m.%d",
    ):
        try:
            return datetime.strptime(ds[:19], fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def _date_filter(items: list[dict], cutoff_days: int) -> tuple[list[dict], int]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)
    kept, dropped = [], 0
    for item in items:
        pub_dt = _parse_pub_date(item.get("pub_date") or item.get("date") or "")
        if pub_dt is None or pub_dt >= cutoff:
            kept.append(item)
        else:
            dropped += 1
    return kept, dropped


# ── Source agent ──────────────────────────────────────────────────────────────

async def _source_agent(
    name: str,
    coro: Any,
    entity: str,
    entity_type: str,
    cutoff_days: int,
) -> dict:
    """Fetch one source and apply strict date filter. Always returns a result dict."""
    try:
        raw = await coro
        if not isinstance(raw, list):
            raw = []
    except Exception as e:
        logger.warning("[pipeline] %s / '%s' failed: %s", name, entity, e)
        return {
            "source": name, "entity": entity, "entity_type": entity_type,
            "raw": 0, "after_date": 0, "items": [], "selected": [],
            "error": str(e),
        }

    kept, dropped = _date_filter(raw, cutoff_days)
    logger.info("[pipeline] %s / '%s': raw=%d → date=%d (dropped %d older than %dd)",
                name, entity, len(raw), len(kept), dropped, cutoff_days)
    return {
        "source": name, "entity": entity, "entity_type": entity_type,
        "raw": len(raw), "after_date": len(kept), "items": kept, "selected": [],
    }


# ── AI semantic filter (per source batch) ────────────────────────────────────

async def _ai_filter_batch(items: list[dict], entity: str, source: str, api_key: str) -> list[dict]:
    """Keep only items DIRECTLY about `entity`. Rejects generic noise."""
    if not items or not api_key:
        return items

    import anthropic as _ant, json as _json

    lines = [
        f"[{i}] {(it.get('title') or it.get('text') or '')[:120]}"
        for i, it in enumerate(items)
    ]
    prompt = (
        f'Entity: "{entity}"\nSource: {source}\n\n'
        f'Keep ONLY items DIRECTLY about "{entity}" as an organization: '
        f"their products, employees, finances, legal issues, reputation, strategy.\n"
        f"REJECT: generic industry news, articles that mention the entity only in passing, "
        f"unrelated content, noise, spam.\n\n"
        f'Return JSON only: {{"keep": [0, 2, ...]}} with 0-based indices to keep.\n\n'
        + "\n".join(lines)
    )

    def _call():
        c = _ant.Anthropic(api_key=api_key, max_retries=0)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system='JSON API. Respond ONLY with {"keep": [...]}. No explanation.',
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        return _json.loads(raw).get("keep", [])

    try:
        indices = await asyncio.to_thread(_call)
        result = [items[i] for i in indices if isinstance(i, int) and 0 <= i < len(items)]
        logger.info("[pipeline] AI filter %s / '%s': %d/%d kept", source, entity, len(result), len(items))
        return result
    except Exception as e:
        logger.warning("[pipeline] AI filter %s / '%s' failed: %s — keeping all", source, entity, e)
        return items


# ── Batch risk annotation ────────────────────────────────────────────────────

async def _ai_annotate_own_rows(rows: list[dict], company_name: str, api_key: str) -> None:
    """Annotate batches of 4 own articles with a short risk note. Mutates rows in-place."""
    if not rows or not api_key:
        return

    import anthropic as _ant, json as _json

    async def _annotate_batch(batch: list[dict]) -> list[str]:
        lines = [f"[{i}] {r['title'][:100]}" for i, r in enumerate(batch)]
        prompt = (
            f'Компания: "{company_name}"\n\n'
            f'Для каждого материала напиши 1 короткую фразу (до 10 слов) — '
            f'какой риск для компании он отражает или почему важен.\n\n'
            + "\n".join(lines)
            + f'\n\nJSON: {{"notes": [...]}} — ровно {len(batch)} элементов.'
        )

        def _call():
            c = _ant.Anthropic(api_key=api_key, max_retries=0)
            msg = c.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=300,
                system='JSON API. Respond ONLY with {"notes": [...]}. No explanation.',
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
            return _json.loads(raw).get("notes", [])

        try:
            return await asyncio.to_thread(_call)
        except Exception as e:
            logger.warning("[pipeline] annotate batch failed: %s", e)
            return [""] * len(batch)

    batch_size = 4
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        notes = await _annotate_batch(batch)
        for j, row in enumerate(batch):
            row["risk_note"] = notes[j] if j < len(notes) else ""
        if i + batch_size < len(rows):
            await asyncio.sleep(0.5)

    logger.info("[pipeline] annotated %d own articles in %d batches",
                len(rows), (len(rows) + batch_size - 1) // batch_size)


# ── Risk fingerprinting & escalation ─────────────────────────────────────────

def _risk_fingerprint(risk: dict) -> str:
    cat   = (risk.get("category") or "").lower().strip()
    title = (risk.get("title") or risk.get("text") or "").lower().strip()
    return hashlib.md5(f"{cat}::{title[:60]}".encode()).hexdigest()[:16]


def _positive_reduction(signals: list[dict], category: str) -> int:
    """Sum score reduction from positive signals for a given category."""
    weight_map = {"low": 5, "medium": 12, "high": 20}
    return sum(
        weight_map.get(s.get("weight", "low"), 5)
        for s in signals
        if s.get("category") == category
    )


def escalate_risks(
    new_risks: list[dict], prev_fps: list[dict],
    positive_signals: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Returns (active_risks, decayed_fps).

    active_risks  — risks found this run, with bumped scores for persisted ones.
    decayed_fps   — previous risks NOT found this run; score decays each miss,
                    dropped after 3 consecutive misses (resolved).

    Score rules:
      Found again:   score = min(100, base + 5 * consecutive_runs)
      Missed once:   score = score * 0.75
      Missed twice:  score = score * 0.60
      Missed 3+:     removed (resolved)
    """
    prev_map = {fp["key"]: fp for fp in (prev_fps or []) if fp.get("key")}
    found_keys: set[str] = set()

    signals = positive_signals or []

    for risk in new_risks:
        fp = _risk_fingerprint(risk)
        risk["fingerprint"] = fp
        found_keys.add(fp)
        if fp in prev_map:
            prev = prev_map[fp]
            runs = prev.get("consecutive_runs", 1)
            risk["consecutive_runs"] = runs + 1
            risk["missed_runs"]      = 0
            base = risk.get("score") or 50
            risk["score"]    = min(100, int(base) + 5 * runs)
            risk["persisted"] = True
        else:
            risk["consecutive_runs"] = 1
            risk["missed_runs"]      = 0
            risk["persisted"]        = False

        # Apply positive signal reduction for this risk's category
        reduction = _positive_reduction(signals, risk.get("category", ""))
        if reduction:
            risk["score"] = max(0, risk["score"] - reduction)
            risk["positive_reduction"] = reduction

    # Risks from previous run that weren't found this time
    decayed: list[dict] = []
    decay_factor = {1: 0.75, 2: 0.60}
    for fp in (prev_fps or []):
        if not fp.get("key") or fp["key"] in found_keys:
            continue
        missed = fp.get("missed_runs", 0) + 1
        if missed >= 3:
            logger.info("[pipeline] risk resolved after 3 misses: %s", fp.get("title", "")[:50])
            continue
        factor = decay_factor.get(missed, 0.60)
        decayed.append({
            **fp,
            "missed_runs":      missed,
            "score":            max(5, int(fp.get("score", 50) * factor)),
            "consecutive_runs": fp.get("consecutive_runs", 1),
        })

    logger.info("[pipeline] risks: %d active, %d decaying, %d resolved",
                len(new_risks), len(decayed),
                sum(1 for fp in (prev_fps or []) if fp.get("key") not in found_keys and fp.get("missed_runs", 0) >= 2))
    return new_risks, decayed


# ── Main pipeline ─────────────────────────────────────────────────────────────

async def run_pipeline(
    *,
    job_id: str,
    company_id: str,
    company_name: str,
    news_client: Any,
    hh_client: Any,
    instagram_client: Any,
    settings: Any,
    supabase: Any,
    api_key: str,
    on_stage: Callable[[str], None] | None = None,
    cutoff_days: int = CUTOFF_DAYS,
    # Items pre-fetched by frontend (merged with server-side)
    frontend_news: list[dict] | None = None,
    frontend_threads: list[dict] | None = None,
    frontend_instagram: list[dict] | None = None,
) -> dict[str, Any]:
    from datetime import datetime, timezone

    def _stage(s: str) -> None:
        logger.info("[pipeline job=%s] %s", job_id, s)
        if on_stage:
            on_stage(s)

    sk = settings.SUPABASE_SERVICE_KEY or ""

    # ── Load competitors ──────────────────────────────────────────────────────
    competitors: list[dict] = []
    if sk:
        try:
            competitors = await supabase.rest_select_service(
                table="competitors", service_key=sk,
                query_params={"company_id": f"eq.{company_id}"},
            ) or []
        except Exception as e:
            logger.warning("[pipeline] load competitors failed: %s", e)
    comp_names = [c["name"] for c in competitors if c.get("name")]

    # ── Step 1: Parallel scraping ─────────────────────────────────────────────
    _stage(" Сбор данных из всех источников…")

    own_coros = [
        _source_agent("google_news", news_client.fetch_google_news_only(company_name, limit=50), company_name, "own", cutoff_days),
        _source_agent("gdelt",       news_client.fetch_gdelt_news(company_name, limit=30),       company_name, "own", cutoff_days),
        _source_agent("yahoo_news",  news_client.fetch_yahoo_news(company_name, limit=20),       company_name, "own", cutoff_days),
        _source_agent("hh",          hh_client.fetch_vacancies(company_name, limit=20),          company_name, "own", cutoff_days),
    ]
    if settings.SERPAPI_KEY:
        own_coros.append(
            _source_agent("serpapi",
                news_client.fetch_serpapi_news(company_name, settings.SERPAPI_KEY, limit=20),
                company_name, "own", cutoff_days)
        )
    if settings.INSTAGRAM_USERNAME and settings.INSTAGRAM_PASSWORD:
        own_coros.append(
            _source_agent("instagram",
                instagram_client.fetch_instagram_posts(
                    company_name, settings.INSTAGRAM_USERNAME,
                    settings.INSTAGRAM_PASSWORD, limit=15),
                company_name, "own", cutoff_days)
        )

    comp_coros = []
    for cn in comp_names:
        comp_coros += [
            _source_agent("google_news", news_client.fetch_google_news_only(cn, limit=30), cn, "competitor", cutoff_days),
            _source_agent("gdelt",       news_client.fetch_gdelt_news(cn, limit=20),       cn, "competitor", cutoff_days),
            _source_agent("yahoo_news",  news_client.fetch_yahoo_news(cn, limit=15),       cn, "competitor", cutoff_days),
        ]

    all_agent_results = await asyncio.gather(*own_coros, *comp_coros, return_exceptions=True)
    batches: list[dict] = [r for r in all_agent_results if isinstance(r, dict)]

    # Merge frontend-provided items (already date-filtered)
    for src_name, items in [
        ("threads",   frontend_threads   or []),
        ("news",      frontend_news      or []),
        ("instagram", frontend_instagram or []),
    ]:
        if items:
            kept, _ = _date_filter(items, cutoff_days)
            if kept:
                batches.append({
                    "source": src_name, "entity": company_name, "entity_type": "own",
                    "raw": len(items), "after_date": len(kept), "items": kept, "selected": [],
                })

    raw_total  = sum(b["raw"]         for b in batches)
    date_total = sum(b["after_date"]  for b in batches)
    logger.info("[pipeline] Step 1 done: %d raw → %d after date filter across %d batches",
                raw_total, date_total, len(batches))

    # ── Step 1.5: Deduplicate against news_archive (last 3 days only) ─────────
    seen_urls: set[str] = set()
    if sk:
        try:
            _archive_cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            seen_rows = await supabase.rest_select_service(
                table="news_archive", service_key=sk,
                select="url",
                query_params={
                    "company_id": f"eq.{company_id}",
                    "seen_at":    f"gte.{_archive_cutoff}",
                },
            )
            seen_urls = {r["url"] for r in (seen_rows or []) if r.get("url")}
            logger.info("[pipeline] loaded %d seen URLs from archive (last 7 days)", len(seen_urls))
        except Exception as e:
            logger.warning("[pipeline] load seen URLs failed: %s", e)

    if seen_urls:
        # Save originals before filtering (for fallback)
        for b in batches:
            if b["entity_type"] == "own":
                b["_pre_dedup"] = list(b["items"])
        _own_before = sum(len(b["_pre_dedup"]) for b in batches if b["entity_type"] == "own")
        dedup_dropped = 0
        for b in batches:
            if b["entity_type"] == "own":
                before = len(b["items"])
                b["items"] = [
                    item for item in b["items"]
                    if (item.get("link") or item.get("url") or "") not in seen_urls
                ]
                b["after_date"] = len(b["items"])
                dedup_dropped += before - len(b["items"])
        _own_after = sum(len(b["items"]) for b in batches if b["entity_type"] == "own")
        if _own_after == 0 and _own_before > 0:
            # All own articles were deduplicated — revert to allow re-analysis
            logger.info("[pipeline] dedup filtered ALL %d own articles — reverting", _own_before)
            for b in batches:
                if b["entity_type"] == "own":
                    b["items"] = b["_pre_dedup"]
        else:
            logger.info("[pipeline] dedup: dropped %d already-seen items", dedup_dropped)

    # ── Step 2: AI semantic filter per source batch ───────────────────────────
    _stage(" ИИ фильтрует материалы по источникам…")

    active_batches = [b for b in batches if b.get("items")]
    if api_key and active_batches:
        for i, b in enumerate(active_batches):
            b["selected"] = await _ai_filter_batch(b["items"], b["entity"], b["source"], api_key)
            if i < len(active_batches) - 1:
                await asyncio.sleep(1)  # avoid overload
    else:
        for b in batches:
            b["selected"] = b["items"]

    selected_total = sum(len(b.get("selected", [])) for b in batches)
    logger.info("[pipeline] Step 2 done: %d items after AI semantic filter", selected_total)

    # ── Step 3: Upsert to analysis_articles staging ───────────────────────────
    _stage("💾 Сохранение в базу…")

    staging_rows: list[dict] = []
    for b in batches:
        for item in b.get("selected", []):
            title    = (item.get("title") or item.get("text") or "")[:120]
            url      = item.get("link") or item.get("url") or ""
            pub_date = item.get("pub_date") or item.get("date") or ""
            pub_dt   = _parse_pub_date(pub_date)
            staging_rows.append({
                "id":              str(_uuid.uuid4()),
                "company_id":      company_id,
                "job_id":          job_id,
                "title":           title or "—",
                "url":             url,
                "source":          item.get("source") or b["source"],
                "source_type":     b["source"],
                "excerpt":         (item.get("text") or item.get("description") or item.get("excerpt") or "")[:300],
                "pub_date":        pub_date,
                "published_at":    pub_dt.isoformat() if pub_dt else None,
                "entity_type":     b["entity_type"],
                "entity_name":     b["entity"],
                "status":          "selected" if b["entity_type"] == "own" else "competitor_selected",
                "competitor_name": b["entity"] if b["entity_type"] == "competitor" else "",
                "risk_note":       "",
            })

    # ── Step 2.5: Annotate own articles with risk context ────────────────────
    _stage(" ИИ аннотирует материалы…")
    own_staging = [r for r in staging_rows if r["entity_type"] == "own"]
    if api_key and own_staging:
        await _ai_annotate_own_rows(own_staging, company_name, api_key)

    if sk and staging_rows:
        try:
            await supabase.rest_delete_service("analysis_articles", sk, {"company_id": f"eq.{company_id}"})
        except Exception:
            pass
        for i in range(0, len(staging_rows), 100):
            try:
                await supabase.rest_insert_service("analysis_articles", sk, staging_rows[i:i+100])
            except Exception as e:
                logger.error("[pipeline] staging insert batch %d failed: %s", i, e)
        logger.info("[pipeline] inserted %d staging rows (%d own, %d competitor)",
                    len(staging_rows),
                    sum(1 for r in staging_rows if r["entity_type"] == "own"),
                    sum(1 for r in staging_rows if r["entity_type"] == "competitor"))

    # ── Load previous risk run ─────────────────────────────────────────────────
    prev_fps: list[dict] = []
    prev_run_id: str | None = None
    prev_rows: list[dict] = []
    if sk:
        try:
            prev_rows = await supabase.rest_select_service(
                table="risk_runs", service_key=sk,
                select="id,risk_fingerprints,top_articles",
                query_params={
                    "company_id": f"eq.{company_id}",
                    "status":     "eq.done",
                    "order":      "created_at.desc",
                    "limit":      "1",
                },
            )
            if prev_rows:
                prev_run_id = prev_rows[0].get("id")
                prev_fps    = prev_rows[0].get("risk_fingerprints") or []
                logger.info("[pipeline] prev run id=%s, %d fingerprints, %d articles loaded",
                            prev_run_id, len(prev_fps), len(prev_rows[0].get("top_articles") or []))
        except Exception as e:
            logger.warning("[pipeline] load prev run failed: %s", e)

    # ── Load per-category settings (preferred sources + keywords) ─────────────
    category_hints: dict = {}
    if sk:
        try:
            cs_rows = await supabase.rest_select_service(
                table="category_settings", service_key=sk,
                select="category,preferred_sources,custom_keywords",
                query_params={"company_id": f"eq.{company_id}"},
            ) or []
            for r in cs_rows:
                cat = r.get("category")
                srcs = r.get("preferred_sources") or []
                kws = r.get("custom_keywords") or []
                if cat and (srcs or kws):
                    category_hints[cat] = {"sources": srcs, "keywords": kws}
            if category_hints:
                logger.info("[pipeline] category hints: %s", list(category_hints.keys()))
        except Exception as e:
            logger.warning("[pipeline] category_settings load failed: %s", e)

    # ── Step 4: AI risk analysis with history ─────────────────────────────────
    _stage(" Анализ рисков с учётом истории…")

    own_rows  = [r for r in staging_rows if r["entity_type"] == "own"]
    comp_rows = [r for r in staging_rows if r["entity_type"] == "competitor"]

    prev_articles = (prev_rows[0].get("top_articles") or []) if prev_rows else []

    from .ai import analyze_with_history as _awh
    analysis = await asyncio.to_thread(
        _awh,
        company_name=company_name,
        own_rows=own_rows,
        comp_rows=comp_rows,
        prev_fps=prev_fps,
        prev_articles=prev_articles,
        api_key=api_key,
        category_hints=category_hints,
    )

    # Apply escalation + decay + positive signal reduction
    positive_signals = analysis.get("positive_signals", [])
    if positive_signals:
        logger.info("[pipeline] %d positive signals: %s",
                    len(positive_signals),
                    ", ".join(f"{s.get('category')}:{s.get('weight')}" for s in positive_signals[:5]))
    risks, decayed_fps = escalate_risks(analysis.get("risks", []), prev_fps, positive_signals)
    analysis["risks"] = risks

    # ── Loss scenarios for top risks (AI-1) ──────────────────────────────────
    loss_scenarios: list[dict] = []
    if api_key and risks:
        try:
            from .ai import generate_loss_scenarios as _gls
            _top_sorted = sorted(risks, key=lambda r: r.get("score", 0), reverse=True)
            loss_scenarios = await asyncio.to_thread(_gls, company_name, _top_sorted, "", api_key)
            logger.info("[pipeline] generated %d loss scenarios", len(loss_scenarios))
        except Exception as e:
            logger.warning("[pipeline] loss scenarios failed: %s", e)
    analysis["loss_scenarios"] = loss_scenarios

    # Fingerprints = active risks + decaying (not yet resolved) risks
    new_fps = [
        {
            "key":              r.get("fingerprint", _risk_fingerprint(r)),
            "title":            r.get("title", ""),
            "category":         r.get("category", ""),
            "score":            r.get("score", 50),
            "consecutive_runs": r.get("consecutive_runs", 1),
            "missed_runs":      r.get("missed_runs", 0),
        }
        for r in risks
    ] + decayed_fps

    analysis.update({
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "previous_run_id":   prev_run_id,
        "positive_signals":  positive_signals,
        "loss_scenarios":    loss_scenarios,
        "risk_fingerprints": new_fps,
        "staging_rows":      staging_rows,
        "source_stats": [
            {
                "source":      b["source"],
                "entity":      b["entity"],
                "entity_type": b["entity_type"],
                "raw":         b["raw"],
                "after_date":  b["after_date"],
                "selected":    len(b.get("selected", [])),
            }
            for b in batches
        ],
    })

    # ── Client per-type risk model (financial/operational/reputational/…) ────
    # Extract 0–10 factors from gathered signals; scores computed by formula.
    try:
        from . import risk_formulas as _rf
        from .ai import extract_risk_factors as _erf
        _sig_lines = []
        for r in own_rows[:40]:
            _note = (r.get("risk_note") or "").strip()
            _sig_lines.append(f"- {r.get('title','')}" + (f" | {_note}" if _note else ""))
        domain_factors: dict = {}
        if _sig_lines and api_key:
            _sig_text = (f"Компания: {company_name}\nСигналы:\n" + "\n".join(_sig_lines))[:6000]
            domain_factors = await _erf(_sig_text, api_key)
        active_formulas: dict = {}
        if sk:
            try:
                _frows = await supabase.rest_select_service(
                    table="formula_defs", service_key=sk,
                    select="domain,expression,variables",
                    query_params={"company_id": f"eq.{company_id}",
                                  "is_active": "eq.true", "source": "eq.custom"}) or []
                for _fr in _frows:
                    if _fr.get("domain") in _rf.DOMAINS and _fr.get("expression"):
                        active_formulas[_fr["domain"]] = {
                            "expression": _fr["expression"], "variables": _fr.get("variables")}
            except Exception:
                active_formulas = {}
        analysis["domain_factors"] = domain_factors
        analysis["domain_scores"] = _rf.score_all(domain_factors, active_formulas) if domain_factors else {}
        if sk and domain_factors:
            try:
                await supabase.rest_insert_service("signal_factors", sk, [{
                    "company_id": company_id, "signal_text": f"Прогон · {company_name}",
                    "source": "analysis", "factors": domain_factors}])
            except Exception:
                pass
    except Exception as _e:
        logger.warning("[pipeline] per-type risk model failed: %s", _e)
        analysis.setdefault("domain_factors", {})
        analysis.setdefault("domain_scores", {})

    return analysis
