"""
Section-based analysis pipeline.

For each risk section (media, hr, gr, pr, market):
  Agent 1 — Source Agent:  generates targeted search queries for this section
  Agent 2 — Fetch Agent:   fetches articles using those queries + standard sources
  Agent 3 — Risk Agent:    analyzes articles → score, risks[], summary

Master Agent — aggregates all 5 section results → overall score + advice + scenarios

All 5 sections run in parallel. Results saved to section_runs table.
"""
import asyncio
import json
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any

import anthropic

logger = logging.getLogger("conexiai.sections")

_MODEL = "claude-haiku-4-5-20251001"

_SECTION_LABELS = {
    "media":  "Инфополе и медиа",
    "hr":     "HR и персонал",
    "gr":     "GR и регуляторика",
    "pr":     "PR и репутация",
    "market": "Рынок и конкуренты",
}

_SECTION_PROMPTS = {
    "media": (
        "Раздел: Инфополе и медиа. "
        "Ищи: новости о компании в СМИ, упоминания в медиа, публикации журналистов, "
        "скандалы, расследования, негативные публикации, PR-кризисы."
    ),
    "hr": (
        "Раздел: HR и персонал. "
        "Ищи: отзывы сотрудников, массовые увольнения, забастовки, вакансии (признак проблем), "
        "судебные иски от сотрудников, текучка кадров, условия труда."
    ),
    "gr": (
        "Раздел: GR и регуляторика. "
        "Ищи: проверки госорганов, штрафы, лицензии, судебные дела, налоговые споры, "
        "изменения законодательства влияющие на компанию, коррупционные расследования."
    ),
    "pr": (
        "Раздел: PR и репутация. "
        "Ищи: отзывы клиентов, рейтинги, репутационные скандалы, бойкоты, "
        "публичные конфликты, социальные сети, мнения лидеров мнений."
    ),
    "market": (
        "Раздел: Рынок и конкуренты. "
        "Ищи: рыночная доля, действия конкурентов, финансовые результаты, "
        "сделки M&A, партнёрства, новые продукты конкурентов, ценовые войны."
    ),
}


def _haiku_sync(prompt: str, api_key: str, max_tokens: int = 800) -> dict | list:
    import time
    c = anthropic.Anthropic(api_key=api_key, max_retries=0)
    attempt = 0
    while True:
        try:
            msg = c.messages.create(
                model=_MODEL,
                max_tokens=max_tokens,
                system="JSON API. Respond ONLY with valid JSON. No markdown, no explanation.",
                messages=[{"role": "user", "content": prompt}],
            )
            raw = msg.content[0].text.strip()
            if raw.startswith("```"):
                raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
            return json.loads(raw)
        except Exception as e:
            if "overloaded" in str(e).lower():
                wait = min(10 * (2 ** attempt), 60)
                attempt += 1
                logger.info("Haiku overloaded, waiting %ds…", wait)
                time.sleep(wait)
            else:
                raise


# ─────────────────────────────────────────────
# Agent 1 — Source Agent
# ─────────────────────────────────────────────

def _source_agent_sync(
    section: str,
    company_name: str,
    industry: str,
    api_key: str,
) -> list[str]:
    """Generate 5-7 targeted search queries for this section."""
    context = _SECTION_PROMPTS.get(section, "")
    prompt = (
        f"Компания: «{company_name}» | Отрасль: {industry or 'неизвестна'}\n"
        f"{context}\n\n"
        f"Сгенерируй 6 конкретных поисковых запросов для Google/SerpAPI "
        f"чтобы найти максимум релевантных материалов по этому разделу.\n"
        f"Запросы должны быть на русском и английском, разные формулировки.\n"
        f'Верни JSON: {{"queries": ["запрос 1", "запрос 2", ...]}}'
    )
    try:
        result = _haiku_sync(prompt, api_key, max_tokens=400)
        queries = result.get("queries", [])
        # Always include base query
        base = f'"{company_name}"'
        if base not in queries:
            queries.insert(0, base)
        return queries[:7]
    except Exception as e:
        logger.warning("[%s] source_agent failed: %s", section, e)
        return [f'"{company_name}"']


# ─────────────────────────────────────────────
# Agent 2 — Fetch Agent
# ─────────────────────────────────────────────

async def _fetch_agent(
    section: str,
    queries: list[str],
    company_name: str,
    serpapi_key: str,
    cutoff_days: int = 90,
) -> list[dict]:
    """Fetch articles using AI-generated queries + standard sources."""
    from . import news as news_client

    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_days)

    def _is_fresh(pub_date_str: str) -> bool:
        if not pub_date_str:
            return True
        ds = pub_date_str.strip().lower()
        if any(w in ds for w in ("ago", "назад", "hour", "час", "день", "мин", "min",
                                  "today", "сегодня", "вчера", "yesterday")):
            return True
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                d = datetime.strptime(ds[:19], fmt).replace(tzinfo=timezone.utc)
                return d >= cutoff
            except ValueError:
                pass
        return True

    seen: set[str] = set()
    articles: list[dict] = []

    def _add(items: list, source_tag: str = ""):
        for item in (items or []):
            title = (item.get("title") or item.get("text") or "")[:200]
            key = title.lower()[:60]
            if not title or key in seen:
                continue
            pub_date = item.get("pub_date") or item.get("date") or ""
            if not _is_fresh(pub_date):
                continue
            seen.add(key)
            articles.append({
                "title":    title,
                "url":      item.get("link") or item.get("url") or "",
                "source":   item.get("source") or source_tag,
                "pub_date": pub_date,
                "excerpt":  (item.get("snippet") or item.get("text") or "")[:300],
                "section":  section,
            })

    from .news import _fetch_serpapi_query, _fetch_by_query, _fetch_yandex_lang, _fetch_kase, fetch_hh_vacancies

    tasks: list[tuple] = []

    # SerpAPI — section-specific AI queries, up to 4 queries × 2 regions
    if serpapi_key:
        for q in queries[:4]:
            tasks.append((_fetch_serpapi_query(q, serpapi_key, gl="kz", hl="ru", limit=15), "SerpAPI KZ"))
            tasks.append((_fetch_serpapi_query(q, serpapi_key, gl="ru", hl="ru", limit=15), "SerpAPI RU"))

    # Google News — first 3 AI queries, KZ + RU regions
    for q in queries[:3]:
        tasks.append((_fetch_by_query(q, 30, region="RU"), "Google News RU"))
        tasks.append((_fetch_by_query(q, 30, region="KZ"), "Google News KZ"))

    # Yandex News — best for CIS companies, first 2 queries
    for q in queries[:2]:
        tasks.append((_fetch_yandex_lang(q, 20, "ru"), "Yandex News"))

    # Section-specific extra sources
    if section in ("market", "gr"):
        tasks.append((_fetch_kase(company_name, 30), "KASE"))
    if section == "hr":
        tasks.append((fetch_hh_vacancies(company_name, limit=30, serpapi_key=serpapi_key), "hh.kz"))

    if not tasks:
        logger.info("[%s] fetch_agent: 0 articles (no tasks)", section)
        return []

    coros, tags = zip(*tasks)
    results = await asyncio.gather(*coros, return_exceptions=True)

    for r, tag in zip(results, tags):
        if isinstance(r, list):
            _add(r, tag)

    logger.info("[%s] fetch_agent: %d articles", section, len(articles))
    return articles[:100]  # cap per section


# ─────────────────────────────────────────────
# Agent 3 — Risk Agent
# ─────────────────────────────────────────────

def _risk_agent_sync(
    section: str,
    company_name: str,
    articles: list[dict],
    competitor_articles: list[dict],
    api_key: str,
) -> dict:
    """Analyze articles → score, risks[], summary for this section."""
    label = _SECTION_LABELS.get(section, section)
    context = _SECTION_PROMPTS.get(section, "")

    art_lines = "\n".join(
        f"[{i+1}] {a['title']} | {a.get('source','')} | {a.get('pub_date','')} | {a.get('excerpt','')[:100]}"
        for i, a in enumerate(articles[:40])
    ) or "Нет статей."

    comp_lines = ""
    if competitor_articles:
        comp_lines = "\n=== КОНКУРЕНТЫ ===\n" + "\n".join(
            f"- [{a.get('competitor_name','?')}] {a['title']} | {a.get('pub_date','')}"
            for a in competitor_articles[:20]
        )

    today = date.today().strftime("%d.%m.%Y")
    no_articles = art_lines == "Нет статей."
    prompt = (
        f"Компания: «{company_name}» | Дата: {today}\n"
        f"Раздел анализа: {label}\n"
        f"{context}\n\n"
        f"=== НАЙДЕННЫЕ МАТЕРИАЛЫ ===\n{art_lines}{comp_lines}\n\n"
        + (
            f"Статей не найдено. Оцени риски на основе отраслевого контекста и типичных рисков "
            f"для компаний такого типа в данном разделе. Используй score 20-40 (умеренный риск при отсутствии данных).\n\n"
            if no_articles else
            f"Проанализируй материалы и оцени риски в разделе «{label}». "
            f"Score 0-30 = низкий риск, 31-60 = средний, 61-100 = высокий.\n\n"
        ) +
        f"Верни JSON:\n"
        f'{{"score": 0-100, '
        f'"risks": [{{"text": "описание риска", "severity": "high|medium|low"}}], '
        f'"positives": ["позитивный сигнал"], '
        f'"summary": "1-2 предложения вывод по разделу", '
        f'"key_articles": [0, 2, 5]}}'
    )
    try:
        result = _haiku_sync(prompt, api_key, max_tokens=900)
        # Mark key articles
        key_indices = set(result.get("key_articles", []))
        selected = [articles[i] for i in key_indices if i < len(articles)]
        return {
            "score":     int(result.get("score", 0)),
            "risks":     result.get("risks", []),
            "positives": result.get("positives", []),
            "summary":   result.get("summary", ""),
            "selected_articles": selected,
        }
    except Exception as e:
        logger.warning("[%s] risk_agent failed: %s", section, e)
        return {"score": 0, "risks": [], "positives": [], "summary": "Анализ недоступен", "selected_articles": []}


# ─────────────────────────────────────────────
# Master Agent — aggregates all sections
# ─────────────────────────────────────────────

def _master_agent_sync(
    company_name: str,
    section_results: dict[str, dict],
    api_key: str,
) -> dict:
    """Aggregate 5 section results → overall score, advice, scenarios."""
    sections_text = "\n".join(
        f"- {_SECTION_LABELS.get(s, s)}: скор {r['score']}/100 | {r['summary']}"
        for s, r in section_results.items()
    )
    all_risks = []
    for s, r in section_results.items():
        for risk in r.get("risks", []):
            all_risks.append(f"[{_SECTION_LABELS.get(s,s)}] {risk.get('text','')}")

    risks_text = "\n".join(all_risks[:20]) or "Рисков не выявлено."
    today = date.today().strftime("%d.%m.%Y")

    prompt = (
        f"Компания: «{company_name}» | Дата: {today}\n\n"
        f"Результаты анализа по разделам:\n{sections_text}\n\n"
        f"Ключевые риски:\n{risks_text}\n\n"
        f"Как главный аналитик дай итоговую оценку.\n"
        f"Верни JSON:\n"
        f'{{"score": 0-100, '
        f'"advice": "1-2 предложения главный совет", '
        f'"scenarios": [{{"title": "сценарий", "probability": "high|medium|low", "impact": "high|medium|low", "description": "описание"}}], '
        f'"overall_summary": "3-4 предложения общий вывод"}}'
    )
    try:
        result = _haiku_sync(prompt, api_key, max_tokens=800)
        score = int(result.get("score", 0))
        if score == 0:
            # fallback: average of section scores
            scores = [r["score"] for r in section_results.values() if r.get("score")]
            score = int(sum(scores) / len(scores)) if scores else 0
        logger.info("master_agent score=%d", score)
        return {
            "score":           score,
            "advice":          result.get("advice", ""),
            "scenarios":       result.get("scenarios", []),
            "overall_summary": result.get("overall_summary", ""),
        }
    except Exception as e:
        logger.warning("master_agent failed: %s", e)
        scores = [r["score"] for r in section_results.values() if r.get("score")]
        fallback_score = int(sum(scores) / len(scores)) if scores else 0
        return {"score": fallback_score, "advice": "Анализ недоступен", "scenarios": [], "overall_summary": ""}


# ─────────────────────────────────────────────
# Section Pipeline — runs one section end-to-end
# ─────────────────────────────────────────────

async def run_section(
    section: str,
    company_name: str,
    industry: str,
    competitor_articles: list[dict],
    serpapi_key: str,
    api_key: str,
    on_progress=None,  # optional callable(section, stage)
) -> dict:
    """Run full 3-agent pipeline for one section. Returns section result dict."""
    def _notify(stage: str):
        if on_progress:
            on_progress(section, stage)

    # Agent 1 — Source
    _notify("source")
    queries = await asyncio.to_thread(
        _source_agent_sync, section, company_name, industry, api_key
    )
    logger.info("[%s] queries: %s", section, queries)

    # Agent 2 — Fetch
    _notify("fetch")
    articles = await _fetch_agent(section, queries, company_name, serpapi_key)

    # Agent 3 — Risk
    _notify("risk")
    result = await asyncio.to_thread(
        _risk_agent_sync, section, company_name, articles, competitor_articles, api_key
    )
    _notify("done")

    return {
        "section":   section,
        "queries":   queries,
        "articles":  articles,
        "risks":     result["risks"],
        "positives": result["positives"],
        "score":     result["score"],
        "summary":   result["summary"],
        "selected_articles": result["selected_articles"],
    }


# ─────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────

async def run_full_analysis(
    company_id: str,
    company_name: str,
    industry: str,
    job_id: str,
    competitor_articles: list[dict],
    serpapi_key: str,
    api_key: str,
    supabase_service_key: str,
    on_stage=None,  # callable(stage_text)
) -> dict:
    """
    Run all 5 sections in parallel, then master agent.
    Saves each section to section_runs table as it completes.
    Returns final result dict compatible with existing risk_runs schema.
    """
    from .supabase import supabase

    SECTIONS = ["media", "hr", "gr", "pr", "market"]

    # Insert pending rows for all sections
    if supabase_service_key:
        try:
            await supabase.rest_insert_service(
                "section_runs", supabase_service_key,
                [{"company_id": company_id, "job_id": job_id,
                  "section": s, "status": "running"} for s in SECTIONS],
            )
        except Exception as e:
            logger.warning("section_runs insert failed: %s", e)

    if on_stage:
        on_stage("🔀 Параллельный анализ 5 разделов…")

    # Progress tracker
    progress: dict[str, str] = {s: "pending" for s in SECTIONS}

    def _on_progress(section: str, stage: str):
        progress[section] = stage
        if on_stage:
            icons = {"source": "🔍", "fetch": "📡", "risk": "🧠", "done": "✅", "error": "❌"}
            labels = {s: _SECTION_LABELS.get(s, s) for s in SECTIONS}
            parts = [f"{icons.get(progress[s],'⏳')} {labels[s]}" for s in SECTIONS]
            on_stage(" | ".join(parts))

    # Run all 5 sections in parallel
    tasks = [
        run_section(
            section=s,
            company_name=company_name,
            industry=industry,
            competitor_articles=[a for a in competitor_articles
                                  if a.get("competitor_name")] if s == "market" else [],
            serpapi_key=serpapi_key,
            api_key=api_key,
            on_progress=_on_progress,
        )
        for s in SECTIONS
    ]
    section_results_list = await asyncio.gather(*tasks, return_exceptions=True)

    # Collect results
    section_results: dict[str, dict] = {}
    for s, r in zip(SECTIONS, section_results_list):
        if isinstance(r, Exception):
            logger.warning("[%s] section failed: %s", s, r)
            section_results[s] = {"score": 0, "risks": [], "positives": [],
                                   "summary": "Ошибка анализа", "articles": [],
                                   "queries": [], "selected_articles": []}
        else:
            section_results[s] = r

    # Save completed sections to DB
    if supabase_service_key:
        for s, r in section_results.items():
            try:
                await supabase.rest_update_service(
                    f"rest/v1/section_runs?company_id=eq.{company_id}&job_id=eq.{job_id}&section=eq.{s}",
                    supabase_service_key,
                    {
                        "status":   "done",
                        "queries":  r["queries"],
                        "articles": r["articles"],
                        "risks":    r["risks"],
                        "score":    r["score"],
                        "summary":  r["summary"],
                    },
                )
            except Exception as e:
                logger.warning("[%s] section_runs update failed: %s", s, e)

    # Master Agent
    if on_stage:
        on_stage("🎯 Главный агент — итоговый анализ…")

    master = await asyncio.to_thread(
        _master_agent_sync, company_name, section_results, api_key
    )

    # Build categories dict (compatible with existing risk_runs schema)
    categories: dict[str, dict] = {}
    for s, r in section_results.items():
        categories[s] = {
            "score": r["score"],
            "risks": r["risks"],
            "summary": r["summary"],
        }

    # Blend CI score if market section has competitor data
    raw_score = master["score"]

    # Collect all selected articles across sections
    all_selected = []
    for r in section_results.values():
        all_selected.extend(r.get("selected_articles", []))

    return {
        "score":           raw_score,
        "advice":          master["advice"],
        "overall_summary": master["overall_summary"],
        "scenarios":       master["scenarios"],
        "categories":      categories,
        "section_results": section_results,  # full data per section
        "top_items":       all_selected[:20],
        "problems":        [
            risk["text"]
            for r in section_results.values()
            for risk in r.get("risks", [])
            if risk.get("severity") == "high"
        ][:6],
    }
