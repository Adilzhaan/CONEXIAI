import asyncio
import base64
import hashlib
import json
import logging
import secrets
from urllib.parse import quote
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from .config import settings
from .supabase import supabase
from . import news as news_client
from . import apify as apify_client
from . import hh as hh_client
from . import ai as ai_client
from . import finance as finance_client
from . import monitor as monitor_client
from . import ci as ci_client
from . import gr_sources as gr_client
from . import agent as agent_client
from .pdf import generate_report

_EMPTY_SOCIAL = {"threads": [], "instagram": [], "tiktok": [], "youtube": [], "twitter": [], "facebook": []}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("conexiai")


def _friendly_error(e: Exception, context: str = "auth") -> str:
    msg = str(e).lower()
    if "invalid login credentials" in msg or "invalid_credentials" in msg:
        return "Неверный email или пароль. Проверьте данные и попробуйте снова."
    if "email not confirmed" in msg:
        return "Email не подтверждён. Проверьте почту и перейдите по ссылке из письма."
    if "user already registered" in msg or "already been registered" in msg or "already registered" in msg or "email address is already" in msg:
        return "exists"  # special marker — template will show a redirect to login
    if "password should be at least" in msg or "weak_password" in msg:
        return "Пароль слишком простой. Используйте минимум 6 символов."
    if "rate limit" in msg or "too many requests" in msg:
        return "Слишком много попыток. Подождите немного и попробуйте снова."
    if "network" in msg or "connection" in msg:
        return "Ошибка сети. Проверьте подключение к интернету."
    logger.exception("Auth error (%s)", context)
    return "Произошла ошибка. Попробуйте позже."


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


@asynccontextmanager
async def lifespan(_app: FastAPI):
    scheduler = AsyncIOScheduler()
    if settings.MONITORING_ENABLED:
        async def _run_cycle():
            await monitor_client.run_monitoring_cycle(
                supabase=supabase,
                news_client=news_client,
                ai_client=ai_client,
                apify_client=apify_client,
                hh_client=hh_client,
                finance_client=finance_client,
                settings=settings,
                ci_client=ci_client,
            )
        from datetime import datetime
        scheduler.add_job(
            _run_cycle,
            trigger="interval",
            minutes=settings.MONITORING_INTERVAL_MINUTES,
            id="smart_monitor",
            replace_existing=True,
            next_run_time=datetime.now(),
        )
        logger.info("Smart monitor scheduler started (every %d min, first run: immediate)", settings.MONITORING_INTERVAL_MINUTES)
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)
    await supabase.close()
    await news_client.close()
    await apify_client.close()
    await hh_client.close()
    await ai_client.close()
    await finance_client.close()
    await ci_client.close()
    await gr_client.close()


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)

import os as _os
_BASE = _os.path.dirname(_os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=_os.path.join(_BASE, "static")), name="static")
templates = Jinja2Templates(directory=_os.path.join(_BASE, "templates"))
import json as _json_mod
templates.env.filters["tojson"] = lambda v: _json_mod.dumps(v, ensure_ascii=False)


def _cookie_params() -> dict[str, Any]:
    return {
        "httponly": True,
        "secure": settings.SESSION_COOKIE_SECURE,
        "samesite": "lax",
        # For a prototype. In production, set an explicit domain + path.
        "path": "/",
    }


def _set_tokens(resp: RedirectResponse, access_token: str, refresh_token: str) -> None:
    resp.set_cookie(
        settings.SESSION_ACCESS_COOKIE_NAME,
        access_token,
        **_cookie_params(),
    )
    resp.set_cookie(
        settings.SESSION_REFRESH_COOKIE_NAME,
        refresh_token,
        **_cookie_params(),
    )


def _clear_tokens(resp: RedirectResponse) -> None:
    resp.delete_cookie(settings.SESSION_ACCESS_COOKIE_NAME)
    resp.delete_cookie(settings.SESSION_REFRESH_COOKIE_NAME)


def _get_tokens(req: Request) -> tuple[str | None, str | None]:
    access_token = req.cookies.get(settings.SESSION_ACCESS_COOKIE_NAME)
    refresh_token = req.cookies.get(settings.SESSION_REFRESH_COOKIE_NAME)
    return access_token, refresh_token


async def get_current_user(req: Request) -> dict[str, Any] | None:
    access_token, refresh_token = _get_tokens(req)
    if not access_token or not refresh_token:
        return None

    try:
        return await supabase.auth_get_user(access_token)
    except Exception:
        # Access token expired/invalid -> try refresh
        try:
            refreshed = await supabase.auth_refresh(refresh_token)
            new_access_token = refreshed.get("access_token")
            if not new_access_token:
                return None
            # Update cookies by letting handlers set them;
            # here we only validate that refresh works and return user.
            user = await supabase.auth_get_user(new_access_token)
            req.state.new_access_token = new_access_token
            req.state.new_refresh_token = refreshed.get("refresh_token", refresh_token)
            return user
        except Exception:
            return None


async def _activate_pending_memberships(user_email: str, user_id: str, access_token: str) -> None:
    """Activate any pending invites that match this user's email."""
    try:
        svc = settings.SUPABASE_SERVICE_KEY
        if svc:
            await supabase.rest_update_service(
                f"rest/v1/company_members?invited_email=eq.{user_email}&status=eq.pending",
                service_key=svc,
                patch={"user_id": user_id, "status": "active", "joined_at": "now()"},
            )
        else:
            await supabase.rest_update_raw(
                f"rest/v1/company_members?invited_email=eq.{user_email}&status=eq.pending",
                access_token=access_token,
                patch={"user_id": user_id, "status": "active", "joined_at": "now()"},
                returning="minimal",
            )
        logger.info("Activated memberships for %s", user_email)
    except Exception as e:
        logger.warning("Could not activate memberships for %s: %s", user_email, e)



@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def index(req: Request):
    user = await get_current_user(req)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return RedirectResponse(url="/dashboard", status_code=302)


@app.get("/login", response_class=HTMLResponse)
async def login_page(req: Request):
    return templates.TemplateResponse(req, "login.html", {"error": None, "app_name": settings.APP_NAME})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(
    req: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        auth = await supabase.auth_sign_in_password(email=email, password=password)
        access_token = auth.get("access_token")
        refresh_token = auth.get("refresh_token")
        if not access_token or not refresh_token:
            raise RuntimeError("No access/refresh token returned by Supabase.")
        user_id = auth.get("user", {}).get("id") or auth.get("user_id") or ""
        await _activate_pending_memberships(email, user_id, access_token)
        resp = RedirectResponse(url="/dashboard", status_code=302)
        _set_tokens(resp, access_token, refresh_token)
        return resp
    except Exception as e:
        return templates.TemplateResponse(
            req, "login.html",
            {"error": _friendly_error(e, "login"), "app_name": settings.APP_NAME},
            status_code=400,
        )


@app.get("/register", response_class=HTMLResponse)
async def register_page(req: Request):
    return templates.TemplateResponse(
        req, "register.html",
        {"message": None, "error": None, "app_name": settings.APP_NAME},
    )


@app.post("/register", response_class=HTMLResponse)
async def register_submit(
    req: Request,
    email: str = Form(...),
    password: str = Form(...),
    full_name: str = Form(""),
):
    try:
        auth = await supabase.auth_sign_up(email=email, password=password, full_name=full_name.strip())
        # If email confirmations disabled, Supabase may return tokens immediately.
        access_token = auth.get("access_token")
        refresh_token = auth.get("refresh_token")
        resp: RedirectResponse
        if access_token and refresh_token:
            user_id = auth.get("user", {}).get("id") or auth.get("user_id") or ""
            await _activate_pending_memberships(email, user_id, access_token)
            resp = RedirectResponse(url="/dashboard", status_code=302)
            _set_tokens(resp, access_token, refresh_token)
            return resp
        return templates.TemplateResponse(
            req, "register.html",
            {"message": "Пользователь создан. Если включено подтверждение email, зайдите по ссылке из письма.", "error": None, "app_name": settings.APP_NAME},
        )
    except Exception as e:
        err = _friendly_error(e, "register")
        if err == "exists":
            return templates.TemplateResponse(
                req, "register.html",
                {"message": None, "error": None, "email_exists": True, "prefill_email": email, "app_name": settings.APP_NAME},
                status_code=400,
            )
        return templates.TemplateResponse(
            req, "register.html",
            {"message": None, "error": err, "email_exists": False, "app_name": settings.APP_NAME},
            status_code=400,
        )


@app.get("/logout")
async def logout(_req: Request):
    resp = RedirectResponse(url="/login", status_code=302)
    _clear_tokens(resp)
    return resp


@app.get("/auth/google")
async def auth_google(req: Request):
    verifier, challenge = _pkce_pair()
    base = settings.SITE_URL.rstrip("/") if settings.SITE_URL else str(req.base_url).rstrip("/")
    callback_url = base + "/auth/callback"
    oauth_url = (
        f"{settings.SUPABASE_URL}/auth/v1/authorize"
        f"?provider=google"
        f"&redirect_to={quote(callback_url, safe='')}"
        f"&code_challenge={challenge}"
        f"&code_challenge_method=S256"
    )
    resp = RedirectResponse(url=oauth_url, status_code=302)
    resp.set_cookie("pkce_verifier", verifier, httponly=True,
                    secure=settings.SESSION_COOKIE_SECURE, samesite="lax", max_age=300)
    return resp


@app.get("/auth/callback")
async def auth_callback(req: Request, code: str | None = None, error: str | None = None):
    if error or not code:
        return RedirectResponse(url="/login?error=oauth_failed", status_code=302)

    verifier = req.cookies.get("pkce_verifier")
    if not verifier:
        return RedirectResponse(url="/login?error=oauth_failed", status_code=302)

    try:
        url = f"{settings.SUPABASE_URL}/auth/v1/token?grant_type=pkce"
        r = await supabase._http.post(
            url,
            headers=supabase._headers(),
            json={"auth_code": code, "code_verifier": verifier},
        )
        r.raise_for_status()
        auth = r.json()
        access_token = auth.get("access_token")
        refresh_token = auth.get("refresh_token")
        if not access_token or not refresh_token:
            raise RuntimeError("No tokens from Google OAuth")

        user_obj = auth.get("user", {})
        user_email = user_obj.get("email", "")
        user_id = user_obj.get("id", "")
        if user_email and user_id:
            await _activate_pending_memberships(user_email, user_id, access_token)

        resp = RedirectResponse(url="/dashboard", status_code=302)
        _set_tokens(resp, access_token, refresh_token)
        resp.delete_cookie("pkce_verifier")
        return resp
    except Exception as e:
        logger.error("Google OAuth callback failed: %s", e)
        return RedirectResponse(url="/login?error=oauth_failed", status_code=302)


_SECTOR_MULTIPLIERS = {
    "финансы": 8.0, "банкинг": 8.0, "добывающая": 10.0, "нефть": 10.0,
    "энергетика": 7.0, "строительство": 4.0, "недвижимость": 4.0,
    "технологии": 3.0, "it": 3.0, "телеком": 5.0, "медиа": 3.0,
    "ритейл": 3.0, "логистика": 3.0, "здравоохранение": 4.0,
    "фарма": 5.0, "сельское": 2.0, "образование": 1.5,
}
_CAT_LABELS = {
    "media": "InfoField & Media", "hr": "Human Resources",
    "gr": "Gov. Relations", "pr": "PR Environment", "market": "Market & Industry",
}
_CAT_DONT = {
    "media":  "Не замалчивать негативные публикации и не игнорировать запросы СМИ",
    "hr":     "Не проводить массовых сокращений и не игнорировать внутреннее недовольство",
    "gr":     "Не вступать в открытый конфликт с регуляторами и госструктурами",
    "pr":     "Не давать публичных комментариев без согласования антикризисной стратегии",
    "market": "Не принимать крупных инвестиционных решений без пересмотра рисков",
}
_CAT_RESULT = {
    "media":  "Снижение репутационного давления и рост доверия аудитории",
    "hr":     "Стабилизация команды и удержание ключевых сотрудников",
    "gr":     "Нормализация отношений с государством и снижение регуляторных рисков",
    "pr":     "Восстановление публичного имиджа и доверия партнёров",
    "market": "Защита рыночной позиции и снижение финансовых потерь",
}


def _compute_ceo_view(
    risk_map: dict,
    name_map: dict[str, str],
    industry_map: dict[str, str],
    prev_risk_map: dict | None = None,
) -> dict | None:
    scores = [(cid, r["score"]) for cid, r in risk_map.items() if r.get("score") is not None]
    if not scores:
        return None

    global_score = round(sum(s for _, s in scores) / len(scores))

    if global_score >= 65:
        crisis_status, crisis_color, crisis_label = "crisis", "#f87171", "Crisis"
    elif global_score >= 35:
        crisis_status, crisis_color, crisis_label = "warning", "#fbbf24", "Warning"
    else:
        crisis_status, crisis_color, crisis_label = "normal", "#34d399", "Normal"

    # Sector multiplier from highest-risk company industry
    primary_cid = max(scores, key=lambda x: x[1])[0]
    industry = industry_map.get(primary_cid, "")
    sector_mult = 1.5
    for kw, mult in _SECTOR_MULTIPLIERS.items():
        if kw in industry:
            sector_mult = mult
            break

    # Financial Impact: sentiment × reach × sector_multiplier
    max_score = max(s for _, s in scores)
    sentiment_factor = round(max_score / 100, 2)
    reach_factor = round(1_000_000 * (1 + max_score / 50))  # estimated audience reach
    potential_loss = round(sentiment_factor * reach_factor * sector_mult)

    # Category breakdown for primary company
    primary_risk = risk_map[primary_cid]
    cat_scores: dict[str, int | None] = {}
    for cat_key in ("pr", "hr", "gr", "media", "market"):
        cat_data = (primary_risk.get("categories") or {}).get(cat_key)
        if isinstance(cat_data, dict) and cat_data.get("score") is not None:
            cat_scores[cat_key] = int(cat_data["score"])
        else:
            cat_scores[cat_key] = None

    # Growth rate vs previous run
    prev_primary = (prev_risk_map or {}).get(primary_cid, {})
    prev_cats = (prev_primary.get("categories") or {}) if prev_primary else {}
    growth_map: dict[str, int | None] = {}
    for cat_key in ("pr", "hr", "gr", "media", "market"):
        curr = cat_scores.get(cat_key)
        prev_cat = prev_cats.get(cat_key)
        if curr is not None and isinstance(prev_cat, dict) and prev_cat.get("score") is not None:
            growth_map[cat_key] = curr - int(prev_cat["score"])
        else:
            growth_map[cat_key] = None

    # Global delta vs previous
    prev_scores = [(cid, r.get("score")) for cid, r in (prev_risk_map or {}).items() if r.get("score") is not None]
    prev_global = round(sum(s for _, s in prev_scores) / len(prev_scores)) if prev_scores else None
    global_delta = (global_score - prev_global) if prev_global is not None else None

    # Top 5 risks from all categories across all companies
    all_risks: list[dict] = []
    for cid, r in risk_map.items():
        cats = r.get("categories") or {}
        for cat_key, cat in cats.items():
            if not isinstance(cat, dict):
                continue
            cat_score = int(cat.get("score") or 0)
            gd = growth_map.get(cat_key)
            for risk in (cat.get("risks") or []):
                text = (risk.get("text") if isinstance(risk, dict) else str(risk) or "").strip()
                if not text or text.startswith("Анализ недоступен") or "Error code:" in text or "authentication_error" in text:
                    continue
                all_risks.append({
                    "text": text,
                    "category": _CAT_LABELS.get(cat_key, cat_key),
                    "cat_key": cat_key,
                    "probability": min(99, round(cat_score * 0.92)),
                    "cat_score": cat_score,
                    "impact": round((cat_score / 100) * potential_loss / 5),
                    "growth": gd,
                    "company": name_map.get(cid, ""),
                })
    all_risks.sort(key=lambda x: x["cat_score"], reverse=True)
    top_risks = all_risks[:5]

    # Action plan from highest-risk company
    advice = (primary_risk.get("advice") or "").strip()

    # Top 2 risk categories for "don't" and "result"
    cats_sorted = sorted(
        ((k, v) for k, v in (primary_risk.get("categories") or {}).items() if isinstance(v, dict)),
        key=lambda kv: int(kv[1].get("score") or 0), reverse=True
    )
    top_cat_key = cats_sorted[0][0] if cats_sorted else "pr"
    dont_text = _CAT_DONT.get(top_cat_key, "Не принимать поспешных решений без данных")
    result_text = _CAT_RESULT.get(top_cat_key, "Снижение общего уровня риска и стабилизация позиций")

    return {
        "global_score": global_score,
        "global_delta": global_delta,
        "crisis_status": crisis_status,
        "crisis_color": crisis_color,
        "crisis_label": crisis_label,
        "potential_loss": potential_loss,
        "sentiment_factor": sentiment_factor,
        "reach_factor": reach_factor,
        "sector_mult": sector_mult,
        "cat_scores": cat_scores,
        "growth_map": growth_map,
        "top_risks": top_risks,
        "advice": advice,
        "dont_text": dont_text,
        "result_text": result_text,
        "company_count": len(scores),
        "primary_company": name_map.get(primary_cid, ""),
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(req: Request):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    # Use refreshed token if available
    new_access = getattr(req.state, "new_access_token", None)
    new_refresh = getattr(req.state, "new_refresh_token", None)
    effective_token = new_access or access_token

    try:
        member_rows = await supabase.rest_select(
            table="company_members",
            access_token=effective_token,
            select="id,role,status,company_id,companies(id,name,ceo_email,industry,created_at)",
            order_by="joined_at.desc",
            query_params={"user_id": f"eq.{user.get('id')}", "status": "eq.active"},
        )
    except Exception as e:
        if "401" in str(e) or "403" in str(e):
            resp = RedirectResponse(url="/login", status_code=302)
            _clear_tokens(resp)
            return resp
        member_rows = []

    # Fetch latest risk score + categories + advice for each company
    company_ids = [
        m["companies"]["id"]
        for m in member_rows
        if m.get("companies") and m["companies"].get("id")
    ]
    risk_map: dict[str, dict] = {}
    prev_risk_map: dict[str, dict] = {}
    snapshot_map: dict[str, dict] = {}

    if company_ids and settings.SUPABASE_SERVICE_KEY:
        ids_filter = f"in.({','.join(company_ids)})"
        try:
            risk_rows, snap_rows = await asyncio.gather(
                supabase.rest_select_service(
                    table="risk_runs",
                    service_key=settings.SUPABASE_SERVICE_KEY,
                    select="company_id,score,categories,advice,created_at",
                    query_params={"company_id": ids_filter, "order": "created_at.desc"},
                ),
                supabase.rest_select_service(
                    table="monitoring_snapshots",
                    service_key=settings.SUPABASE_SERVICE_KEY,
                    select="company_id,last_checked_at,last_score,recent_news",
                    query_params={"company_id": ids_filter},
                ),
            )
            prev_risk_map: dict[str, dict] = {}
            seen_cids: set[str] = set()
            for r in risk_rows:
                cid = r.get("company_id")
                if not cid:
                    continue
                if cid not in risk_map:
                    risk_map[cid] = r
                    seen_cids.add(cid)
                elif cid not in prev_risk_map:
                    prev_risk_map[cid] = r
            for s in snap_rows:
                cid = s.get("company_id")
                if cid:
                    snapshot_map[cid] = s
        except Exception as e:
            logger.warning("Dashboard risk fetch failed: %s", e)

    # Build name/industry maps
    name_map: dict[str, str] = {}
    industry_map: dict[str, str] = {}
    for m in member_rows:
        c = m.get("companies")
        if c and c.get("id"):
            name_map[c["id"]] = c.get("name", "")
            industry_map[c["id"]] = (c.get("industry") or "").lower()

    # Compute CEO view (aggregated risk intelligence)
    ceo_view = _compute_ceo_view(risk_map, name_map, industry_map, prev_risk_map)

    # Build combined news feed from monitoring snapshots
    dashboard_news: list[dict] = []
    for s in snapshot_map.values():
        cid = s.get("company_id")
        for item in (s.get("recent_news") or []):
            dashboard_news.append({**item, "company_id": cid, "company_name": name_map.get(cid, "")})
    dashboard_news.sort(key=lambda x: x.get("captured_at", ""), reverse=True)
    dashboard_news = dashboard_news[:40]

    ctx = {
        "member_rows": member_rows,
        "risk_map": risk_map,
        "snapshot_map": snapshot_map,
        "ceo_view": ceo_view,
        "dashboard_news": dashboard_news,
        "user_id": user.get("id"),
        "app_name": settings.APP_NAME,
    }

    if new_access and new_refresh:
        resp = templates.TemplateResponse(req, "dashboard.html", ctx)
        resp.set_cookie(settings.SESSION_ACCESS_COOKIE_NAME, new_access, **_cookie_params())
        resp.set_cookie(settings.SESSION_REFRESH_COOKIE_NAME, new_refresh, **_cookie_params())
        return resp

    return templates.TemplateResponse(req, "dashboard.html", ctx)


@app.get("/companies/new", response_class=HTMLResponse)
async def company_new_page(req: Request):
    user = await get_current_user(req)
    if not user:
        return RedirectResponse(url="/login", status_code=302)
    return templates.TemplateResponse(req, "company_new.html", {"app_name": settings.APP_NAME})


@app.post("/companies/create")
async def companies_create(
    req: Request,
    name: str = Form(...),
    ceo_email: str = Form(...),
    ceo_name: str = Form(""),
    website: str = Form(""),
    location: str = Form(""),
    industry: str = Form(""),
    description: str = Form(""),
    market_info: str = Form(""),
    competitors_json: str = Form("[]"),
    news_sources_json: str = Form("[]"),
):
    import json as _json
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    row = {
        "name": name.strip(),
        "ceo_email": ceo_email.strip(),
    }
    if ceo_name.strip():   row["ceo_name"]    = ceo_name.strip()
    if website.strip():    row["website"]      = website.strip()
    if location.strip():   row["location"]     = location.strip()
    if industry.strip():   row["industry"]     = industry.strip()
    if description.strip():row["description"]  = description.strip()
    if market_info.strip():row["market_info"]  = market_info.strip()

    try:
        news_list = _json.loads(news_sources_json) if news_sources_json else []
        if isinstance(news_list, list):
            row["news_sources"] = news_list
    except Exception:
        pass

    company = await supabase.rest_insert(
        table="companies",
        access_token=effective_token,
        row=row,
    )
    company_id = company["id"]

    # Insert competitors into the competitors table
    try:
        comp_list = _json.loads(competitors_json) if competitors_json else []
        for c in comp_list[:5]:
            if isinstance(c, dict) and c.get("name", "").strip():
                await supabase.rest_insert(
                    table="competitors",
                    access_token=effective_token,
                    row={
                        "company_id": company_id,
                        "name": c["name"].strip(),
                        "website": c.get("website", "").strip(),
                    },
                )
    except Exception as e:
        logger.warning("Failed to insert competitors: %s", e)

    return RedirectResponse(url=f"/companies/{company_id}", status_code=302)


@app.get("/signal", response_class=HTMLResponse)
async def signal_page(req: Request):
    return templates.TemplateResponse(req, "signal.html", {})


@app.get("/companies/{company_id}", response_class=HTMLResponse)
async def company_detail(req: Request, company_id: str):
    try:
        return await _company_detail_inner(req, company_id)
    except Exception:
        import traceback
        logger.error("company_detail ERROR:\n%s", traceback.format_exc())
        raise


async def _company_detail_inner(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    async def _safe_emails():
        try:
            return await supabase.rest_select(
                table="emails",
                access_token=effective_token,
                select="*",
                order_by="created_at.desc",
                query_params={"company_id": f"eq.{company_id}"},
            )
        except Exception as e:
            logger.warning("Could not fetch emails: %s", e)
            return []

    async def _safe_members():
        try:
            svc = settings.SUPABASE_SERVICE_KEY
            if svc:
                return await supabase.rest_select_service(
                    table="company_members",
                    service_key=svc,
                    select="id,invited_email,role,status,joined_at",
                    query_params={"company_id": f"eq.{company_id}"},
                )
            return await supabase.rest_select(
                table="company_members",
                access_token=effective_token,
                select="id,invited_email,role,status,joined_at",
                order_by="invited_at.asc",
                query_params={"company_id": f"eq.{company_id}"},
            )
        except Exception as e:
            logger.warning("Could not fetch members: %s", e)
            return []

    async def _get_user_role():
        try:
            rows = await supabase.rest_select(
                table="company_members",
                access_token=effective_token,
                select="role",
                query_params={"company_id": f"eq.{company_id}", "user_id": f"eq.{user.get('id')}"},
            )
            return rows[0]["role"] if rows else "member"
        except Exception:
            return "member"

    async def _get_monitor_snapshot():
        if not settings.SUPABASE_SERVICE_KEY:
            return None
        try:
            rows = await supabase.rest_select_service(
                table="monitoring_snapshots",
                service_key=settings.SUPABASE_SERVICE_KEY,
                select="last_checked_at,last_run_at,last_score,recent_news",
                query_params={"company_id": f"eq.{company_id}"},
            )
            return rows[0] if rows else None
        except Exception:
            return None

    async def _get_employees():
        try:
            return await supabase.rest_select(
                table="employees",
                access_token=effective_token,
                select="id,full_name,email,position,department,photo_url",
                order_by="created_at.desc",
                query_params={"company_id": f"eq.{company_id}"},
            )
        except Exception:
            try:
                return await supabase.rest_select(
                    table="employees",
                    access_token=effective_token,
                    select="id,full_name,email,position,department",
                    order_by="created_at.desc",
                    query_params={"company_id": f"eq.{company_id}"},
                )
            except Exception:
                return []

    async def _safe_risk_runs(token: str, cid: str):
        try:
            return await supabase.rest_select(
                table="risk_runs",
                access_token=token,
                select="id,status,created_at,updated_at,score,advice,risks,categories,scenarios,social_posts",
                order_by="created_at.desc",
                query_params={"company_id": f"eq.{cid}"},
            )
        except Exception:
            # social_posts column may not exist yet — fallback without it
            try:
                return await supabase.rest_select(
                    table="risk_runs",
                    access_token=token,
                    select="id,status,created_at,updated_at,score,advice,risks,categories,scenarios",
                    order_by="created_at.desc",
                    query_params={"company_id": f"eq.{cid}"},
                )
            except Exception:
                return []

    company_rows, all_companies, employees, risk_runs, emails_rows, members, user_role, monitor_snapshot = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name,ceo_email,industry,created_at",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name",
            order_by="name.asc",
        ),
        _get_employees(),
        _safe_risk_runs(effective_token, company_id),  # noqa: E501
        _safe_emails(),
        _safe_members(),
        _get_user_role(),
        _get_monitor_snapshot(),
    )
    company = company_rows[0] if company_rows else None

    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    finance_data = await (
        finance_client.fetch_market_data(company["name"]) if settings.FINANCE_ENABLED
        else asyncio.sleep(0, result={"found": False})
    )
    company_news: list = []  # loaded lazily via /companies/{id}/news

    msg = req.query_params.get("msg")
    error = req.query_params.get("error")

    # Compute financial impact metrics for Risk Overview
    latest_run = risk_runs[0] if risk_runs else None
    risk_impact: dict = {}
    if latest_run and latest_run.get("score") is not None:
        score = int(latest_run["score"])
        industry_str = (company.get("industry") or "").lower()
        sect_mult = 1.5
        for kw, mult in _SECTOR_MULTIPLIERS.items():
            if kw in industry_str:
                sect_mult = mult
                break
        sentiment_f = round(score / 100, 2)
        reach_f = round(1_000_000 * (1 + score / 50))
        risk_impact = {
            "potential_loss": round(sentiment_f * reach_f * sect_mult),
            "sentiment_factor": sentiment_f,
            "reach_factor": reach_f,
            "sector_mult": sect_mult,
        }

    return templates.TemplateResponse(
        req, "company_detail.html",
        {
            "company": company,
            "all_companies": [c for c in all_companies if c["id"] != company_id],
            "employees": employees,
            "risk_runs": risk_runs,
            "company_news": company_news,
            "emails": emails_rows,
            "members": members,
            "user_role": user_role,
            "is_admin": user_role == "admin",
            "finance": finance_data,
            "monitor_snapshot": monitor_snapshot,
            "risk_impact": risk_impact,
            "msg": msg,
            "error": error,
            "app_name": settings.APP_NAME,
        },
    )


async def _get_market_cache(company_id: str) -> dict | None:
    try:
        rows = await supabase.rest_select_service(
            table="market_cache",
            service_key=settings.SUPABASE_SERVICE_KEY,
            query_params={"company_id": f"eq.{company_id}"},
        )
        return rows[0] if rows else None
    except Exception:
        return None


async def _refresh_market_cache(company_id: str, company_name: str) -> dict:
    """Fetch fresh market data + AI, save to cache, return result."""
    market_data, company_news = await asyncio.gather(
        finance_client.fetch_full_market_data(company_name, twelve_key=settings.TWELVE_DATA_API_KEY),
        news_client.fetch_news(company_name),
    )

    # Don't overwrite existing cache with empty data (e.g. when APIs are rate-limited)
    has_useful_data = market_data.get("found") or market_data.get("market_indices") or market_data.get("top_stocks")
    if not has_useful_data:
        existing = await _get_market_cache(company_id)
        if existing:
            logger.info("Market fetch returned no data for %s — keeping existing cache", company_name)
            return existing

    ai_analysis = None
    if settings.ANTHROPIC_API_KEY:
        ai_analysis = await ai_client.analyze_market_position(
            company_name=company_name,
            stock=market_data.get("stock"),
            market_indices=market_data.get("market_indices", []),
            top_stocks=market_data.get("top_stocks", []),
            news=company_news,
            api_key=settings.ANTHROPIC_API_KEY,
        )
    if settings.SUPABASE_SERVICE_KEY:
        try:
            from datetime import datetime, timezone
            await supabase.rest_upsert_service(
                table="market_cache",
                service_key=settings.SUPABASE_SERVICE_KEY,
                rows=[{
                    "company_id": company_id,
                    "finance": market_data,
                    "ai_analysis": ai_analysis,
                    "news": company_news,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }],
                on_conflict="company_id",
            )
        except Exception as e:
            logger.warning("Failed to save market cache: %s", e)
    return {"finance": market_data, "ai_analysis": ai_analysis, "news": company_news}


@app.get("/companies/{company_id}/news")
async def company_news_api(req: Request, company_id: str):
    """Fetch news for a company lazily (called from frontend after page load)."""
    user = await get_current_user(req)
    if not user:
        return {"error": "unauthorized"}

    access_token, _ = _get_tokens(req)
    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="name,ceo_name,industry,location,news_sources",
        query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return {"news": []}

    company = company_rows[0]
    news = await news_client.fetch_news(company["name"], company=company)
    return {"news": news}


@app.get("/companies/{company_id}/social")
async def company_social(req: Request, company_id: str):
    """Fetch social media posts for a company (async, called from frontend)."""
    user = await get_current_user(req)
    if not user:
        return {"error": "unauthorized"}

    access_token, _ = _get_tokens(req)
    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="name",
        query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return {"error": "not found"}

    company_name = company_rows[0]["name"]

    # Optional social account handles passed from frontend (stored in localStorage)
    instagram_url   = req.query_params.get("instagram_url", "").strip()
    tiktok_url      = req.query_params.get("tiktok_url", "").strip()
    platform_filter = req.query_params.get("platform", "").strip()  # e.g. "threads"

    if not settings.APIFY_TOKEN:
        return {"posts": [], "message": "Apify не настроен"}

    try:
        if platform_filter == "threads":
            social_data = {"threads": await apify_client.fetch_threads_posts(
                company_name, settings.APIFY_TOKEN, max_posts=20
            )}
        else:
            social_data = await apify_client.fetch_all_social(
                company_name=company_name,
                token=settings.APIFY_TOKEN,
                limit=20,
                instagram_url=instagram_url or None,
                tiktok_url=tiktok_url or None,
            )
    except Exception as e:
        return {"posts": [], "message": str(e)}

    posts = []
    platform_map = {
        "threads":   ("Threads",   "🧵"),
        "instagram": ("Instagram", "📸"),
        "tiktok":    ("TikTok",    "🎵"),
        "youtube":   ("YouTube",   "▶️"),
        "twitter":   ("Twitter/X", "🐦"),
        "facebook":  ("Facebook",  "👤"),
    }
    for platform, (label, icon) in platform_map.items():
        for p in social_data.get(platform, []):
            text = (p.get("captionText") or p.get("text") or p.get("title") or "").strip()
            url  = (p.get("postUrl") or p.get("url") or "").strip()
            author = (p.get("username") or p.get("author") or "").strip()
            if text or url:
                posts.append({
                    "platform": platform,
                    "label":    label,
                    "icon":     icon,
                    "author":   author,
                    "text":     text[:300],
                    "url":      url,
                })

    return {"posts": posts}


@app.get("/companies/{company_id}/market", response_class=HTMLResponse)
async def market_analysis_page(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token
    embed = req.query_params.get("embed") == "1"

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="id,name,ceo_email",
        query_params={"id": f"eq.{company_id}"},
    )
    company = company_rows[0] if company_rows else None
    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    # Load from cache instantly — no blocking
    cache = await _get_market_cache(company_id)
    if cache:
        return templates.TemplateResponse(req, "market.html", {
            "app_name": settings.APP_NAME,
            "company": company,
            "finance": cache.get("finance") or {},
            "ai_analysis": cache.get("ai_analysis"),
            "company_news": cache.get("news") or [],
            "cache_updated_at": cache.get("updated_at"),
            "loading": False,
            "embed": embed,
        })

    # No cache yet — show skeleton, JS will poll for data
    return templates.TemplateResponse(req, "market.html", {
        "app_name": settings.APP_NAME,
        "company": company,
        "finance": {},
        "ai_analysis": None,
        "company_news": [],
        "cache_updated_at": None,
        "loading": True,
        "embed": embed,
    })


@app.get("/companies/{company_id}/market/data")
async def market_data_api(req: Request, company_id: str):
    """JSON endpoint — fetches fresh market data and saves to cache."""
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="id,name",
        query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return JSONResponse({"error": "not found"}, status_code=404)

    company_name = company_rows[0]["name"]
    result = await _refresh_market_cache(company_id, company_name)
    return JSONResponse({"status": "ready", **result})


@app.post("/companies/{company_id}/market/refresh")
async def market_refresh(req: Request, company_id: str):
    """Trigger background cache refresh, return immediately."""
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="id,name",
        query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return JSONResponse({"error": "not found"}, status_code=404)

    company_name = company_rows[0]["name"]
    asyncio.ensure_future(_refresh_market_cache(company_id, company_name))
    return JSONResponse({"status": "refreshing"})


@app.post("/members/invite")
async def members_invite(
    req: Request,
    company_id: str = Form(...),
    invited_email: str = Form(...),
    role: str = Form(""),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    email_clean = invited_email.lower().strip()
    role_clean = role.strip() or None

    try:
        company_rows = await supabase.rest_select(
            table="companies",
            access_token=access_token,
            select="name",
            query_params={"id": f"eq.{company_id}"},
        )
        company_name = company_rows[0]["name"] if company_rows else ""
        logger.info("Inviting %s to company '%s' (role=%s)", email_clean, company_name, role_clean)

        invite_token = secrets.token_urlsafe(32)
        try:
            await supabase.rest_insert(
                table="company_members",
                access_token=access_token,
                row={
                    "company_id": company_id,
                    "invited_email": email_clean,
                    "role": role_clean,
                    "owner_user_id": user.get("id"),
                    "invite_token": invite_token,
                },
            )
            logger.info("Member row created for %s", email_clean)
        except Exception as insert_err:
            if "409" in str(insert_err):
                logger.info("Member %s already exists, updating invite_token", email_clean)
                # Update existing row with fresh token
                try:
                    svc = settings.SUPABASE_SERVICE_KEY
                    if svc:
                        await supabase.rest_update_service(
                            f"rest/v1/company_members?company_id=eq.{company_id}&invited_email=eq.{email_clean}",
                            service_key=svc,
                            patch={"invite_token": invite_token},
                        )
                    else:
                        await supabase.rest_update_raw(
                            f"rest/v1/company_members?company_id=eq.{company_id}&invited_email=eq.{email_clean}",
                            access_token=access_token,
                            patch={"invite_token": invite_token},
                            returning="minimal",
                        )
                    logger.info("invite_token updated for %s", email_clean)
                except Exception as upd_err:
                    logger.error("Failed to update invite_token: %s", upd_err)
            else:
                raise

        # Send invite email via n8n if webhook is configured
        if settings.N8N_INVITE_WEBHOOK_URL:
            logger.info("Sending invite webhook to %s", settings.N8N_INVITE_WEBHOOK_URL)
            _role_labels = {
                "media": "InfoField & Media", "hr": "Human Resources",
                "gr": "Gov. Relations", "pr": "PR Environment", "market": "Market & Industry",
            }
            base_url = str(req.base_url).rstrip("/")
            try:
                await supabase.webhook_post(
                    settings.N8N_INVITE_WEBHOOK_URL,
                    payload={
                        "invited_email": email_clean,
                        "company_id": company_id,
                        "company_name": company_name,
                        "role": role_clean,
                        "role_label": _role_labels.get(role_clean or "", ""),
                        "join_url": f"{base_url}/join/{invite_token}",
                        "invited_by_user_id": user.get("id"),
                    },
                )
                logger.info("Invite webhook sent OK")
            except Exception as e:
                logger.error("Invite webhook failed: %s", e)
        else:
            logger.warning("N8N_INVITE_WEBHOOK_URL not set, skipping email")

    except Exception as e:
        logger.error("Invite failed: %s", e)
    return RedirectResponse(url=f"/companies/{company_id}?msg=invited", status_code=302)


@app.get("/join/{token}", response_class=HTMLResponse)
async def join_page(req: Request, token: str):
    service_key = settings.SUPABASE_SERVICE_KEY
    try:
        if service_key:
            rows = await supabase.rest_select_service(
                table="company_members",
                service_key=service_key,
                select="id,invited_email,role,status,company_id,invite_token,companies(name)",
                query_params={"invite_token": f"eq.{token}"},
            )
        else:
            rows = []
    except Exception as e:
        logger.warning("join_page token lookup failed: %s", e)
        rows = []

    if not rows:
        return templates.TemplateResponse(req, "join.html", {
            "invalid": True, "app_name": settings.APP_NAME,
            "company_name": "", "role_label": "", "invited_email": "", "token": token,
        })

    m = rows[0]
    _role_labels = {
        "media": "InfoField & Media", "hr": "Human Resources",
        "gr": "Gov. Relations", "pr": "PR Environment", "market": "Market & Industry",
    }
    role = m.get("role") or ""
    company_obj = m.get("companies") or {}
    return templates.TemplateResponse(req, "join.html", {
        "invalid": False,
        "token": token,
        "company_name": company_obj.get("name", ""),
        "role_label": _role_labels.get(role, ""),
        "invited_email": m.get("invited_email", ""),
        "app_name": settings.APP_NAME,
    })


@app.post("/members/{member_id}/remove")
async def members_remove(req: Request, member_id: str, company_id: str = Form(...)):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    try:
        await supabase.rest_delete(
            table="company_members",
            access_token=access_token,
            query_params={"id": f"eq.{member_id}"},
        )
    except Exception as e:
        logger.warning("Remove member failed: %s", e)
    return RedirectResponse(url=f"/companies/{company_id}", status_code=302)


@app.post("/employees/create")
async def employees_create(
    req: Request,
    company_id: str = Form(...),
    full_name: str = Form(...),
    email: str = Form(...),
    position: str = Form(""),
    department: str = Form(""),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    await supabase.rest_insert(
        table="employees",
        access_token=effective_token,
        row={
            "company_id": company_id,
            "full_name": full_name,
            "email": email,
            "position": position,
            "department": department,
        },
    )
    return RedirectResponse(url=f"/companies/{company_id}", status_code=302)


@app.post("/employees/{employee_id}/photo")
async def employee_upload_photo(
    req: Request,
    employee_id: str,
    photo: UploadFile = File(...),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    data = await photo.read()
    if len(data) > 5 * 1024 * 1024:
        return Response("File too large (max 5 MB)", status_code=400)

    mime = photo.content_type or "image/jpeg"
    b64 = base64.b64encode(data).decode()
    data_url = f"data:{mime};base64,{b64}"

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    # Fetch company_id first (needed for redirect even on error)
    rows = await supabase.rest_select(
        table="employees",
        access_token=effective_token,
        select="company_id",
        query_params={"id": f"eq.{employee_id}"},
    )
    company_id = rows[0]["company_id"] if rows else ""

    if not settings.SUPABASE_SERVICE_KEY:
        return RedirectResponse(
            url=f"/companies/{company_id}?tab=team&error=no_service_key",
            status_code=302,
        )

    try:
        await supabase.rest_update_service(
            f"rest/v1/employees?id=eq.{employee_id}",
            settings.SUPABASE_SERVICE_KEY,
            {"photo_url": data_url},
        )
    except Exception as e:
        logger.error("Photo upload failed for employee %s: %s", employee_id, e)
        return RedirectResponse(
            url=f"/companies/{company_id}?tab=team&error=photo_upload_failed",
            status_code=302,
        )

    return RedirectResponse(url=f"/companies/{company_id}?tab=team", status_code=302)


@app.get("/monitor/status")
async def monitor_status():
    """Returns last check/analysis times from monitoring_snapshots for all companies."""
    if not settings.SUPABASE_SERVICE_KEY:
        return {"error": "SUPABASE_SERVICE_KEY not configured"}
    try:
        snapshots = await supabase.rest_select_service(
            table="monitoring_snapshots",
            service_key=settings.SUPABASE_SERVICE_KEY,
            select="company_id,last_checked_at,last_run_at,last_score",
            order_by="last_checked_at.desc",
        )
        companies = await supabase.rest_select_service(
            table="companies",
            service_key=settings.SUPABASE_SERVICE_KEY,
            select="id,name",
        )
        name_map = {c["id"]: c["name"] for c in companies}
        result = []
        for s in snapshots:
            cid = s.get("company_id")
            result.append({
                "company_id": cid,
                "company_name": name_map.get(cid, "unknown"),
                "last_checked_at": s.get("last_checked_at"),
                "last_run_at": s.get("last_run_at"),
                "last_score": s.get("last_score"),
            })
        from datetime import datetime, timezone
        return {
            "server_time": datetime.now(timezone.utc).isoformat(),
            "monitoring_enabled": settings.MONITORING_ENABLED,
            "interval_hours": settings.MONITORING_INTERVAL_HOURS,
            "companies": result,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/monitor/run")
async def monitor_run_now():
    """Manually trigger one monitoring cycle for all companies."""
    if not settings.SUPABASE_SERVICE_KEY:
        return {"error": "SUPABASE_SERVICE_KEY not configured"}
    asyncio.ensure_future(monitor_client.run_monitoring_cycle(
        supabase=supabase,
        news_client=news_client,
        ai_client=ai_client,
        apify_client=apify_client,
        hh_client=hh_client,
        finance_client=finance_client,
        settings=settings,
        ci_client=ci_client,
    ))
    return {"status": "started", "message": "Monitoring cycle started in background"}


# ── Background analysis job tracker (in-memory, resets on deploy) ──
_job_stages: dict[str, dict] = {}


def _build_social_posts(social_data: dict) -> list[dict]:
    posts = []
    for platform, label in (("threads", "Threads"), ("instagram", "Instagram"), ("tiktok", "TikTok"), ("youtube", "YouTube")):
        for p in social_data.get(platform, []):
            text   = (p.get("text") or p.get("captionText") or p.get("title") or "").strip()
            url    = (p.get("postUrl") or p.get("url") or "").strip()
            author = (p.get("username") or p.get("author") or "").strip()
            if text or url:
                posts.append({"platform": platform, "label": label, "author": author, "text": text[:400], "url": url})
    return posts


async def _run_analysis_bg(
    run_id: str,
    company_name: str,
    company_profile: dict,
    employees: list,
    emails_rows: list,
) -> None:
    def _set(stage: str, pct: int) -> None:
        _job_stages[run_id] = {"stage": stage, "pct": pct}

    try:
        _set("Загружаем новости и данные…", 15)
        news, yandex_news, vacancies, regulatory_news, market_news, gr_news, finance_data = await asyncio.gather(
            news_client.fetch_news(company_name, limit=12, company=company_profile),
            news_client.fetch_yandex_news(company_name, limit=8, company=company_profile),
            hh_client.fetch_vacancies(company_name, limit=10),
            news_client.fetch_regulatory_news(company_name, limit=6, company=company_profile),
            news_client.fetch_market_news(company_name, limit=6, company=company_profile),
            gr_client.fetch_gr_news_all(company_name, limit=20),
            finance_client.fetch_market_data(company_name) if settings.FINANCE_ENABLED else asyncio.sleep(0, result={"found": False}),
        )

        _set("Загружаем соцсети (Threads, TikTok, YouTube…)", 40)
        social_data = await (
            apify_client.fetch_all_social(company_name, settings.APIFY_TOKEN, limit=15)
            if settings.APIFY_TOKEN else asyncio.sleep(0, result=_EMPTY_SOCIAL)
        )

        threads_posts = social_data.get("threads", [])

        reliable_emails = [e for e in emails_rows if e.get("confidence_score") is None or (e.get("confidence_score") or 0) >= 40]

        def _by_type(emails: list, kws: list) -> list:
            kw = [k.lower() for k in kws]
            matched   = [e for e in emails if any(k in (e.get("position") or "").lower() for k in kw)]
            unmatched = [e for e in emails if not (e.get("position") or "").strip()]
            seen = {id(e) for e in matched}
            return matched + [e for e in unmatched if id(e) not in seen]

        hr_emails = _by_type(reliable_emails, ["hr", "кадр", "персонал", "human"])
        pr_emails = _by_type(reliable_emails, ["pr", "маркетинг", "marketing", "коммуникац", "медиа"])
        gr_emails = _by_type(reliable_emails, ["gr", "юрид", "legal", "compliance", "регулятор", "government"])
        classified = {id(e) for lst in (hr_emails, pr_emails, gr_emails) for e in lst}
        other = [e for e in reliable_emails if id(e) not in classified and (e.get("position") or "").strip()]
        hr_emails += other; pr_emails += other; gr_emails += other

        _set("AI анализирует риски…", 70)
        analysis = await ai_client.analyze_company_risks(
            company_name=company_name, employees=employees,
            news=news, yandex_news=yandex_news, threads_posts=threads_posts,
            social={"instagram": social_data.get("instagram", []), "tiktok": social_data.get("tiktok", []), "youtube": social_data.get("youtube", [])},
            reviews=[], vacancies=vacancies,
            regulatory_news=regulatory_news + gr_news, market_news=market_news,
            finance=finance_data, hr_emails=hr_emails, pr_emails=pr_emails, gr_emails=gr_emails,
            api_key=settings.ANTHROPIC_API_KEY,
        )

        _set("Сохраняем результаты…", 92)
        await supabase.rest_update_service(
            path_with_query=f"rest/v1/risk_runs?id=eq.{run_id}",
            service_key=settings.SUPABASE_SERVICE_KEY,
            patch={
                "status":       "done",
                "score":        analysis["score"],
                "advice":       analysis["advice"],
                "risks":        analysis["risks"],
                "categories":   analysis["categories"],
                "scenarios":    analysis.get("scenarios", []),
                "social_posts": _build_social_posts(social_data),
            },
        )
        _job_stages[run_id] = {"stage": "Готово", "pct": 100, "done": True, "score": analysis["score"]}
        logger.info("Background analysis done run_id=%s score=%s", run_id, analysis["score"])

    except Exception:
        import traceback
        logger.error("Background analysis failed run_id=%s:\n%s", run_id, traceback.format_exc())
        _job_stages[run_id] = {"stage": "Ошибка анализа", "pct": 0, "error": True}
        try:
            await supabase.rest_update_service(
                path_with_query=f"rest/v1/risk_runs?id=eq.{run_id}",
                service_key=settings.SUPABASE_SERVICE_KEY,
                patch={"status": "error"},
            )
        except Exception:
            pass


@app.get("/risks/{run_id}/status")
async def risk_run_status(run_id: str, req: Request):
    user = await get_current_user(req)
    if not user:
        return {"error": "unauthorized"}
    info = _job_stages.get(run_id)
    if info:
        status = "error" if info.get("error") else ("done" if info.get("done") else "running")
        return {"status": status, **info}
    # Not in memory (server restarted) — check DB
    try:
        rows = await supabase.rest_select_service(
            table="risk_runs",
            service_key=settings.SUPABASE_SERVICE_KEY,
            select="status,score",
            query_params={"id": f"eq.{run_id}"},
        )
        if rows:
            r = rows[0]
            db_status = r.get("status", "unknown")
            return {"status": db_status, "pct": 100 if db_status == "done" else 0, "stage": db_status}
    except Exception:
        pass
    return {"status": "unknown", "pct": 0, "stage": ""}


@app.post("/risks/run")
async def risks_run(
    req: Request,
    company_id: str = Form(...),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    # Быстро: профиль компании + сотрудники + emails
    company_rows, employees, emails_rows = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="name,ceo_name,location,industry,description,market_info,news_sources,website",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="employees",
            access_token=effective_token,
            select="full_name,position,department",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="emails",
            access_token=effective_token,
            select="from_email,position,text,confidence_score",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
    )
    company_profile = company_rows[0] if company_rows else {}
    company_name = company_profile.get("name", "")

    # Создаём запись со статусом "running" — пользователь сразу видит прогресс
    run_row = await supabase.rest_insert(
        table="risk_runs",
        access_token=effective_token,
        row={"company_id": company_id, "status": "running", "score": None,
             "advice": None, "risks": [], "categories": {}, "scenarios": [], "social_posts": []},
    )
    run_id = run_row.get("id", "")
    _job_stages[run_id] = {"stage": "Запуск анализа…", "pct": 5}

    # Запускаем фоновую задачу — не блокируем HTTP ответ
    asyncio.create_task(_run_analysis_bg(
        run_id=run_id,
        company_name=company_name,
        company_profile=company_profile,
        employees=employees,
        emails_rows=emails_rows,
    ))

    return RedirectResponse(url=f"/companies/{company_id}?run_id={run_id}", status_code=302)


_CAT_META = {
    "media":  ("📰", "InfoField & Media",  "#93bbff"),
    "hr":     ("👥", "Human Resources",    "#4ade80"),
    "gr":     ("⚖️", "Gov. Relations",     "#f87171"),
    "pr":     ("🌐", "PR Environment",     "#c4b5fd"),
    "market": ("📊", "Market & Industry",  "#fbbf24"),
}


@app.get("/companies/{company_id}/category/{category_key}", response_class=HTMLResponse)
async def category_detail(req: Request, company_id: str, category_key: str):
    if category_key not in _CAT_META:
        return RedirectResponse(url=f"/companies/{company_id}", status_code=302)

    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows, risk_runs = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name,ceo_email",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="risk_runs",
            access_token=effective_token,
            select="id,categories",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
    )

    company = company_rows[0] if company_rows else None
    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    latest = risk_runs[0] if risk_runs else None
    cats = latest.get("categories") if latest else None

    icon, label, color = _CAT_META[category_key]
    cat_data = cats.get(category_key) if cats else None

    # Fetch resolved risk hashes for this company + category
    resolutions = await supabase.rest_select(
        table="risk_resolutions",
        access_token=effective_token,
        select="risk_hash",
        query_params={"company_id": f"eq.{company_id}", "category": f"eq.{category_key}"},
    )
    resolved_hashes = {r["risk_hash"] for r in (resolutions or [])}

    # Attach _hash and _resolved to each risk item
    if cat_data and cat_data.get("risks"):
        for risk in cat_data["risks"]:
            risk_text = risk.get("text", "") if isinstance(risk, dict) else str(risk)
            h = hashlib.md5(f"{company_id}:{category_key}:{risk_text}".encode()).hexdigest()
            if isinstance(risk, dict):
                risk["_hash"] = h
                risk["_resolved"] = h in resolved_hashes

    return templates.TemplateResponse(
        req, "category_detail.html",
        {
            "company": company,
            "active_key": category_key,
            "cat_icon": icon,
            "cat_label": label,
            "cat_color": color,
            "cat": cat_data,
            "cats": cats,
            "app_name": settings.APP_NAME,
        },
    )


@app.post("/companies/{company_id}/risks/resolve")
async def resolve_risk(
    req: Request,
    company_id: str,
    category: str = Form(...),
    risk_text: str = Form(...),
    action: str = Form("resolve"),
):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token
    risk_hash = hashlib.md5(f"{company_id}:{category}:{risk_text}".encode()).hexdigest()

    if action == "resolve":
        try:
            await supabase.rest_insert(
                table="risk_resolutions",
                access_token=effective_token,
                row={
                    "company_id": company_id,
                    "risk_hash": risk_hash,
                    "category": category,
                    "risk_text": risk_text[:500],
                },
                returning="minimal",
            )
        except Exception:
            pass  # already exists (UNIQUE constraint) — that's fine
    else:
        try:
            await supabase.rest_delete(
                table="risk_resolutions",
                access_token=effective_token,
                query_params={"company_id": f"eq.{company_id}", "risk_hash": f"eq.{risk_hash}"},
            )
        except Exception:
            pass

    return JSONResponse({"ok": True, "hash": risk_hash, "action": action})


@app.post("/companies/{company_id}/risk_runs/cleanup")
async def cleanup_risk_runs(
    req: Request,
    company_id: str,
    keep_last: int = Form(default=0),
    older_than_days: int = Form(default=0),
):
    from fastapi.responses import JSONResponse
    from datetime import datetime, timezone, timedelta
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    all_runs = await supabase.rest_select(
        table="risk_runs",
        access_token=effective_token,
        select="id,created_at",
        order_by="created_at.desc",
        query_params={"company_id": f"eq.{company_id}"},
    )

    to_delete: list[str] = []

    if keep_last > 0:
        to_delete = [r["id"] for r in all_runs[keep_last:]]
    elif older_than_days > 0:
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        for r in all_runs:
            if r.get("created_at"):
                try:
                    dt = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                    if dt < cutoff:
                        to_delete.append(r["id"])
                except Exception:
                    pass

    if not to_delete:
        return JSONResponse({"deleted": 0})

    ids_filter = ",".join(to_delete)
    await supabase.rest_delete(
        table="risk_runs",
        access_token=effective_token,
        query_params={"id": f"in.({ids_filter})", "company_id": f"eq.{company_id}"},
    )

    return JSONResponse({"deleted": len(to_delete)})


@app.post("/companies/{company_id}/risk_runs/{run_id}/delete")
async def delete_risk_run(req: Request, company_id: str, run_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token
    await supabase.rest_delete(
        table="risk_runs",
        access_token=effective_token,
        query_params={"id": f"eq.{run_id}", "company_id": f"eq.{company_id}"},
    )
    return JSONResponse({"ok": True})


@app.get("/api/companies/{company_id}/emails")
async def api_emails(req: Request, company_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    effective_token = getattr(req.state, "new_access_token", None) or access_token
    try:
        rows = await supabase.rest_select(
            table="emails",
            access_token=effective_token,
            select="*",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        )
        return JSONResponse(rows)
    except Exception as e:
        logger.warning("api_emails error: %s", e)
        return JSONResponse([], status_code=200)


@app.post("/emails/add")
async def emails_add(
    req: Request,
    company_id: str = Form(...),
    from_email: str = Form(""),
    position: str = Form(""),
    text: str = Form(...),
    certainty: str = Form("unknown"),
    confirmed_by_others: str = Form("unknown"),
    source_type: str = Form("observation"),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    certainty_score = {"confident": 40, "uncertain": 0, "unknown": 15}.get(certainty, 15)
    confirmed_score = {"yes": 35, "no": 0, "unknown": 10}.get(confirmed_by_others, 10)
    source_score = {"experience": 25, "observation": 15, "rumor": 5}.get(source_type, 10)
    confidence_score = certainty_score + confirmed_score + source_score

    await supabase.rest_insert(
        table="emails",
        access_token=access_token,
        row={
            "company_id": company_id,
            "from_email": from_email,
            "position": position,
            "text": text,
            "certainty": certainty,
            "confirmed_by_others": confirmed_by_others,
            "source_type": source_type,
            "confidence_score": confidence_score,
        },
    )
    return RedirectResponse(url=f"/companies/{company_id}?msg=email_added", status_code=302)


@app.post("/ceo/send")
async def ceo_send(
    req: Request,
    company_id: str = Form(...),
    subject: str = Form(""),
    message: str = Form(""),
    risk_run_id: str = Form(""),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    async def _fetch_risk_run():
        if not risk_run_id:
            return []
        return await supabase.rest_select(
            table="risk_runs",
            access_token=access_token,
            select="score,advice,risks",
            query_params={"id": f"eq.{risk_run_id}"},
        )

    company_rows, risk_run_rows = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=access_token,
            select="name,ceo_email",
            query_params={"id": f"eq.{company_id}"},
        ),
        _fetch_risk_run(),
    )
    if not company_rows:
        return RedirectResponse(url=f"/companies/{company_id}", status_code=302)

    company = company_rows[0]
    ceo_email = company["ceo_email"]
    risk_run = risk_run_rows[0] if risk_run_rows else {}

    # Build risk text for email body
    risks_list = risk_run.get("risks", [])
    risks_text = "\n".join(
        f"{i+1}. {r['text'] if isinstance(r, dict) else r}"
        for i, r in enumerate(risks_list)
    ) if risks_list else ""

    try:
        await supabase.webhook_post(
            settings.N8N_CEO_EMAIL_WEBHOOK_URL,
            payload={
                "company_id": company_id,
                "company_name": company.get("name", ""),
                "ceo_email": ceo_email,
                "risk_run_id": risk_run_id,
                "score": risk_run.get("score"),
                "advice": risk_run.get("advice", ""),
                "risks_text": risks_text,
                "subject": subject,
                "message": message,
                "requested_by_user_id": user.get("id"),
            },
        )
        return RedirectResponse(url=f"/companies/{company_id}?msg=sent", status_code=302)
    except Exception as e:
        logger.error("CEO webhook failed: %s", e)
        return RedirectResponse(url=f"/companies/{company_id}?error=webhook_failed", status_code=302)


@app.get("/companies/{company_id}/report.pdf")
async def company_report_pdf(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows, risk_runs, employees = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name,ceo_email",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="risk_runs",
            access_token=effective_token,
            select="id,status,score,advice,risks,created_at",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="employees",
            access_token=effective_token,
            select="full_name,email,position,department",
            query_params={"company_id": f"eq.{company_id}"},
        ),
    )

    if not company_rows:
        return RedirectResponse(url="/dashboard", status_code=302)

    company = company_rows[0]
    latest_run = next((r for r in risk_runs if r.get("score") is not None), risk_runs[0] if risk_runs else {})
    news = await news_client.fetch_news(company["name"], limit=8, company=company)

    pdf_bytes = generate_report(
        company=company,
        risk_run=latest_run,
        news=news,
        employees=employees,
    )

    filename = f"report_{company['name'].replace(' ', '_')}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Competitive Intelligence ──────────────────────────────────────────────────

async def _get_ci_report(company_id: str) -> dict | None:
    try:
        rows = await supabase.rest_select_service(
            table="ci_reports",
            service_key=settings.SUPABASE_SERVICE_KEY,
            query_params={"company_id": f"eq.{company_id}"},
        )
        return rows[0] if rows else None
    except Exception:
        return None


async def _run_ci_and_save(company_id: str, company_name: str, competitors: list[dict]) -> dict:
    from datetime import datetime, timezone
    result = await ci_client.run_ci(company_name, competitors, settings.ANTHROPIC_API_KEY)
    if settings.SUPABASE_SERVICE_KEY:
        try:
            await supabase.rest_upsert_service(
                table="ci_reports",
                service_key=settings.SUPABASE_SERVICE_KEY,
                rows=[{
                    "company_id": company_id,
                    "report": result,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }],
                on_conflict="company_id",
            )
        except Exception as e:
            logger.warning("Failed to save CI report: %s", e)
    return result


@app.get("/companies/{company_id}/crisis", response_class=HTMLResponse)
async def crisis_page(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows, risk_runs, employees, members = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name,ceo_email",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="risk_runs",
            access_token=effective_token,
            select="id,score,advice,categories,scenarios,created_at",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
            limit=1,
        ),
        supabase.rest_select(
            table="employees",
            access_token=effective_token,
            select="id,full_name,email,position,department",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="company_members",
            access_token=effective_token,
            select="id,invited_email,role,status",
            query_params={"company_id": f"eq.{company_id}", "status": "eq.active"},
        ),
    )

    company = company_rows[0] if company_rows else None
    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    latest = risk_runs[0] if risk_runs else None

    return templates.TemplateResponse(
        req, "crisis.html",
        {
            "company": company,
            "latest": latest,
            "employees": employees,
            "members": members,
            "app_name": settings.APP_NAME,
        },
    )


@app.post("/companies/{company_id}/risk_runs/{run_id}/scenarios")
async def save_scenarios(req: Request, company_id: str, run_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    body = await req.json()
    scenarios = body.get("scenarios")
    if not isinstance(scenarios, list):
        return JSONResponse({"error": "invalid payload"}, status_code=400)

    await supabase.rest_update_raw(
        path_with_query=f"rest/v1/risk_runs?id=eq.{run_id}&company_id=eq.{company_id}",
        access_token=effective_token,
        patch={"scenarios": scenarios},
        returning="minimal",
    )
    return JSONResponse({"ok": True})


@app.post("/companies/{company_id}/crisis/delegate")
async def crisis_delegate(
    req: Request,
    company_id: str,
    run_id: str = Form(...),
    scenario_id: str = Form(...),
    step_index: int = Form(...),
    assignee_name: str = Form(...),
    assignee_email: str = Form(""),
    note: str = Form(""),
):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    rows = await supabase.rest_select(
        table="risk_runs",
        access_token=effective_token,
        select="scenarios",
        query_params={"id": f"eq.{run_id}"},
    )
    if not rows:
        return JSONResponse({"error": "run not found"}, status_code=404)

    scenarios = rows[0].get("scenarios") or []
    for sc in scenarios:
        if str(sc.get("id")) == str(scenario_id):
            steps = sc.get("steps") or []
            if 0 <= step_index < len(steps):
                steps[step_index]["delegated_to"] = assignee_name
                steps[step_index]["delegated_email"] = assignee_email
                if note:
                    steps[step_index]["delegate_note"] = note
            break

    await supabase.rest_update_raw(
        path_with_query=f"rest/v1/risk_runs?id=eq.{run_id}&company_id=eq.{company_id}",
        access_token=effective_token,
        patch={"scenarios": scenarios},
        returning="minimal",
    )
    return JSONResponse({"ok": True, "assignee": assignee_name})


@app.post("/companies/{company_id}/crisis/complete")
async def crisis_complete(
    req: Request,
    company_id: str,
    run_id: str = Form(...),
    scenario_id: str = Form(...),
    step_index: int = Form(...),
):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    rows = await supabase.rest_select(
        table="risk_runs",
        access_token=effective_token,
        select="scenarios",
        query_params={"id": f"eq.{run_id}"},
    )
    if not rows:
        return JSONResponse({"error": "run not found"}, status_code=404)

    scenarios = rows[0].get("scenarios") or []
    for sc in scenarios:
        if str(sc.get("id")) == str(scenario_id):
            steps = sc.get("steps") or []
            if 0 <= step_index < len(steps):
                steps[step_index]["completed"] = True
            break

    await supabase.rest_update_raw(
        path_with_query=f"rest/v1/risk_runs?id=eq.{run_id}&company_id=eq.{company_id}",
        access_token=effective_token,
        patch={"scenarios": scenarios},
        returning="minimal",
    )
    return JSONResponse({"ok": True})


@app.post("/companies/{company_id}/crisis/generate")
async def crisis_generate(
    req: Request,
    company_id: str,
    scenario_id: str = Form(...),
    comm_type: str = Form(...),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    risk_runs = await supabase.rest_select(
        table="risk_runs",
        access_token=effective_token,
        select="score,advice,categories,scenarios",
        order_by="created_at.desc",
        query_params={"company_id": f"eq.{company_id}"},
        limit=1,
    )

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="name",
        query_params={"id": f"eq.{company_id}"},
    )

    from fastapi.responses import JSONResponse

    if not risk_runs or not company_rows:
        return JSONResponse({"error": "no_data"}, status_code=404)

    latest = risk_runs[0]
    company_name = company_rows[0]["name"]

    scenarios = latest.get("scenarios") or []
    scenario = next((s for s in scenarios if s.get("id") == scenario_id), None)
    if not scenario and scenarios:
        scenario = scenarios[0]
    if not scenario:
        return JSONResponse({"error": "no_scenario"}, status_code=404)

    cats = latest.get("categories") or {}
    top_risks = []
    for cat in cats.values():
        for r in (cat.get("risks") or [])[:1]:
            top_risks.append(r.get("text", ""))
    risk_summary = (
        f"Общий балл риска: {latest.get('score', '—')}/100\n"
        f"Ключевые риски: {'; '.join(top_risks[:4])}\n"
        f"Рекомендация AI: {latest.get('advice', '')}"
    )

    try:
        text = await ai_client.generate_communication(
            company_name=company_name,
            comm_type=comm_type,
            scenario=scenario,
            risk_summary=risk_summary,
            api_key=settings.ANTHROPIC_API_KEY,
        )
        return JSONResponse({"text": text})
    except Exception as e:
        logger.exception("Crisis generate failed")
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Integrations (per-company) ─────────────────────────────────────────────

@app.post("/companies/{company_id}/integrations/test")
async def test_integration(req: Request, company_id: str, provider: str = Form(...)):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    effective_token = getattr(req.state, "new_access_token", None) or access_token
    rows = await supabase.rest_select(
        table="integrations", access_token=effective_token,
        select="provider,config,enabled",
        query_params={"company_id": f"eq.{company_id}", "provider": f"eq.{provider}"},
    )
    if not rows:
        return JSONResponse({"error": "integration_not_found"}, status_code=404)
    cfg = rows[0].get("config") or {}
    try:
        if provider == "telegram":
            result = await agent_client._send_telegram(cfg["token"], cfg["chat_id"], "✅ Тест CONEXIAI: интеграция с Telegram работает!")
        elif provider == "slack":
            result = await agent_client._send_slack(cfg["webhook_url"], "✅ Тест CONEXIAI: интеграция со Slack работает!")
        elif provider == "email":
            result = await agent_client._send_email(
                cfg.get("smtp_host", "smtp.gmail.com"), int(cfg.get("smtp_port", 587)),
                cfg["smtp_user"], cfg["smtp_password"],
                cfg.get("default_to", cfg["smtp_user"]),
                "Тест CONEXIAI", "Интеграция с Email работает!"
            )
        else:
            return JSONResponse({"error": "unknown_provider"}, status_code=400)
        return JSONResponse({"ok": True, "result": result})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/companies/{company_id}/integrations")
async def get_integrations(req: Request, company_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    effective_token = getattr(req.state, "new_access_token", None) or access_token
    rows = await supabase.rest_select(
        table="integrations",
        access_token=effective_token,
        select="id,provider,config,enabled",
        query_params={"company_id": f"eq.{company_id}"},
    )
    # Strip secrets from config before returning
    safe = []
    for r in rows:
        cfg = dict(r.get("config") or {})
        if "token" in cfg:
            cfg["token"] = cfg["token"][:8] + "…" if cfg["token"] else ""
        if "smtp_password" in cfg:
            cfg["smtp_password"] = "••••••••"
        if "webhook_url" in cfg:
            cfg["webhook_url"] = cfg["webhook_url"][:30] + "…" if cfg["webhook_url"] else ""
        safe.append({"id": r["id"], "provider": r["provider"], "config": cfg, "enabled": r.get("enabled", True)})
    return JSONResponse({"integrations": safe})


@app.post("/companies/{company_id}/integrations/save")
async def save_integration(
    req: Request,
    company_id: str,
    provider: str = Form(...),
    config_json: str = Form("{}"),
    enabled: str = Form("true"),
):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    effective_token = getattr(req.state, "new_access_token", None) or access_token

    try:
        config = json.loads(config_json)
    except Exception:
        return JSONResponse({"error": "invalid_json"}, status_code=400)

    svc = settings.SUPABASE_SERVICE_KEY
    if not svc:
        return JSONResponse({"error": "no_service_key"}, status_code=500)

    # Keep old secrets if new value is masked placeholder
    existing = await supabase.rest_select(
        table="integrations", access_token=effective_token,
        select="config", query_params={"company_id": f"eq.{company_id}", "provider": f"eq.{provider}"},
    )
    if existing:
        old_cfg = existing[0].get("config") or {}
        for key in ("token", "smtp_password", "webhook_url"):
            if key in config and config[key] and (config[key].endswith("…") or config[key] == "••••••••"):
                config[key] = old_cfg.get(key, "")

    await supabase.rest_upsert_service(
        table="integrations", service_key=svc,
        rows=[{"company_id": company_id, "provider": provider, "config": config, "enabled": enabled == "true"}],
        on_conflict="company_id,provider",
    )
    return JSONResponse({"ok": True})


@app.post("/companies/{company_id}/crisis/agent/execute")
async def crisis_agent_execute(
    req: Request,
    company_id: str,
    scenario_id: str = Form(...),
    step_index: str = Form(None),
):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    effective_token = getattr(req.state, "new_access_token", None) or access_token

    if not settings.ANTHROPIC_API_KEY:
        return JSONResponse({"error": "no_api_key"}, status_code=500)

    # Load company + scenario + integrations in parallel
    company_rows, risk_runs, integrations_rows = await asyncio.gather(
        supabase.rest_select(table="companies", access_token=effective_token,
                             select="name", query_params={"id": f"eq.{company_id}"}),
        supabase.rest_select(table="risk_runs", access_token=effective_token,
                             select="scenarios", order_by="created_at.desc",
                             query_params={"company_id": f"eq.{company_id}"}, limit=1),
        supabase.rest_select(table="integrations", access_token=effective_token,
                             select="provider,config,enabled",
                             query_params={"company_id": f"eq.{company_id}"}),
    )

    if not company_rows or not risk_runs:
        return JSONResponse({"error": "no_data"}, status_code=404)

    company_name = company_rows[0]["name"]
    scenarios = risk_runs[0].get("scenarios") or []
    scenario = next((s for s in scenarios if s.get("id") == scenario_id), None)
    if not scenario:
        return JSONResponse({"error": "scenario_not_found"}, status_code=404)

    # Build integrations dict from DB rows
    integrations: dict = {}
    for row in integrations_rows:
        if row.get("enabled") and row.get("config"):
            integrations[row["provider"]] = row["config"]

    # Execute single step or full scenario
    if step_index is not None:
        try:
            idx = int(step_index)
            step = scenario["steps"][idx]
        except (ValueError, IndexError):
            return JSONResponse({"error": "invalid_step"}, status_code=400)
        result = await agent_client.execute_step(
            step, company_name, scenario.get("label", ""), integrations, settings.ANTHROPIC_API_KEY
        )
        return JSONResponse(result)
    else:
        results = await agent_client.execute_scenario(
            scenario, company_name, integrations, settings.ANTHROPIC_API_KEY
        )
        return JSONResponse({"results": results})


@app.get("/companies/{company_id}/ci", response_class=HTMLResponse)
async def ci_page(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token
    embed = req.query_params.get("embed") == "1"

    company_rows = await supabase.rest_select(
        table="companies", access_token=effective_token,
        select="id,name", query_params={"id": f"eq.{company_id}"},
    )
    company = company_rows[0] if company_rows else None
    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    competitors = await supabase.rest_select(
        table="competitors", access_token=effective_token,
        select="id,name,website", order_by="created_at.asc",
        query_params={"company_id": f"eq.{company_id}"},
    )

    cached = await _get_ci_report(company_id)
    report = cached.get("report") if cached else None
    updated_at = cached.get("updated_at") if cached else None

    return templates.TemplateResponse(req, "ci.html", {
        "app_name": settings.APP_NAME,
        "company": company,
        "competitors": competitors,
        "report": report,
        "updated_at": updated_at,
        "loading": not cached and len(competitors) > 0,
        "embed": embed,
    })


@app.post("/companies/{company_id}/ci/competitors/add")
async def ci_add_competitor(
    req: Request,
    company_id: str,
    name: str = Form(...),
    website: str = Form(""),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    # Max 5 competitors
    existing = await supabase.rest_select(
        table="competitors", access_token=effective_token,
        select="id", query_params={"company_id": f"eq.{company_id}"},
    )
    if len(existing) >= 5:
        return RedirectResponse(url=f"/companies/{company_id}/ci?error=max5", status_code=302)

    await supabase.rest_insert(
        table="competitors", access_token=effective_token,
        row={"company_id": company_id, "name": name.strip(), "website": website.strip()},
    )
    return RedirectResponse(url=f"/companies/{company_id}/ci", status_code=302)


@app.post("/companies/{company_id}/ci/competitors/{competitor_id}/delete")
async def ci_delete_competitor(req: Request, company_id: str, competitor_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token
    await supabase.rest_delete(
        table="competitors", access_token=effective_token,
        query_params={"id": f"eq.{competitor_id}", "company_id": f"eq.{company_id}"},
    )
    return RedirectResponse(url=f"/companies/{company_id}/ci", status_code=302)


@app.post("/companies/{company_id}/ci/refresh")
async def ci_refresh(req: Request, company_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies", access_token=effective_token,
        select="id,name", query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return JSONResponse({"error": "not found"}, status_code=404)

    competitors = await supabase.rest_select(
        table="competitors", access_token=effective_token,
        select="id,name,website", query_params={"company_id": f"eq.{company_id}"},
    )

    asyncio.ensure_future(_run_ci_and_save(company_id, company_rows[0]["name"], competitors))
    return JSONResponse({"status": "refreshing"})


@app.get("/companies/{company_id}/ci/data")
async def ci_data(req: Request, company_id: str):
    from fastapi.responses import JSONResponse
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies", access_token=effective_token,
        select="id,name", query_params={"id": f"eq.{company_id}"},
    )
    if not company_rows:
        return JSONResponse({"error": "not found"}, status_code=404)

    competitors = await supabase.rest_select(
        table="competitors", access_token=effective_token,
        select="id,name,website", query_params={"company_id": f"eq.{company_id}"},
    )

    result = await _run_ci_and_save(company_id, company_rows[0]["name"], competitors)
    return JSONResponse({"status": "ready", "report": result})


