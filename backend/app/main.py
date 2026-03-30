import asyncio
import base64
import hashlib
import logging
import secrets
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import settings
from .supabase import supabase
from . import news as news_client
from . import apify as apify_client
from . import hh as hh_client
from . import ai as ai_client
from . import finance as finance_client
from .pdf import generate_report

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
    yield
    await supabase.close()
    await news_client.close()
    await apify_client.close()
    await hh_client.close()
    await ai_client.close()
    await finance_client.close()


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
    callback_url = str(req.base_url).rstrip("/") + "/auth/callback"
    oauth_url = (
        f"{settings.SUPABASE_URL}/auth/v1/authorize"
        f"?provider=google"
        f"&redirect_to={callback_url}"
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
            json={"code": code, "code_verifier": verifier},
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
            select="id,role,status,company_id,companies(id,name,ceo_email,created_at)",
            order_by="joined_at.desc",
            query_params={"user_id": f"eq.{user.get('id')}", "status": "eq.active"},
        )
    except Exception as e:
        if "401" in str(e) or "403" in str(e):
            resp = RedirectResponse(url="/login", status_code=302)
            _clear_tokens(resp)
            return resp
        member_rows = []

    ctx = {
        "member_rows": member_rows,
        "user_id": user.get("id"),
        "app_name": settings.APP_NAME,
    }

    if new_access and new_refresh:
        resp = templates.TemplateResponse(req, "dashboard.html", ctx)
        resp.set_cookie(settings.SESSION_ACCESS_COOKIE_NAME, new_access, **_cookie_params())
        resp.set_cookie(settings.SESSION_REFRESH_COOKIE_NAME, new_refresh, **_cookie_params())
        return resp

    return templates.TemplateResponse(req, "dashboard.html", ctx)


@app.post("/companies/create")
async def companies_create(
    req: Request,
    name: str = Form(...),
    ceo_email: str = Form(...),
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    company = await supabase.rest_insert(
        table="companies",
        access_token=access_token,
        row={"name": name, "ceo_email": ceo_email},
    )
    return RedirectResponse(url=f"/companies/{company['id']}", status_code=302)


@app.get("/signal", response_class=HTMLResponse)
async def signal_page(req: Request):
    return templates.TemplateResponse(req, "signal.html", {})


@app.get("/companies/{company_id}", response_class=HTMLResponse)
async def company_detail(req: Request, company_id: str):
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
                select="id,from_email,position,text,created_at",
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

    company_rows, employees, risk_runs, emails_rows, members, user_role = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="id,name,ceo_email,created_at",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="employees",
            access_token=effective_token,
            select="id,full_name,email,position,department",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="risk_runs",
            access_token=effective_token,
            select="id,status,created_at,updated_at,score,advice,risks,categories",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        _safe_emails(),
        _safe_members(),
        _get_user_role(),
    )
    company = company_rows[0] if company_rows else None

    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    company_news, finance_data = await asyncio.gather(
        news_client.fetch_news(company["name"]),
        finance_client.fetch_market_data(company["name"]) if settings.FINANCE_ENABLED else asyncio.sleep(0, result={"found": False}),
    )

    msg = req.query_params.get("msg")
    error = req.query_params.get("error")

    return templates.TemplateResponse(
        req, "company_detail.html",
        {
            "company": company,
            "employees": employees,
            "risk_runs": risk_runs,
            "company_news": company_news,
            "emails": emails_rows,
            "members": members,
            "user_role": user_role,
            "is_admin": user_role == "admin",
            "finance": finance_data,
            "msg": msg,
            "error": error,
            "app_name": settings.APP_NAME,
        },
    )


@app.get("/companies/{company_id}/market", response_class=HTMLResponse)
async def market_analysis_page(req: Request, company_id: str):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    effective_token = getattr(req.state, "new_access_token", None) or access_token

    company_rows = await supabase.rest_select(
        table="companies",
        access_token=effective_token,
        select="id,name,ceo_email",
        query_params={"id": f"eq.{company_id}"},
    )
    company = company_rows[0] if company_rows else None
    if not company:
        return RedirectResponse(url="/dashboard", status_code=302)

    market_data, company_news = await asyncio.gather(
        finance_client.fetch_full_market_data(company["name"], twelve_key=settings.TWELVE_DATA_API_KEY),
        news_client.fetch_news(company["name"]),
    )

    ai_analysis = None
    if settings.ANTHROPIC_API_KEY:
        ai_analysis = await ai_client.analyze_market_position(
            company_name=company["name"],
            stock=market_data.get("stock"),
            market_index=market_data.get("market_index"),
            top_stocks=market_data.get("top_stocks", []),
            news=company_news,
            api_key=settings.ANTHROPIC_API_KEY,
        )

    return templates.TemplateResponse(
        req, "market.html",
        {
            "app_name": settings.APP_NAME,
            "company": company,
            "finance": market_data,
            "ai_analysis": ai_analysis,
            "company_news": company_news,
        },
    )


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

    employee = await supabase.rest_insert(
        table="employees",
        access_token=access_token,
        row={
            "company_id": company_id,
            "full_name": full_name,
            "email": email,
            "position": position,
            "department": department,
        },
    )
    return RedirectResponse(url=f"/companies/{company_id}", status_code=302)


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

    # 1) Получаем название компании и список сотрудников параллельно
    company_rows, employees = await asyncio.gather(
        supabase.rest_select(
            table="companies",
            access_token=effective_token,
            select="name",
            query_params={"id": f"eq.{company_id}"},
        ),
        supabase.rest_select(
            table="employees",
            access_token=effective_token,
            select="full_name,position,department",
            query_params={"company_id": f"eq.{company_id}"},
        ),
    )
    company_name = company_rows[0]["name"] if company_rows else ""

    # 2) Параллельно тянем все источники данных
    (
        social_data, news, yandex_news, vacancies,
        regulatory_news, market_news, emails_rows,
        finance_data,
    ) = await asyncio.gather(
        apify_client.fetch_all_social(company_name=company_name, token=settings.APIFY_TOKEN, limit=30),
        news_client.fetch_news(company_name, limit=12),
        news_client.fetch_yandex_news(company_name, limit=8),
        hh_client.fetch_vacancies(company_name, limit=10),
        news_client.fetch_regulatory_news(company_name, limit=6),
        news_client.fetch_market_news(company_name, limit=6),
        supabase.rest_select(
            table="emails",
            access_token=effective_token,
            select="from_email,position,text",
            order_by="created_at.desc",
            query_params={"company_id": f"eq.{company_id}"},
        ),
        finance_client.fetch_market_data(company_name) if settings.FINANCE_ENABLED else {"found": False},
    )

    # Разбиваем social по платформам
    threads_posts = social_data.get("threads", [])

    # Сортируем emails по типу должности; неклассифицированные попадают во все категории
    def _emails_by_type(emails: list[dict], keywords: list[str]) -> list[dict]:
        kw = [k.lower() for k in keywords]
        matched = [e for e in emails if any(k in (e.get("position") or "").lower() for k in kw)]
        unmatched = [e for e in emails if not (e.get("position") or "").strip()]
        # deduplicate
        seen = {id(e) for e in matched}
        return matched + [e for e in unmatched if id(e) not in seen]

    hr_emails = _emails_by_type(emails_rows, ["hr", "кадр", "персонал", "human"])
    pr_emails = _emails_by_type(emails_rows, ["pr", "маркетинг", "marketing", "коммуникац", "медиа"])
    gr_emails = _emails_by_type(emails_rows, ["gr", "юрид", "legal", "compliance", "регулятор", "government"])

    # Emails с нераспознанной должностью — добавляем отдельным блоком
    classified = set()
    for lst in (hr_emails, pr_emails, gr_emails):
        classified.update(id(e) for e in lst)
    other_emails = [e for e in emails_rows if id(e) not in classified and (e.get("position") or "").strip()]
    # Добавляем other_emails во все три категории чтобы Claude сам решил куда отнести
    hr_emails = hr_emails + other_emails
    pr_emails = pr_emails + other_emails
    gr_emails = gr_emails + other_emails

    logger.info(
        "Risk analysis for '%s': news=%d, yandex=%d, threads=%d, ig=%d, tt=%d, yt=%d, vacancies=%d, finance_found=%s, emails=%d",
        company_name, len(news), len(yandex_news), len(threads_posts),
        len(social_data.get("instagram", [])), len(social_data.get("tiktok", [])),
        len(social_data.get("youtube", [])),
        len(vacancies), finance_data.get("found"), len(emails_rows),
    )

    # 3) AI-анализ
    analysis = await ai_client.analyze_company_risks(
        company_name=company_name,
        employees=employees,
        news=news,
        yandex_news=yandex_news,
        threads_posts=threads_posts,
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
        hr_emails=hr_emails,
        pr_emails=pr_emails,
        gr_emails=gr_emails,
        api_key=settings.ANTHROPIC_API_KEY,
    )

    # 4) Сохраняем один финальный risk_run
    await supabase.rest_insert(
        table="risk_runs",
        access_token=effective_token,
        row={
            "company_id": company_id,
            "status": "done",
            "score": analysis["score"],
            "advice": analysis["advice"],
            "risks": analysis["risks"],
            "categories": analysis["categories"],
        },
    )

    return RedirectResponse(url=f"/companies/{company_id}", status_code=302)


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
            select="id,from_email,position,text,created_at",
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
):
    user = await get_current_user(req)
    access_token, _ = _get_tokens(req)
    if not user or not access_token:
        return RedirectResponse(url="/login", status_code=302)

    await supabase.rest_insert(
        table="emails",
        access_token=access_token,
        row={
            "company_id": company_id,
            "from_email": from_email,
            "position": position,
            "text": text,
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
    news = await news_client.fetch_news(company["name"], limit=8)

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


