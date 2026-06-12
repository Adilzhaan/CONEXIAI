"""
Google Calendar integration — creates Google Meet meetings and invites employees.
AI picks which employees to invite based on risk categories and their roles.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import anthropic

logger = logging.getLogger("conexiai.gcal")


def _build_creds(client_id: str, client_secret: str, refresh_token: str, scopes: list[str]):
    from google.oauth2.credentials import Credentials
    return Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )


def _build_calendar_service(client_id: str, client_secret: str, refresh_token: str):
    from googleapiclient.discovery import build
    creds = _build_creds(client_id, client_secret, refresh_token,
                         ["https://www.googleapis.com/auth/calendar"])
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _build_gmail_service(client_id: str, client_secret: str, refresh_token: str):
    from googleapiclient.discovery import build
    creds = _build_creds(client_id, client_secret, refresh_token,
                         ["https://www.googleapis.com/auth/gmail.send"])
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _ai_select_employees(
    employees: list[dict],
    risk_categories: dict,
    score: int,
    company_name: str,
    api_key: str,
) -> list[dict]:
    """Haiku picks which employees to invite based on their role and risk categories."""
    if not employees or not api_key:
        return employees[:5]

    emp_lines = "\n".join(
        f"[{i}] {e.get('name', '?')} — {e.get('position', '?')} ({e.get('email', '')})"
        for i, e in enumerate(employees)
    )
    risk_lines = "\n".join(
        f"- {cat.upper()}: скор {data.get('score', '?')} — "
        + "; ".join(r.get("text", "")[:80] for r in data.get("risks", [])[:2])
        for cat, data in risk_categories.items()
        if data.get("score", 0) >= 40
    )

    prompt = (
        f"Компания «{company_name}» имеет риск-скор {score}/100. Нужно провести экстренное совещание.\n\n"
        f"Активные риски:\n{risk_lines}\n\n"
        f"Сотрудники:\n{emp_lines}\n\n"
        f"Выбери сотрудников которые должны участвовать в совещании для устранения этих рисков.\n"
        f"Учитывай должности: PR/медиа риски → PR/Communications, HR риски → HR, "
        f"GR/регуляторные → Legal/GR, рыночные → CEO/Strategy/Sales.\n"
        f"CEO приглашается всегда если скор >= 75.\n\n"
        f"Верни JSON: {{\"indices\": [0, 2, 5], \"reason\": \"<1 предложение почему эти люди>\"}}"
    )

    try:
        c = anthropic.Anthropic(api_key=api_key)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=256,
            system="JSON API. Respond ONLY with valid JSON.",
            messages=[{"role": "user", "content": prompt}],
        )
        import json
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        result = json.loads(raw)
        indices = [i for i in result.get("indices", []) if isinstance(i, int) and 0 <= i < len(employees)]
        selected = [employees[i] for i in indices]
        logger.info("AI selected %d/%d employees: %s", len(selected), len(employees), result.get("reason", ""))
        return selected if selected else employees[:3]
    except Exception as e:
        logger.warning("AI employee selection failed: %s", e)
        return employees[:3]


async def create_risk_meeting(
    company_name: str,
    score: int,
    risk_categories: dict,
    employees: list[dict],
    api_key: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict[str, Any]:
    """
    1. AI selects employees to invite
    2. Creates Google Calendar event with Meet link
    3. Returns {meet_url, event_id, invited, reason}
    """
    if not client_id or not client_secret or not refresh_token:
        return {"error": "Google Calendar не настроен"}

    # AI picks employees
    invited = await asyncio.to_thread(
        _ai_select_employees, employees, risk_categories, score, company_name, api_key
    )

    # Build attendees list (only those with email)
    attendees = [
        {"email": e["email"]}
        for e in invited
        if e.get("email")
    ]

    if not attendees:
        return {"error": "Нет сотрудников с email адресами"}

    # Meeting in 30 minutes, 1 hour duration
    now = datetime.now(timezone.utc)
    start = now + timedelta(minutes=30)
    end   = start + timedelta(hours=1)

    # Top risks for description
    top_risks = [
        r.get("text", "")
        for cat_data in risk_categories.values()
        for r in cat_data.get("risks", [])
        if r.get("severity") == "high"
    ][:5]
    risk_desc = "\n".join(f"• {r}" for r in top_risks) or "Повышенный уровень риска"

    event = {
        "summary": f"⚠️ Экстренное совещание — {company_name} (риск {score})",
        "description": (
            f"Риск-скор компании достиг {score}/100. Требуется немедленное обсуждение.\n\n"
            f"Ключевые риски:\n{risk_desc}\n\n"
            f"Сформировано автоматически системой CONEXIAI."
        ),
        "start": {"dateTime": start.isoformat(), "timeZone": "Asia/Almaty"},
        "end":   {"dateTime": end.isoformat(),   "timeZone": "Asia/Almaty"},
        "attendees": attendees,
        "conferenceData": {
            "createRequest": {
                "requestId": f"conexiai-{company_name}-{int(now.timestamp())}",
                "conferenceSolutionKey": {"type": "hangoutsMeet"},
            }
        },
        "reminders": {
            "useDefault": False,
            "overrides": [{"method": "email", "minutes": 10}, {"method": "popup", "minutes": 5}],
        },
    }

    try:
        service = await asyncio.to_thread(_build_calendar_service, client_id, client_secret, refresh_token)

        created = await asyncio.to_thread(
            lambda: service.events().insert(
                calendarId="primary",
                body=event,
                conferenceDataVersion=1,
                sendUpdates="all",
            ).execute()
        )

        meet_url = (
            created.get("conferenceData", {})
            .get("entryPoints", [{}])[0]
            .get("uri", "")
        )
        logger.info("Meet created: %s, invited: %s", meet_url, [a["email"] for a in attendees])

        return {
            "meet_url":  meet_url,
            "event_id":  created.get("id"),
            "event_url": created.get("htmlLink"),
            "invited":   invited,
            "start":     start.strftime("%d.%m.%Y %H:%M"),
        }

    except Exception as e:
        logger.error("Google Calendar error: %s", e)
        return {"error": str(e)}


def _ai_generate_employee_plan(
    employee: dict,
    company_name: str,
    score: int,
    risk_categories: dict,
    api_key: str,
) -> str:
    """Haiku generates a personalized crisis action plan for one employee."""
    top_risks = [
        f"[{cat.upper()}] " + "; ".join(r.get("text", "")[:100] for r in data.get("risks", [])[:3])
        for cat, data in risk_categories.items()
        if data.get("score", 0) >= 40
    ]
    risk_block = "\n".join(top_risks) or "Повышенный общий уровень риска"

    prompt = (
        f"Ты — кризисный менеджер компании «{company_name}».\n"
        f"Риск-скор компании: {score}/100.\n\n"
        f"Активные риски:\n{risk_block}\n\n"
        f"Сотрудник: {employee.get('name', '?')} — {employee.get('position', '?')}\n\n"
        f"Напиши персональный план действий для этого сотрудника на ближайшие 48 часов.\n"
        f"Учитывай его должность — что именно ОН должен сделать для снижения рисков.\n"
        f"Формат: 3-5 конкретных шагов с дедлайнами. Язык — русский. Без лишних слов."
    )
    try:
        c = anthropic.Anthropic(api_key=api_key)
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=600,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as e:
        logger.warning("Plan generation failed for %s: %s", employee.get("name"), e)
        return "Свяжитесь с руководством для получения инструкций."


def _send_gmail(service: Any, to: str, subject: str, body: str) -> dict:
    import base64
    from email.mime.text import MIMEText
    msg = MIMEText(body, "plain", "utf-8")
    msg["To"] = to
    msg["Subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    return {"ok": True, "to": to}


async def send_invite_email(
    to_email: str,
    company_name: str,
    role_label: str,
    join_url: str,
    inviter_name: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict[str, Any]:
    """Send a company invitation email via Gmail API."""
    if not client_id or not client_secret or not refresh_token:
        return {"error": "Google не настроен"}
    if not to_email:
        return {"error": "Нет email получателя"}
    try:
        gmail = await asyncio.to_thread(_build_gmail_service, client_id, client_secret, refresh_token)
    except Exception as e:
        return {"error": f"Gmail API: {e}"}

    role_line = f"Роль: {role_label}\n" if role_label else ""
    by_line = f"{inviter_name} приглашает вас" if inviter_name else "Вас приглашают"
    subject = f"Приглашение в «{company_name}» — CONEXIAI"
    body = (
        f"Здравствуйте!\n\n"
        f"{by_line} присоединиться к компании «{company_name}» "
        f"на платформе CONEXIAI — системе мониторинга рисков.\n\n"
        f"{role_line}"
        f"\nЧтобы принять приглашение, перейдите по ссылке:\n{join_url}\n\n"
        f"Если вы не ожидали это письмо — просто проигнорируйте его.\n\n"
        f"—\nCONEXIAI Risk Intelligence"
    )
    try:
        await asyncio.to_thread(_send_gmail, gmail, to_email, subject, body)
        logger.info("Invite email sent to %s", to_email)
        return {"ok": True, "to": to_email}
    except Exception as e:
        logger.warning("Invite email failed for %s: %s", to_email, e)
        return {"error": str(e)}


async def send_crisis_emails(
    company_name: str,
    score: int,
    risk_categories: dict,
    employees: list[dict],
    api_key: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> dict[str, Any]:
    """
    For each employee with email:
    1. AI generates a personalized crisis action plan
    2. Sends it via Gmail API
    Returns {sent: [...], failed: [...]}
    """
    if not client_id or not client_secret or not refresh_token:
        return {"error": "Google не настроен"}

    targets = [e for e in employees if e.get("email")]
    if not targets:
        return {"error": "Нет сотрудников с email"}

    try:
        gmail = await asyncio.to_thread(_build_gmail_service, client_id, client_secret, refresh_token)
    except Exception as e:
        return {"error": f"Gmail API: {e}"}

    sent, failed = [], []

    for emp in targets:
        try:
            plan = await asyncio.to_thread(
                _ai_generate_employee_plan, emp, company_name, score, risk_categories, api_key
            )
            subject = f"⚠️ Кризисный план действий — {company_name} (риск {score}/100)"
            body = (
                f"Уважаемый(ая) {emp.get('name', '')},\n\n"
                f"Риск-скор компании «{company_name}» достиг {score}/100. "
                f"Требуются немедленные действия.\n\n"
                f"Ваш персональный план действий на 48 часов:\n\n"
                f"{plan}\n\n"
                f"—\nCONEXIAI Risk Intelligence\n"
                f"Это сообщение сформировано автоматически системой мониторинга рисков."
            )
            await asyncio.to_thread(_send_gmail, gmail, emp["email"], subject, body)
            logger.info("Crisis email sent to %s <%s>", emp.get("name"), emp["email"])
            sent.append({"name": emp.get("name"), "email": emp["email"], "plan": plan})
        except Exception as e:
            logger.warning("Failed to send to %s: %s", emp.get("email"), e)
            failed.append({"name": emp.get("name"), "email": emp["email"], "error": str(e)})

    return {"sent": sent, "failed": failed}
