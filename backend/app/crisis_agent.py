"""
Crisis Agent — runs automatically after each analysis when score >= 50.
Decides what actions to take, executes them, and logs results to crisis_actions table.

Actions the agent can take:
- email: send personalized task email to an employee
- meet: create Google Meet for key stakeholders
- log: record an observation/decision that can't be automated
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import anthropic

logger = logging.getLogger("conexiai.crisis_agent")

# Maps risk category to relevant job title keywords
_CAT_ROLES = {
    "media": ["pr", "press", "media", "communication", "маркетинг", "smm", "пресс", "коммуникац"],
    "hr":    ["hr", "human resources", "персонал", "кадр", "рекрут"],
    "gr":    ["gr", "government", "legal", "юрист", "goverment", "регулятор", "государств", "compliance"],
    "pr":    ["pr", "press", "public relations", "пресс", "коммуникац", "бренд"],
    "market":["ceo", "coo", "директор", "strategy", "sales", "продаж", "стратег", "коммерч"],
}


def _match_employee_to_categories(employee: dict, categories: dict) -> list[str]:
    """Return list of risk categories this employee is responsible for."""
    position = (employee.get("position") or "").lower()
    matched = []
    for cat, keywords in _CAT_ROLES.items():
        if cat in categories and categories[cat].get("score", 0) >= 50:
            if any(kw in position for kw in keywords):
                matched.append(cat)
    # CEO/director gets all high-risk categories
    if any(kw in position for kw in ["ceo", "генеральный", "директор", "president"]):
        matched = [c for c in categories if categories[c].get("score", 0) >= 65]
    return matched


def _decide_actions(
    company_name: str,
    score: int,
    categories: dict,
    employees: list[dict],
    api_key: str,
) -> list[dict]:
    """Haiku decides what actions to take based on risks and available employees."""
    emp_lines = "\n".join(
        f"[{i}] {e['name']} — {e.get('position', '?')} <{e.get('email', '')}>"
        for i, e in enumerate(employees)
    ) or "Нет сотрудников"

    cat_lines = "\n".join(
        f"- {cat.upper()}: скор {data.get('score', 0)} | "
        + " | ".join(r.get("text", "")[:80] for r in data.get("risks", [])[:2])
        for cat, data in categories.items()
        if data.get("score", 0) >= 50
    )

    prompt = (
        f"Ты — кризисный агент компании «{company_name}». Риск-скор: {score}/100.\n\n"
        f"Активные риски (скор >= 50):\n{cat_lines}\n\n"
        f"Доступные сотрудники:\n{emp_lines}\n\n"
        f"Реши какие действия нужно выполнить ПРЯМО СЕЙЧАС для предотвращения кризиса.\n"
        f"Ты можешь:\n"
        f"1. Отправить email сотруднику с конкретной задачей (action: 'email')\n"
        f"2. Зафиксировать решение/наблюдение (action: 'log')\n"
        f"{'3. Создать экстренную встречу (action: \"meet\") — только если скор >= 65' if score >= 65 else ''}\n\n"
        f"Правила:\n"
        f"- Каждому сотруднику максимум 1 email\n"
        f"- Email только тем у кого есть адрес и кто отвечает за проблемную категорию\n"
        f"- Задача в email должна быть конкретной и выполнимой за 24-48 часов\n"
        f"- meet только если скор >= 65, приглашай не более 5 ключевых людей\n\n"
        f"Верни JSON:\n"
        f'{{"actions": [\n'
        f'  {{"action": "email", "employee_idx": 0, "subject": "...", "task": "конкретная задача на 24-48ч"}},\n'
        f'  {{"action": "meet", "employee_indices": [0, 1], "reason": "..."}},\n'
        f'  {{"action": "log", "text": "наблюдение или решение"}}\n'
        f'], "summary": "<1 предложение что агент делает>"}}'
    )

    try:
        c = anthropic.Anthropic(api_key=api_key)
        import json
        msg = c.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1000,
            system="JSON API. Respond ONLY with valid JSON object. No markdown.",
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:]).rsplit("```", 1)[0].strip()
        return json.loads(raw)
    except Exception as e:
        logger.warning("Crisis agent decision failed: %s", e)
        return {"actions": [], "summary": "Агент не смог принять решение"}


async def run_crisis_agent(
    company_id: str,
    company_name: str,
    score: int,
    categories: dict,
    employees: list[dict],
    api_key: str,
    google_client_id: str,
    google_client_secret: str,
    google_refresh_token: str,
) -> dict[str, Any]:
    """
    Main entry point. Called from _run_combined_job when score >= 50.
    Returns {actions: [...], summary: str} for saving to crisis_actions.
    """
    from . import google_calendar as gcal

    if not employees:
        return {"actions": [], "summary": "Нет сотрудников для уведомления"}

    # AI decides what to do
    decision = await asyncio.to_thread(
        _decide_actions, company_name, score, categories, employees, api_key
    )

    planned = decision.get("actions", [])
    summary = decision.get("summary", "")
    executed: list[dict] = []

    for action in planned:
        kind = action.get("action")

        if kind == "email":
            idx = action.get("employee_idx")
            if not isinstance(idx, int) or idx >= len(employees):
                continue
            emp = employees[idx]
            if not emp.get("email"):
                executed.append({"type": "email", "target": emp.get("name", "?"),
                                  "status": "skipped", "reason": "нет email"})
                continue
            try:
                # Generate personalized email body
                plan = await asyncio.to_thread(
                    gcal._ai_generate_employee_plan,
                    emp, company_name, score, categories, api_key
                )
                subject = action.get("subject") or f"⚠️ Кризисная задача — {company_name}"
                body = (
                    f"Уважаемый(ая) {emp.get('name', '')},\n\n"
                    f"Риск-скор компании «{company_name}»: {score}/100.\n\n"
                    f"Ваша задача:\n{action.get('task', plan)}\n\n"
                    f"Детальный план действий:\n{plan}\n\n"
                    f"—\nCONEXIAI Crisis Agent"
                )
                gmail = await asyncio.to_thread(
                    gcal._build_gmail_service,
                    google_client_id, google_client_secret, google_refresh_token
                )
                await asyncio.to_thread(gcal._send_gmail, gmail, emp["email"], subject, body)
                executed.append({
                    "type":    "email",
                    "target":  emp.get("name", "?"),
                    "email":   emp["email"],
                    "subject": subject,
                    "task":    action.get("task", ""),
                    "status":  "sent",
                    "at":      datetime.now(timezone.utc).isoformat(),
                })
                logger.info("Crisis email sent to %s", emp["email"])
            except Exception as e:
                logger.warning("Crisis email failed for %s: %s", emp.get("email"), e)
                executed.append({"type": "email", "target": emp.get("name", "?"),
                                  "status": "error", "reason": str(e)})

        elif kind == "meet" and score >= 65 and google_client_id and google_refresh_token:
            indices = action.get("employee_indices", [])
            invite_emps = [employees[i] for i in indices if isinstance(i, int) and i < len(employees)]
            if not invite_emps:
                continue
            try:
                result = await gcal.create_risk_meeting(
                    company_name=company_name,
                    score=score,
                    risk_categories=categories,
                    employees=invite_emps,
                    api_key=api_key,
                    client_id=google_client_id,
                    client_secret=google_client_secret,
                    refresh_token=google_refresh_token,
                )
                executed.append({
                    "type":     "meet",
                    "meet_url": result.get("meet_url", ""),
                    "invited":  [e.get("name") for e in invite_emps],
                    "reason":   action.get("reason", ""),
                    "start":    result.get("start", ""),
                    "status":   "created" if result.get("meet_url") else "error",
                    "at":       datetime.now(timezone.utc).isoformat(),
                })
                logger.info("Crisis meet created: %s", result.get("meet_url"))
            except Exception as e:
                logger.warning("Crisis meet failed: %s", e)
                executed.append({"type": "meet", "status": "error", "reason": str(e)})

        elif kind == "log":
            executed.append({
                "type":   "log",
                "text":   action.get("text", ""),
                "status": "logged",
                "at":     datetime.now(timezone.utc).isoformat(),
            })

    logger.info("Crisis agent done: %d actions for %s (score=%d)", len(executed), company_name, score)
    return {"actions": executed, "summary": summary}
