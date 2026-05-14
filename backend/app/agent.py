"""
Crisis Action Agent — executes crisis scenario steps using Claude tool_use.
Supports: Telegram Bot API, Slack Incoming Webhooks, Email (SMTP/Gmail App Password).
"""
import json
import logging
from typing import Any

import httpx
import anthropic

logger = logging.getLogger("conexiai")

_TOOL_DEFINITIONS = [
    {
        "name": "send_telegram",
        "description": "Send a message to a Telegram chat via Bot API. Use for urgent alerts, CEO notifications, team alerts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "The message text to send (Markdown supported). Keep it concise and actionable."
                }
            },
            "required": ["message"]
        }
    },
    {
        "name": "send_slack",
        "description": "Post a message to a Slack channel via Incoming Webhook. Use for team notifications, status updates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "The message text to post to Slack."
                },
                "blocks": {
                    "type": "array",
                    "description": "Optional Slack Block Kit blocks for rich formatting.",
                    "items": {"type": "object"}
                }
            },
            "required": ["message"]
        }
    },
    {
        "name": "send_email",
        "description": "Send an email. Use for formal communications, press releases, partner notifications.",
        "input_schema": {
            "type": "object",
            "properties": {
                "to": {
                    "type": "string",
                    "description": "Recipient email address."
                },
                "subject": {
                    "type": "string",
                    "description": "Email subject line."
                },
                "body": {
                    "type": "string",
                    "description": "Email body text (plain text or HTML)."
                }
            },
            "required": ["to", "subject", "body"]
        }
    },
    {
        "name": "log_action",
        "description": "Log a completed action or decision when no external tool is needed (e.g. internal meeting scheduled, document drafted).",
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {
                    "type": "string",
                    "description": "Brief description of what was done or decided."
                }
            },
            "required": ["summary"]
        }
    }
]


async def _send_telegram(token: str, chat_id: str, message: str) -> dict:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        })
        r.raise_for_status()
        return {"ok": True, "platform": "telegram"}


async def _send_slack(webhook_url: str, message: str, blocks: list | None = None) -> dict:
    payload: dict[str, Any] = {"text": message}
    if blocks:
        payload["blocks"] = blocks
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(webhook_url, json=payload)
        r.raise_for_status()
        return {"ok": True, "platform": "slack"}


async def _send_email(smtp_host: str, smtp_port: int, smtp_user: str, smtp_password: str,
                      to: str, subject: str, body: str) -> dict:
    import aiosmtplib
    from email.mime.text import MIMEText
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = smtp_user
    msg["To"] = to
    msg["Subject"] = subject
    await aiosmtplib.send(
        msg,
        hostname=smtp_host,
        port=smtp_port,
        username=smtp_user,
        password=smtp_password,
        use_tls=(smtp_port == 465),
        start_tls=(smtp_port == 587),
    )
    return {"ok": True, "platform": "email", "to": to}


async def execute_step(
    step: dict,
    company_name: str,
    scenario_label: str,
    integrations: dict,
    api_key: str,
) -> dict:
    """
    Execute a single crisis scenario step using Claude tool_use.

    integrations: {
      "telegram": {"token": "...", "chat_id": "..."},
      "slack":    {"webhook_url": "..."},
      "email":    {"smtp_host": "...", "smtp_port": 587, "smtp_user": "...", "smtp_password": "...", "default_to": "..."}
    }

    Returns: {"status": "done"|"error", "tool": "...", "result": {...}, "message": "..."}
    """
    client = anthropic.AsyncAnthropic(api_key=api_key)

    # Build available tools list based on configured integrations
    available_tools = [t for t in _TOOL_DEFINITIONS if t["name"] == "log_action"]
    if integrations.get("telegram", {}).get("token"):
        available_tools = [t for t in _TOOL_DEFINITIONS if t["name"] == "send_telegram"] + available_tools
    if integrations.get("slack", {}).get("webhook_url"):
        available_tools = [t for t in _TOOL_DEFINITIONS if t["name"] == "send_slack"] + available_tools
    if integrations.get("email", {}).get("smtp_user"):
        available_tools = [t for t in _TOOL_DEFINITIONS if t["name"] == "send_email"] + available_tools

    has_telegram = bool(integrations.get("telegram", {}).get("token"))
    has_slack = bool(integrations.get("slack", {}).get("webhook_url"))
    has_email = bool(integrations.get("email", {}).get("smtp_user"))

    channel_instruction = ""
    if has_telegram:
        channel_instruction = "ОБЯЗАТЕЛЬНО используй send_telegram — Telegram настроен и готов к работе."
    elif has_slack:
        channel_instruction = "ОБЯЗАТЕЛЬНО используй send_slack — Slack настроен и готов к работе."
    elif has_email:
        channel_instruction = "ОБЯЗАТЕЛЬНО используй send_email — Email настроен и готов к работе."
    else:
        channel_instruction = "Внешние каналы не настроены. Используй log_action."

    prompt = f"""Ты — антикризисный агент компании «{company_name}».

Активирован сценарий: {scenario_label}

Выполни следующий шаг антикризисного плана:
- Действие: {step.get("action")}
- Ответственный: {step.get("owner")}
- Дедлайн: {step.get("deadline")}

{channel_instruction}

Составь конкретное, профессиональное сообщение на русском языке и отправь его через указанный инструмент.
Сообщение должно содержать: суть действия, компания, срочность, что нужно сделать."""

    logs = []
    try:
        response = await client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            tools=available_tools,
            messages=[{"role": "user", "content": prompt}]
        )

        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_name = block.name
            tool_input = block.input
            logs.append({"tool": tool_name, "input": tool_input})

            try:
                if tool_name == "send_telegram":
                    cfg = integrations["telegram"]
                    result = await _send_telegram(cfg["token"], cfg["chat_id"], tool_input["message"])
                elif tool_name == "send_slack":
                    cfg = integrations["slack"]
                    result = await _send_slack(cfg["webhook_url"], tool_input["message"], tool_input.get("blocks"))
                elif tool_name == "send_email":
                    cfg = integrations["email"]
                    result = await _send_email(
                        cfg.get("smtp_host", "smtp.gmail.com"),
                        int(cfg.get("smtp_port", 587)),
                        cfg["smtp_user"],
                        cfg["smtp_password"],
                        tool_input.get("to") or cfg.get("default_to", ""),
                        tool_input["subject"],
                        tool_input["body"],
                    )
                elif tool_name == "log_action":
                    result = {"ok": True, "summary": tool_input["summary"]}
                else:
                    result = {"ok": False, "error": f"Unknown tool: {tool_name}"}

                logs[-1]["result"] = result
                logs[-1]["status"] = "done"

            except Exception as e:
                logs[-1]["result"] = {"error": str(e)}
                logs[-1]["status"] = "error"
                logger.warning("Agent tool %s failed: %s", tool_name, e)

        if not logs:
            # Claude didn't call a tool — extract text response
            text = next((b.text for b in response.content if hasattr(b, "text")), "")
            logs.append({"tool": "log_action", "input": {"summary": text}, "result": {"ok": True}, "status": "done"})

        overall = "error" if any(l.get("status") == "error" for l in logs) else "done"
        return {"status": overall, "logs": logs}

    except Exception as e:
        logger.error("Agent execution failed: %s", e)
        return {"status": "error", "logs": [{"tool": "none", "error": str(e)}]}


async def execute_scenario(
    scenario: dict,
    company_name: str,
    integrations: dict,
    api_key: str,
) -> list[dict]:
    """Execute all steps of a scenario sequentially, returning a log per step."""
    results = []
    label = scenario.get("label", "Сценарий")
    for step in scenario.get("steps", []):
        result = await execute_step(step, company_name, label, integrations, api_key)
        result["step"] = step
        results.append(result)
    return results
