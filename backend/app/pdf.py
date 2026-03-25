from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from fpdf import FPDF

# Arial Unicode TTF — supports Cyrillic
_FONT_DIR = "/System/Library/Fonts/Supplemental"
_FONT_REGULAR = os.path.join(_FONT_DIR, "Arial Unicode.ttf")
_FONT_BOLD = os.path.join(_FONT_DIR, "Arial Bold.ttf")

_FONT_NAME = "ArialUnicode"
_FONT_NAME_BOLD = "ArialBold"


class ReportPDF(FPDF):
    def __init__(self, company_name: str):
        super().__init__()
        self.company_name = company_name
        self.set_margins(20, 20, 20)
        self.set_auto_page_break(auto=True, margin=20)
        self.add_font(_FONT_NAME, style="", fname=_FONT_REGULAR)
        self.add_font(_FONT_NAME_BOLD, style="", fname=_FONT_BOLD)

    def header(self):
        self.set_font(_FONT_NAME, size=9)
        self.set_text_color(120, 120, 140)
        self.cell(0, 8, f"CONEXIAI  |  Risk Report  |  {self.company_name}", align="L")
        self.set_draw_color(60, 80, 140)
        self.set_line_width(0.3)
        self.line(20, 16, 190, 16)
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font(_FONT_NAME, size=8)
        self.set_text_color(150, 150, 160)
        self.cell(0, 8, f"CONEXIAI  —  стр. {self.page_no()}", align="C")

    # ── Helpers ────────────────────────────────────────────────────────────────

    def section_title(self, text: str):
        self.ln(4)
        self.set_font(_FONT_NAME_BOLD, size=11)
        self.set_text_color(60, 80, 200)
        self.cell(0, 8, text, ln=True)
        self.set_draw_color(60, 80, 200)
        self.set_line_width(0.25)
        self.line(20, self.get_y(), 190, self.get_y())
        self.ln(4)

    def body_text(self, text: str, size: int = 10, color=(40, 40, 50)):
        self.set_font(_FONT_NAME, size=size)
        self.set_text_color(*color)
        self.multi_cell(0, 6, text)

    def score_badge(self, score: int):
        """Draw a colored score badge."""
        if score < 35:
            r, g, b = 40, 180, 100
            label = "Низкий риск"
        elif score < 65:
            r, g, b = 220, 160, 30
            label = "Умеренный риск"
        else:
            r, g, b = 220, 60, 60
            label = "Высокий риск"

        x, y = self.get_x(), self.get_y()
        # Outer box
        self.set_fill_color(r, g, b)
        self.set_draw_color(r, g, b)
        self.rounded_rect(x, y, 60, 22, 4, "FD")
        # Score number
        self.set_font(_FONT_NAME_BOLD, size=20)
        self.set_text_color(255, 255, 255)
        self.set_xy(x, y + 1)
        self.cell(60, 12, str(score), align="C")
        # Label
        self.set_font(_FONT_NAME, size=8)
        self.set_xy(x, y + 13)
        self.cell(60, 6, label, align="C")
        self.set_xy(x + 66, y)

    def progress_bar(self, value: int, w: float = 150, h: float = 6):
        x, y = self.get_x(), self.get_y()
        # Background
        self.set_fill_color(220, 220, 230)
        self.rounded_rect(x, y, w, h, 2, "F")
        # Fill
        if value < 35:
            self.set_fill_color(40, 180, 100)
        elif value < 65:
            self.set_fill_color(220, 160, 30)
        else:
            self.set_fill_color(220, 60, 60)
        fill_w = max(4, w * value / 100)
        self.rounded_rect(x, y, fill_w, h, 2, "F")
        self.ln(h + 3)


def _clean(text: str) -> str:
    """Sanitize text — remove null bytes etc."""
    return text.replace("\x00", "").replace("\r", "")


def generate_report(
    company: dict[str, Any],
    risk_run: dict[str, Any],
    news: list[dict[str, Any]],
    employees: list[dict[str, Any]],
) -> bytes:
    score = int(risk_run.get("score") or 50)
    advice = risk_run.get("advice") or ""
    risks = risk_run.get("risks") or []
    created_at = risk_run.get("created_at", "")
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            created_at = dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            created_at = created_at[:16]

    pdf = ReportPDF(company_name=_clean(company.get("name", "")))
    pdf.add_page()

    # ── Title block ────────────────────────────────────────────────────────────
    pdf.set_font(_FONT_NAME_BOLD, size=22)
    pdf.set_text_color(20, 20, 40)
    pdf.cell(0, 12, _clean(company.get("name", "Компания")), ln=True)

    pdf.set_font(_FONT_NAME, size=10)
    pdf.set_text_color(100, 100, 120)
    pdf.cell(0, 6, f"CEO: {_clean(company.get('ceo_email', ''))}", ln=True)
    pdf.cell(0, 6, f"Дата анализа: {_clean(created_at)}", ln=True)
    pdf.ln(6)

    # ── Score ──────────────────────────────────────────────────────────────────
    pdf.section_title("Risk Score")
    pdf.score_badge(score)
    pdf.ln(6)
    pdf.progress_bar(score)

    # ── Advice ─────────────────────────────────────────────────────────────────
    if advice:
        pdf.section_title("Рекомендации")
        pdf.set_fill_color(240, 243, 255)
        pdf.set_draw_color(100, 130, 220)
        x, y = pdf.get_x(), pdf.get_y()
        pdf.set_line_width(0.8)
        pdf.line(x, y, x, y + 30)  # left accent line
        pdf.set_x(x + 4)
        pdf.set_font(_FONT_NAME, size=10)
        pdf.set_text_color(40, 40, 70)
        pdf.multi_cell(0, 6, _clean(advice))
        pdf.ln(2)

    # ── Risks ──────────────────────────────────────────────────────────────────
    if risks:
        pdf.section_title(f"Выявленные риски ({len(risks)})")
        for i, risk in enumerate(risks):
            if isinstance(risk, dict):
                risk_text = risk.get("text", "")
                sources = risk.get("sources", [])
            else:
                risk_text = str(risk)
                sources = []

            # Color dot + text
            idx = i
            if idx < 2:
                dot_color = (220, 60, 60)
            elif idx < 4:
                dot_color = (220, 160, 30)
            else:
                dot_color = (80, 120, 220)

            x, y = pdf.get_x(), pdf.get_y()
            pdf.set_fill_color(*dot_color)
            pdf.ellipse(x + 1, y + 2, 4, 4, "F")
            pdf.set_x(x + 8)
            pdf.set_font(_FONT_NAME_BOLD, size=10)
            pdf.set_text_color(20, 20, 40)
            pdf.multi_cell(0, 6, f"{i+1}. {_clean(risk_text)}")

            # Sources
            if sources:
                for src in sources:
                    url = src.get("url", "")
                    title = src.get("title", url)
                    src_type = src.get("type", "")
                    icon = {"news": "[News]", "threads": "[Threads]", "hh": "[HH.ru]"}.get(src_type, "[src]")
                    pdf.set_x(pdf.get_x() + 12)
                    pdf.set_font(_FONT_NAME, size=8)
                    pdf.set_text_color(80, 110, 200)
                    pdf.multi_cell(0, 5, f"  {icon} {_clean(title)}")
                    if url:
                        pdf.set_x(pdf.get_x() + 12)
                        pdf.set_font(_FONT_NAME, size=7)
                        pdf.set_text_color(140, 140, 160)
                        pdf.multi_cell(0, 4, f"  {_clean(url[:90])}")
            pdf.ln(2)

    # ── News ───────────────────────────────────────────────────────────────────
    if news:
        pdf.section_title(f"Последние новости (Google News)")
        for i, n in enumerate(news[:8]):
            pdf.set_font(_FONT_NAME_BOLD, size=9)
            pdf.set_text_color(20, 20, 60)
            pdf.multi_cell(0, 5, f"{i+1}. {_clean(n.get('title', ''))}")
            meta = []
            if n.get("source"):
                meta.append(n["source"])
            if n.get("pub_date"):
                meta.append(n["pub_date"])
            if meta:
                pdf.set_font(_FONT_NAME, size=8)
                pdf.set_text_color(120, 120, 140)
                pdf.cell(0, 4, _clean("  " + "  ·  ".join(meta)), ln=True)
            pdf.ln(1)

    # ── Employees ──────────────────────────────────────────────────────────────
    if employees:
        pdf.section_title(f"Сотрудники ({len(employees)})")
        for e in employees:
            name = _clean(e.get("full_name", ""))
            pos = _clean(e.get("position") or "")
            dept = _clean(e.get("department") or "")
            meta = "  ·  ".join(filter(None, [pos, dept]))
            pdf.set_font(_FONT_NAME_BOLD, size=9)
            pdf.set_text_color(20, 20, 40)
            pdf.cell(0, 5, name, ln=True)
            if meta:
                pdf.set_font(_FONT_NAME, size=8)
                pdf.set_text_color(120, 120, 140)
                pdf.cell(0, 4, meta, ln=True)
            pdf.ln(1)

    # ── Footer note ────────────────────────────────────────────────────────────
    pdf.ln(6)
    pdf.set_font(_FONT_NAME, size=8)
    pdf.set_text_color(160, 160, 170)
    pdf.cell(0, 5, f"Отчёт сгенерирован CONEXIAI  —  {datetime.now().strftime('%d.%m.%Y %H:%M')}", align="C", ln=True)

    return bytes(pdf.output())
