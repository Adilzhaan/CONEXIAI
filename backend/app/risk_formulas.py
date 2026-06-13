"""Client risk-scoring formulas — THE single source of truth.

Pipeline: signal → [AI extracts factors 0–10] → [these pure formulas → 0–100]
→ [user-tuned domain weights → total]. The model never produces a score; it
only extracts factors. Everything here is deterministic and unit-tested.

Do NOT duplicate thresholds / weight bounds / formulas anywhere else — import
from this module.
"""
from __future__ import annotations

# Five client risk domains.
DOMAINS = ("financial", "operational", "reputational", "regulatory", "supplier")

# Factors each domain's formula consumes (also drives AI extraction + UI).
DOMAIN_FACTORS = {
    "financial":    ("probability", "loss"),
    "operational":  ("probability", "impact", "duration"),
    "reputational": ("reach", "sentiment", "audience_influence"),
    "regulatory":   ("inspection_probability", "potential_fine"),
    "supplier":     ("failure_probability", "downtime_loss"),
}

# Standard formulas AS DATA (so a client formula can replace one per type).
# Each maps to the pure function below; clients store custom rows in formula_defs.
STANDARD_FORMULAS = {
    "financial":    {"expression": "probability * loss",
                     "variables": list(DOMAIN_FACTORS["financial"])},
    "operational":  {"expression": "probability * impact * duration / 10",
                     "variables": list(DOMAIN_FACTORS["operational"])},
    "reputational": {"expression": "reach * sentiment * audience_influence / 10",
                     "variables": list(DOMAIN_FACTORS["reputational"])},
    "regulatory":   {"expression": "inspection_probability * potential_fine",
                     "variables": list(DOMAIN_FACTORS["regulatory"])},
    "supplier":     {"expression": "failure_probability * downtime_loss",
                     "variables": list(DOMAIN_FACTORS["supplier"])},
}

DOMAIN_RU = {
    "financial":    "Финансовый риск",
    "operational":  "Операционный риск",
    "reputational": "Репутационный риск",
    "regulatory":   "Регуляторный риск",
    "supplier":     "Риск поставщика",
}

FACTOR_RU = {
    "probability":            "Вероятность",
    "loss":                   "Убыток",
    "impact":                 "Воздействие",
    "duration":               "Длительность",
    "reach":                  "Охват",
    "sentiment":              "Тональность",
    "audience_influence":     "Влияние аудитории",
    "inspection_probability": "Вероятность проверки",
    "potential_fine":         "Размер штрафа",
    "failure_probability":    "Вероятность сбоя",
    "downtime_loss":          "Потери от простоя",
}

# Risk levels — ONE source of truth. (upper-bound inclusive, name)
LEVELS = ((20, "low"), (50, "medium"), (75, "high"), (100, "critical"))
LEVEL_RU = {"low": "низкий", "medium": "средний", "high": "высокий", "critical": "критический"}

# Tunable bounds (enforced on the backend — user cannot break scoring).
WEIGHT_MIN, WEIGHT_MAX = 0.0, 2.0
DEFAULT_WEIGHT = 1.0
ALLOWED_THRESHOLDS = (60, 70, 80)
DEFAULT_THRESHOLD = 70


# ── primitives ────────────────────────────────────────────────────────────
def _f(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def clamp_factor(v) -> float:
    """Clamp an AI-extracted factor into the 0–10 scale."""
    return clamp(_f(v), 0.0, 10.0)


# ── the five client formulas (factors 0–10 → score 0–100) ─────────────────
def financial_score(probability, loss) -> int:
    # 2 factors: max 10×10 = 100
    return round(clamp(clamp_factor(probability) * clamp_factor(loss), 0, 100))


def operational_score(probability, impact, duration) -> int:
    # 3 factors: max 10×10×10 = 1000 → /10 = 100
    return round(clamp(clamp_factor(probability) * clamp_factor(impact)
                       * clamp_factor(duration) / 10, 0, 100))


def reputational_score(reach, sentiment, audience_influence) -> int:
    return round(clamp(clamp_factor(reach) * clamp_factor(sentiment)
                       * clamp_factor(audience_influence) / 10, 0, 100))


def regulatory_score(inspection_probability, potential_fine) -> int:
    return round(clamp(clamp_factor(inspection_probability) * clamp_factor(potential_fine), 0, 100))


def supplier_score(failure_probability, downtime_loss) -> int:
    return round(clamp(clamp_factor(failure_probability) * clamp_factor(downtime_loss), 0, 100))


_DOMAIN_FN = {
    "financial":    financial_score,
    "operational":  operational_score,
    "reputational": reputational_score,
    "regulatory":   regulatory_score,
    "supplier":     supplier_score,
}


def domain_score(domain: str, factors: dict,
                 expression: str | None = None,
                 variables: list | None = None) -> int:
    """0–100 score for ONE domain from its extracted factors.

    If `expression` (a validated custom client formula) is given it is used;
    on any evaluation error we auto-fall back to the standard formula so the
    pipeline never crashes."""
    f = factors or {}
    if expression:
        from . import formula_eval as fe
        vs = variables or list(DOMAIN_FACTORS.get(domain, list(f.keys())))
        vals = {k: clamp_factor(f.get(k)) for k in vs}
        try:
            return round(fe.safe_eval(expression, vals))
        except Exception:
            pass  # fall back to standard
    if domain not in _DOMAIN_FN:
        raise ValueError(f"unknown domain: {domain}")
    return _DOMAIN_FN[domain](*(f.get(k) for k in DOMAIN_FACTORS[domain]))


def score_all(factors_by_domain: dict, formulas: dict | None = None) -> dict:
    """{domain: {factors}} → {domain: 0–100 score} for every domain.
    `formulas` optionally maps {domain: {"expression", "variables"}} (active
    custom formula); domains without one use the standard formula."""
    fbd = factors_by_domain or {}
    fm = formulas or {}
    out = {}
    for d in DOMAINS:
        cf = fm.get(d) or {}
        out[d] = domain_score(d, fbd.get(d, {}),
                              cf.get("expression"), cf.get("variables"))
    return out


# ── user tuning (weights + threshold) ─────────────────────────────────────
def clamp_weight(v) -> float:
    return clamp(_f(v) if v is not None else DEFAULT_WEIGHT, WEIGHT_MIN, WEIGHT_MAX)


def normalize_weights(weights: dict | None) -> dict:
    """Clamp every domain weight into 0–2; default 1.0. Junk → default."""
    w = weights or {}
    return {d: clamp_weight(w.get(d, DEFAULT_WEIGHT)) for d in DOMAINS}


def clamp_threshold(v) -> int:
    try:
        v = int(float(v))
    except (TypeError, ValueError):
        return DEFAULT_THRESHOLD
    return v if v in ALLOWED_THRESHOLDS else DEFAULT_THRESHOLD


# ── per-type config (enabled + threshold) — NO aggregate ──────────────────
def normalize_per_type(per_type: dict | None) -> dict:
    """Each type independently: {enabled: bool, threshold: 60|70|80}. Junk →
    sensible defaults. A disabled type is not monitored and never alerts."""
    pt = per_type or {}
    out = {}
    for d in DOMAINS:
        c = pt.get(d) if isinstance(pt.get(d), dict) else {}
        out[d] = {
            "enabled":   bool(c.get("enabled", True)),
            "threshold": clamp_threshold(c.get("threshold", DEFAULT_THRESHOLD)),
        }
    return out


def is_alert(domain: str, score, per_type: dict | None) -> bool:
    """Alert iff the type is enabled AND its own score crosses its own
    threshold. Types are judged independently — never aggregated."""
    c = normalize_per_type(per_type)[domain]
    return c["enabled"] and clamp(_f(score), 0, 100) >= c["threshold"]


def total_risk(domain_scores: dict, weights: dict | None = None) -> int:
    """Weighted average of domain scores. A weight of 0 excludes that domain
    from the index entirely."""
    w = normalize_weights(weights)
    den = sum(w[d] for d in DOMAINS)
    if den <= 0:
        return 0
    num = sum(_f(domain_scores.get(d, 0)) * w[d] for d in DOMAINS)
    return round(num / den)


# ── levels + transparency ─────────────────────────────────────────────────
def risk_level(score) -> str:
    s = clamp(_f(score), 0, 100)
    for ceiling, name in LEVELS:
        if s <= ceiling:
            return name
    return "critical"


def risk_level_ru(score) -> str:
    return LEVEL_RU[risk_level(score)]


def formula_explain(domain: str, factors: dict) -> str:
    """Human-readable derivation, e.g.
    'Охват 8 × Тональность 9 × Влияние аудитории 9 / 10 = 68'."""
    keys = DOMAIN_FACTORS[domain]
    f = factors or {}
    parts = [f"{FACTOR_RU.get(k, k)} {clamp_factor(f.get(k)):g}" for k in keys]
    body = " × ".join(parts)
    if len(keys) == 3:
        body += " / 10"
    return f"{body} = {domain_score(domain, f)}"


def default_config() -> dict:
    """Fresh per-type config: every type monitored, threshold 70."""
    return {
        "per_type": {d: {"enabled": True, "threshold": DEFAULT_THRESHOLD} for d in DOMAINS},
        "preset": "balanced",
    }
