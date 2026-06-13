"""Unit tests for the client risk-scoring formulas (the single source of truth).

Run: `python -m pytest backend/tests/test_risk_formulas.py`
or standalone: `python backend/tests/test_risk_formulas.py`
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app import risk_formulas as rf  # noqa: E402


# ── the five formulas (incl. 3-factor normalisation) ──────────────────────
def test_financial_two_factor_max():
    assert rf.financial_score(10, 10) == 100        # 10×10
    assert rf.financial_score(5, 6) == 30
    assert rf.financial_score(0, 10) == 0


def test_operational_three_factor_norm():
    assert rf.operational_score(10, 10, 10) == 100  # 1000 / 10
    assert rf.operational_score(10, 10, 5) == 50
    assert rf.operational_score(5, 4, 2) == 4       # 40 / 10


def test_reputational_three_factor_norm():
    assert rf.reputational_score(10, 10, 10) == 100
    assert rf.reputational_score(8, 9, 9) == 65     # 648 / 10 = 64.8 → 65
    assert rf.reputational_score(0, 9, 9) == 0


def test_regulatory_two_factor():
    assert rf.regulatory_score(10, 10) == 100
    assert rf.regulatory_score(7, 5) == 35


def test_supplier_two_factor():
    assert rf.supplier_score(10, 10) == 100
    assert rf.supplier_score(0, 10) == 0            # supplier off when no failure


# ── clamping (junk / out-of-range factors) ────────────────────────────────
def test_factor_clamped_to_0_10():
    assert rf.financial_score(99, 99) == 100        # over-range clamps to 10×10
    assert rf.financial_score(-5, 10) == 0          # negative clamps to 0
    assert rf.financial_score("8", "8") == 64       # numeric strings coerced
    assert rf.financial_score(None, 10) == 0        # junk → 0


def test_domain_score_dispatch_and_unknown():
    assert rf.domain_score("reputational", {"reach": 10, "sentiment": 10, "audience_influence": 10}) == 100
    try:
        rf.domain_score("nope", {})
        assert False, "expected ValueError"
    except ValueError:
        pass


# ── determinism: same factors → same score ────────────────────────────────
def test_deterministic():
    factors = {"reputational": {"reach": 8, "sentiment": 9, "audience_influence": 9}}
    a = rf.score_all(factors)
    b = rf.score_all(factors)
    assert a == b
    assert a["reputational"] == 65


# ── weights / total_risk ──────────────────────────────────────────────────
def test_total_risk_weighted_average():
    scores = {"financial": 100, "operational": 0, "reputational": 0,
              "regulatory": 0, "supplier": 0}
    # equal weights → average of 5 = 20
    assert rf.total_risk(scores, None) == 20


def test_weight_zero_excludes_domain():
    scores = {"financial": 100, "operational": 40, "reputational": 0,
              "regulatory": 0, "supplier": 0}
    # only financial counts (others weight 0) → 100
    w = {"financial": 1, "operational": 0, "reputational": 0, "regulatory": 0, "supplier": 0}
    assert rf.total_risk(scores, w) == 100


def test_weight_clamped_0_2():
    assert rf.clamp_weight(99) == 2.0
    assert rf.clamp_weight(-3) == 0.0
    assert rf.clamp_weight(None) == 1.0
    assert rf.clamp_weight("garbage") == 0.0
    nw = rf.normalize_weights({"financial": 5, "supplier": "x"})
    assert nw["financial"] == 2.0 and nw["supplier"] == 0.0 and nw["operational"] == 1.0


def test_all_weights_zero_yields_zero():
    scores = {d: 80 for d in rf.DOMAINS}
    assert rf.total_risk(scores, {d: 0 for d in rf.DOMAINS}) == 0


# ── thresholds (single source of truth) ───────────────────────────────────
def test_threshold_clamp():
    assert rf.clamp_threshold(60) == 60
    assert rf.clamp_threshold(70) == 70
    assert rf.clamp_threshold(80) == 80
    assert rf.clamp_threshold(65) == rf.DEFAULT_THRESHOLD   # not allowed → default
    assert rf.clamp_threshold("junk") == rf.DEFAULT_THRESHOLD


# ── levels ────────────────────────────────────────────────────────────────
def test_levels():
    assert rf.risk_level(0) == "low"
    assert rf.risk_level(20) == "low"
    assert rf.risk_level(21) == "medium"
    assert rf.risk_level(50) == "medium"
    assert rf.risk_level(51) == "high"
    assert rf.risk_level(75) == "high"
    assert rf.risk_level(76) == "critical"
    assert rf.risk_level(100) == "critical"
    assert rf.risk_level_ru(68) == "высокий"


# ── transparency string ───────────────────────────────────────────────────
def test_formula_explain():
    s = rf.formula_explain("reputational", {"reach": 8, "sentiment": 9, "audience_influence": 9})
    assert s == "Охват 8 × Тональность 9 × Влияние аудитории 9 / 10 = 65"
    s2 = rf.formula_explain("financial", {"probability": 7, "loss": 5})
    assert s2 == "Вероятность 7 × Убыток 5 = 35"


if __name__ == "__main__":
    import traceback
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = 0
    for fn in fns:
        try:
            fn()
            passed += 1
            print(f"PASS  {fn.__name__}")
        except Exception:
            print(f"FAIL  {fn.__name__}")
            traceback.print_exc()
    print(f"\n{passed}/{len(fns)} passed")
    sys.exit(0 if passed == len(fns) else 1)
