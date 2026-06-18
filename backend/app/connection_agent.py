"""
Connection Agent — EARLY RISK DETECTION & CAUSAL INTELLIGENCE ENGINE.

Runs once per analysis on Sonnet, AFTER analyze_with_history has produced the
filtered facts/risks. It does NOT re-read raw news — it works on the already
filtered facts + run history, and treats every signal as a piece of ONE puzzle.

Output (stored whole in risk_runs.connection_map):

  - assets          registry of affected objects (company/plant/brand/executive/…)
  - nodes           unique events (a repeated event is ONE node = a hub); each
                    references the assets it affects
  - connections     directed edges between node ids (cause → effect)   [graph]
  - chains          named "stories" — ordered paths through node ids    [graph]
  - risks           per-risk intelligence: cause, 1st/2nd/3rd-order consequences,
                    probability, risk_level, escalation_speed, impact_scale,
                    4-category damage forecast, A/B/C response scenarios,
                    priority, recommended_actions, required_agents (flags)
  - patterns        recurring sequences detected across run history
  - early_warnings  weak signals NOW that precede a likely crisis (watch-list)

Graph metrics (degree / is_hub / terminal) are computed deterministically in
Python here — NOT by the LLM — so hubs and sinks are reliable.

Sync — call via asyncio.to_thread().
"""
import json
import logging
import re
from datetime import date as _date
from typing import Any

logger = logging.getLogger("conexiai.connection_agent")

_MODEL = "claude-sonnet-4-6"

_CATS = ("media", "hr", "gr", "pr", "market")
# Crisis types align with the domain risk model in ai.py (terminal nodes link here).
_CRISIS_TYPES = ("financial", "operational", "reputational", "regulatory")
_SEV = ("high", "medium", "low")
_EDGE_TYPES = ("causal", "escalation", "correlation")
_CONF = ("low", "medium", "high")
_LEVELS = ("low", "medium", "high", "critical")
_SCALE = ("local", "regional", "corporate", "systemic")
_ASSET_TYPES = ("company", "plant", "brand", "executive", "contractor",
                "supplier", "license", "equipment", "is", "project",
                "facility", "product", "other")
_DMG_CATS = ("financial", "operational", "reputational", "legal")
_SCN_KEYS = ("A_no_action", "B_partial", "C_immediate")
# Whitelist of execution agents that a risk may be handed off to (flags only —
# no agent is actually launched here; that is a later stage).
_AGENTS = ("crisis_commander", "communications", "stakeholder",
           "intelligence", "execution", "reporting")
_HUB_DEGREE = 3

_SYSTEM = (
    "Ты — автономный агент раннего выявления рисков и причинно-следственного "
    "анализа для крупных компаний (Enterprise, РК).\n"
    "Работай не с сырыми новостями, а с уже отфильтрованными фактами + историей прогонов.\n"
    "Относись к каждому сигналу как к кусочку ОДНОГО пазла.\n\n"
    "Твои задачи:\n"
    "1. Найти слабые сигналы будущих проблем.\n"
    "2. Построить причинно-следственные цепочки (граф событий), связывая разные категории.\n"
    "3. Определить затронутые АКТИВЫ и объекты.\n"
    "4. Для каждого события ответить: что происходит · почему · какие объекты затронуты · "
    "что произойдёт дальше · насколько быстро ухудшится · вторичные эффекты · какие активы под угрозой.\n"
    "5. Построить граф рисков: источник → причина → первичные → вторичные → третичные последствия.\n"
    "6. Оценить: вероятность 0–100, скорость эскалации, масштаб, уровень риска.\n"
    "7. Спрогнозировать ущерб по 4 категориям: финансовый, операционный, репутационный, юридический.\n"
    "8. Построить сценарии A (ничего не делать) / B (частично) / C (немедленно).\n"
    "9. Дать приоритет, рекомендуемые действия и список агентов для передачи исполнения.\n\n"
    "Не «создать отчёт», а «локализовать угрозу и вернуть контроль».\n"
    "Без общих фраз. Каждое утверждение опирается на конкретный сигнал.\n"
    "Отвечай ТОЛЬКО валидным JSON, без markdown и пояснений."
)


def _empty_map() -> dict[str, Any]:
    return {
        "assets": [],
        "nodes": [],
        "connections": [],
        "chains": [],
        "risks": [],
        "conclusions": [],
        "patterns": [],
        "early_warnings": [],
        "kb_sources": [],
    }


def _article_registry(own_rows: list[dict]) -> dict[str, dict]:
    """Stable s1..sN ids for current-run articles — risks reference them by id."""
    from . import source_categories as _sc
    reg: dict[str, dict] = {}
    for i, r in enumerate(own_rows[:50], start=1):
        title = str(r.get("title", "")).strip()
        if not title:
            continue
        url = str(r.get("url") or r.get("link") or "")
        src = str(r.get("source") or r.get("source_type") or "")
        reg[f"s{i}"] = {
            "title":    title[:200],
            "source":   src[:60],
            "url":      url[:500],
            "date":     str(r.get("pub_date") or "")[:40],
            "category": _sc.classify(source=src, url=url, source_type=r.get("source_type")),
        }
    return reg


def _fmt_signals(own_rows: list[dict]) -> str:
    """Current-run facts with their AI risk-notes — the puzzle pieces (with sN ids)."""
    lines = []
    for i, r in enumerate(own_rows[:50], start=1):
        title = str(r.get("title", "")).strip()
        if not title:
            continue
        note = (r.get("risk_note") or "").strip()
        src = r.get("source_type") or r.get("source") or ""
        date = r.get("pub_date", "")
        lines.append(
            f"[s{i}] «{title}» [{src} {date}]"
            + (f" → {note}" if note else "")
        )
    return "\n".join(lines) or "Нет материалов за этот прогон."


def _fmt_risks(risks: list[dict]) -> str:
    if not risks:
        return "Риски не выделены."
    lines = []
    for r in risks[:25]:
        kind = "ИНЦИДЕНТ" if r.get("type") == "incident" else "риск"
        lines.append(
            f"- [{r.get('category', '?')}/{kind}] {r.get('title', '')} "
            f"(score={r.get('score', '?')}, {r.get('severity', '?')})"
        )
    return "\n".join(lines)


def _fmt_prev_articles(articles: list[dict]) -> str:
    if not articles:
        return "Нет."
    return "\n".join(
        f"- «{a.get('title', '')}» [{a.get('source', '')} {a.get('pub_date', '')}]"
        + (f" → {a['risk_note']}" if a.get("risk_note") else "")
        for a in articles[:25]
    )


def _fmt_history(history: list[dict]) -> str:
    """Compact timeline of past runs so the agent can spot trends/recurrences."""
    if not history:
        return "Это первый анализ — истории прогонов пока нет."
    lines = []
    for h in history[:8]:
        date = (h.get("created_at") or "")[:10]
        score = h.get("score", "?")
        fps = h.get("risk_fingerprints") or []
        top = ", ".join(
            f"{fp.get('category', '?')}:{fp.get('title', '')[:40]}"
            f"(x{fp.get('consecutive_runs', 1)})"
            for fp in sorted(fps, key=lambda f: f.get("score", 0), reverse=True)[:4]
        )
        lines.append(f"  {date} · score={score} · {top or 'нет рисков'}")
    return "\n".join(lines)


def _build_prompt(
    company_name: str,
    own_rows: list[dict],
    risks: list[dict],
    prev_articles: list[dict],
    history: list[dict],
    kb_context: str = "",
) -> str:
    today = _date.today().strftime("%d.%m.%Y")
    kb_section = ""
    if kb_context:
        kb_section = f"""
=== БАЗА ЗНАНИЙ КОМПАНИИ (из загруженных документов) ===
{kb_context}

Используй это как ПРАВИЛА и контекст ИМЕННО этой компании (плейбуки, регламенты,
политики, прошлые кейсы). Если фрагмент релевантен риску/выводу — следуй ему и
явно указывай, на какой документ опираешься (например: «по Кризис-плейбуку, разд. 3»).
"""
    return f"""Компания: «{company_name}». Дата: {today}.

=== СИГНАЛЫ ТЕКУЩЕГО ПРОГОНА (отфильтрованные факты + ИИ-пометки) ===
{_fmt_signals(own_rows)}

=== ВЫДЕЛЕННЫЕ РИСКИ ТЕКУЩЕГО ПРОГОНА ===
{_fmt_risks(risks)}

=== МАТЕРИАЛЫ ПРОШЛОГО ПРОГОНА (контекст) ===
{_fmt_prev_articles(prev_articles)}

=== ИСТОРИЯ ПРОГОНОВ (для поиска закономерностей и трендов) ===
{_fmt_history(history)}
{kb_section}

КАК СТРОИТЬ:
- Сначала выдели затронутые АКТИВЫ (assets) с короткими id (a1, a2, …). Один актив
  = один id, не дублируй; события ссылаются на активы по id.
- Затем выдели УНИКАЛЬНЫЕ события как nodes с id (n1, n2, …). Повторяющееся событие
  — ОДИН узел (хаб), не дублируй. chains и connections ссылаются на узлы по id.
- Для каждого риска построй цепочку (chain_node_ids), раздели последствия на
  первичные/вторичные/третичные, спрогнозируй ущерб по 4 категориям и сценарии
  A/B/C. required_agents — только из: {", ".join(_AGENTS)}.
- ПРОВЕНАНС (откуда что взято):
  · каждому риску дай короткий id (r1, r2, …) и based_on_article_ids — id сигналов
    (s1, s2, …) из списка выше, на которых построен риск. Бери ТОЛЬКО реальные id;
    если источника нет — оставь пустой список, не выдумывай новости.
  · каждому выводу (conclusions) дай id (c1, …) и based_on_risks — id рисков,
    на которых он построен (риски описаны ниже в блоке "risks" — ссылайся на их id).
ВЫВОДЫ (conclusions) — ОБЯЗАТЕЛЬНО, это главный верхнеуровневый блок. Дай 3–5
коротких выводов, вытекающих ИМЕННО из найденных связей и истории — то, что НЕ видно
из отдельной новости (например: «компания в ловушке молчащего кризиса», «системная
деградация контроля качества»). Каждый по возможности ссылается на риски (based_on_risks).
Risks делай компактно, но не в ущерб conclusions/patterns/early_warnings.

Верни СТРОГО такой JSON (без markdown):
{{
  "assets": [
    {{"id": "a1", "name": "Бренд BI Group", "type": "company|plant|brand|executive|contractor|supplier|license|equipment|is|project"}}
  ],
  "nodes": [
    {{"id": "n1", "label": "краткое событие",
      "category": "media|hr|gr|pr|market",
      "crisis_type": "financial|operational|reputational|regulatory|",
      "severity": "high|medium|low", "role": "cause|hub|effect|event",
      "affected_assets": ["a1"], "evidence": ["заголовок-подтверждение"]}}
  ],
  "connections": [
    {{"from": "n1", "to": "n5", "type": "causal|escalation|correlation",
      "confidence": "low|medium|high", "rationale": "почему связаны (1 фраза)"}}
  ],
  "chains": [
    {{"title": "Отравление → репутация → падение продаж",
      "category": "media|hr|gr|pr|market", "severity": "high|medium|low",
      "status": "forming|active|resolved", "node_ids": ["n1", "n5", "n8"]}}
  ],
  "conclusions": [
    {{"id": "c1", "text": "инсайт из связей (то, что не видно из одной новости)", "based_on_risks": ["r1", "r3"]}}
  ],
  "patterns": [
    {{"pattern": "что повторяется/нарастает", "based_on": "история N прогонов", "implication": "что это значит"}}
  ],
  "early_warnings": [
    {{"signal": "слабый сигнал сейчас", "leads_to": "вероятный кризис",
      "horizon": "1-2 недели|месяц|квартал", "confidence": "low|medium|high",
      "category": "media|hr|gr|pr|market", "watch": "что отслеживать"}}
  ],
  "risks": [
    {{"id": "r1",
      "risk": "краткое название риска",
      "category": "media|hr|gr|pr|market",
      "kind": "risk|incident",
      "description": "что происходит и почему (опора на сигнал)",
      "signal_source": "СМИ + Telegram",
      "based_on_article_ids": ["s1", "s3"],
      "weak_signals": ["слабый сигнал 1", "слабый сигнал 2"],
      "cause": "первопричина",
      "chain_node_ids": ["n1", "n5", "n8"],
      "affected_assets": ["a1"],
      "related_objects": ["конкретные объекты"],
      "consequences": {{
        "primary":   ["первичное последствие"],
        "secondary": ["вторичное последствие"],
        "tertiary":  ["третичное последствие"]
      }},
      "probability": 0-100,
      "risk_level": "low|medium|high|critical",
      "escalation_speed": "low|medium|high|critical",
      "impact_scale": "local|regional|corporate|systemic",
      "damage_forecast": {{
        "financial":    {{"estimate": "...", "basis": "..."}},
        "operational":  {{"estimate": "...", "basis": "..."}},
        "reputational": {{"estimate": "...", "basis": "..."}},
        "legal":        {{"estimate": "...", "basis": "..."}}
      }},
      "scenarios": {{
        "A_no_action": {{"probability": 0-100, "financial": "...", "operational": "...", "reputational": "...", "legal": "...", "timeline": "..."}},
        "B_partial":   {{"probability": 0-100, "financial": "...", "operational": "...", "reputational": "...", "legal": "...", "timeline": "..."}},
        "C_immediate": {{"probability": 0-100, "financial": "...", "operational": "...", "reputational": "...", "legal": "...", "timeline": "..."}}
      }},
      "priority": "low|medium|high|critical",
      "recommended_actions": ["действие 1", "действие 2", "действие 3"],
      "required_agents": ["crisis_commander", "communications", "stakeholder", "intelligence", "execution", "reporting"]
    }}
  ]
}}"""


def _coerce_pct(v: Any, default: int = 0) -> int:
    try:
        return max(0, min(100, int(round(float(v)))))
    except (TypeError, ValueError):
        return default


def _norm_label(s: str) -> str:
    """Normalize free text so the same item collapses to one entry (dedup)."""
    return re.sub(r"\s+", " ", str(s or "").strip().lower())[:80]


def _val(v: Any, allowed: tuple, default: str) -> str:
    return v if v in allowed else default


def _strlist(v: Any, limit: int, item_len: int = 200) -> list[str]:
    if not isinstance(v, list):
        return []
    return [str(x)[:item_len] for x in v if x][:limit]


def _parse_assets(raw: dict) -> tuple[list[dict], set[str]]:
    """Unique asset registry. Dedup by id and by normalized name (one asset = one id)."""
    assets: list[dict] = []
    ids: set[str] = set()
    seen_names: set[str] = set()
    for a in raw.get("assets", []) or []:
        if not isinstance(a, dict):
            continue
        aid = str(a.get("id", "")).strip()[:20]
        name = str(a.get("name", "")).strip()[:120]
        if not aid or not name or aid in ids:
            continue
        norm = _norm_label(name)
        if norm in seen_names:
            continue
        atype = str(a.get("type", "")).strip().lower()[:30] or "other"
        if atype not in _ASSET_TYPES:
            # keep raw type but flag unknowns as 'other' family for the UI
            atype = atype if atype else "other"
        assets.append({"id": aid, "name": name, "type": atype})
        ids.add(aid)
        seen_names.add(norm)
    return assets, ids


def _parse_nodes(raw: dict, valid_asset_ids: set[str]) -> tuple[list[dict], dict[str, str]]:
    """
    Build the unique node list (see module docstring). Returns (nodes, label_to_id).
    Dedup is by id first; a fallback path synthesizes ids from chain/connection
    text so a repeated event still becomes ONE node.
    """
    nodes: list[dict] = []
    by_id: dict[str, dict] = {}
    label_to_id: dict[str, str] = {}

    def _add(nid: str, label: str, src: dict) -> None:
        nid = nid.strip()[:20]
        label = str(label or "").strip()[:160]
        if not nid or not label or nid in by_id:
            return
        norm = _norm_label(label)
        if norm in label_to_id:   # same event under a different id → collapse (hub)
            return
        affected = [x for x in _strlist(src.get("affected_assets"), 12, 20)
                    if x in valid_asset_ids]
        node = {
            "id": nid,
            "label": label,
            "category": _val(src.get("category"), _CATS, "media"),
            "crisis_type": _val(src.get("crisis_type"), _CRISIS_TYPES, ""),
            "severity": _val(src.get("severity"), _SEV, "medium"),
            "role": _val(src.get("role"), ("cause", "hub", "effect", "event"), "event"),
            "affected_assets": affected,
            "evidence": _strlist(src.get("evidence"), 5, 160),
        }
        by_id[nid] = node
        label_to_id[norm] = nid
        nodes.append(node)

    for n in raw.get("nodes", []) or []:
        if isinstance(n, dict):
            _add(str(n.get("id", "")), n.get("label", ""), n)

    # ── Fallback: model returned no usable nodes — synthesize from chain/edge text.
    if not nodes:
        seq_counter = 0

        def _ensure(label: str) -> None:
            nonlocal seq_counter
            norm = _norm_label(label)
            if not norm or norm in label_to_id:
                return
            seq_counter += 1
            _add(f"n{seq_counter}", label, {})

        for ch in raw.get("chains", []) or []:
            if isinstance(ch, dict):
                for s in ch.get("sequence", []) or []:
                    _ensure(str(s))
        for c in raw.get("connections", []) or []:
            if isinstance(c, dict):
                _ensure(str(c.get("from", "")))
                _ensure(str(c.get("to", "")))

    return nodes, label_to_id


def _resolve_ref(ref: Any, valid_ids: set[str], label_to_id: dict[str, str]) -> str | None:
    """Resolve a node reference that may be an id OR free text to a valid node id."""
    r = str(ref or "").strip()
    if not r:
        return None
    if r in valid_ids:
        return r
    return label_to_id.get(_norm_label(r))


def _clean_damage(d: Any) -> dict[str, dict]:
    """Always return all 4 categories so high/critical risks are never blank."""
    d = d if isinstance(d, dict) else {}
    out = {}
    for cat in _DMG_CATS:
        c = d.get(cat) if isinstance(d.get(cat), dict) else {}
        out[cat] = {
            "estimate": str(c.get("estimate", ""))[:200],
            "basis": str(c.get("basis", ""))[:200],
        }
    return out


def _clean_scenarios(s: Any) -> dict[str, dict]:
    """Always return A/B/C with all fields so a scenario is never missing."""
    s = s if isinstance(s, dict) else {}
    out = {}
    for key in _SCN_KEYS:
        sc = s.get(key) if isinstance(s.get(key), dict) else {}
        out[key] = {
            "probability": _coerce_pct(sc.get("probability")),
            "financial": str(sc.get("financial", ""))[:200],
            "operational": str(sc.get("operational", ""))[:200],
            "reputational": str(sc.get("reputational", ""))[:200],
            "legal": str(sc.get("legal", ""))[:200],
            "timeline": str(sc.get("timeline", ""))[:80],
        }
    return out


def _resolve_articles(r: dict, registry: dict[str, dict]) -> list[dict]:
    """Resolve based_on_article_ids (s1, s3, …) → real articles. No fabrication."""
    out: list[dict] = []
    seen: set[str] = set()
    # preferred path — references by id into the current-run article registry
    for ref in (r.get("based_on_article_ids") or []):
        a = registry.get(str(ref).strip())
        if a and a.get("title"):
            key = a.get("url") or a["title"]
            if key not in seen:
                seen.add(key)
                out.append(a)
    # tolerate inline article objects (must carry a real title)
    if not out:
        from . import source_categories as _sc
        for a in (r.get("based_on_articles") or []):
            if isinstance(a, dict) and str(a.get("title", "")).strip():
                key = a.get("url") or a["title"]
                if key in seen:
                    continue
                seen.add(key)
                _src, _url = str(a.get("source", "")), str(a.get("url", ""))
                out.append({
                    "title": str(a.get("title", ""))[:200],
                    "source": _src[:60],
                    "url": _url[:500],
                    "date": str(a.get("date") or a.get("pub_date") or "")[:40],
                    "category": _sc.classify(source=_src, url=_url),
                })
    return out[:8]


def _clean_risk(
    r: dict, idx: int, used_ids: set[str], valid_node_ids: set[str],
    valid_asset_ids: set[str], label_to_id: dict[str, str],
    registry: dict[str, dict],
) -> dict | None:
    name = str(r.get("risk", ""))[:160]
    if not name:
        return None
    rid = str(r.get("id", "")).strip()[:20]
    if not rid or rid in used_ids:
        rid = f"r{idx}"
    used_ids.add(rid)
    cons = r.get("consequences") if isinstance(r.get("consequences"), dict) else {}
    chain_ids: list[str] = []
    seen: set[str] = set()
    for ref in (r.get("chain_node_ids") or []):
        nid = _resolve_ref(ref, valid_node_ids, label_to_id)
        if nid and nid not in seen:
            seen.add(nid)
            chain_ids.append(nid)
    agents = [a for a in (r.get("required_agents") or [])
              if isinstance(a, str) and a in _AGENTS]
    return {
        "id": rid,
        "risk": name,
        "category": _val(r.get("category"), _CATS, "media"),
        "kind": _val(r.get("kind"), ("risk", "incident"), "risk"),
        "based_on_articles": _resolve_articles(r, registry),
        "description": str(r.get("description", ""))[:500],
        "signal_source": str(r.get("signal_source", ""))[:120],
        "weak_signals": _strlist(r.get("weak_signals"), 6, 160),
        "cause": str(r.get("cause", ""))[:300],
        "chain_node_ids": chain_ids,
        "affected_assets": [x for x in _strlist(r.get("affected_assets"), 12, 20)
                            if x in valid_asset_ids],
        "related_objects": _strlist(r.get("related_objects"), 8, 120),
        "consequences": {
            "primary": _strlist(cons.get("primary"), 6, 200),
            "secondary": _strlist(cons.get("secondary"), 6, 200),
            "tertiary": _strlist(cons.get("tertiary"), 6, 200),
        },
        "probability": _coerce_pct(r.get("probability")),
        "risk_level": _val(r.get("risk_level"), _LEVELS, "medium"),
        "escalation_speed": _val(r.get("escalation_speed"), _LEVELS, "medium"),
        "impact_scale": _val(r.get("impact_scale"), _SCALE, "regional"),
        "damage_forecast": _clean_damage(r.get("damage_forecast")),
        "scenarios": _clean_scenarios(r.get("scenarios")),
        "priority": _val(r.get("priority"), _LEVELS, "medium"),
        "recommended_actions": _strlist(r.get("recommended_actions"), 8, 240),
        "required_agents": list(dict.fromkeys(agents)),  # dedup, preserve order
    }


def _clean(raw: dict, own_rows: list[dict] | None = None) -> dict[str, Any]:
    """Validate/normalize the model output into a stable shape."""
    out = _empty_map()
    registry = _article_registry(own_rows or [])

    assets, valid_asset_ids = _parse_assets(raw)
    out["assets"] = assets

    nodes, label_to_id = _parse_nodes(raw, valid_asset_ids)
    out["nodes"] = nodes
    valid_ids = {n["id"] for n in nodes}

    # ── Edges: endpoints must resolve to existing nodes; no self-loops; dedup. ──
    seen_edges: set[tuple] = set()
    for c in raw.get("connections", []) or []:
        if not isinstance(c, dict):
            continue
        a = _resolve_ref(c.get("from"), valid_ids, label_to_id)
        b = _resolve_ref(c.get("to"), valid_ids, label_to_id)
        if not a or not b or a == b or (a, b) in seen_edges:
            continue
        seen_edges.add((a, b))
        out["connections"].append({
            "from": a, "to": b,
            "type": _val(c.get("type"), _EDGE_TYPES, "causal"),
            "confidence": _val(c.get("confidence"), _CONF, "medium"),
            "rationale": str(c.get("rationale", ""))[:200],
        })

    # ── Chains: ordered paths of valid node ids (≥2 distinct). ──
    for ch in raw.get("chains", []) or []:
        if not isinstance(ch, dict):
            continue
        refs = ch.get("node_ids") or ch.get("sequence")  # tolerate legacy text
        ids: list[str] = []
        seen: set[str] = set()
        for ref in (refs or []):
            rid = _resolve_ref(ref, valid_ids, label_to_id)
            if rid and rid not in seen:
                seen.add(rid)
                ids.append(rid)
        if len(ids) < 2:
            continue
        first_cat = next((n["category"] for n in nodes if n["id"] == ids[0]), "media")
        out["chains"].append({
            "title": str(ch.get("title", ""))[:120],
            "category": _val(ch.get("category"), _CATS, first_cat),
            "severity": _val(ch.get("severity"), _SEV, "medium"),
            "status": _val(ch.get("status"), ("forming", "active", "resolved"), "active"),
            "node_ids": ids,
        })

    # ── Per-risk intelligence (with stable ids + article provenance) ──
    used_rids: set[str] = set()
    for i, r in enumerate(raw.get("risks", []) or [], start=1):
        if not isinstance(r, dict):
            continue
        cr = _clean_risk(r, i, used_rids, valid_ids, valid_asset_ids, label_to_id, registry)
        if cr:
            out["risks"].append(cr)
    valid_risk_ids = {r["id"] for r in out["risks"]}

    # ── Conclusions → risks provenance (objects; tolerate legacy strings) ──
    for i, c in enumerate(raw.get("conclusions", []) or [], start=1):
        if isinstance(c, str):
            text, cid, based = c.strip(), f"c{i}", []
        elif isinstance(c, dict):
            text = str(c.get("text", "")).strip()
            cid = str(c.get("id", "")).strip()[:20] or f"c{i}"
            based = [x for x in (c.get("based_on_risks") or [])
                     if isinstance(x, str) and x in valid_risk_ids]
        else:
            continue
        if not text:
            continue
        out["conclusions"].append({"id": cid, "text": text[:300], "based_on_risks": based})
        if len(out["conclusions"]) >= 6:
            break

    for p in raw.get("patterns", []) or []:
        if not isinstance(p, dict) or not p.get("pattern"):
            continue
        out["patterns"].append({
            "pattern": str(p.get("pattern", ""))[:200],
            "based_on": str(p.get("based_on", ""))[:160],
            "implication": str(p.get("implication", ""))[:200],
        })

    for w in raw.get("early_warnings", []) or []:
        if not isinstance(w, dict) or not w.get("signal"):
            continue
        out["early_warnings"].append({
            "signal": str(w.get("signal", ""))[:200],
            "leads_to": str(w.get("leads_to", ""))[:200],
            "horizon": str(w.get("horizon", ""))[:40],
            "confidence": _val(w.get("confidence"), _CONF, "medium"),
            "category": _val(w.get("category"), _CATS, "media"),
            "watch": str(w.get("watch", ""))[:200],
        })

    # ── Backward-compat passthrough for older stored maps (optional key). ──
    if isinstance(raw.get("forecast"), dict) and raw.get("forecast"):
        out["forecast"] = raw["forecast"]

    return out


def _compute_graph_metrics(out: dict[str, Any]) -> None:
    """
    Deterministically annotate nodes with degree / is_hub / terminal (in-place).

    degree   = incoming + outgoing edges (hub = high degree → bigger on the map).
    is_hub   = degree >= _HUB_DEGREE.
    terminal = has incoming but no outgoing edges (a sink — "к чему ведёт").
    """
    nodes = out["nodes"]
    indeg = {n["id"]: 0 for n in nodes}
    outdeg = {n["id"]: 0 for n in nodes}
    for e in out["connections"]:
        outdeg[e["from"]] = outdeg.get(e["from"], 0) + 1
        indeg[e["to"]] = indeg.get(e["to"], 0) + 1

    for n in nodes:
        nid = n["id"]
        deg = indeg.get(nid, 0) + outdeg.get(nid, 0)
        n["degree"] = deg
        n["is_hub"] = deg >= _HUB_DEGREE
        n["terminal"] = indeg.get(nid, 0) > 0 and outdeg.get(nid, 0) == 0
        if n["is_hub"] and n["role"] == "event":
            n["role"] = "hub"


def build_connection_map(
    company_name: str,
    own_rows: list[dict],
    risks: list[dict],
    api_key: str,
    prev_articles: list[dict] | None = None,
    history: list[dict] | None = None,
    kb_chunks: list[dict] | None = None,
) -> dict[str, Any]:
    """
    Synthesize the cross-event risk graph + per-risk intelligence. Never raises.

    own_rows      — current-run own articles (with risk_note) from analysis_articles.
    risks         — flat risks produced by analyze_with_history.
    prev_articles — top_articles from the previous run (context).
    history       — recent risk_runs [{created_at, score, risk_fingerprints}] for patterns.
    """
    if not api_key or (not own_rows and not risks):
        return _empty_map()

    import anthropic as _a
    import time as _t
    from .knowledge_base import build_context_block as _kb_ctx

    kb_context, kb_sources = _kb_ctx(kb_chunks or [])
    prompt = _build_prompt(
        company_name, own_rows, risks, prev_articles or [], history or [], kb_context
    )

    def _call() -> dict:
        c = _a.Anthropic(api_key=api_key, max_retries=0)
        attempt = 0
        while True:
            try:
                msg = c.messages.create(
                    model=_MODEL,
                    max_tokens=16000,
                    system=_SYSTEM,
                    messages=[{"role": "user", "content": prompt}],
                )
                txt = msg.content[0].text.strip()
                if txt.startswith("```"):
                    txt = "\n".join(txt.split("\n")[1:]).rsplit("```", 1)[0].strip()
                try:
                    return json.loads(txt)
                except json.JSONDecodeError:
                    from json_repair import repair_json
                    return json.loads(repair_json(txt))
            except Exception as e:
                if "overloaded" in str(e) and attempt < 4:
                    wait = min(8 * (2 ** attempt), 60)
                    attempt += 1
                    logger.info("connection_agent overloaded, waiting %ds", wait)
                    _t.sleep(wait)
                else:
                    raise

    try:
        raw = _call()
    except Exception as e:
        logger.warning("build_connection_map failed: %s", e)
        return _empty_map()

    result = _clean(raw if isinstance(raw, dict) else {}, own_rows)
    result["kb_sources"] = kb_sources   # documents used as context (transparency)
    _compute_graph_metrics(result)
    logger.info(
        "[connection_agent] %s: %d assets, %d nodes (%d hubs), %d edges, "
        "%d chains, %d risks, %d conclusions, %d warnings",
        company_name, len(result["assets"]), len(result["nodes"]),
        sum(1 for n in result["nodes"] if n["is_hub"]),
        len(result["connections"]), len(result["chains"]),
        len(result["risks"]), len(result["conclusions"]), len(result["early_warnings"]),
    )
    # conclusions is the top-level insight block — flag when it came back empty
    # despite having data (truncation / model omission), so regressions are visible.
    if not result["conclusions"] and (result["risks"] or result["nodes"]):
        logger.warning(
            "[connection_agent] %s: conclusions EMPTY despite %d risks / %d nodes "
            "(возможна обрезка по токенам или пропуск моделью)",
            company_name, len(result["risks"]), len(result["nodes"]),
        )
    return result
