"""
Knowledge base (RAG) — parse client documents, chunk, embed, store in pgvector,
and retrieve the top-k relevant chunks at analysis time. Retrieval is ALWAYS
scoped to one company_id (hard isolation). NOT fine-tuning — pure retrieval.
"""
import logging
from typing import Any

from .config import settings
from .supabase import supabase
from . import embeddings as _emb

logger = logging.getLogger("conexiai.knowledge_base")

SUPPORTED_MIME = {
    "application/pdf": "pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "docx",
    "text/plain": "txt",
    "text/markdown": "md",
}


def detect_kind(filename: str, mime: str) -> str | None:
    name = (filename or "").lower()
    if name.endswith(".pdf") or mime == "application/pdf":
        return "pdf"
    if name.endswith(".docx") or "wordprocessingml" in (mime or ""):
        return "docx"
    if name.endswith(".md"):
        return "md"
    if name.endswith(".txt") or (mime or "").startswith("text/"):
        return "txt"
    return None


# ── Parsing → list of (text, metadata) sections ──────────────────────────────

def extract_sections(kind: str, data: bytes) -> list[tuple[str, dict]]:
    if kind == "pdf":
        return _extract_pdf(data)
    if kind == "docx":
        return _extract_docx(data)
    # txt / md
    text = data.decode("utf-8", errors="replace")
    return [(text, {})] if text.strip() else []


def _extract_pdf(data: bytes) -> list[tuple[str, dict]]:
    import io
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(data))
    out: list[tuple[str, dict]] = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            out.append((t, {"page": i}))
    return out


def _extract_docx(data: bytes) -> list[tuple[str, dict]]:
    import io
    import docx  # python-docx
    doc = docx.Document(io.BytesIO(data))
    paras = [p.text for p in doc.paragraphs if p.text and p.text.strip()]
    text = "\n".join(paras)
    return [(text, {})] if text.strip() else []


# ── Chunking (~tokens via ~4 chars/token) with overlap ───────────────────────

def _chunk(text: str) -> list[str]:
    chunk_chars = max(400, settings.KB_CHUNK_TOKENS * 4)
    overlap_chars = max(0, settings.KB_CHUNK_OVERLAP * 4)
    paras = [p.strip() for p in text.replace("\r", "").split("\n\n") if p.strip()]
    chunks: list[str] = []
    buf = ""
    for p in paras:
        if buf and len(buf) + len(p) + 2 > chunk_chars:
            chunks.append(buf.strip())
            buf = (buf[-overlap_chars:] + "\n\n" + p) if overlap_chars else p
        else:
            buf = (buf + "\n\n" + p) if buf else p
    if buf.strip():
        chunks.append(buf.strip())
    # hard-split any oversized chunk (e.g. a single huge paragraph)
    out: list[str] = []
    for c in chunks:
        if len(c) <= chunk_chars * 1.5:
            out.append(c)
        else:
            step = chunk_chars - overlap_chars or chunk_chars
            for i in range(0, len(c), step):
                out.append(c[i:i + chunk_chars])
    return [c for c in out if c.strip()]


# ── Indexing: chunk → embed → store ──────────────────────────────────────────

async def index_document(company_id: str, document_id: int, name: str,
                         sections: list[tuple[str, dict]]) -> int:
    sk = settings.SUPABASE_SERVICE_KEY
    if not sk:
        return 0
    chunks: list[tuple[str, dict]] = []
    for sec_text, sec_meta in sections:
        for ch in _chunk(sec_text):
            md = {"document": name}
            md.update(sec_meta)
            chunks.append((ch, md))
    if not chunks:
        return 0

    # Embed in batches (graceful: [] when no provider key → store text only)
    vectors: list[list[float]] = []
    if _emb.is_configured():
        for i in range(0, len(chunks), 64):
            batch = [c for c, _ in chunks[i:i + 64]]
            vectors.extend(await _emb.embed(batch, input_type="document"))

    rows: list[dict] = []
    for i, (text, md) in enumerate(chunks):
        row: dict[str, Any] = {
            "company_id": company_id, "document_id": document_id,
            "chunk_text": text, "metadata": md,
        }
        if vectors and i < len(vectors) and vectors[i]:
            row["embedding"] = _emb.to_pgvector(vectors[i])
        rows.append(row)

    for i in range(0, len(rows), 100):
        await supabase.rest_insert_service("knowledge_chunks", sk, rows[i:i + 100])
    logger.info("[kb] indexed %d chunks for doc=%s company=%s (embedded=%s)",
                len(rows), document_id, company_id, bool(vectors))
    return len(rows)


# ── Retrieval (hard company isolation) ───────────────────────────────────────

async def search(company_id: str, query: str, k: int | None = None) -> list[dict]:
    """Top-k chunks for this company only. [] if KB empty / not configured."""
    sk = settings.SUPABASE_SERVICE_KEY
    if not sk or not company_id or not query or not _emb.is_configured():
        return []
    vec = await _emb.embed_one(query, input_type="query")
    if not vec:
        return []
    try:
        rows = await supabase.rest_rpc("match_knowledge_chunks", sk, {
            "p_company_id": company_id,
            "p_query_embedding": _emb.to_pgvector(vec),
            "p_match_count": int(k or settings.KB_TOP_K),
        })
        return rows or []
    except Exception as e:
        logger.warning("[kb] search failed: %s", e)
        return []


def build_context_block(chunks: list[dict]) -> tuple[str, list[str]]:
    """Render retrieved chunks for the prompt + the list of source document names."""
    if not chunks:
        return "", []
    lines: list[str] = []
    docs: list[str] = []
    for c in chunks:
        md = c.get("metadata") or {}
        doc = md.get("document") or "документ"
        page = md.get("page")
        ref = doc + (f", стр. {page}" if page else "")
        if doc not in docs:
            docs.append(doc)
        lines.append(f"[{ref}]\n{(c.get('chunk_text') or '')[:900]}")
    return "\n\n".join(lines), docs
