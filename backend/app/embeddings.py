"""
Embeddings — provider is configured in settings (not hardcoded).
Supports OpenAI (text-embedding-3-*) and Voyage (voyage-3*). Uses httpx directly
so no extra SDK dependency. Returns [] (graceful) when no key is configured — the
rest of the system must work without a knowledge base.
"""
import logging
from typing import Any

import httpx

from .config import settings

logger = logging.getLogger("conexiai.embeddings")


def _api_key() -> str:
    if settings.EMBEDDING_PROVIDER == "voyage":
        return settings.VOYAGE_API_KEY
    return settings.OPENAI_API_KEY


def is_configured() -> bool:
    return bool(settings.KNOWLEDGE_BASE_ENABLED and _api_key())


def to_pgvector(vec: list[float]) -> str:
    """Format a vector for pgvector text input: '[0.1,0.2,...]'."""
    return "[" + ",".join(repr(float(x)) for x in vec) + "]"


async def embed(texts: list[str], *, input_type: str = "document") -> list[list[float]]:
    """
    Return one embedding per input text. [] if not configured or on error
    (caller must treat empty as "no knowledge base").

    input_type: "document" when indexing, "query" when searching (Voyage uses it).
    """
    texts = [t for t in (texts or []) if t and t.strip()]
    if not texts or not is_configured():
        return []

    provider = settings.EMBEDDING_PROVIDER
    key = _api_key()
    try:
        if provider == "voyage":
            return await _embed_voyage(texts, key, input_type)
        return await _embed_openai(texts, key)
    except Exception as e:
        logger.warning("embeddings (%s/%s) failed: %s", provider, settings.EMBEDDING_MODEL, e)
        return []


async def embed_one(text: str, *, input_type: str = "query") -> list[float]:
    out = await embed([text], input_type=input_type)
    return out[0] if out else []


async def _embed_openai(texts: list[str], key: str) -> list[list[float]]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(
            "https://api.openai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": settings.EMBEDDING_MODEL, "input": texts},
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        return [d["embedding"] for d in sorted(data, key=lambda d: d.get("index", 0))]


async def _embed_voyage(texts: list[str], key: str, input_type: str) -> list[list[float]]:
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(
            "https://api.voyageai.com/v1/embeddings",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": settings.EMBEDDING_MODEL, "input": texts,
                  "input_type": "document" if input_type == "document" else "query"},
        )
        r.raise_for_status()
        data = r.json().get("data", [])
        return [d["embedding"] for d in sorted(data, key=lambda d: d.get("index", 0))]
