-- Knowledge base (client documents → RAG retrieval into the connection agent).
-- Vector dimension below MUST match settings.EMBEDDING_DIM (default 1536 =
-- OpenAI text-embedding-3-small). If you switch the embedding model, recreate
-- the embedding column with the new dimension.

create extension if not exists vector;

-- ── Documents ────────────────────────────────────────────────────────────────
create table if not exists public.knowledge_documents (
  id           bigserial primary key,
  company_id   uuid not null references public.companies(id) on delete cascade,
  name         text not null,
  mime         text,
  size         bigint default 0,
  status       text not null default 'parsing',   -- parsing | ready | failed
  error        text,
  chunk_count  int default 0,
  created_at   timestamptz not null default now()
);
create index if not exists knowledge_documents_company_idx on public.knowledge_documents(company_id);

-- ── Chunks + embeddings ──────────────────────────────────────────────────────
create table if not exists public.knowledge_chunks (
  id          bigserial primary key,
  company_id  uuid not null references public.companies(id) on delete cascade,
  document_id bigint not null references public.knowledge_documents(id) on delete cascade,
  chunk_text  text not null,
  embedding   vector(1536),
  metadata    jsonb default '{}'::jsonb,
  created_at  timestamptz default now()
);
create index if not exists knowledge_chunks_company_idx on public.knowledge_chunks(company_id);
create index if not exists knowledge_chunks_doc_idx on public.knowledge_chunks(document_id);
-- cosine similarity index (build after some data exists for best results)
create index if not exists knowledge_chunks_embedding_idx
  on public.knowledge_chunks using ivfflat (embedding vector_cosine_ops) with (lists = 100);

-- ── Similarity search (ALWAYS scoped to one company — hard isolation) ─────────
-- embedding passed as text "[...]" and cast inside to avoid PostgREST type issues.
create or replace function public.match_knowledge_chunks(
  p_company_id uuid,
  p_query_embedding text,
  p_match_count int default 6
)
returns table (
  id bigint,
  document_id bigint,
  chunk_text text,
  metadata jsonb,
  similarity float
)
language sql stable
as $$
  select kc.id, kc.document_id, kc.chunk_text, kc.metadata,
         1 - (kc.embedding <=> p_query_embedding::vector) as similarity
  from public.knowledge_chunks kc
  where kc.company_id = p_company_id
    and kc.embedding is not null
  order by kc.embedding <=> p_query_embedding::vector
  limit greatest(1, p_match_count);
$$;

alter table public.knowledge_documents enable row level security;
alter table public.knowledge_chunks    enable row level security;
-- Access is via the service key from the backend only (no client-side RLS policies),
-- mirroring how risk_runs / analysis_articles are accessed in this app.
