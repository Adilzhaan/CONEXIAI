-- Pipeline v2 migration
-- Run in Supabase SQL editor

-- analysis_articles: add entity columns + published_at
alter table analysis_articles
  add column if not exists published_at  timestamptz,
  add column if not exists entity_type   text not null default 'own',
  add column if not exists entity_name   text not null default '';

create index if not exists idx_analysis_articles_entity
  on analysis_articles(company_id, entity_type, entity_name);

-- risk_runs: add fingerprints + previous_run link
alter table risk_runs
  add column if not exists risk_fingerprints jsonb,
  add column if not exists previous_run_id   uuid;

-- Remove old categories/scenarios columns if they don't exist yet (noop if already there)
alter table risk_runs
  add column if not exists categories       jsonb,
  add column if not exists scenarios        jsonb,
  add column if not exists positive_signals jsonb,
  add column if not exists top_articles     jsonb,
  add column if not exists loss_scenarios   jsonb;

-- Per-category scoring settings (weight + sensitivity) — department "cabinets"
create table if not exists category_settings (
  company_id        uuid not null,
  category          text not null,
  weight            numeric not null default 1.0,
  sensitivity       numeric not null default 1.0,
  preferred_sources jsonb,
  custom_keywords   jsonb,
  updated_by        uuid,
  updated_at        timestamptz default now(),
  primary key (company_id, category)
);

-- If the table already exists, add the new columns:
alter table category_settings
  add column if not exists preferred_sources jsonb,
  add column if not exists custom_keywords   jsonb;
