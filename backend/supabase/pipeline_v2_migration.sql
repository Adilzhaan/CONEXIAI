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
