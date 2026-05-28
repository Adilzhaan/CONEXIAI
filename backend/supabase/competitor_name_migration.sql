-- Add competitor_name to analysis_articles
-- Empty string = article for main company
-- Non-empty = article for that competitor

alter table analysis_articles
  add column if not exists competitor_name text not null default '';

create index if not exists idx_analysis_articles_competitor
  on analysis_articles(company_id, competitor_name);
