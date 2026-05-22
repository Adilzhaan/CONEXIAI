-- Run in Supabase SQL editor
create table if not exists analysis_articles (
  id           uuid        primary key default gen_random_uuid(),
  company_id   uuid        not null,
  job_id       text        not null,
  title        text        not null default '',
  url          text        default '',
  source       text        default '',
  source_type  text        default 'news',  -- news | reddit | youtube | instagram | threads
  excerpt      text        default '',
  pub_date     text        default '',
  status       text        default 'pending',  -- pending | selected | rejected
  created_at   timestamptz default now()
);

create index if not exists idx_analysis_articles_job_id     on analysis_articles(job_id);
create index if not exists idx_analysis_articles_company_id on analysis_articles(company_id);
create index if not exists idx_analysis_articles_status     on analysis_articles(status);

-- Allow service role full access (RLS disabled for service key)
alter table analysis_articles disable row level security;
