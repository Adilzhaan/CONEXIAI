create table if not exists section_runs (
  id           uuid primary key default gen_random_uuid(),
  company_id   uuid not null references companies(id) on delete cascade,
  job_id       text not null,
  section      text not null, -- media | hr | gr | pr | market
  queries      jsonb default '[]',
  articles     jsonb default '[]',
  risks        jsonb default '[]',
  score        int,
  summary      text,
  status       text default 'pending', -- pending | running | done | error
  created_at   timestamptz default now()
);

create index if not exists section_runs_company_job on section_runs(company_id, job_id);
create index if not exists section_runs_company_section on section_runs(company_id, section);

-- Keep only last 5 jobs per company (cleanup via app logic)
