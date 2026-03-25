-- CONEXIAI Supabase schema
-- Run in Supabase SQL editor (or via migrations).

-- Enable required extension for gen_random_uuid()
create extension if not exists pgcrypto;

-- Companies owned by authenticated users
create table if not exists public.companies (
  id uuid primary key default gen_random_uuid(),
  owner_user_id uuid not null default auth.uid(),
  name text not null,
  ceo_email text not null,
  created_at timestamptz not null default now()
);

create index if not exists companies_owner_user_id_idx on public.companies(owner_user_id);

-- Employees belonging to a company
create table if not exists public.employees (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  full_name text not null,
  email text not null,
  position text,
  department text,
  created_at timestamptz not null default now()
);

create index if not exists employees_company_id_idx on public.employees(company_id);

-- Risk runs triggered by the user; n8n writes results back.
create table if not exists public.risk_runs (
  id uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  created_by_user_id uuid not null default auth.uid(),
  status text not null default 'queued',
  -- Results produced by n8n / AI
  risks jsonb,
  score numeric,
  advice text,
  report_url text,
  error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists risk_runs_company_id_idx on public.risk_runs(company_id);
create index if not exists risk_runs_created_by_user_id_idx on public.risk_runs(created_by_user_id);

-- Keep updated_at in sync for n8n updates (and any future patches).
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_risk_runs_updated_at on public.risk_runs;
create trigger trg_risk_runs_updated_at
before update on public.risk_runs
for each row execute function public.set_updated_at();

-- RLS policies
alter table public.companies enable row level security;
alter table public.employees enable row level security;
alter table public.risk_runs enable row level security;

-- Companies: owner can read & create
drop policy if exists companies_owner_select on public.companies;
create policy companies_owner_select
on public.companies
for select
to authenticated
using (owner_user_id = auth.uid());

drop policy if exists companies_owner_insert on public.companies;
create policy companies_owner_insert
on public.companies
for insert
to authenticated
with check (owner_user_id = auth.uid());

-- Employees: owner can read & create when the employee belongs to their company
drop policy if exists employees_owner_select on public.employees;
create policy employees_owner_select
on public.employees
for select
to authenticated
using (
  exists (
    select 1 from public.companies c
    where c.id = employees.company_id
      and c.owner_user_id = auth.uid()
  )
);

drop policy if exists employees_owner_insert on public.employees;
create policy employees_owner_insert
on public.employees
for insert
to authenticated
with check (
  exists (
    select 1 from public.companies c
    where c.id = employees.company_id
      and c.owner_user_id = auth.uid()
  )
);

-- Risk runs: owner can read & create; n8n (service role) updates results.
drop policy if exists risk_runs_owner_select on public.risk_runs;
create policy risk_runs_owner_select
on public.risk_runs
for select
to authenticated
using (
  exists (
    select 1 from public.companies c
    where c.id = risk_runs.company_id
      and c.owner_user_id = auth.uid()
  )
);

drop policy if exists risk_runs_owner_insert on public.risk_runs;
create policy risk_runs_owner_insert
on public.risk_runs
for insert
to authenticated
with check (
  exists (
    select 1 from public.companies c
    where c.id = risk_runs.company_id
      and c.owner_user_id = auth.uid()
  )
);

-- No UPDATE policy for authenticated users by default.
-- n8n should update risk_runs using its Supabase service role key.

