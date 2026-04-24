-- Migration: add company profile fields
-- Run this in the Supabase SQL editor

alter table public.companies add column if not exists ceo_name    text;
alter table public.companies add column if not exists website      text;
alter table public.companies add column if not exists location     text;
alter table public.companies add column if not exists industry     text;
alter table public.companies add column if not exists description  text;
alter table public.companies add column if not exists market_info  text;
alter table public.companies add column if not exists news_sources jsonb default '[]'::jsonb;

-- competitors table (if not exists)
create table if not exists public.competitors (
  id         uuid primary key default gen_random_uuid(),
  company_id uuid not null references public.companies(id) on delete cascade,
  name       text not null,
  website    text default '',
  created_at timestamptz not null default now()
);

create index if not exists competitors_company_id_idx on public.competitors(company_id);

alter table public.competitors enable row level security;

drop policy if exists competitors_owner_select on public.competitors;
create policy competitors_owner_select on public.competitors for select to authenticated
using (exists (select 1 from public.companies c where c.id = competitors.company_id and c.owner_user_id = auth.uid()));

drop policy if exists competitors_owner_insert on public.competitors;
create policy competitors_owner_insert on public.competitors for insert to authenticated
with check (exists (select 1 from public.companies c where c.id = competitors.company_id and c.owner_user_id = auth.uid()));

drop policy if exists competitors_owner_delete on public.competitors;
create policy competitors_owner_delete on public.competitors for delete to authenticated
using (exists (select 1 from public.companies c where c.id = competitors.company_id and c.owner_user_id = auth.uid()));

-- Allow owner to update company profile fields
drop policy if exists companies_owner_update on public.companies;
create policy companies_owner_update on public.companies for update to authenticated
using (owner_user_id = auth.uid()) with check (owner_user_id = auth.uid());
