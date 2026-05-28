-- monitoring_items: stores news/posts from all sources after full analysis
-- Run in Supabase SQL editor

create table if not exists public.monitoring_items (
  id          uuid primary key default gen_random_uuid(),
  company_id  uuid not null references public.companies(id) on delete cascade,
  source      text not null,   -- 'news' | 'reddit' | 'youtube' | 'instagram' | 'threads'
  source_name text,            -- e.g. 'Reuters', 'r/investing', 'CNN'
  title       text not null,
  url         text,
  excerpt     text,
  pub_date    text,
  is_top      boolean not null default false,
  importance  text,            -- AI explanation (top items only)
  created_at  timestamptz not null default now()
);

create index if not exists monitoring_items_company_id_idx
  on public.monitoring_items(company_id, created_at desc);

alter table public.monitoring_items enable row level security;

-- Owner can read/write
drop policy if exists mon_items_owner_select on public.monitoring_items;
create policy mon_items_owner_select on public.monitoring_items for select to authenticated
using (exists (
  select 1 from public.companies c
  where c.id = monitoring_items.company_id and c.owner_user_id = auth.uid()
));

drop policy if exists mon_items_owner_insert on public.monitoring_items;
create policy mon_items_owner_insert on public.monitoring_items for insert to authenticated
with check (exists (
  select 1 from public.companies c
  where c.id = monitoring_items.company_id and c.owner_user_id = auth.uid()
));

-- Members can read
drop policy if exists mon_items_member_select on public.monitoring_items;
create policy mon_items_member_select on public.monitoring_items for select to authenticated
using (exists (
  select 1 from public.company_members m
  where m.company_id = monitoring_items.company_id
    and m.user_id = auth.uid() and m.status = 'active'
));
