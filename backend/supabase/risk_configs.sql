-- Client risk-scoring: per-type formulas (as data), per-type user tuning,
-- and extracted factors. Run in Supabase SQL editor (idempotent).
-- Each risk TYPE is scored independently — there is no aggregate total.

-- ── Formulas AS DATA (standard + client-custom), one active per (company,type).
--    Standard formulas live in code (risk_formulas.STANDARD_FORMULAS); a row
--    here only exists when a client overrides a type. expression is evaluated
--    in a sandbox (formula_eval), never eval(). ──
create table if not exists formula_defs (
    id          uuid primary key default gen_random_uuid(),
    company_id  uuid not null references companies(id) on delete cascade,
    domain      text not null
                    check (domain in ('financial','operational','reputational','regulatory','supplier')),
    source      text not null default 'custom' check (source in ('standard','custom')),
    expression  text not null,                  -- e.g. "reach * sentiment * audience_influence / 10"
    variables   jsonb not null default '[]'::jsonb,   -- factors the AI must extract for this formula
    is_active   boolean not null default false,
    version     int not null default 1,
    created_by  uuid,
    created_at  timestamptz not null default now()
);
create index if not exists formula_defs_company_domain on formula_defs (company_id, domain);
-- at most one active formula per company+type
create unique index if not exists formula_defs_one_active
    on formula_defs (company_id, domain) where is_active;

-- ── Per-type tuning (enabled + alert threshold). Formulas NOT stored here.
--    Resolution: user → department → company. ──
create table if not exists risk_configs (
    id          uuid primary key default gen_random_uuid(),
    company_id  uuid not null references companies(id) on delete cascade,
    scope       text not null default 'company'
                    check (scope in ('user','department','company')),
    owner_id    uuid,
    per_type    jsonb not null default '{}'::jsonb,   -- {type: {enabled bool, threshold 60|70|80}}
    preset      text not null default 'balanced',
    version     int  not null default 1,
    updated_by  uuid,
    updated_at  timestamptz not null default now()
);
create unique index if not exists risk_configs_unique
    on risk_configs (company_id, scope, coalesce(owner_id, '00000000-0000-0000-0000-000000000000'::uuid));
create index if not exists risk_configs_company on risk_configs (company_id);

-- ── Extracted factors per signal (powers /preview "было→стало" by type and the
--    transparent risk card). Scores are always recomputed from factors. ──
create table if not exists signal_factors (
    id           uuid primary key default gen_random_uuid(),
    company_id   uuid not null references companies(id) on delete cascade,
    signal_text  text,
    source       text,
    factors      jsonb not null default '{}'::jsonb,  -- {domain: {factor:0..10,...,why}}
    created_at   timestamptz not null default now()
);
create index if not exists signal_factors_company_time
    on signal_factors (company_id, created_at desc);

-- ── Per-type domain scoring stored on each analysis run (Overview + notifications).
--    Factors are stored; scores are recomputed from factors at read time. ──
alter table risk_runs add column if not exists domain_factors jsonb default '{}'::jsonb;
alter table risk_runs add column if not exists domain_scores  jsonb default '{}'::jsonb;
