-- Connection Agent: stores the synthesized "risk map" produced once per run.
-- Holds causal chains, the event graph, conclusions, cross-run patterns,
-- best/base/worst forecast and early-warning signals.
alter table public.risk_runs
  add column if not exists connection_map jsonb default '{}'::jsonb;
