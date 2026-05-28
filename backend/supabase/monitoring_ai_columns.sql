-- Add AI analysis columns to monitoring_snapshots
-- Run in Supabase SQL editor

alter table public.monitoring_snapshots
  add column if not exists ai_summary      text,
  add column if not exists ai_problems     jsonb,
  add column if not exists ai_losses       jsonb,
  add column if not exists ai_recommendation text;
