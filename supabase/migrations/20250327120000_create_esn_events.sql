-- ESN activities scraped from activities.esn.org (mirror of events.json shape).
-- Run in Supabase SQL Editor or via: supabase db push (if using Supabase CLI).

create table if not exists public.esn_events (
    id uuid primary key default gen_random_uuid (),
    event_name text not null,
    organizer_section text,
    event_date jsonb not null,
    is_upcoming boolean not null default true,
    organizer_section_website_link text,
    location text,
    event_page_link text not null,
    created_at timestamptz not null default now (),
    constraint esn_events_event_page_link_key unique (event_page_link)
);

create index if not exists esn_events_organizer_section_idx on public.esn_events (organizer_section);

create index if not exists esn_events_event_date_start_idx on public.esn_events ((event_date ->> 'start'));

comment on
table public.esn_events is 'ESN activity rows; upsert key is event_page_link.';
