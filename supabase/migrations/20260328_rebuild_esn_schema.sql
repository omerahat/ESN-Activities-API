-- =============================================================================
-- ESN Schema Rebuild Migration
-- Date: 2026-03-28
-- Description:
--   Introduces esn_countries and esn_sections tables and rebuilds esn_events
--   to support a modular web-scraping architecture with cron jobs.
--   All tables carry three audit columns: created_at, updated_at, last_scraped_at.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. esn_countries
-- ---------------------------------------------------------------------------
create table if not exists public.esn_countries (
    country_code  varchar(2)   not null,
    country_name  text,
    url           text,
    email         text,
    website       text,
    social_links  jsonb,

    -- Audit columns
    created_at    timestamptz  not null default now(),
    updated_at    timestamptz  not null default now(),
    last_scraped_at timestamptz not null default now(),

    constraint esn_countries_pkey primary key (country_code)
);

comment on table public.esn_countries is
    'One row per ESN national organisation; keyed by ISO-3166-1 alpha-2 country code.';

-- ---------------------------------------------------------------------------
-- 2. esn_sections
-- ---------------------------------------------------------------------------
create table if not exists public.esn_sections (
    id                   uuid         not null default gen_random_uuid(),
    section_name         text         not null,
    country_code         varchar(2),
    city                 text,
    logo_url             text,
    address              text,
    university_name      text,
    university_website   text,
    email                text,
    website              text,
    social_links         jsonb,

    -- Audit columns
    created_at           timestamptz  not null default now(),
    updated_at           timestamptz  not null default now(),
    last_scraped_at      timestamptz  not null default now(),

    constraint esn_sections_pkey         primary key (id),
    constraint esn_sections_name_key     unique      (section_name),
    constraint esn_sections_country_fkey foreign key (country_code)
        references public.esn_countries (country_code)
        on delete cascade
);

comment on table public.esn_sections is
    'One row per ESN local section; section_name is the human-readable unique key used by esn_events.';

-- Index on FK for efficient joins / cascades
create index if not exists esn_sections_country_code_idx
    on public.esn_sections (country_code);

-- ---------------------------------------------------------------------------
-- 3. esn_events  (drop & recreate to incorporate FK to esn_sections)
-- ---------------------------------------------------------------------------
-- Drop the old table if it exists so we can apply the new shape cleanly.
-- Adjust to ALTER TABLE instead if you need to preserve existing data.
drop table if exists public.esn_events;

create table public.esn_events (
    id                              uuid         not null default gen_random_uuid(),
    event_name                      text         not null,
    organizer_section               text,
    event_date                      jsonb        not null,
    is_upcoming                     boolean      not null default true,
    organizer_section_website_link  text,
    location                        text,
    event_page_link                 text         not null,

    -- Audit columns
    created_at      timestamptz  not null default now(),
    updated_at      timestamptz  not null default now(),
    last_scraped_at timestamptz  not null default now(),

    constraint esn_events_pkey               primary key (id),
    constraint esn_events_event_page_link_key unique      (event_page_link),
    constraint esn_events_organizer_fkey     foreign key (organizer_section)
        references public.esn_sections (section_name)
        on delete set null
);

comment on table public.esn_events is
    'ESN activity rows scraped from activities.esn.org; upsert key is event_page_link.';

-- Index on FK (organizer_section) for efficient lookups
create index if not exists esn_events_organizer_section_idx
    on public.esn_events (organizer_section);

-- Index on event start date (JSONB extraction) for date-range queries
create index if not exists esn_events_event_date_start_idx
    on public.esn_events ((event_date ->> 'start'));

-- ---------------------------------------------------------------------------
-- 4. updated_at trigger helper (optional but recommended for cron hygiene)
--    Creates a shared function that sets updated_at = now() on every UPDATE.
-- ---------------------------------------------------------------------------
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

-- Attach trigger to esn_countries
create or replace trigger esn_countries_set_updated_at
    before update on public.esn_countries
    for each row execute function public.set_updated_at();

-- Attach trigger to esn_sections
create or replace trigger esn_sections_set_updated_at
    before update on public.esn_sections
    for each row execute function public.set_updated_at();

-- Attach trigger to esn_events
create or replace trigger esn_events_set_updated_at
    before update on public.esn_events
    for each row execute function public.set_updated_at();
