import os
from datetime import datetime
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import Client, create_client

load_dotenv()


def _parse_iso_timestamp(value: str) -> datetime:
    """Parse DB/ISO timestamps; normalize trailing Z for Python 3.9 fromisoformat."""
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(normalized)


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL and SUPABASE_KEY environment variables are required.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(
    title="ESN Activities API",
    description="API for ESN countries, sections, and activities.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root() -> Dict[str, str]:
    return {
        "message": "Welcome to the ESN Activities API! Visit /docs for documentation.",
    }


@app.get("/api/v1/health")
def get_health() -> Dict[str, Any]:
    """
    Check the API health and return the most recent sync time across all scraped tables.
    """
    try:
        max_dt: Optional[datetime] = None

        for table in ("esn_countries", "esn_sections", "esn_events"):
            response = (
                supabase.table(table)
                .select("last_scraped_at")
                .order("last_scraped_at", desc=True)
                .limit(1)
                .execute()
            )
            data = response.data
            if data and "last_scraped_at" in data[0] and data[0]["last_scraped_at"]:
                ts_raw = data[0]["last_scraped_at"]
                if isinstance(ts_raw, str):
                    ts_dt = _parse_iso_timestamp(ts_raw)
                    if max_dt is None or ts_dt > max_dt:
                        max_dt = ts_dt

        return {
            "status": "ok",
            "last_sync_time": max_dt.isoformat() if max_dt is not None else None,
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Database unavailable: {e!s}",
        ) from e


@app.get("/api/v1/countries")
def get_countries() -> Dict[str, Any]:
    """Retrieve all ESN national organisations."""
    try:
        response = (
            supabase.table("esn_countries")
            .select("*")
            .order("country_name")
            .execute()
        )
        return {
            "status": "success",
            "count": len(response.data),
            "data": response.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/countries/{country_code}/sections")
def get_country_sections(country_code: str) -> Dict[str, Any]:
    """
    Retrieve all local sections belonging to a specific country code.
    Raises 404 if the country does not exist.
    """
    code_upper = country_code.upper()
    try:
        # Check if country exists
        country_resp = (
            supabase.table("esn_countries")
            .select("country_code")
            .eq("country_code", code_upper)
            .execute()
        )
        if not country_resp.data:
            raise HTTPException(status_code=404, detail=f"Country '{code_upper}' not found.")

        # Fetch sections
        sections_resp = (
            supabase.table("esn_sections")
            .select("*")
            .eq("country_code", code_upper)
            .order("section_name")
            .execute()
        )
        return {
            "status": "success",
            "count": len(sections_resp.data),
            "data": sections_resp.data,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sections")
def get_sections(
    city: Optional[str] = Query(None, description="Filter sections by city name (case-insensitive partial match)."),
    limit: int = Query(50, description="Max number of sections to return.", ge=1, le=500),
) -> Dict[str, Any]:
    """Retrieve ESN local sections with optional filtering."""
    try:
        query = supabase.table("esn_sections").select("*")
        if city:
            query = query.ilike("city", f"%{city}%")
        
        response = query.order("section_name").limit(limit).execute()
        return {
            "status": "success",
            "count": len(response.data),
            "data": response.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/events")
def get_events(
    is_upcoming: Optional[bool] = Query(None, description="Filter by strictly upcoming/past events."),
    organizer_section: Optional[str] = Query(None, description="Exact match for the organizer section name."),
    limit: int = Query(50, description="Max number of events to return.", ge=1, le=100),
    skip: int = Query(0, description="Number of events to skip (pagination).", ge=0),
) -> Dict[str, Any]:
    """Retrieve the global feed of ESN activities with filters and pagination."""
    try:
        query = supabase.table("esn_events").select("*")
        
        if is_upcoming is not None:
            query = query.eq("is_upcoming", is_upcoming)
        if organizer_section is not None:
            query = query.eq("organizer_section", organizer_section)

        response = (
            query.order("event_start_date", desc=True)
            .range(skip, skip + limit - 1)
            .execute()
        )
        
        return {
            "status": "success",
            "count": len(response.data),
            "skip": skip,
            "limit": limit,
            "data": response.data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    debug_mode = os.environ.get("DEBUG", "false").lower() == "true"
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=debug_mode)
