import os

from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import Client, create_client

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL ve SUPABASE_KEY ortam değişkenleri gerekli.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(
    title="ESN Activities API",
    description="ESN şubelerinin etkinliklerini sunan API",
    version="1.0.0",
)

# Wildcard origin ile credentials birlikte kullanılamaz; allow_credentials=False.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {
        "mesaj": "ESN Activities API'a Hoş Geldin! Dokümantasyon için /docs adresine git.",
    }


@app.get("/api/v1/activities")
def get_activities(
    limit: int = Query(50, description="Kaç veri gelsin?", le=100, ge=1),
    offset: int = Query(0, description="Kaçıncı veriden başlasın?", ge=0),
):
    """
    Veritabanındaki etkinlikleri sayfalar halinde getirir.
    Maksimum 100 veri çekilebilir.
    """
    try:
        response = (
            supabase.table("esn_events")
            .select("*")
            .order("event_date->>start", desc=True)
            .range(offset, offset + limit - 1)
            .execute()
        )
        return {
            "status": "success",
            "count": len(response.data),
            "data": response.data,
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
