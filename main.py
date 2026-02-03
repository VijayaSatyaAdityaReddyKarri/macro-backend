import os
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI()

# --- CORS SETUP ---
# This allows your Vercel frontend to talk to your Render backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- DATABASE SETUP ---
# 1. Try to get the DATABASE_URL from Render's environment variables
# 2. If it doesn't exist (like on your laptop), it defaults to your local setup
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # This is your local fallback
    DATABASE_URL = "postgresql://postgres:Qwertyuiop12$$@localhost:5432/macro_db"

# FIX: Render/SQLAlchemy compatibility fix
# Render provides 'postgres://', but SQLAlchemy requires 'postgresql://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)

@app.get("/")
def read_root():
    return {"status": "online", "message": "Macro Data API is connected!"}

# Endpoint 1: Get list of all available series
@app.get("/series")
def get_series_list():
    try:
        with engine.connect() as conn:
            query = text("SELECT slug, title, frequency, units FROM series_registry")
            result = conn.execute(query)
            series = [row._asdict() for row in result]
            return {"data": series}
    except Exception as e:
        return {"error": str(e)}

# Endpoint 2: Get data for a specific series
@app.get("/series/{slug}")
def get_series_data(slug: str):
    try:
        query = text("SELECT date, value FROM observations WHERE series_slug = :slug ORDER BY date ASC")
        
        # Using a dictionary for params is safer and prevents SQL injection
        df = pd.read_sql(query, engine, params={"slug": slug})
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Series not found")

        df['date'] = df['date'].astype(str)
        
        return {
            "slug": slug,
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"error": str(e)}