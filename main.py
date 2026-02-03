from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import pandas as pd

app = FastAPI()

# --- DATABASE SETUP ---
# I filled this in based on your ingest_data.py file
db_user = 'postgres'
db_password = 'Qwertyuiop12$$' 
db_host = 'localhost'
db_port = '5432'
db_name = 'macro_db'

# Connect to the database
connection_str = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_str)

@app.get("/")
def read_root():
    return {"status": "online", "message": "Macro Data API is connected to Postgres!"}

# Endpoint 1: Get list of all available series
@app.get("/series")
def get_series_list():
    try:
        with engine.connect() as conn:
            # We fetch the metadata from the registry table
            query = text("SELECT slug, title, frequency, units FROM series_registry")
            result = conn.execute(query)
            # Convert to a list of dictionaries
            series = [row._asdict() for row in result]
            return {"data": series}
    except Exception as e:
        return {"error": str(e)}

# Endpoint 2: Get data for a specific series (e.g., /series/real_gdp)
@app.get("/series/{slug}")
def get_series_data(slug: str):
    try:
        query = text(f"""
            SELECT date, value 
            FROM observations 
            WHERE series_slug = '{slug}' 
            ORDER BY date ASC
        """)
        
        # Use Pandas to make it easy
        df = pd.read_sql(query, engine)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Series not found")

        # Convert date to string so it displays nicely in JSON
        df['date'] = df['date'].astype(str)
        
        return {
            "slug": slug,
            "count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        return {"error": str(e)}