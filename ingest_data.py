import pandas as pd
from sqlalchemy import create_engine, text
import requests
import io

# --- 1. SETUP DATABASE CONNECTION ---
db_user = 'postgres'
db_password = 'Qwertyuiop12$$'  
db_host = 'localhost'
db_port = '5432'
db_name = 'macro_db'

connection_str = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_str)

series_map = [
    {
        "slug": "real_gdp",
        "provider": "fred",
        "provider_code": "GDPC1",
        "title": "Real Gross Domestic Product",
        "frequency": "Quarterly",
        "units": "Billions of Chained 2012 Dollars",
        "default_transform": "yoy",
        "source_url": "https://fred.stlouisfed.org/series/GDPC1"
    },
    {
        "slug": "unemployment_rate",
        "provider": "fred",
        "provider_code": "UNRATE",
        "title": "Unemployment Rate",
        "frequency": "Monthly",
        "units": "Percent",
        "default_transform": "none",
        "source_url": "https://fred.stlouisfed.org/series/UNRATE"
    },
    {
        "slug": "cpi_headline",
        "provider": "fred",
        "provider_code": "CPIAUCSL",
        "title": "Consumer Price Index (All Urban)",
        "frequency": "Monthly",
        "units": "Index 1982-1984=100",
        "default_transform": "yoy",
        "source_url": "https://fred.stlouisfed.org/series/CPIAUCSL"
    }
]

def ingest_data():
    print("🚀 Starting Data Ingestion (V3)...")

    for item in series_map:
        slug = item['slug']
        fred_id = item['provider_code']
        print(f"\nProcessing: {item['title']} ({slug})...")

        try:
            # --- 1. CLEANUP (Child -> Parent) ---
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM observations WHERE series_slug = '{slug}'"))
                conn.execute(text(f"DELETE FROM series_registry WHERE slug = '{slug}'"))
                conn.commit()

            # --- 2. INSERT METADATA ---
            df_meta = pd.DataFrame([item])
            df_meta.to_sql('series_registry', engine, if_exists='append', index=False)
            print(f"   ✅ Metadata registered.")

            # --- 3. FETCH & INSERT DATA ---
            url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={fred_id}"
            response = requests.get(url)
            
            # Check for empty response
            if "observation_date" not in response.text:
                 print(f"   ❌ Error: FRED returned unexpected format.\n   Preview: {response.text[:100]}")
                 continue

            df_data = pd.read_csv(io.StringIO(response.text))

            # --- THE FIX: RENAME 'observation_date' to 'date' ---
            df_data.rename(columns={'observation_date': 'date', fred_id: 'value'}, inplace=True)
            
            df_data['series_slug'] = slug
            df_data['date'] = pd.to_datetime(df_data['date'])
            df_data['value'] = pd.to_numeric(df_data['value'], errors='coerce')
            df_data.dropna(inplace=True)

            # Load
            df_data.to_sql('observations', engine, if_exists='append', index=False)
            print(f"   ✅ Loaded {len(df_data)} rows of data.")

        except Exception as e:
            print(f"   ❌ Error: {e}")

    print("\n🎉 Phase 1 Complete: Database is FULL!")

if __name__ == "__main__":
    ingest_data()