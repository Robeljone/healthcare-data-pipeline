from dagster import job, op, schedule
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime

PROCESSED_DIR = Path("data/processed")
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "healthcare"

# -----------------------------
# Step 1: Load latest CSV
# -----------------------------
@op
def load_latest_csv():
    files = sorted(PROCESSED_DIR.glob("patients_clean_*.csv"), reverse=True)
    if not files:
        raise FileNotFoundError("No processed CSV files found.")
    df = pd.read_csv(files[0])
    return df

# -----------------------------
# Step 2: Transform / clean data
# -----------------------------
@op
def transform_data(df: pd.DataFrame):
    # Convert birthDate
    df['birth_date'] = [
        x.to_pydatetime() if pd.notnull(x) else None
        for x in pd.to_datetime(df['birthDate'], errors='coerce')
    ]
    df.drop(columns=['birthDate'], inplace=True)
    df = df.where(pd.notnull(df), None)
    df['ingestion_ts'] = datetime.utcnow()
    return df

# -----------------------------
# Step 3: Load into PostgreSQL
# -----------------------------
@op
def load_postgres(df: pd.DataFrame):
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    from psycopg2.extras import execute_batch
    insert_sql = """
    INSERT INTO patients (id, first_name, last_name, gender, birth_date, city, state, country, ingestion_ts)
    VALUES (%(id)s, %(first_name)s, %(last_name)s, %(gender)s, %(birth_date)s, %(city)s, %(state)s, %(country)s, %(ingestion_ts)s)
    ON CONFLICT (id) DO UPDATE
    SET first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        gender = EXCLUDED.gender,
        birth_date = EXCLUDED.birth_date,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        country = EXCLUDED.country,
        ingestion_ts = EXCLUDED.ingestion_ts;
    """

    conn = engine.raw_connection()
    try:
        cursor = conn.cursor()
        execute_batch(cursor, insert_sql, df.to_dict(orient='records'))
        conn.commit()
    finally:
        conn.close()
    print(f"Loaded {len(df)} records into PostgreSQL")

# -----------------------------
# Define the Job
# -----------------------------
@job
def patient_etl_job():
    df = load_latest_csv()
    df_transformed = transform_data(df)
    load_postgres(df_transformed)
    
@schedule(cron_schedule="0 * * * *", job=patient_etl_job, execution_timezone="UTC")
def hourly_patient_etl():
    """Runs every hour"""
    return {}