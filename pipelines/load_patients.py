import pandas as pd
from sqlalchemy import create_engine, text
from pathlib import Path
from datetime import datetime

# -----------------------------
# Paths & DB Config
# -----------------------------
PROCESSED_DIR = Path("data/processed")
DB_USER = "admin"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "healthcare"

# -----------------------------
# Get Latest Processed CSV
# -----------------------------
def get_latest_csv():
    files = sorted(PROCESSED_DIR.glob("patients_clean_*.csv"), reverse=True)
    if not files:
        raise FileNotFoundError("No processed CSV files found.")
    return files[0]

# -----------------------------
# Load CSV to PostgreSQL with UPSERT
# -----------------------------
def load_to_postgres(csv_file: Path):
    # Load CSV
    df = pd.read_csv(csv_file)

    # Ensure birth_date column matches DB column
    if 'birthDate' in df.columns:
        df['birth_date'] = pd.to_datetime(df['birthDate'], errors='coerce')
        df['birth_date'] = df['birth_date'].where(df['birth_date'].notna(), None)
        df.drop(columns=['birthDate'], inplace=True)

    # Add ingestion timestamp
    df['ingestion_ts'] = datetime.utcnow()

    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    # UPSERT each row to avoid duplicates
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = text("""
                INSERT INTO patients (id, first_name, last_name, gender, birth_date, city, state, country, ingestion_ts)
                VALUES (:id, :first_name, :last_name, :gender, :birth_date, :city, :state, :country, :ingestion_ts)
                ON CONFLICT (id) DO UPDATE
                SET first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    gender = EXCLUDED.gender,
                    birth_date = EXCLUDED.birth_date,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    ingestion_ts = EXCLUDED.ingestion_ts;
            """)
            conn.execute(stmt, **row.to_dict())

    print(f"Loaded {len(df)} records into PostgreSQL from {csv_file.name}")

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    csv_file = get_latest_csv()
    load_to_postgres(csv_file)
