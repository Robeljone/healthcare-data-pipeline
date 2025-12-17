import json
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def load_latest_raw():
    files = sorted(RAW_DIR.glob("patients_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError("No raw patient files found.")
    with open(files[0], "r", encoding="utf-8") as f:
        return json.load(f)

def flatten_patient(patient):
    """
    Extract key fields from FHIR Patient resource.
    Handles missing fields gracefully.
    """
    resource = patient.get("resource", {})
    
    return {
        "id": resource.get("id"),
        "first_name": resource.get("name", [{}])[0].get("given", [None])[0],
        "last_name": resource.get("name", [{}])[0].get("family"),
        "gender": resource.get("gender"),
        "birthDate": resource.get("birthDate"),
        "city": resource.get("address", [{}])[0].get("city"),
        "state": resource.get("address", [{}])[0].get("state"),
        "country": resource.get("address", [{}])[0].get("country"),
    }

def transform_bundle(bundle):
    entries = bundle.get("entry", [])
    return pd.DataFrame([flatten_patient(e) for e in entries])

def save_transformed(df: pd.DataFrame):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    file_path = PROCESSED_DIR / f"patients_clean_{timestamp}.csv"
    df.to_csv(file_path, index=False)
    print(f"Transformed data saved to {file_path}")

if __name__ == "__main__":
    bundle = load_latest_raw()
    df = transform_bundle(bundle)
    save_transformed(df)
