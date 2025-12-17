import requests
import json
from datetime import datetime
from pathlib import Path

BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCE = "Patient"
COUNT = 50

def extract_patients():
    url = f"{BASE_URL}/{RESOURCE}?_count={COUNT}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    return response.json()

def save_raw_data(data: dict):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"patients_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Raw data saved to {file_path}")

if __name__ == "__main__":
    bundle = extract_patients()
    save_raw_data(bundle)
