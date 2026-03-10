import json
import csv
import uuid
from datetime import date

INPUT = "data/processed/observations/raw_observations.json"
OUTPUT = "site_visit_structured.csv"


capacity_mapping = {
    "<30": ("Operating at <30%", -1.8),
    "30-50": ("30-50%", -1.0),
    "50-70": ("50-70%", -0.4),
    "70-90": ("70-90%", 0.3),
    ">90": (">90%", 0.6)
}

structured_records = []

with open(INPUT) as f:
    data = json.load(f)


for item in data:

    record = {

        "visit_id": str(uuid.uuid4()),
        "company_id": "unknown",
        "case_id": "auto_generated",

        "visit_date": str(date.today()),

        "observation_category": item["category"],

        "observation_dropdown_selection": "Needs Review",

        "additional_notes": item["text"],

        "risk_impact_direction": "negative",

        "linked_to_c_category": "Capacity",

        "score_adjustment_points": -0.5,

        "verification_status": "pending"
    }

    structured_records.append(record)


# Write to CSV file
with open(OUTPUT, "w", newline='', encoding='utf-8') as f:
    if structured_records:
        fieldnames = structured_records[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(structured_records)