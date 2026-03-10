import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

# ==============================
# FILES
# ==============================

COMPANY_FILE = "../structured/companies_financial_scenarios.csv"
SITE_FILE = "site_visit_structured.csv"

OUTPUT_FILE = "site_visit_cleaned.csv"

# ==============================
# ENUM DEFINITIONS
# ==============================

OBSERVATION_CATEGORIES = {
    "capacity_utilization": [
        "Below 50%",
        "50-70%",
        "70-90%",
        "Near Full Capacity"
    ],
    "inventory_condition": [
        "Inventory well organised",
        "Inventory mismatch observed",
        "Excess stock",
        "Low inventory levels"
    ],
    "workforce_headcount": [
        "Adequate workforce",
        "Minor shortage",
        "Contract workers dominant",
        "High employee turnover"
    ],
    "machinery_condition": [
        "Machines well maintained",
        "Minor maintenance required",
        "Outdated machinery",
        "New machines installed"
    ],
    "housekeeping_safety": [
        "Clean and compliant",
        "Minor safety issues",
        "Poor housekeeping",
        "Safety equipment missing"
    ],
    "infrastructure_quality": [
        "Modern infrastructure",
        "Average infrastructure",
        "Aging facilities",
        "Expansion under progress"
    ]
}

RISK_DIRECTIONS = ["positive","neutral","negative","critical_negative"]

C_CATEGORIES = ["Capacity","Character","Capital","Collateral","Conditions"]

NOTES = [
    "Production operations were active during the visit.",
    "Workforce presence consistent with operational scale.",
    "Inventory levels appeared consistent with reported output.",
    "Minor maintenance activities observed on some machinery.",
    "Facility operations appear stable with normal activity.",
    "Production lines running during inspection."
]

# ==============================
# LOAD DATA
# ==============================

companies = pd.read_csv(COMPANY_FILE)
site = pd.read_csv(SITE_FILE)

# Strip whitespace from column names
companies.columns = companies.columns.str.strip()
site.columns = site.columns.str.strip()

companies["DATE OF LISTING"] = pd.to_datetime(companies["DATE OF LISTING"], format="%d-%b-%Y")

# ==============================
# HELPERS
# ==============================

def generate_visit_date(listing_date):

    start = listing_date + timedelta(days=30)
    end = datetime.today()

    delta = (end - start).days

    if delta <= 0:
        return datetime.today().date()

    random_days = random.randint(0, delta)

    return (start + timedelta(days=random_days)).date()


def generate_score(risk):

    if risk == "positive":
        return round(random.uniform(0.3,1.5),2)

    if risk == "neutral":
        return 0

    if risk == "negative":
        return round(random.uniform(-1.5,-0.3),2)

    if risk == "critical_negative":
        return round(random.uniform(-2,-1.5),2)


# ==============================
# PROCESS ROWS
# ==============================

clean_rows = []

for i,row in site.iterrows():

    # random company mapping
    comp = companies.sample(1).iloc[0]

    company_id = comp["company_id"]
    case_id = comp["case_id"]

    listing_date = comp["DATE OF LISTING"]

    # visit date generation
    visit_date = generate_visit_date(listing_date)

    # observation category
    category = row["observation_category"]

    if category not in OBSERVATION_CATEGORIES:
        category = random.choice(list(OBSERVATION_CATEGORIES.keys()))

    # dropdown
    dropdown = random.choice(OBSERVATION_CATEGORIES[category])

    # risk
    risk = random.choice(RISK_DIRECTIONS)

    # score
    score = generate_score(risk)

    # notes
    notes = random.choice(NOTES)

    # verification
    verification = random.choice(["pending","verified_by_manager","requires_reinspection"])

    # visit_id validation
    try:
        uuid.UUID(str(row["visit_id"]))
        visit_id = row["visit_id"]
    except:
        visit_id = str(uuid.uuid4())

    clean_rows.append({
        "visit_id": visit_id,
        "company_id": company_id,
        "case_id": case_id,
        "visit_date": visit_date,
        "observation_category": category,
        "observation_dropdown_selection": dropdown,
        "additional_notes": notes,
        "risk_impact_direction": risk,
        "linked_to_c_category": random.choice(C_CATEGORIES),
        "score_adjustment_points": score,
        "verification_status": verification
    })

# ==============================
# SAVE OUTPUT
# ==============================

clean_df = pd.DataFrame(clean_rows)

clean_df.to_csv(OUTPUT_FILE,index=False)

print("===================================")
print("Site visit dataset repaired")
print("Rows processed:", len(clean_df))
print("Output:", OUTPUT_FILE)
print("===================================")