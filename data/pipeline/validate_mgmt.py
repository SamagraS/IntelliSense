import pandas as pd
import uuid
import random
from datetime import datetime, timedelta

# ==============================
# FILE PATHS
# ==============================

COMPANY_FILE = "../structured/companies_financial_scenarios.csv"
INTERVIEW_FILE = "management_interview_notes.csv"

OUTPUT_FILE = "management_interview_cleaned.csv"

# ==============================
# ENUM VALUES
# ==============================

TOPIC_CATEGORIES = {
    "governance_concern": [
        "Board independence discussed",
        "Internal governance controls explained",
        "Audit committee oversight reviewed"
    ],
    "revenue_trend_explanation": [
        "Management explained recent revenue fluctuations",
        "Growth attributed to expansion into new markets",
        "Revenue volatility explained due to commodity cycles"
    ],
    "strategic_direction": [
        "Expansion strategy outlined",
        "Long term growth roadmap explained",
        "Management emphasised operational efficiency initiatives"
    ],
    "debt_management": [
        "Debt reduction plan explained",
        "Management confident about servicing obligations",
        "Refinancing strategy discussed"
    ],
    "working_capital_management": [
        "Receivables cycle explained",
        "Inventory optimization initiatives discussed",
        "Working capital discipline highlighted"
    ]
}

MANAGEMENT_NAMES = [
    "Rajesh Gupta",
    "Anil Mehta",
    "Suresh Iyer",
    "Vivek Sharma",
    "Arjun Kapoor",
    "Ramesh Nair"
]

DESIGNATIONS = [
    "Managing Director",
    "Chief Financial Officer",
    "Chief Executive Officer",
    "Director Operations",
    "Executive Director"
]

CREDIT_OFFICERS = [
    "Rahul Sharma",
    "Neha Kapoor",
    "Vikram Iyer",
    "Anita Desai",
    "Sanjay Mehta"
]

MANAGEMENT_ASSESSMENT = [
    "confident_and_consistent",
    "evasive_or_inconsistent",
    "overly_optimistic",
    "transparent_and_detailed"
]

C_CATEGORIES = ["Character","Capacity","Capital","Conditions"]

# ==============================
# NOTES
# ==============================

NOTES = [
    "Management provided clear explanations regarding operational performance.",
    "Responses from management appeared transparent and aligned with financial disclosures.",
    "Management acknowledged operational challenges and outlined mitigation plans.",
    "Management discussed expansion plans and investment priorities.",
    "Leadership expressed confidence in long-term demand outlook."
]

# ==============================
# LOAD DATA
# ==============================

companies = pd.read_csv(COMPANY_FILE)
interviews = pd.read_csv(INTERVIEW_FILE)

# Strip whitespace from column names
companies.columns = companies.columns.str.strip()
interviews.columns = interviews.columns.str.strip()

companies["DATE OF LISTING"] = pd.to_datetime(
    companies["DATE OF LISTING"],
    format="%d-%b-%Y"
)

# ==============================
# HELPER FUNCTIONS
# ==============================

def generate_interview_date(listing_date):

    start = listing_date + timedelta(days=60)
    end = datetime.today()

    delta = (end - start).days

    if delta <= 0:
        return datetime.today().date()

    return (start + timedelta(days=random.randint(0, delta))).date()


def generate_score(assessment):

    if assessment == "confident_and_consistent":
        return round(random.uniform(0.3,1.0),2)

    if assessment == "transparent_and_detailed":
        return round(random.uniform(0.5,1.5),2)

    if assessment == "overly_optimistic":
        return round(random.uniform(-0.5,0.2),2)

    if assessment == "evasive_or_inconsistent":
        return round(random.uniform(-1.5,-0.3),2)

    return 0


# ==============================
# CLEAN DATA
# ==============================

clean_rows = []

for _,row in interviews.iterrows():

    comp = companies.sample(1).iloc[0]

    company_id = comp["company_id"]
    case_id = comp["case_id"]

    interview_date = generate_interview_date(comp["DATE OF LISTING"])

    topic = random.choice(list(TOPIC_CATEGORIES.keys()))

    note = random.choice(TOPIC_CATEGORIES[topic])

    assessment = random.choice(MANAGEMENT_ASSESSMENT)

    score = generate_score(assessment)

    verification = random.choice(
        ["pending","verified_by_manager","requires_reinspection"]
    )

    evidence = ""

    if verification == "verified_by_manager":
        evidence = "Interview recording and meeting minutes verified"

    # validate UUID
    try:
        uuid.UUID(str(row["interview_id"]))
        interview_id = row["interview_id"]
    except:
        interview_id = str(uuid.uuid4())

    clean_rows.append({

        "interview_id": interview_id,
        "company_id": company_id,
        "case_id": case_id,
        "interview_date": interview_date,
        "interviewer_credit_officer": random.choice(CREDIT_OFFICERS),
        "interviewee_name": random.choice(MANAGEMENT_NAMES),
        "interviewee_designation": random.choice(DESIGNATIONS),
        "interview_topic_category": topic,
        "note_detail_text": note,
        "management_credibility_assessment": assessment,
        "linked_to_c_category": random.choice(C_CATEGORIES),
        "score_adjustment_points": score,
        "requires_document_verification": random.choice([True,False]),
        "verification_status": verification,
        "verification_evidence": evidence,
        "timestamp_created": datetime.now()

    })

# ==============================
# SAVE OUTPUT
# ==============================

clean_df = pd.DataFrame(clean_rows)

clean_df.to_csv(OUTPUT_FILE,index=False)

print("====================================")
print("Management interview dataset cleaned")
print("Rows:", len(clean_df))
print("Saved:", OUTPUT_FILE)
print("====================================")