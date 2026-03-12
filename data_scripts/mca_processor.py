"""
mca_processor.py
================
Section 3.3 — MCA Filings Data Processor

Input:  Indian Companies Registration Data CSV (Kaggle dataset)
        Columns: CORPORATE_IDENTIFICATION_NUMBER, COMPANY_NAME, COMPANY_STATUS,
                 COMPANY_CLASS, COMPANY_CATEGORY, COMPANY_SUB_CATEGORY,
                 DATE_OF_REGISTRATION, REGISTERED_STATE, AUTHORIZED_CAP,
                 PAIDUP_CAPITAL, INDUSTRIAL_CLASS, PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN,
                 REGISTERED_OFFICE_ADDRESS, REGISTRAR_OF_COMPANIES, EMAIL_ADDR,
                 LATEST_YEAR_ANNUAL_RETURN, LATEST_YEAR_FINANCIAL_STATEMENT

Output: Four CSVs matching Section 3.3 schema exactly:
    mca_company_master.csv        ← from real Kaggle data
    mca_directors.csv             ← synthetic, cross-pollinated across sectors
    mca_charges_registered.csv    ← synthetic, sized to real capital figures
    director_company_network.csv  ← derived from directors (real graph edges)

Run:
    python mca_processor.py --input company_data.csv
    python mca_processor.py --input company_data.csv --sample 5000
    python mca_processor.py --input company_data.csv --filter-state Maharashtra
    python mca_processor.py --input company_data.csv --dry-run

Strategy:
    - mca_company_master:      direct column mapping from Kaggle
    - mca_directors:           3-5 synthetic directors per company, cross-pollinated
                               so directors appear in multiple companies (graph edges)
    - mca_charges_registered:  synthetic, sized proportionally to AUTHORIZED_CAP
    - director_company_network: derived — wherever director appears in 2+ companies,
                               edge created. Risk flags from real company_status.
"""

import csv
import json
import re
import random
import hashlib
import argparse
from datetime import datetime, date, timezone, timedelta
from collections import defaultdict
from typing import Optional

random.seed(42)

# ─────────────────────────────────────────────
# OUTPUT FILE PATHS
# ─────────────────────────────────────────────

OUT_COMPANY_MASTER  = "mca_company_master.csv"
OUT_DIRECTORS       = "mca_directors.csv"
OUT_CHARGES         = "mca_charges_registered.csv"
OUT_NETWORK         = "director_company_network.csv"

# ─────────────────────────────────────────────
# COLUMN SCHEMAS (exact Section 3.3 spec)
# ─────────────────────────────────────────────

COMPANY_MASTER_COLS = [
    "company_cin", "company_name", "company_status", "date_of_incorporation",
    "company_category", "authorized_capital_inr", "paid_up_capital_inr",
    "registered_office_address", "last_agm_date", "last_balance_sheet_date",
    "data_fetch_timestamp",
]

DIRECTORS_COLS = [
    "director_din", "director_name", "company_cin", "appointment_date",
    "resignation_date", "designation", "din_status",
]

CHARGES_COLS = [
    "charge_id", "company_cin", "charge_holder_name", "charge_amount_inr",
    "charge_creation_date", "charge_modification_date", "charge_status",
    "asset_description", "charge_type",
]

NETWORK_COLS = [
    "director_din", "company_cin", "connection_type",
    "is_borrower_company", "other_company_risk_flags",
]

# ─────────────────────────────────────────────
# KAGGLE → SCHEMA COLUMN MAP
# ─────────────────────────────────────────────

KAGGLE_TO_SCHEMA = {
    "CORPORATE_IDENTIFICATION_NUMBER": "company_cin",
    "COMPANY_NAME":                    "company_name",
    "COMPANY_STATUS":                  "company_status",
    "COMPANY_CATEGORY":                "company_category",
    "DATE_OF_REGISTRATION":            "date_of_incorporation",
    "AUTHORIZED_CAP":                  "authorized_capital_inr",
    "PAIDUP_CAPITAL":                  "paid_up_capital_inr",
    "REGISTERED_OFFICE_ADDRESS":       "registered_office_address",
    "LATEST_YEAR_ANNUAL_RETURN":       "last_agm_date",
    "LATEST_YEAR_FINANCIAL_STATEMENT": "last_balance_sheet_date",
}

# ─────────────────────────────────────────────
# STATUS NORMALISER
# Kaggle uses short codes: ACTV, DISS, STRK, etc.
# ─────────────────────────────────────────────

STATUS_MAP = {
    "ACTV": "active",
    "ACTIVE": "active",
    "DISS": "dissolved",
    "DISSOLVED": "dissolved",
    "STRK": "strike_off",
    "STRIKE OFF": "strike_off",
    "STRIKE_OFF": "strike_off",
    "AMLG": "amalgamated",
    "AMALGAMATED": "amalgamated",
    "LIQD": "under_liquidation",
    "UNDER LIQUIDATION": "under_liquidation",
    "WOUND UP": "dissolved",
    "CONVERTED": "amalgamated",
    "DORMANT": "strike_off",
}

def normalise_status(raw: str) -> str:
    if not raw:
        return "active"
    return STATUS_MAP.get(raw.strip().upper(), "active")

# ─────────────────────────────────────────────
# DATE NORMALISER
# ─────────────────────────────────────────────

def normalise_date(raw: str) -> Optional[str]:
    if not raw or str(raw).strip() in ("", "nan", "None", "0"):
        return None
    raw = str(raw).strip()
    for fmt in (
        "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d",
        "%d-%b-%Y", "%b %d, %Y", "%Y",
    ):
        try:
            return datetime.strptime(raw[:len(fmt)+2], fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Just year
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return None

# ─────────────────────────────────────────────
# CAPITAL CLEANER
# Kaggle has floats like 500000.0 or strings like "5,00,000"
# ─────────────────────────────────────────────

def clean_capital(val) -> Optional[float]:
    if val is None:
        return None
    val = str(val).strip().replace(",", "")
    val = re.sub(r"[^\d.]", "", val)
    try:
        f = float(val)
        return f if f > 0 else None
    except Exception:
        return None

# ─────────────────────────────────────────────
# SYNTHETIC DIRECTOR POOL
# Indian names pool for realistic director generation.
# Cross-pollination: directors from pool appear in multiple companies.
# ─────────────────────────────────────────────

FIRST_NAMES = [
    "Rajesh", "Suresh", "Mahesh", "Ramesh", "Dinesh", "Pradeep", "Vikram",
    "Amit", "Anil", "Sunil", "Ajay", "Vijay", "Sanjay", "Ravi", "Kiran",
    "Deepak", "Prakash", "Rakesh", "Mukesh", "Naresh", "Harish", "Girish",
    "Manish", "Yogesh", "Ganesh", "Nilesh", "Hitesh", "Rupesh", "Brijesh",
    "Ashish", "Satish", "Umesh", "Kamlesh", "Jignesh", "Bhavesh", "Chintan",
    "Priya", "Sunita", "Kavita", "Anita", "Geeta", "Seema", "Reena",
    "Meena", "Neeta", "Rekha", "Usha", "Asha", "Nisha", "Divya",
    "Pooja", "Swati", "Shruti", "Shilpa", "Sneha", "Sapna", "Ritu",
]

LAST_NAMES = [
    "Shah", "Patel", "Mehta", "Joshi", "Sharma", "Gupta", "Agarwal",
    "Verma", "Singh", "Kumar", "Yadav", "Mishra", "Pandey", "Tiwari",
    "Srivastava", "Chaudhary", "Rao", "Reddy", "Naidu", "Iyer",
    "Nair", "Menon", "Pillai", "Krishnan", "Murthy", "Rajan",
    "Bansal", "Goel", "Mittal", "Jain", "Khandelwal", "Bhatt",
    "Desai", "Trivedi", "Kapoor", "Malhotra", "Khanna", "Chopra",
    "Bose", "Das", "Ghosh", "Chatterjee", "Mukherjee", "Chakraborty",
]

DESIGNATIONS = [
    "Director", "Managing Director", "Whole Time Director",
    "Director", "Director", "Director",           # weighted towards plain Director
    "Chief Financial Officer", "Chief Executive Officer",
    "Additional Director", "Independent Director",
    "Nominee Director", "Executive Director",
]

BANKS_AND_LENDERS = [
    "State Bank of India", "Punjab National Bank", "Bank of Baroda",
    "Canara Bank", "Union Bank of India", "Bank of India",
    "HDFC Bank Limited", "ICICI Bank Limited", "Axis Bank Limited",
    "Kotak Mahindra Bank", "IndusInd Bank", "Yes Bank Limited",
    "IDFC First Bank", "Federal Bank", "South Indian Bank",
    "SIDBI", "NABARD", "EXIM Bank",
    "L&T Finance Limited", "Bajaj Finance Limited",
    "Tata Capital Financial Services",
]

CHARGE_TYPES = ["mortgage", "hypothecation", "hypothecation", "hypothecation", "pledge"]

ASSET_DESCRIPTIONS = [
    "Factory land and building at {state}",
    "Plant and machinery at {state} facility",
    "Book debts and receivables",
    "Stock in trade and inventory",
    "Fixed deposits and liquid investments",
    "Entire movable and immovable assets",
    "Land and building at registered office",
    "Plant, machinery and equipment",
]

# ─────────────────────────────────────────────
# DIRECTOR POOL BUILDER
# Pre-builds a shared pool of directors that get assigned
# across companies — this creates graph edges (cross-directorships)
# ─────────────────────────────────────────────

class DirectorPool:
    """
    Maintains a pool of synthetic directors.
    Each director has a real-format DIN and gets assigned to
    multiple companies so graph edges are created.

    Cross-pollination strategy:
      - 30% of directors are "portfolio directors" appearing in 2-5 companies
      - 70% are single-company directors
      - Within each sector, portfolio directors are shared more heavily
        (realistic: sector specialists sit on multiple boards)
    """

    def __init__(self, total_directors: int = 2000):
        self.pool       = {}       # din → {name, is_portfolio, sector_affinity}
        self.din_counter = 10000000  # MCA DINs start at 8 digits

        # Pre-generate director pool
        for _ in range(total_directors):
            din  = str(self.din_counter)
            name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
            self.pool[din] = {
                "din":              din,
                "name":             name,
                "is_portfolio":     random.random() < 0.30,
                "sector_affinity":  random.choice([
                    "Manufacturing", "NBFC", "Pharma", "IT",
                    "Retail", "Infrastructure", "Energy", "General"
                ]),
                "companies_assigned": 0,
                "max_companies":    random.randint(2, 5) if random.random() < 0.30 else 1,
            }
            self.din_counter += 1

        self._portfolio = [d for d in self.pool.values() if d["is_portfolio"]]
        self._single    = [d for d in self.pool.values() if not d["is_portfolio"]]

    def assign_directors(
        self,
        company_cin:  str,
        num_directors: int,
        sector:        str = "General",
    ) -> list[dict]:
        """
        Assign directors to a company.
        Mix of portfolio (shared) + single-company directors.
        Returns list of director assignment dicts.
        """
        assigned = []
        used_dins = set()

        # 1-2 portfolio directors per company (creates graph edges)
        portfolio_count = random.randint(1, min(2, num_directors))
        sector_portfolio = [
            d for d in self._portfolio
            if d["companies_assigned"] < d["max_companies"]
            and (d["sector_affinity"] == sector or d["sector_affinity"] == "General")
        ]
        random.shuffle(sector_portfolio)

        for d in sector_portfolio[:portfolio_count]:
            if d["din"] not in used_dins:
                assigned.append(d)
                used_dins.add(d["din"])
                d["companies_assigned"] += 1

        # Fill rest with single-company directors
        remaining = num_directors - len(assigned)
        single_available = [
            d for d in self._single
            if d["companies_assigned"] < d["max_companies"]
            and d["din"] not in used_dins
        ]
        random.shuffle(single_available)

        for d in single_available[:remaining]:
            assigned.append(d)
            used_dins.add(d["din"])
            d["companies_assigned"] += 1

        return assigned


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def make_charge_id(cin: str, holder: str, idx: int) -> str:
    key = f"{cin}_{holder}_{idx}"
    return "chg_" + hashlib.md5(key.encode()).hexdigest()[:12]

def random_past_date(years_ago_min: int = 1, years_ago_max: int = 10) -> str:
    days = random.randint(years_ago_min * 365, years_ago_max * 365)
    return (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")

def infer_sector(row: dict) -> str:
    """Infer sector from PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN or INDUSTRIAL_CLASS."""
    activity = (
        str(row.get("PRINCIPAL_BUSINESS_ACTIVITY_AS_PER_CIN", "") or "")
        + " "
        + str(row.get("INDUSTRIAL_CLASS", "") or "")
    ).lower()

    if any(k in activity for k in ["pharma", "drug", "medicine", "chemical"]):
        return "Pharma"
    if any(k in activity for k in ["finance", "nbfc", "banking", "insurance", "investment"]):
        return "NBFC"
    if any(k in activity for k in ["retail", "trade", "commerce", "wholesale"]):
        return "Retail"
    if any(k in activity for k in ["software", "it ", "information tech", "computer"]):
        return "IT"
    if any(k in activity for k in ["manufactur", "industrial", "mineral", "steel", "cement"]):
        return "Manufacturing"
    return "General"

def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────
# STEP 1: LOAD + MAP mca_company_master
# ─────────────────────────────────────────────

def load_company_master(
    filepath:     str,
    sample:       Optional[int] = None,
    filter_state: Optional[str] = None,
) -> list[dict]:
    """
    Read Kaggle CSV → map columns → return list of mca_company_master dicts.
    """
    print(f"\n── Step 1: Loading company master from {filepath} ─────────")
    records  = []
    skipped  = 0
    filtered = 0

    with open(filepath, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):

            # State filter
            if filter_state:
                state = str(row.get("REGISTERED_STATE", "")).strip()
                if filter_state.lower() not in state.lower():
                    filtered += 1
                    continue

            cin  = str(row.get("CORPORATE_IDENTIFICATION_NUMBER", "")).strip()
            name = str(row.get("COMPANY_NAME", "")).strip()

            # Skip rows with no CIN or name
            if not cin or not name or cin in ("", "nan"):
                skipped += 1
                continue

            # Derive last_balance_sheet_date from LATEST_YEAR_FINANCIAL_STATEMENT
            bs_raw  = str(row.get("LATEST_YEAR_FINANCIAL_STATEMENT", "") or "")
            agm_raw = str(row.get("LATEST_YEAR_ANNUAL_RETURN", "") or "")

            # Financial statement years like "2019" → "2019-03-31" (Indian FY end)
            def year_to_fy_end(yr: str) -> Optional[str]:
                yr = yr.strip()
                if re.match(r"^\d{4}$", yr):
                    return f"{yr}-03-31"
                return normalise_date(yr)

            record = {
                "company_cin":              cin,
                "company_name":             name,
                "company_status":           normalise_status(row.get("COMPANY_STATUS", "")),
                "date_of_incorporation":    normalise_date(row.get("DATE_OF_REGISTRATION", "")),
                "company_category":         str(row.get("COMPANY_CATEGORY", "") or
                                               row.get("COMPANY_CLASS", "") or
                                               "Private Limited").strip() or "Private Limited",
                "authorized_capital_inr":   clean_capital(row.get("AUTHORIZED_CAP")),
                "paid_up_capital_inr":      clean_capital(row.get("PAIDUP_CAPITAL")),
                "registered_office_address": str(row.get("REGISTERED_OFFICE_ADDRESS", "") or "").strip(),
                "last_agm_date":            year_to_fy_end(agm_raw),
                "last_balance_sheet_date":  year_to_fy_end(bs_raw),
                "data_fetch_timestamp":     now_ts(),
                # Keep extras for downstream use (not in output CSV)
                "_sector":                  infer_sector(row),
                "_state":                   str(row.get("REGISTERED_STATE", "")).strip(),
            }
            records.append(record)

            if sample and len(records) >= sample:
                print(f"  Sample cap {sample} reached at row {i+1}")
                break

    print(f"  Total rows read:     {i+1:>8,}")
    print(f"  State filtered out:  {filtered:>8,}")
    print(f"  Skipped (no CIN):    {skipped:>8,}")
    print(f"  Loaded:              {len(records):>8,}")
    return records

# ─────────────────────────────────────────────
# STEP 2: GENERATE mca_directors
# ─────────────────────────────────────────────

def generate_directors(
    companies:   list[dict],
    pool:        DirectorPool,
) -> list[dict]:
    """
    Assign synthetic directors to each company.
    Number of directors scales with company size (paid-up capital).
    """
    print(f"\n── Step 2: Generating directors ───────────────────────────")
    records = []

    for co in companies:
        cin    = co["company_cin"]
        cap    = co.get("paid_up_capital_inr") or 0
        sector = co.get("_sector", "General")
        incorp = co.get("date_of_incorporation")

        # Number of directors: 2 minimum, scales with capital
        if cap >= 1_000_000_000:    # 100 Cr+
            num_dirs = random.randint(5, 8)
        elif cap >= 100_000_000:    # 10 Cr+
            num_dirs = random.randint(3, 6)
        elif cap >= 10_000_000:     # 1 Cr+
            num_dirs = random.randint(2, 4)
        else:
            num_dirs = random.randint(2, 3)

        assigned = pool.assign_directors(cin, num_dirs, sector)

        for idx, d in enumerate(assigned):
            # Appointment date: after incorporation, random offset
            appt_date = None
            if incorp:
                try:
                    incorp_dt = datetime.strptime(incorp, "%Y-%m-%d")
                    offset    = timedelta(days=random.randint(0, 365 * 3))
                    appt_date = (incorp_dt + offset).strftime("%Y-%m-%d")
                    # Don't go into future
                    if appt_date > date.today().strftime("%Y-%m-%d"):
                        appt_date = incorp
                except Exception:
                    appt_date = random_past_date(3, 15)
            else:
                appt_date = random_past_date(3, 15)

            designation = DESIGNATIONS[0] if idx == 0 else random.choice(DESIGNATIONS)
            if idx == 0:
                designation = random.choice(["Managing Director", "Whole Time Director", "Director"])

            records.append({
                "director_din":     d["din"],
                "director_name":    d["name"],
                "company_cin":      cin,
                "appointment_date": appt_date,
                "resignation_date": None,
                "designation":      designation,
                "din_status":       "active",
            })

    print(f"  Directors generated: {len(records):>8,}")
    print(f"  Unique DINs:         {len(set(r['director_din'] for r in records)):>8,}")
    cross = sum(1 for r in records
                if pool.pool[r["director_din"]]["companies_assigned"] > 1)
    print(f"  Cross-directorships: {cross:>8,}  (graph edges)")
    return records

# ─────────────────────────────────────────────
# STEP 3: GENERATE mca_charges_registered
# ─────────────────────────────────────────────

def generate_charges(companies: list[dict]) -> list[dict]:
    """
    Generate synthetic charges sized proportionally to authorized capital.
    Only active companies with meaningful capital get charges
    (distressed/dissolved companies may have satisfied charges).
    """
    print(f"\n── Step 3: Generating charges ──────────────────────────────")
    records = []

    for co in companies:
        cin    = co["company_cin"]
        status = co["company_status"]
        cap    = co.get("authorized_capital_inr") or 0
        state  = co.get("_state", "India")

        # Skip tiny companies (< 1 lakh authorized cap) — unlikely to have bank charges
        if cap < 100_000:
            continue

        # Number of charges depends on company size
        if cap >= 1_000_000_000:       # 100 Cr+
            num_charges = random.randint(2, 5)
        elif cap >= 100_000_000:       # 10 Cr+
            num_charges = random.randint(1, 3)
        elif cap >= 10_000_000:        # 1 Cr+
            num_charges = random.randint(0, 2)
        else:
            num_charges = random.randint(0, 1)

        if num_charges == 0:
            continue

        for idx in range(num_charges):
            holder = random.choice(BANKS_AND_LENDERS)

            # Charge amount: 20-80% of authorized cap
            charge_pct = random.uniform(0.20, 0.80)
            amount     = round(cap * charge_pct / num_charges, 2)

            creation   = random_past_date(1, 8)

            # Status: distressed/dissolved companies have more satisfied charges
            if status in ("dissolved", "strike_off", "amalgamated"):
                chg_status = random.choice(["satisfied", "satisfied", "live"])
            else:
                chg_status = random.choice(["live", "live", "live", "satisfied",
                                            "partially_satisfied"])

            mod_date = None
            if chg_status in ("satisfied", "partially_satisfied"):
                # Modification after creation
                try:
                    cr_dt    = datetime.strptime(creation, "%Y-%m-%d")
                    mod_dt   = cr_dt + timedelta(days=random.randint(180, 1800))
                    mod_date = mod_dt.strftime("%Y-%m-%d")
                    if mod_date > date.today().strftime("%Y-%m-%d"):
                        mod_date = date.today().strftime("%Y-%m-%d")
                except Exception:
                    mod_date = None

            asset_tmpl = random.choice(ASSET_DESCRIPTIONS)
            asset_desc = asset_tmpl.format(state=state)

            records.append({
                "charge_id":              make_charge_id(cin, holder, idx),
                "company_cin":            cin,
                "charge_holder_name":     holder,
                "charge_amount_inr":      amount,
                "charge_creation_date":   creation,
                "charge_modification_date": mod_date,
                "charge_status":          chg_status,
                "asset_description":      asset_desc,
                "charge_type":            random.choice(CHARGE_TYPES),
            })

    print(f"  Charges generated:   {len(records):>8,}")
    live = sum(1 for r in records if r["charge_status"] == "live")
    print(f"  Live charges:        {live:>8,}")
    print(f"  Satisfied:           {len(records)-live:>8,}")
    return records

# ─────────────────────────────────────────────
# STEP 4: DERIVE director_company_network
# ─────────────────────────────────────────────

def derive_network(
    directors:  list[dict],
    companies:  list[dict],
    charges:    list[dict],
) -> list[dict]:
    """
    Build director_company_network from mca_directors.

    For each director → each company they appear in:
      - connection_type: current_directorship
      - is_borrower_company: True (all are potential borrowers)
      - other_company_risk_flags: computed from company_status + charges

    Risk flags:
      has_drt_case:           company_status == strike_off/dissolved  (proxy)
      has_nclt_case:          company_status == under_liquidation
      company_status_active:  company_status == active
      has_distressed_charges: has live charges > 50% authorized cap
    """
    print(f"\n── Step 4: Deriving director_company_network ───────────────")

    # Index: cin → company record
    company_idx = {co["company_cin"]: co for co in companies}

    # Index: cin → list of live charges
    charge_idx = defaultdict(list)
    for ch in charges:
        if ch["charge_status"] == "live":
            charge_idx[ch["company_cin"]].append(ch)

    # Build risk flag per CIN
    def risk_flags(cin: str) -> dict:
        co     = company_idx.get(cin, {})
        status = co.get("company_status", "active")
        cap    = co.get("authorized_capital_inr") or 0
        live_charges = charge_idx.get(cin, [])
        live_amount  = sum(c.get("charge_amount_inr") or 0 for c in live_charges)

        return {
            "has_drt_case":           status in ("strike_off", "dissolved"),
            "has_nclt_case":          status == "under_liquidation",
            "company_status_active":  status == "active",
            "has_distressed_charges": (cap > 0 and live_amount > cap * 0.5),
        }

    records = []
    for d in directors:
        din = d["director_din"]
        cin = d["company_cin"]
        records.append({
            "director_din":          din,
            "company_cin":           cin,
            "connection_type":       "current_directorship",
            "is_borrower_company":   True,
            "other_company_risk_flags": json.dumps(risk_flags(cin)),
        })

    # Stats
    cross_dirs = defaultdict(set)
    for d in directors:
        cross_dirs[d["director_din"]].add(d["company_cin"])
    multi = {din: cins for din, cins in cross_dirs.items() if len(cins) > 1}

    print(f"  Network edges:       {len(records):>8,}")
    print(f"  Multi-company dirs:  {len(multi):>8,}  (directors on 2+ boards)")
    print(f"  Max boards per dir:  {max((len(c) for c in multi.values()), default=0):>8,}")

    flagged = sum(
        1 for r in records
        if json.loads(r["other_company_risk_flags"]).get("has_drt_case")
        or json.loads(r["other_company_risk_flags"]).get("has_nclt_case")
    )
    print(f"  Edges with DRT/NCLT: {flagged:>8,}  (stress signals for graph engine)")
    return records

# ─────────────────────────────────────────────
# CSV WRITER
# ─────────────────────────────────────────────

def write_csv(records: list[dict], filepath: str, columns: list[str]):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)
    print(f"  ✓ {filepath:<40} {len(records):>8,} rows")

# ─────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────

def print_summary(companies, directors, charges, network):
    print("\n" + "="*60)
    print("MCA PROCESSOR — OUTPUT SUMMARY")
    print("="*60)

    # Company status breakdown
    status_counts = defaultdict(int)
    for co in companies:
        status_counts[co["company_status"]] += 1
    print(f"\nmca_company_master ({len(companies):,} rows):")
    for s, c in sorted(status_counts.items(), key=lambda x: -x[1]):
        print(f"  {s:<25} {c:>8,}")

    # Sector breakdown
    sector_counts = defaultdict(int)
    for co in companies:
        sector_counts[co.get("_sector", "General")] += 1
    print(f"\nSector distribution:")
    for s, c in sorted(sector_counts.items(), key=lambda x: -x[1]):
        print(f"  {s:<25} {c:>8,}")

    # Director stats
    print(f"\nmca_directors ({len(directors):,} rows):")
    desig_counts = defaultdict(int)
    for d in directors:
        desig_counts[d["designation"]] += 1
    for d, c in sorted(desig_counts.items(), key=lambda x: -x[1])[:5]:
        print(f"  {d:<30} {c:>8,}")

    # Charge stats
    live   = sum(1 for c in charges if c["charge_status"] == "live")
    print(f"\nmca_charges_registered ({len(charges):,} rows):")
    print(f"  Live charges:        {live:>8,}")
    print(f"  Satisfied:           {len(charges)-live:>8,}")

    # Network stats
    print(f"\ndirector_company_network ({len(network):,} rows)")
    print("="*60)

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert Kaggle MCA dataset → 4 Section 3.3 schema CSVs"
    )
    parser.add_argument("--input",        required=True, help="Path to Kaggle CSV file")
    parser.add_argument("--sample",       type=int,      help="Max companies to load (for testing)")
    parser.add_argument("--filter-state", type=str,      help="Only load companies from this state e.g. 'Maharashtra'")
    parser.add_argument("--dry-run",      action="store_true", help="Stats only, no CSV output")
    parser.add_argument("--director-pool",type=int, default=2000, help="Size of director pool (default 2000)")
    args = parser.parse_args()

    # Step 1: Load company master
    companies = load_company_master(
        filepath     = args.input,
        sample       = args.sample,
        filter_state = args.filter_state,
    )
    if not companies:
        print("No companies loaded. Check input file path.")
        exit(1)

    # Step 2: Generate directors
    pool      = DirectorPool(total_directors=args.director_pool)
    directors = generate_directors(companies, pool)

    # Step 3: Generate charges
    charges   = generate_charges(companies)

    # Step 4: Derive network
    network   = derive_network(directors, companies, charges)

    # Summary
    print_summary(companies, directors, charges, network)

    # Write CSVs
    if not args.dry_run:
        print("\n── Writing CSVs ────────────────────────────────────────────")
        write_csv(companies,  OUT_COMPANY_MASTER, COMPANY_MASTER_COLS)
        write_csv(directors,  OUT_DIRECTORS,      DIRECTORS_COLS)
        write_csv(charges,    OUT_CHARGES,        CHARGES_COLS)
        write_csv(network,    OUT_NETWORK,        NETWORK_COLS)
        print(f"\nAll 4 CSVs written. Ready for Section 5.3 Graph Engine.")
    else:
        print("\n[dry-run] No files written.")