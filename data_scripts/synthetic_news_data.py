"""
synthetic_news_data.py
======================
Generates a realistic synthetic news dataset for testing the FinBERT
+ scoring pipeline (Section 5.1).

Covers all 10 companies across 5 sectors with varied:
  - signal_category types (all 8 from schema)
  - severity levels (low 0.1-0.4, medium 0.4-0.6, high 0.6-1.0)
  - crawl_phases (background_deep_crawl + live_refresh)
  - publications (ET, Business Standard, LiveMint, etc.)

Output:
  - synthetic_news_articles.json   → load into news_articles_crawled
  - synthetic_news_articles.csv    → for quick inspection in Excel
  - seed_db.py                     → helper to load into SQLite or PostgreSQL
"""

import json
import csv
import random
import hashlib
from datetime import date, timedelta, datetime, timezone

random.seed(42)  # reproducible

# ─────────────────────────────────────────────
# COMPANIES
# ─────────────────────────────────────────────

COMPANIES = [
    {"company_id": "20microns",           "company_name": "20 Microns Limited",                      "promoter_name": "Chandresh Parikh",    "sector": "Manufacturing"},
    {"company_id": "360one",              "company_name": "360 ONE WAM Limited",                     "promoter_name": "Karan Bhagat",        "sector": "NBFC"},
    {"company_id": "5paisa",              "company_name": "5Paisa Capital Limited",                  "promoter_name": "Prakarsh Gagdani",    "sector": "NBFC"},
    {"company_id": "aadhar_housing",      "company_name": "Aadhar Housing Finance Limited",          "promoter_name": "Rishi Anand",         "sector": "NBFC"},
    {"company_id": "aavas",               "company_name": "Aavas Financiers Limited",                "promoter_name": "Sushil Kumar Agarwal","sector": "NBFC"},
    {"company_id": "aditya_birla_capital","company_name": "Aditya Birla Capital Limited",            "promoter_name": "Vishakha Mulye",      "sector": "NBFC"},
    {"company_id": "aarey_drugs",         "company_name": "Aarey Drugs & Pharmaceuticals Limited",   "promoter_name": "Hasmukh Shah",        "sector": "Pharma"},
    {"company_id": "aarti_drugs",         "company_name": "Aarti Drugs Limited",                     "promoter_name": "Adhish Patil",        "sector": "Pharma"},
    {"company_id": "aditya_birla_fashion","company_name": "Aditya Birla Fashion and Retail Limited", "promoter_name": "Ashish Dikshit",      "sector": "Retail"},
    {"company_id": "accelya",             "company_name": "Accelya Solutions India Limited",         "promoter_name": "Anand Venkataraman",  "sector": "IT"},
]

PUBLICATIONS = [
    "Economic Times", "Business Standard", "LiveMint",
    "Moneycontrol", "Financial Express", "Business Today",
    "The Hindu BusinessLine", "NDTV Profit",
]

# ─────────────────────────────────────────────
# ARTICLE TEMPLATES PER SIGNAL CATEGORY
# Each template: (headline_template, body_template, severity_range)
# {cn} = company name, {pn} = promoter name, {sec} = sector
# ─────────────────────────────────────────────

TEMPLATES = {

    "promoter_fraud_allegation": [
        (
            "{pn} of {cn} faces ED inquiry over alleged fund diversion",
            """The Enforcement Directorate (ED) has initiated a preliminary inquiry against {pn}, 
promoter of {cn}, over alleged diversion of funds worth approximately ₹{amount} crore from the 
company to related entities. Sources familiar with the matter said the agency is examining 
transactions conducted between FY2021 and FY2023. {cn} has denied any wrongdoing, stating in a 
regulatory filing that all transactions were conducted at arm's length and duly approved by the 
board. "We are fully cooperating with authorities and are confident that the inquiry will 
conclude without adverse findings," a company spokesperson said. Legal experts note that a 
preliminary inquiry does not constitute a formal case and often concludes without charges. 
However, market participants have flagged the development as a near-term overhang on the stock. 
Shares of {cn} fell {pct}% on BSE following the news.""",
            (0.70, 0.92),
        ),
        (
            "SEBI issues show-cause notice to {cn} promoter {pn} for alleged insider trading",
            """The Securities and Exchange Board of India (SEBI) has issued a show-cause notice to 
{pn}, the promoter of {cn}, alleging violations of insider trading regulations during the 
quarter ended September 2023. According to the SEBI order, {pn} allegedly traded in company 
shares while in possession of unpublished price-sensitive information related to a major contract 
win. The regulator has given {pn} 21 days to respond. {cn}'s legal team has called the 
allegations "factually incorrect and legally untenable." This is the second regulatory notice 
the promoter group has received in three years, raising governance concerns among institutional 
investors. The stock declined {pct}% intraday on BSE on heavy volumes.""",
            (0.65, 0.88),
        ),
    ],

    "promoter_legal_trouble": [
        (
            "{pn} named in DRT suit filed by Punjab National Bank against {cn}",
            """Punjab National Bank has filed a recovery suit at the Debt Recovery Tribunal (DRT), 
Mumbai, naming {pn} and {cn} as defendants over non-repayment of a term loan of ₹{amount} crore 
extended in 2020. The bank's petition alleges that the company defaulted on three consecutive 
quarterly instalments beginning April 2024 and failed to respond to notices. {pn} has been 
named as personal guarantor to the loan facility. The DRT has issued notice and scheduled the 
first hearing for next month. {cn} said it is in active discussions with the bank to restructure 
the facility and expects to reach a resolution. Banking sector analysts say DRT filings are 
increasingly common in the current credit cycle but the personal guarantee exposure adds 
pressure on the promoter.""",
            (0.72, 0.95),
        ),
        (
            "{pn} arrested by CBI in connection with alleged bank fraud at {cn}",
            """The Central Bureau of Investigation (CBI) arrested {pn}, Managing Director of {cn}, 
on charges of bank fraud and criminal conspiracy. The arrest follows a multi-month investigation 
into alleged manipulation of books of accounts and submission of false financial statements to 
obtain credit facilities from a consortium of lenders. Investigators allege that funds totalling 
₹{amount} crore were diverted through a web of shell companies controlled by associates of {pn}. 
{cn} shares were circuit-locked at the lower end on BSE. The company's board has convened an 
emergency meeting. Legal experts say such arrests signal advanced stages of investigation and 
significantly heighten credit risk for outstanding lenders.""",
            (0.88, 1.00),
        ),
    ],

    "company_financial_stress": [
        (
            "{cn} reports {pct}% YoY decline in net profit; CFO flags working capital pressure",
            """{cn} reported a sharp {pct}% year-on-year decline in net profit for Q3 FY25, 
significantly below analyst estimates. The company's CFO, in a post-results call, flagged 
elevated working capital requirements and tightening credit terms from suppliers as key 
headwinds. Gross debt rose to ₹{amount} crore from ₹{amount2} crore a year ago, pushing the 
net debt-to-equity ratio to {ratio}x. The management guided for margin recovery in H1 FY26, 
citing operational efficiencies. However, three brokerages downgraded the stock following the 
results, with one cutting its target price by 35%. "The pace of deleveraging is slower than 
expected and liquidity remains tight," a credit analyst at a domestic rating agency noted.""",
            (0.55, 0.78),
        ),
        (
            "ICRA downgrades {cn} long-term rating to BB+; outlook negative",
            """ICRA Limited has downgraded the long-term credit rating of {cn} from BBB- to BB+, 
with a negative outlook, citing deteriorating financial metrics and weakening debt-service 
coverage. The rating agency noted that {cn}'s interest coverage ratio declined to {ratio}x in 
FY24 from {ratio2}x in FY23 and that cash accruals are insufficient to meet repayment 
obligations over the next 12 months without refinancing support. ICRA flagged concentration 
risk in the company's revenue profile and elevated receivables from a single customer 
accounting for {pct}% of revenue. A negative outlook indicates a likelihood of further 
downgrade if financial performance does not improve within the next 12-18 months. {cn} 
management said they are "actively working on balance sheet optimisation."  """,
            (0.62, 0.82),
        ),
        (
            "{cn} auditor raises going concern doubt in FY24 annual report",
            """The statutory auditor of {cn} has included a going concern qualification in the 
company's FY24 annual report, citing accumulated losses of ₹{amount} crore and net worth 
erosion of over 50% over two years. The auditor noted that the company's ability to continue 
as a going concern depends on successful completion of a proposed rights issue and renegotiation 
of bank debt covenants. Management has disclosed a restructuring plan to the board but has not 
made it public. Rating agencies CARE and ICRA have both placed the company's instruments on 
"credit watch with negative implications." Going concern qualifications are considered a severe 
red flag by Indian banks and significantly impair the borrower's ability to access fresh credit.""",
            (0.80, 0.97),
        ),
    ],

    "company_default_news": [
        (
            "{cn} defaults on ₹{amount} crore NCD repayment; debenture trustees notify exchanges",
            """{cn} has defaulted on the repayment of Non-Convertible Debentures (NCDs) worth 
₹{amount} crore due on {date_str}. The debenture trustees, IDBI Trusteeship Services, notified 
the BSE and NSE of the default as required under SEBI regulations. The company cited a temporary 
liquidity mismatch and said it expects to make good the payment within 30 days. However, the 
default has triggered cross-default clauses in at least two of {cn}'s bank loan agreements, 
which could accelerate repayment demands from lenders. CRISIL has placed the company's rating 
on 'Rating Watch with Negative Implications'. Market participants noted this as a significant 
escalation in the company's credit stress.""",
            (0.78, 0.96),
        ),
    ],

    "company_litigation_news": [
        (
            "{cn} faces ₹{amount} crore GST demand notice; files appeal at CESTAT",
            """{cn} has received a demand notice of ₹{amount} crore from the GST authorities 
over alleged irregular input tax credit claims during FY2020-22. The company has filed an appeal 
before the Customs, Excise and Service Tax Appellate Tribunal (CESTAT), contending that the 
demand is legally unsustainable. {cn}'s legal team argues that the input tax credits were 
legitimately availed in accordance with CGST Act provisions. The contingent liability has been 
disclosed in the company's balance sheet under notes to accounts. Analysts note that while 
such tax disputes are common in India's corporate landscape, a ₹{amount} crore exposure 
represents approximately {pct}% of {cn}'s FY24 PAT and is material.""",
            (0.35, 0.58),
        ),
        (
            "NCLT admits insolvency petition against {cn} filed by operational creditor",
            """The National Company Law Tribunal (NCLT), Mumbai bench, has admitted an insolvency 
petition filed by an operational creditor against {cn} under Section 9 of the Insolvency and 
Bankruptcy Code (IBC), 2016. The petition alleges non-payment of dues of ₹{amount} crore for 
goods supplied. The NCLT has appointed an Interim Resolution Professional (IRP) and issued a 
moratorium on all legal proceedings against the company. {cn} has filed an application to set 
aside the admission, contending the debt is disputed. Legal experts note that while many NCLT 
admissions are eventually resolved through settlement, the moratorium disrupts business 
operations and banking relationships significantly.""",
            (0.82, 0.98),
        ),
    ],

    "promoter_controversy": [
        (
            "{pn} embroiled in related-party transaction controversy at {cn}",
            """Proxy advisory firm IiAS has flagged multiple related-party transactions between 
{cn} and entities controlled by promoter {pn} as potentially detrimental to minority 
shareholders. The advisory note, circulated ahead of {cn}'s AGM, highlights that the company 
paid ₹{amount} crore in management fees to a promoter-owned entity in FY24, a {pct}% increase 
from the previous year, without adequate disclosure of the services rendered. IiAS has 
recommended that institutional shareholders vote against the resolution approving these 
transactions. Domestic mutual funds holding {pct2}% of the company's equity are yet to declare 
their voting intentions. Governance concerns have historically been a discount factor for {cn}'s 
valuation multiples.""",
            (0.45, 0.68),
        ),
    ],

    "sector_regulatory_headwind": [
        (
            "RBI tightens NBFC lending norms; sector faces margin compression in FY26",
            """The Reserve Bank of India has issued revised guidelines for Non-Banking Financial 
Companies (NBFCs), mandating higher provision coverage ratios and stricter loan-to-value limits 
for secured lending. The circular, effective April 1, 2025, requires NBFCs with assets above 
₹500 crore to maintain a minimum Tier-1 capital ratio of 10%, up from 8%. Industry body FIDC 
has written to RBI seeking a phased implementation. Analysts estimate the new norms will 
compress return on equity for mid-sized NBFCs by 150-200 basis points in FY26. Companies with 
high share of LAP (Loan Against Property) portfolios are most affected. Shares of NBFC index 
fell 3.2% following the circular.""",
            (0.52, 0.72),
        ),
        (
            "CDSCO issues import alert on API manufacturers; pharma sector faces supply disruption",
            """India's Central Drugs Standard Control Organisation (CDSCO) has issued an import 
alert on Active Pharmaceutical Ingredient (API) manufacturers from three Chinese provinces 
following quality compliance failures. The move affects approximately 18% of India's API import 
volume and is expected to cause near-term supply disruptions for domestic formulation companies. 
Domestic API manufacturers are likely to benefit from import substitution but face capacity 
constraints. Industry experts estimate price inflation of 8-12% for affected APIs over the next 
two quarters. Pharma companies with high dependence on Chinese API imports have flagged the 
risk in investor communications.""",
            (0.48, 0.67),
        ),
        (
            "IT sector faces headwinds as US clients defer discretionary spend; deal ramp-ups delayed",
            """India's IT services sector is experiencing a broad-based slowdown in discretionary 
spending from US and European clients, with several large deals seeing delayed ramp-ups and 
scope reductions. Industry body NASSCOM has revised its FY25 revenue growth forecast downward 
to 4-6% from an earlier estimate of 7-9%. Mid-tier IT firms are disproportionately affected as 
large enterprises consolidate vendor relationships with top-tier providers. Attrition, while 
declining, remains above pre-pandemic levels at most companies. Analysts are watching Q4 FY25 
deal TCV numbers closely for signs of demand recovery.""",
            (0.42, 0.63),
        ),
        (
            "Retail sector faces margin squeeze as quick commerce expands; brick-and-mortar traffic falls",
            """India's organised retail sector is facing intensifying competition from quick 
commerce platforms, with same-day delivery services capturing a growing share of urban grocery 
and general merchandise spending. Physical retail traffic in tier-1 cities declined {pct}% 
YoY in Q3 FY25 according to data from retail analytics firm Technopak. Apparel and lifestyle 
retailers are seeing inventory pile-up as consumers shift to online-first purchasing. 
Rental costs for prime mall locations continue to rise, further compressing operating margins. 
Industry observers expect consolidation among mid-sized retail chains over the next 18-24 months.""",
            (0.40, 0.60),
        ),
        (
            "Manufacturing sector hit by rising input costs; steel and energy prices spike 15%",
            """India's manufacturing sector is grappling with a sharp increase in key input costs, 
with steel prices up 15% quarter-on-quarter and industrial energy costs rising 12% following 
state electricity tariff revisions. The cost pressure is particularly acute for small and 
mid-sized manufacturers with limited pricing power. The Manufacturing PMI fell to 51.2 in 
February 2025, a six-month low, indicating slowing expansion. Companies in the specialty 
chemicals and industrial minerals sub-segments are attempting to pass through costs via 
price hikes, but customer resistance is limiting their ability to do so fully. Margins are 
expected to remain under pressure through H1 FY26.""",
            (0.44, 0.64),
        ),
    ],

    "sector_competitive_threat": [
        (
            "Fintech challengers eroding NBFC market share in personal and SME lending",
            """Digital lending platforms backed by large technology conglomerates are increasingly 
competing with traditional NBFCs in the personal loan and SME credit segments. New-age lenders 
with proprietary underwriting models using alternative data are offering faster disbursals at 
competitive rates, eroding the customer acquisition advantage of established NBFCs. RBI data 
shows digital lending grew 42% YoY in FY24 versus 18% for traditional NBFC credit. Mid-sized 
NBFCs focused on consumer and business loans face the most disintermediation risk. Some are 
responding by partnering with fintechs through co-lending arrangements, but this compresses 
yields.""",
            (0.38, 0.58),
        ),
    ],
}

# ─────────────────────────────────────────────
# SECTOR → SIGNAL CATEGORY MAPPING
# Which signal types are relevant for each sector
# ─────────────────────────────────────────────

SECTOR_SIGNALS = {
    "Manufacturing": [
        "sector_regulatory_headwind",   # input cost / energy regulations
        "sector_competitive_threat",
        "company_financial_stress",
        "company_litigation_news",
        "promoter_controversy",
    ],
    "NBFC": [
        "sector_regulatory_headwind",   # RBI norms
        "sector_competitive_threat",    # fintech
        "company_financial_stress",
        "company_default_news",
        "promoter_legal_trouble",
        "promoter_fraud_allegation",
    ],
    "Pharma": [
        "sector_regulatory_headwind",   # CDSCO / API
        "company_litigation_news",
        "company_financial_stress",
        "promoter_controversy",
    ],
    "Retail": [
        "sector_regulatory_headwind",   # quick commerce / competition
        "sector_competitive_threat",
        "company_financial_stress",
        "company_litigation_news",
    ],
    "IT": [
        "sector_regulatory_headwind",   # demand headwinds
        "sector_competitive_threat",
        "company_financial_stress",
        "promoter_controversy",
    ],
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def make_article_id(company_id: str, signal_cat: str, idx: int) -> str:
    key = f"{company_id}_{signal_cat}_{idx}"
    return "art_" + hashlib.md5(key.encode()).hexdigest()[:16]

def random_date(start_days_ago=365, end_days_ago=7) -> date:
    delta = random.randint(end_days_ago, start_days_ago)
    return date.today() - timedelta(days=delta)

def random_amount() -> int:
    return random.choice([12, 25, 48, 75, 120, 180, 240, 350, 500, 750])

def fill_template(template: str, co: dict) -> str:
    return template.format(
        cn      = co["company_name"],
        pn      = co["promoter_name"],
        sec     = co["sector"],
        amount  = random_amount(),
        amount2 = random_amount(),
        pct     = random.randint(8, 42),
        pct2    = random.randint(5, 25),
        ratio   = round(random.uniform(0.8, 2.2), 1),
        ratio2  = round(random.uniform(1.5, 3.5), 1),
        date_str= (date.today() - timedelta(days=random.randint(10,60))).strftime("%d %b %Y"),
    )

# ─────────────────────────────────────────────
# GENERATOR
# ─────────────────────────────────────────────

def generate_articles(articles_per_company: int = 8) -> list[dict]:
    """
    Generate synthetic news_articles_crawled records.
    Ensures:
      - Every company gets articles
      - Every signal category relevant to a sector is covered
      - Mix of high / medium / low severity
      - Mix of crawl phases
    """
    records = []
    crawled_sector_signals: dict[str, set] = {}   # sector → set of signal cats already generated

    for co in COMPANIES:
        sector   = co["sector"]
        sig_cats = SECTOR_SIGNALS.get(sector, list(TEMPLATES.keys()))

        if sector not in crawled_sector_signals:
            crawled_sector_signals[sector] = set()

        generated = 0

        for sig_cat in sig_cats:
            if sig_cat not in TEMPLATES:
                continue

            templates = TEMPLATES[sig_cat]
            tmpl      = random.choice(templates)
            headline_t, body_t, (sev_lo, sev_hi) = tmpl

            # Sector-level signals: generate once per sector, not per company
            is_sector_signal = sig_cat in ("sector_regulatory_headwind", "sector_competitive_threat")
            if is_sector_signal:
                if sig_cat in crawled_sector_signals[sector]:
                    continue   # already generated for this sector
                crawled_sector_signals[sector].add(sig_cat)
                company_id    = None
                promoter_name = None
                sector_val    = sector
            else:
                company_id    = co["company_id"]
                promoter_name = co["promoter_name"] if "promoter" in sig_cat else None
                sector_val    = None

            severity = round(random.uniform(sev_lo, sev_hi), 2)

            record = {
                "article_id":          make_article_id(co["company_id"], sig_cat, generated),
                "company_id":          company_id,
                "promoter_name":       promoter_name,
                "sector":              sector_val,
                "article_url":         f"https://synthetic-data.local/{co['company_id']}/{sig_cat}/{generated}",
                "source_publication":  random.choice(PUBLICATIONS),
                "published_date":      str(random_date()),
                "article_headline":    fill_template(headline_t, co),
                "article_full_text":   fill_template(body_t, co),
                "search_query_used":   f"{co['company_name']} {sig_cat.replace('_',' ')} India",
                "crawl_phase":         random.choice(["background_deep_crawl", "background_deep_crawl", "live_refresh"]),
                "crawl_timestamp":     datetime.now(timezone.utc).isoformat(),
                # Extra fields useful for FinBERT testing:
                "expected_signal_category": sig_cat,
                "expected_severity":        severity,
            }
            records.append(record)
            generated += 1

    return records


# ─────────────────────────────────────────────
# SAVE OUTPUTS
# ─────────────────────────────────────────────

def save_json(records: list[dict], path: str = "synthetic_news_articles.json"):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    print(f"✓ JSON saved: {path}  ({len(records)} articles)")

def save_csv(records: list[dict], path: str = "synthetic_news_articles.csv"):
    if not records:
        return
    fields = list(records[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    print(f"✓ CSV  saved: {path}  ({len(records)} rows)")

def print_summary(records: list[dict]):
    from collections import Counter
    print("\n" + "="*60)
    print("SYNTHETIC DATASET SUMMARY")
    print("="*60)
    print(f"Total articles:  {len(records)}")

    by_sector = Counter(r.get("sector") or
                        next((c["sector"] for c in COMPANIES if c["company_id"] == r.get("company_id")), "?")
                        for r in records)
    print(f"\nBy sector:")
    for sec, cnt in sorted(by_sector.items()):
        print(f"  {sec:<22} {cnt:>4} articles")

    by_cat = Counter(r["expected_signal_category"] for r in records)
    print(f"\nBy signal category:")
    for cat, cnt in sorted(by_cat.items()):
        print(f"  {cat:<38} {cnt:>4}")

    sevs = [r["expected_severity"] for r in records]
    print(f"\nSeverity distribution:")
    print(f"  High   (>0.6):  {sum(1 for s in sevs if s > 0.6):>4}  articles")
    print(f"  Medium (0.4-0.6): {sum(1 for s in sevs if 0.4 <= s <= 0.6):>3}  articles")
    print(f"  Low    (<0.4):  {sum(1 for s in sevs if s < 0.4):>4}  articles")
    print("="*60)


if __name__ == "__main__":
    records = generate_articles()
    print_summary(records)
    save_json(records, "synthetic_news_articles.json")
    save_csv(records,  "synthetic_news_articles.csv")
    print("\nNext step: run  python seed_db.py  to load into SQLite/PostgreSQL")