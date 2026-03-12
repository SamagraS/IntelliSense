"""
dataset_loader.py
=================
Loads all 3 data layers into news_articles_crawled (SQLite or PostgreSQL).

Layer 1: Zenodo Indian Stock Market News CSV  (~3,349 rows) → background_deep_crawl
Layer 2: HuggingFace Financial News CSV       (~10,000 rows, filtered) → live_refresh
Layer 3: Synthetic JSON                        (~50 rows, gap-filling) → background_deep_crawl

Run:
    python dataset_loader.py                    # load all 3 layers
    python dataset_loader.py --layer zenodo     # only Zenodo
    python dataset_loader.py --layer hf         # only HuggingFace
    python dataset_loader.py --layer synthetic  # only synthetic
    python dataset_loader.py --reset            # wipe table before loading
    python dataset_loader.py --dry-run          # stats only, no DB writes

Expected files (same directory):
    zenodo_articles.csv
    hf_financial_news.csv
    synthetic_news_articles.json
    credit_appraisal.db  ← created automatically
"""

import csv
import json
import sqlite3
import hashlib
import argparse
import re
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional

# ─────────────────────────────────────────────
# FILE PATHS
# ─────────────────────────────────────────────

ZENODO_CSV     = "zenodo_articles.csv"
HF_CSV         = "hf_financial_news.csv"
SYNTHETIC_JSON = "synthetic_news_articles.json"
SQLITE_DB      = "credit_appraisal.db"

# ─────────────────────────────────────────────
# FIX 1: source_publication from URL domain
# ─────────────────────────────────────────────

URL_TO_PUBLICATION = {
    "economictimes.indiatimes.com": "Economic Times",
    "businessstandard.com":         "Business Standard",
    "livemint.com":                 "LiveMint",
    "moneycontrol.com":             "Moneycontrol",
    "financialexpress.com":         "Financial Express",
    "thehindu.com":                 "The Hindu",
    "ndtv.com":                     "NDTV Profit",
    "businesstoday.in":             "Business Today",
    "reuters.com":                  "Reuters",
    "bloomberg.com":                "Bloomberg",
    "cnbctv18.com":                 "CNBC TV18",
    "zeebiz.com":                   "Zee Business",
    "thehindubusinessline.com":     "Hindu BusinessLine",
    "indiatimes.com":               "Times of India",
    "cnn.com":                      "CNN",
    "bbc.com":                      "BBC",
    "wsj.com":                      "Wall Street Journal",
}

def publication_from_url(url: str) -> str:
    if not url:
        return "Unknown"
    url_lower = url.lower()
    for domain, name in URL_TO_PUBLICATION.items():
        if domain in url_lower:
            return name
    # Fallback: extract bare domain
    match = re.search(r"https?://(?:www\.)?([^/]+)", url_lower)
    return match.group(1).split(".")[0].title() if match else "Unknown"

# ─────────────────────────────────────────────
# FIX 2 & 3: Company + sector keyword mapping
# Tier 1: company match → company_id + sector
# Tier 2: sector match  → sector only (company_id = NULL)
# Tier 3: no match      → skip
# ─────────────────────────────────────────────

COMPANY_MAP = {
    "aditya_birla_capital": {
        "sector": "NBFC",
        "promoter": "Vishakha Mulye",
        "keywords": [
            "Aditya Birla Capital", "ABCL", "Vishakha Mulye",
            "Aditya Birla Finance",
        ],
    },
    "aditya_birla_fashion": {
        "sector": "Retail",
        "promoter": "Ashish Dikshit",
        "keywords": [
            "Aditya Birla Fashion", "ABFRL", "Pantaloons",
            "Madura Fashion", "Allen Solly", "Louis Philippe",
            "Van Heusen", "Ashish Dikshit",
        ],
    },
    "aarti_drugs": {
        "sector": "Pharma",
        "promoter": "Adhish Patil",
        "keywords": ["Aarti Drugs", "Adhish Patil"],
    },
    "aarey_drugs": {
        "sector": "Pharma",
        "promoter": "Hasmukh Shah",
        "keywords": ["Aarey Drugs", "Aarey Pharmaceuticals", "Hasmukh Shah"],
    },
    "aavas": {
        "sector": "NBFC",
        "promoter": "Sushil Kumar Agarwal",
        "keywords": ["Aavas Financiers", "Aavas Finance", "Sushil Kumar Agarwal"],
    },
    "aadhar_housing": {
        "sector": "NBFC",
        "promoter": "Rishi Anand",
        "keywords": ["Aadhar Housing Finance", "Aadhar Housing", "Rishi Anand"],
    },
    "360one": {
        "sector": "NBFC",
        "promoter": "Karan Bhagat",
        "keywords": [
            "360 ONE", "360ONE", "360 One WAM",
            "IIFL Wealth", "Karan Bhagat",
        ],
    },
    "5paisa": {
        "sector": "NBFC",
        "promoter": "Prakarsh Gagdani",
        "keywords": ["5Paisa", "5 Paisa", "Prakarsh Gagdani"],
    },
    "accelya": {
        "sector": "IT",
        "promoter": "Anand Venkataraman",
        "keywords": ["Accelya", "Accelya Solutions", "Anand Venkataraman"],
    },
    "20microns": {
        "sector": "Manufacturing",
        "promoter": "Chandresh Parikh",
        "keywords": [
            "20 Microns", "20Microns", "Chandresh Parikh",
            "micronized minerals", "specialty minerals",
        ],
    },
}

SECTOR_MAP = {
    "NBFC": [
        "NBFC", "non-banking financial", "non banking financial",
        "housing finance", "home finance", "home loan",
        "microfinance", "micro finance", "MFI",
        "gold loan", "asset finance", "loan against property", "LAP",
        "affordable housing", "HFC", "shadow bank",
        "Bajaj Finance", "Bajaj Finserv", "Muthoot Finance", "Muthoot Fincorp",
        "IIFL Finance", "Shriram Finance", "Shriram Transport",
        "Cholamandalam", "Mahindra Finance", "L&T Finance",
        "Piramal Finance", "Tata Capital", "Hero FinCorp",
        "Manappuram", "CreditAccess", "Spandana",
        "retail lending", "SME lending", "MSME loan", "credit growth",
        "disbursement", "AUM growth", "assets under management",
        "NIM compression", "net interest margin", "yield on advances",
        "gross NPA", "net NPA", "provision coverage",
        "co-lending", "priority sector lending", "PSL",
        "RBI lending", "lending rate", "MCLR",
    ],
    "Pharma": [
        "pharma", "pharmaceutical", "drug maker", "drugmaker",
        "API manufacturer", "active pharmaceutical ingredient",
        "CDSCO", "USFDA", "FDA warning", "FDA inspection",
        "import alert", "warning letter", "483 observations",
        "generics", "generic drug", "formulation", "biosimilar",
        "biotech", "biologics", "contract manufacturing", "CDMO",
        "clinical trial", "drug approval", "ANDA", "NDA",
        "Sun Pharma", "Cipla", "Dr Reddy", "Lupin", "Aurobindo",
        "Divi's Labs", "Biocon", "Glenmark", "Torrent Pharma",
        "Alkem", "Ipca Labs", "Natco Pharma", "Granules",
        "Zydus", "Mankind Pharma", "Abbott India",
        "Nifty Pharma", "pharma index", "pharma exports",
        "PLI pharma", "API park", "bulk drug",
    ],
    "Retail": [
        "retail", "retailer", "e-commerce", "ecommerce",
        "quick commerce", "q-commerce", "omnichannel",
        "fashion retail", "apparel", "clothing", "garment",
        "consumer goods", "FMCG", "fast moving consumer",
        "brick and mortar", "mall", "hypermarket", "supermarket",
        "same store sales", "SSSG", "footfall",
        "D-Mart", "Avenue Supermarts", "Reliance Retail",
        "Trent", "Westside", "Zara India", "H&M India",
        "Zomato", "Swiggy", "Blinkit", "Zepto", "Dunzo",
        "Nykaa", "Meesho", "Myntra", "Ajio", "Flipkart",
        "Amazon India", "BigBasket",
        "consumer spending", "consumer sentiment", "discretionary spend",
        "festive sales", "GST on retail", "inventory days",
    ],
    "IT": [
        "IT services", "information technology", "software services",
        "software company", "tech company", "IT sector",
        "digital transformation", "cloud services", "cloud migration",
        "SaaS", "PaaS", "IaaS", "managed services",
        "outsourcing", "offshoring", "GCC", "global capability centre",
        "NASSCOM", "IT exports",
        "TCS", "Tata Consultancy", "Infosys", "Wipro",
        "HCL Tech", "HCL Technologies", "Tech Mahindra",
        "Mphasis", "Persistent Systems", "Coforge", "LTIMindtree",
        "Hexaware", "Zensar", "KPIT Technologies",
        "deal win", "deal TCV", "total contract value",
        "headcount", "attrition", "fresher hiring",
        "AI services", "generative AI", "automation",
        "US discretionary spend", "BFSI vertical",
    ],
    "Manufacturing": [
        "manufacturing", "manufacturer", "industrial",
        "specialty chemicals", "chemicals company",
        "minerals", "mining", "quarry",
        "steel", "aluminium", "copper", "metal",
        "cement", "construction material",
        "auto component", "auto ancillary", "EV component",
        "capital goods", "engineering goods", "heavy engineering",
        "PLI scheme", "PLI incentive", "production linked incentive",
        "Make in India", "factory", "plant capacity", "capex",
        "MSME manufacturing", "SME manufacturer",
        "Tata Steel", "JSW Steel", "Hindalco", "Vedanta",
        "UltraTech", "Ambuja Cement", "ACC Cement",
        "Larsen & Toubro", "L&T", "Bharat Forge",
        "Thermax", "Cummins India", "ABB India",
        "input cost", "raw material cost", "energy cost",
        "capacity utilisation", "order book", "EBITDA margin",
        "export order", "import substitution",
    ],
}

def match_company(text: str) -> Optional[dict]:
    """Tier 1: check if text mentions a specific company."""
    text_lower = text.lower()
    for company_id, info in COMPANY_MAP.items():
        for kw in info["keywords"]:
            if kw.lower() in text_lower:
                return {
                    "company_id":    company_id,
                    "sector":        info["sector"],
                    "promoter_name": info["promoter"],
                    "matched_kw":    kw,
                }
    return None

def match_sector(text: str) -> Optional[str]:
    """Tier 2: check if text mentions a sector."""
    text_lower = text.lower()
    for sector, keywords in SECTOR_MAP.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                return sector
    return None

# ─────────────────────────────────────────────
# FIX 4: search_query_used reconstruction
# Mimics Tavily query audit trail
# ─────────────────────────────────────────────

RISK_QUERY_SIGNALS = [
    (["fraud", "scam", "diversion", "embezzl"],          "{entity} fraud scam controversy India"),
    (["DRT", "debt recovery", "arrest", "criminal"],     "{entity} court case DRT NCLT India"),
    (["insolvency", "IBC", "NCLT", "liquidat"],          "{entity} insolvency default NPA bank"),
    (["downgrade", "rating", "ICRA", "CRISIL", "CARE"],  "{entity} credit rating downgrade India"),
    (["RBI", "regulation", "circular", "norms"],         "{entity} RBI regulation circular 2025"),
    (["headwind", "challenge", "outlook", "tariff"],     "{entity} India outlook headwinds challenges"),
    (["related party", "governance", "SEBI notice"],     "{entity} related party transaction controversy"),
    (["auditor", "qualification", "going concern"],      "{entity} auditor resignation qualification"),
    (["default", "NCD", "repayment", "restructur"],      "{entity} insolvency default NPA bank"),
    (["competition", "disruption", "market share"],      "{entity} competitive threat disruption India"),
    (["litigation", "court", "case", "dispute"],         "{entity} litigation court case India"),
]

def build_search_query(text: str, entity: str) -> str:
    """Reconstruct a Tavily-style query from the article content."""
    text_lower = text.lower()
    for signals, template in RISK_QUERY_SIGNALS:
        if any(s.lower() in text_lower for s in signals):
            return template.format(entity=entity)
    # Generic fallback
    return f"{entity} latest news India 2025"

# ─────────────────────────────────────────────
# FIX 5: India + finance relevance filter for HF
# ─────────────────────────────────────────────

INDIA_SIGNALS = [
    "india", "indian", "mumbai", "delhi", "bengaluru", "chennai",
    "nse", "bse", "nifty", "sensex", "crore", "lakh", "inr", "rs ",
    "rbi", "sebi", "nclt", "drt", "mca", "cdsco",
]

FINANCE_SIGNALS = [
    "stock", "share", "equity", "bond", "debt", "loan", "credit",
    "revenue", "profit", "loss", "earning", "ipo", "npa",
    "rating", "downgrade", "default", "merger", "acquisition",
    "dividend", "market cap", "valuation", "fund", "portfolio",
    "financial", "bank", "lender", "borrower", "interest rate",
]

def is_india_finance_relevant(text: str) -> bool:
    """
    Returns True only if article has BOTH India signals AND finance signals.
    Filters out CNN politics, NATO, medical research etc.
    """
    text_lower = text.lower()
    has_india   = any(s in text_lower for s in INDIA_SIGNALS)
    has_finance = any(s in text_lower for s in FINANCE_SIGNALS)
    return has_india and has_finance

# ─────────────────────────────────────────────
# ARTICLE ID (deterministic hash)
# ─────────────────────────────────────────────

def make_article_id(url: str, fallback: str = "") -> str:
    key = url or fallback
    return "art_" + hashlib.md5(key.encode()).hexdigest()[:16]

# ─────────────────────────────────────────────
# DATE NORMALISER
# ─────────────────────────────────────────────

def normalise_date(raw: str) -> Optional[str]:
    if not raw:
        return None
    # Try ISO format first (2025-07-08 or 2016-01-01T00:00:00Z)
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw[:len(fmt)+2].strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw[:10] if len(raw) >= 10 else None

# ─────────────────────────────────────────────
# FIX 6: HF headline splitter
# ─────────────────────────────────────────────

def split_hf_text(text: str) -> tuple[str, str]:
    """
    HF text column = 'Headline\n\nBody...' or 'Headline\nBody...'
    Returns (headline, full_text).
    """
    if not text:
        return "", ""
    parts    = text.split("\n", 1)
    headline = parts[0].strip()
    body     = parts[1].strip() if len(parts) > 1 else headline
    # full_text should include both headline and body for FinBERT context
    full_text = text.strip()
    return headline, full_text

# ─────────────────────────────────────────────
# LAYER 1: ZENODO LOADER
# Columns: Title, Date, Description, Author, Content, Keywords, URL
# ─────────────────────────────────────────────

def load_zenodo(filepath: str) -> list[dict]:
    records  = []
    skipped  = 0
    no_match = 0

    print(f"\n── Layer 1: Zenodo ({filepath}) ──────────────────────────")
    try:
        with open(filepath, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                title   = (row.get("Title") or "").strip()
                content = (row.get("Content") or "").strip()
                url     = (row.get("URL") or "").strip()
                date    = (row.get("Date") or "").strip()

                # Skip if no usable text
                if not content or len(content) < 100:
                    skipped += 1
                    continue

                # Match text = headline + first 300 chars of body
                match_text = f"{title} {content[:300]}"

                # Tier 1: company match
                company_match = match_company(match_text)
                if company_match:
                    company_id    = company_match["company_id"]
                    sector        = company_match["sector"]
                    promoter_name = company_match["promoter_name"]
                    entity        = company_match["matched_kw"]
                else:
                    # Tier 2: sector match
                    sector = match_sector(match_text)
                    if not sector:
                        no_match += 1
                        continue  # Tier 3: skip
                    company_id    = None
                    promoter_name = None
                    entity        = sector

                records.append({
                    "article_id":          make_article_id(url, f"zenodo_{i}"),
                    "company_id":          company_id,
                    "promoter_name":       promoter_name,
                    "sector":              sector,
                    "article_url":         url or f"https://zenodo.local/article/{i}",
                    "source_publication":  publication_from_url(url),     # FIX 1
                    "published_date":      normalise_date(date),
                    "article_headline":    title,                          # Clean Title col
                    "article_full_text":   content,
                    "search_query_used":   build_search_query(content, entity),  # FIX 3
                    "crawl_phase":         "background_deep_crawl",        # FIX 4
                    "crawl_timestamp":     datetime.now(timezone.utc).isoformat(),
                    "expected_signal_category": None,
                    "expected_severity":        None,
                })

    except FileNotFoundError:
        print(f"  ⚠ File not found: {filepath}. Skipping Zenodo layer.")
        return []

    print(f"  Rows processed: {i+1 if 'i' in dir() else 0}")
    print(f"  Matched (company+sector): {len(records)}")
    print(f"  Skipped (too short):      {skipped}")
    print(f"  Dropped (no match):       {no_match}")
    return records

# ─────────────────────────────────────────────
# LAYER 2: HUGGINGFACE BATCH FOLDER LOADER
# Reads all batch_001.csv ... batch_100.csv from a folder.
# Columns per file: date, text, extra_fields (JSON string)
# Cap: stops loading once MAX_HF_ROWS kept (avoids memory issues).
# ─────────────────────────────────────────────

HF_BATCH_FOLDER = "financial_news_batches"   # folder containing batch_001.csv etc.
MAX_HF_ROWS     = 15000                       # cap — plenty for hackathon

def _process_hf_row(row: dict, batch_idx: int, row_idx: int) -> tuple[Optional[dict], str]:
    """
    Process a single HF row. Returns (record_or_None, reason_if_none).
    Reason: 'short' | 'filtered' | 'no_match' | 'ok'
    """
    text      = (row.get("text") or "").strip()
    date_raw  = (row.get("date") or "").strip()
    extra_raw = (row.get("extra_fields") or "{}").strip()

    if not text or len(text) < 100:
        return None, "short"

    if not is_india_finance_relevant(text):
        return None, "filtered"

    try:
        extra = json.loads(extra_raw)
    except Exception:
        extra = {}

    url         = extra.get("url", "")
    publication = extra.get("publication", "") or publication_from_url(url)

    # FIX 6: split headline from body
    headline, full_text = split_hf_text(text)

    # Match on headline + first 300 chars
    match_text    = f"{headline} {full_text[:300]}"
    company_match = match_company(match_text)

    if company_match:
        company_id    = company_match["company_id"]
        sector        = company_match["sector"]
        promoter_name = company_match["promoter_name"]
        entity        = company_match["matched_kw"]
    else:
        sector = match_sector(match_text)
        if not sector:
            return None, "no_match"
        company_id    = None
        promoter_name = None
        entity        = sector

    record = {
        "article_id":          make_article_id(url, f"hf_{batch_idx}_{row_idx}"),
        "company_id":          company_id,
        "promoter_name":       promoter_name,
        "sector":              sector,
        "article_url":         url or f"https://hf.local/b{batch_idx}/r{row_idx}",
        "source_publication":  publication,
        "published_date":      normalise_date(date_raw),
        "article_headline":    headline,
        "article_full_text":   full_text,
        "search_query_used":   build_search_query(full_text, entity),
        "crawl_phase":         "live_refresh",
        "crawl_timestamp":     datetime.now(timezone.utc).isoformat(),
        "expected_signal_category": None,
        "expected_severity":        None,
    }
    return record, "ok"


def load_huggingface(folder: str = HF_BATCH_FOLDER) -> list[dict]:
    """
    Load all batch CSV files from folder.
    Processes batch_001.csv → batch_100.csv in order.
    Stops early if MAX_HF_ROWS reached.
    Falls back to single-file mode if folder not found but HF_CSV exists.
    """
    import os
    import glob

    records  = []
    total_processed = 0
    total_filtered  = 0
    total_skipped   = 0
    total_no_match  = 0

    # ── Batch folder mode ──
    if os.path.isdir(folder):
        batch_files = sorted(glob.glob(os.path.join(folder, "*.csv")))
        print(f"\n── Layer 2: HuggingFace batches ({folder}/) ───────────────")
        print(f"  Found {len(batch_files)} batch files")
        print(f"  Row cap: {MAX_HF_ROWS:,}")

        for b_idx, batch_path in enumerate(batch_files, 1):
            if len(records) >= MAX_HF_ROWS:
                print(f"  Cap reached at batch {b_idx-1}. Stopping early.")
                break

            batch_name = os.path.basename(batch_path)
            try:
                with open(batch_path, newline="", encoding="utf-8", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for r_idx, row in enumerate(reader):
                        if len(records) >= MAX_HF_ROWS:
                            break
                        record, reason = _process_hf_row(row, b_idx, r_idx)
                        total_processed += 1
                        if reason == "short":    total_skipped  += 1
                        elif reason == "filtered": total_filtered += 1
                        elif reason == "no_match": total_no_match += 1
                        elif record:               records.append(record)
            except Exception as e:
                print(f"  ⚠ Error reading {batch_name}: {e}")

            # Progress every 10 batches
            if b_idx % 10 == 0:
                print(f"  [{b_idx:>3}/{len(batch_files)}] batches done | kept so far: {len(records):,}")

    # ── Single file fallback ──
    elif os.path.exists(HF_CSV):
        print(f"\n── Layer 2: HuggingFace single file ({HF_CSV}) ───────────")
        try:
            with open(HF_CSV, newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for r_idx, row in enumerate(reader):
                    if len(records) >= MAX_HF_ROWS:
                        break
                    record, reason = _process_hf_row(row, 0, r_idx)
                    total_processed += 1
                    if reason == "short":      total_skipped  += 1
                    elif reason == "filtered": total_filtered += 1
                    elif reason == "no_match": total_no_match += 1
                    elif record:               records.append(record)
        except Exception as e:
            print(f"  ⚠ Error: {e}")
    else:
        print(f"\n── Layer 2: HuggingFace ───────────────────────────────────")
        print(f"  ⚠ Neither folder '{folder}' nor file '{HF_CSV}' found. Skipping.")
        return []

    print(f"\n  Total rows processed:    {total_processed:>8,}")
    print(f"  Filtered (not IN+FIN):   {total_filtered:>8,}")
    print(f"  Skipped (too short):     {total_skipped:>8,}")
    print(f"  Dropped (no match):      {total_no_match:>8,}")
    print(f"  Kept:                    {len(records):>8,}")
    return records

# ─────────────────────────────────────────────
# LAYER 3: SYNTHETIC LOADER
# Fills: promoter_fraud_allegation, promoter_legal_trouble,
#        promoter_controversy, company_default_news
# ─────────────────────────────────────────────

def load_synthetic(filepath: str) -> list[dict]:
    print(f"\n── Layer 3: Synthetic ({filepath}) ───────────────────────")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
        print(f"  Loaded: {len(records)} synthetic articles")
        # Ensure required fields exist
        for r in records:
            r.setdefault("crawl_phase", "background_deep_crawl")
            r.setdefault("crawl_timestamp", datetime.now(timezone.utc).isoformat())
        return records
    except FileNotFoundError:
        print(f"  ⚠ File not found: {filepath}. Skipping synthetic layer.")
        return []

# ─────────────────────────────────────────────
# DEDUPLICATION
# ─────────────────────────────────────────────

def deduplicate(records: list[dict]) -> list[dict]:
    seen = set()
    out  = []
    for r in records:
        aid = r["article_id"]
        if aid not in seen:
            seen.add(aid)
            out.append(r)
    dupes = len(records) - len(out)
    if dupes:
        print(f"\n  Deduplication: removed {dupes} duplicate article_ids")
    return out

# ─────────────────────────────────────────────
# CSV OUTPUT
# Exact column order matches news_articles_crawled schema
# ─────────────────────────────────────────────

CSV_COLUMNS = [
    "article_id",
    "company_id",
    "promoter_name",
    "sector",
    "article_url",
    "source_publication",
    "published_date",
    "article_headline",
    "article_full_text",
    "search_query_used",
    "crawl_phase",
    "crawl_timestamp",
    "expected_signal_category",
    "expected_severity",
]

OUTPUT_CSV = "news_articles_crawled.csv"

def write_to_csv(records: list[dict], out_path: str):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames   = CSV_COLUMNS,
            extrasaction = "ignore",    # drop any extra keys silently
        )
        writer.writeheader()
        writer.writerows(records)

    print(f"\n  ✓ CSV written:  {out_path}")
    print(f"  Rows:           {len(records)}")
    print(f"  Columns ({len(CSV_COLUMNS)}):  {', '.join(CSV_COLUMNS)}")
    print(f"\n  Open in Excel or load into DB later with:")
    print(f"    pandas: pd.read_csv('{out_path}')")
    print(f"    sqlite: .import {out_path} news_articles_crawled")

# ─────────────────────────────────────────────
# SUMMARY REPORT
# ─────────────────────────────────────────────

def print_summary(records: list[dict]):
    by_sector    = defaultdict(int)
    by_company   = defaultdict(int)
    by_phase     = defaultdict(int)
    publications = defaultdict(int)

    for r in records:
        by_phase[r.get("crawl_phase", "unknown")] += 1
        by_sector[r.get("sector") or "Unknown"]   += 1
        by_company[r.get("company_id") or "(sector-level)"] += 1
        publications[r.get("source_publication") or "Unknown"] += 1

    print("\n" + "="*65)
    print("DATASET LOAD SUMMARY")
    print("="*65)
    print(f"Total articles: {len(records)}")

    print(f"\n{'Crawl Phase':<30} {'Count':>8}")
    print("-"*40)
    for phase, cnt in sorted(by_phase.items()):
        print(f"  {phase:<28} {cnt:>8}")

    print(f"\n{'Sector':<25} {'Count':>8}")
    print("-"*35)
    for sector, cnt in sorted(by_sector.items(), key=lambda x: -x[1]):
        print(f"  {sector:<23} {cnt:>8}")

    print(f"\n{'Company':<30} {'Count':>8}")
    print("-"*40)
    for cid, cnt in sorted(by_company.items(), key=lambda x: -x[1])[:15]:
        print(f"  {cid:<28} {cnt:>8}")

    print(f"\nTop publications:")
    for pub, cnt in sorted(publications.items(), key=lambda x: -x[1])[:8]:
        print(f"  {pub:<30} {cnt:>6}")

    print(f"\nSignal category coverage (synthetic layer labels):")
    cats = defaultdict(int)
    for r in records:
        c = r.get("expected_signal_category")
        if c:
            cats[c] += 1
    if cats:
        for c, cnt in sorted(cats.items()):
            print(f"  {c:<40} {cnt:>4}")
    else:
        print("  (Real articles have no labels — FinBERT assigns these.)")
    print("="*65)

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load all 3 data layers → news_articles_crawled.csv"
    )
    parser.add_argument("--layer",   choices=["zenodo","hf","synthetic","all"], default="all",
                        help="Which layer(s) to load")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats only, do not write CSV")
    parser.add_argument("--out",     default=OUTPUT_CSV,
                        help=f"Output CSV path (default: {OUTPUT_CSV})")
    args = parser.parse_args()

    all_records = []

    if args.layer in ("all", "zenodo"):
        all_records += load_zenodo(ZENODO_CSV)

    if args.layer in ("all", "hf"):
        all_records += load_huggingface(HF_BATCH_FOLDER)

    if args.layer in ("all", "synthetic"):
        all_records += load_synthetic(SYNTHETIC_JSON)

    all_records = deduplicate(all_records)
    print_summary(all_records)

    if not args.dry_run:
        write_to_csv(all_records, args.out)
        print("\nNext: run  python finbert_pipeline.py --csv news_articles_crawled.csv")
    else:
        print("\n[dry-run] No file written.")