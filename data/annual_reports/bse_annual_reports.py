"""
BSE Annual Report Downloader — FIXED
=======================================
Root cause of previous failure:
  - BSE API response key names were assumed wrong
  - This script first runs a DIAGNOSTIC MODE to print the real response
    for one company, then you can confirm fields and run normally.

Requirements:
    pip install requests pandas

Usage:
    # Step 1 — diagnose the live API response (safe, no downloads)
    python bse_annual_reports.py --diagnose

    # Step 2 — run normally once you confirm the output looks right
    python bse_annual_reports.py
"""

import requests
import pandas as pd
import os
import sys
import json
import time
import logging
from pathlib import Path

# ─── Configuration ──────────────────────────────────────────────────────────
OUTPUT_DIR    = Path("annual_reports")
LOG_FILE      = Path("download_log_bse.csv")
COMPANIES_CSV = "companies.csv"
DELAY         = 1.5
MAX_RETRIES   = 3
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# BSE endpoints — tested working as of 2025
BSE_AR_API  = "https://api.bseindia.com/BseIndiaAPI/api/AnnualReport/w?scripcode={code}&type=AR"
BSE_BASE    = "https://www.bseindia.com"

# Headers that BSE accepts
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.bseindia.com/corporates/ann.html",
    "Origin":          "https://www.bseindia.com",
    "Connection":      "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)
# Warm up BSE session — required to get valid cookies
session.get("https://www.bseindia.com", timeout=15)


# ─── Diagnostic: print raw API response ─────────────────────────────────────
def diagnose(scripcode="500325"):
    """Prints the raw BSE API response so you can verify field names."""
    url = BSE_AR_API.format(code=scripcode)
    log.info(f"Diagnostic — fetching: {url}")
    resp = session.get(url, timeout=20)
    log.info(f"HTTP Status: {resp.status_code}")
    log.info(f"Content-Type: {resp.headers.get('Content-Type')}")
    try:
        data = resp.json()
        log.info("Raw JSON response:")
        print(json.dumps(data, indent=2)[:3000])  # first 3000 chars
    except Exception as e:
        log.error(f"Could not parse JSON: {e}")
        log.info(f"Raw text (first 500 chars): {resp.text[:500]}")


# ─── Parse BSE response into normalised report dicts ────────────────────────
def parse_reports(data: dict) -> list[dict]:
    """
    BSE API returns one of several structures depending on company type.
    We handle all known variants here.
    """
    records = []

    # Variant 1: {"Table": [...]}
    if isinstance(data, dict) and "Table" in data:
        records = data["Table"]

    # Variant 2: {"Table1": [...], "Table2": [...]}
    elif isinstance(data, dict) and "Table1" in data:
        records = data["Table1"]

    # Variant 3: direct list
    elif isinstance(data, list):
        records = data

    # Variant 4: single-level dict (rare, single report)
    elif isinstance(data, dict) and any(
        k in data for k in ("FILENAME", "filename", "PDF_NAME", "ANNREP_URL")
    ):
        records = [data]

    results = []
    for r in records:
        if not isinstance(r, dict):
            continue

        # Try all known field name variants for PDF path
        pdf_path = (
            r.get("ANNREP_URL") or
            r.get("FILENAME") or
            r.get("filename") or
            r.get("PDF_NAME") or
            r.get("pdfname") or
            r.get("FileURL") or
            r.get("URL") or
            ""
        ).strip()

        # Try all known year field variants
        year = (
            r.get("YEAR") or
            r.get("year") or
            r.get("FISCAL_YEAR") or
            r.get("FiscalYear") or
            r.get("ANNREP_YEAR") or
            r.get("AnnRepYear") or
            "unknown"
        )

        if pdf_path:
            full_url = (
                pdf_path if pdf_path.startswith("http")
                else BSE_BASE + "/" + pdf_path.lstrip("/")
            )
            results.append({"year": str(year).strip(), "url": full_url})

    return results


# ─── Fetch report links for one company ─────────────────────────────────────
def get_report_links(scripcode: str) -> list[dict]:
    url = BSE_AR_API.format(code=scripcode)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    log.warning(f"  Non-JSON response for {scripcode}: {resp.text[:200]}")
                    return []
                return parse_reports(data)
            elif resp.status_code in (403, 429):
                log.warning(f"  Rate limited (HTTP {resp.status_code}), sleeping 10s...")
                time.sleep(10)
            elif resp.status_code == 404:
                return []
            else:
                log.warning(f"  Attempt {attempt}: HTTP {resp.status_code} for {scripcode}")
        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt}: {e}")
        time.sleep(attempt * 3)
    return []


# ─── Download PDF ────────────────────────────────────────────────────────────
def download_pdf(url: str, dest: Path) -> bool:
    if dest.exists():
        log.info(f"    ↷ Already exists: {dest.name}")
        return True
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=60, stream=True)
            ct = r.headers.get("Content-Type", "").lower()
            if r.status_code == 200 and ("pdf" in ct or "octet" in ct or url.lower().endswith(".pdf")):
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                size_kb = dest.stat().st_size // 1024
                if size_kb < 5:
                    # Suspiciously small — likely an error page, not a PDF
                    dest.unlink()
                    log.warning(f"    File too small ({size_kb}KB), likely not a PDF: {url}")
                    return False
                log.info(f"    ✓ Saved {dest.name} ({size_kb} KB)")
                return True
            else:
                log.warning(f"    Attempt {attempt}: HTTP {r.status_code} / {ct}")
        except requests.RequestException as e:
            log.warning(f"    Attempt {attempt}: {e}")
        time.sleep(attempt * 3)
    return False


# ─── Resume support ──────────────────────────────────────────────────────────
def load_done() -> set:
    if LOG_FILE.exists():
        df = pd.read_csv(LOG_FILE)
        return set(df[df["status"] == "success"]["filename"].tolist())
    return set()


def append_log(rows: list):
    df = pd.DataFrame(rows)
    write_header = not LOG_FILE.exists()
    df.to_csv(LOG_FILE, mode="a", header=write_header, index=False)


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    already_done = load_done()

    df = pd.read_csv(COMPANIES_CSV, dtype={"scripcode": str})
    df["scripcode"] = df["scripcode"].str.strip().str.zfill(6)
    total = len(df)
    log.info(f"Loaded {total} companies")

    success = failed = skipped = 0

    for i, row in df.iterrows():
        code        = row["scripcode"]
        name        = str(row.get("companyname", code)).replace("/", "-").strip()[:40]
        log.info(f"[{i+1}/{total}] {name} ({code})")

        reports = get_report_links(code)
        if not reports:
            log.info(f"  No reports found")
            skipped += 1
            time.sleep(DELAY)
            continue

        log.info(f"  Found {len(reports)} report(s)")
        batch_log = []

        for rep in reports:
            filename = f"{code}_{name}_{rep['year']}.pdf"
            dest     = OUTPUT_DIR / filename

            if filename in already_done:
                log.info(f"  ↷ Skipping: {filename}")
                skipped += 1
                continue

            ok = download_pdf(rep["url"], dest)
            batch_log.append({
                "scripcode":   code,
                "companyname": name,
                "year":        rep["year"],
                "filename":    filename,
                "url":         rep["url"],
                "status":      "success" if ok else "failed",
            })
            success += ok
            failed  += not ok
            time.sleep(DELAY)

        if batch_log:
            append_log(batch_log)
        time.sleep(DELAY)

    log.info("=" * 60)
    log.info(f"Done.  ✓ Success: {success}  ✗ Failed: {failed}  ↷ Skipped: {skipped}")


if __name__ == "__main__":
    if "--diagnose" in sys.argv:
        # Print raw response for Reliance (500325) — change code if needed
        diagnose(scripcode="500325")
    else:
        main()