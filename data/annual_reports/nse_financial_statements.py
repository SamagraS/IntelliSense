"""
NSE Financial Statements Downloader (FY21–FY26)
=================================================
Downloads quarterly financial result PDFs for each company.
These are the announcements companies file each quarter containing
P&L, Balance Sheet and Cash Flow statements.

What this gets you:
  - 4 quarters × 5 years = up to 20 PDFs per company
  - Each PDF contains standalone + consolidated financials
  - Maps directly to your extracted_tables schema (P&L / BS / CF)

Source: NSE corporate-announcements API filtered by financial results subjects.

Requirements:
    pip install "nse[local]" pandas

Output structure:
    raw_reports/indian/{SYMBOL}/financials/{SYMBOL}_Q{q}_{year}.pdf
"""

import pandas as pd
import shutil
import time
import logging
from pathlib import Path
from datetime import datetime, date
from nse import NSE
from config import RAW_REPORT_DIR

# ─── Configuration ──────────────────────────────────────────────────────────
OUTPUT_DIR    = Path(RAW_REPORT_DIR) / "indian_financial"
LOG_FILE      = Path("download_log_financials.csv")
COMPANIES_CSV = "../structured/companies_financial_scenarios.csv"
DELAY         = 2.0

# FY21 starts April 2020, FY26 ends March 2026
DATE_FROM = date(2020, 4, 1)
DATE_TO   = date(2026, 3, 31)

# NSE announcement subjects that contain financial statements
# These are the exact subject strings NSE uses — do not change
FINANCIAL_SUBJECTS = {
    "Financial Results",
    "Quarterly Results",
    "Half Yearly Results",
    "Annual Results",
    "Audited Results",
    "Unaudited Results",
    "Financial Results For Quarter",
}
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def is_financial_announcement(ann: dict) -> bool:
    """Returns True if this announcement contains financial statements."""
    subject = str(ann.get("subject", "") or ann.get("desc", "")).strip()
    # Match exact subjects or partial keywords
    if any(s.lower() in subject.lower() for s in FINANCIAL_SUBJECTS):
        return True
    return False


def load_done() -> set:
    if LOG_FILE.exists():
        df = pd.read_csv(LOG_FILE)
        return set(df[df["status"] == "success"]["filename"].tolist())
    return set()


def append_log(rows: list):
    df = pd.DataFrame(rows)
    write_header = not LOG_FILE.exists()
    df.to_csv(LOG_FILE, mode="a", header=write_header, index=False)


def main():
    already_done = load_done()

    df = pd.read_csv(COMPANIES_CSV)
    df.columns = df.columns.str.strip()
    symbols = df["SYMBOL"].str.strip().str.upper().dropna().tolist()
    log.info(f"Loaded {len(symbols)} symbols | Date range: {DATE_FROM} → {DATE_TO}")

    success = failed = skipped = 0

    with NSE(download_folder=str(OUTPUT_DIR), server=False) as nse:
        for idx, symbol in enumerate(symbols, 1):
            log.info(f"[{idx}/{len(symbols)}] {symbol}")

            fin_dir = OUTPUT_DIR / symbol / "financials"
            fin_dir.mkdir(parents=True, exist_ok=True)

            # ── Fetch all announcements for this symbol in date range ──────
            # announcements() returns list of dicts. Each dict has:
            #   attchmntFile  — URL to the PDF attachment
            #   subject       — announcement type/description
            #   an_dt         — announcement date (YYYY-MM-DD)
            #   symbol        — stock symbol
            try:
                announcements = nse.announcements(
                    index="equities",
                    symbol=symbol,
                    from_date=datetime.combine(DATE_FROM, datetime.min.time()),
                    to_date=datetime.combine(DATE_TO, datetime.min.time()),
                )
            except Exception as e:
                log.warning(f"  API error for {symbol}: {e}")
                skipped += 1
                time.sleep(DELAY)
                continue

            if not announcements:
                log.info(f"  No announcements found for {symbol}")
                skipped += 1
                time.sleep(DELAY)
                continue

            # ── Filter to financial results only ──────────────────────────
            financial_anns = [a for a in announcements if is_financial_announcement(a)]
            log.info(f"  {len(financial_anns)} financial result announcements (of {len(announcements)} total)")

            if not financial_anns:
                skipped += 1
                time.sleep(DELAY)
                continue

            batch_log = []

            for ann in financial_anns:
                pdf_url  = str(ann.get("attchmntFile", "")).strip()
                ann_date = str(ann.get("an_dt", "unknown")).strip()[:10]  # YYYY-MM-DD
                subject  = str(ann.get("subject", "results")).strip()[:60].replace("/", "-")

                if not pdf_url or ".pdf" not in pdf_url.lower():
                    continue  # Skip entries without a PDF

                # Clean filename: SYMBOL_YYYY-MM-DD_subject.pdf
                safe_subject = "".join(c if c.isalnum() or c in "-_ " else "" for c in subject)
                filename     = f"{symbol}_{ann_date}_{safe_subject[:40].strip()}.pdf"
                target_path  = fin_dir / filename

                if filename in already_done or target_path.exists():
                    log.info(f"  Already downloaded: {filename}")
                    skipped += 1
                    continue

                log.info(f"  Downloading: {filename}")
                status = "failed"
                try:
                    extracted = nse.download_document(url=pdf_url)
                    if extracted and Path(extracted).exists():
                        shutil.move(str(extracted), str(target_path))
                        size_kb = target_path.stat().st_size // 1024
                        log.info(f"  ✓ Saved ({size_kb} KB)")
                        status = "success"
                        success += 1
                    else:
                        log.warning(f"  download_document returned no file")
                        failed += 1
                except Exception as e:
                    log.error(f"  Exception: {e}")
                    failed += 1

                batch_log.append({
                    "symbol":   symbol,
                    "date":     ann_date,
                    "subject":  subject,
                    "filename": filename,
                    "url":      pdf_url,
                    "status":   status,
                })
                time.sleep(DELAY)

            if batch_log:
                append_log(batch_log)
            time.sleep(DELAY)

    log.info("=" * 60)
    log.info(f"Done.  ✓ {success}  ✗ {failed}  ↷ {skipped}")
    log.info(f"Log: {LOG_FILE}")


if __name__ == "__main__":
    main()