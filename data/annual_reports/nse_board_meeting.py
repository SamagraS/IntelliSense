"""
NSE Board Meeting Outcomes Downloader (FY21–FY26)
==================================================
⚠️  IMPORTANT DISTINCTION — there are TWO things people mean by "board minutes":

1. BOARD MEETING OUTCOMES (what this script downloads):
   — Filed on NSE same-day as the meeting
   — Contains: financial results approved, dividend declared, director changes,
     fund raises, RPTs approved, auditor appointments
   — Available as PDF via nse.announcements() filtered by "Board Meeting" subject
   — THIS IS WHAT YOUR 5C MODEL NEEDS (Character C: RPT approvals, governance)

2. VERBATIM BOARD MINUTES (full transcript of the meeting):
   — Filed with MCA/ROC as Form MGT-14 within 30 days
   — NOT available on NSE API
   — Requires MCA21 portal access (paid, ₹100/document)

This script downloads type #1 — board meeting outcome PDFs from NSE.

Requirements:
    pip install "nse[local]" pandas

Output structure:
    raw_reports/indian/{SYMBOL}/board_meetings/{SYMBOL}_{date}_{subject}.pdf
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
OUTPUT_DIR    = Path(RAW_REPORT_DIR) / "indian_board"
LOG_FILE      = Path("download_log_board_meetings.csv")
COMPANIES_CSV = "../structured/companies_financial_scenarios.csv"
DELAY         = 2.0

DATE_FROM = date(2020, 4, 1)   # FY21 start
DATE_TO   = date(2026, 3, 31)  # FY26 end

# NSE subject strings that relate to board meetings
BOARD_SUBJECTS = {
    "Board Meeting",
    "Outcome of Board Meeting",
    "Board Meeting Outcome",
    "Outcome Of Board Meeting",
    "Board Meeting Intimation",
    "Board Meeting- Intimation",
}
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def is_board_meeting(ann: dict) -> bool:
    subject = str(ann.get("subject", "") or ann.get("desc", "")).lower()
    return "board meeting" in subject


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

            bm_dir = OUTPUT_DIR / symbol / "board_meetings"
            bm_dir.mkdir(parents=True, exist_ok=True)

            # ── Fetch all announcements in date range ─────────────────────
            # Each dict contains:
            #   attchmntFile  — PDF URL (the outcome document)
            #   subject       — e.g. "Outcome of Board Meeting"
            #   an_dt         — announcement date
            #   desc          — longer description of what was decided
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
                log.info(f"  No announcements for {symbol}")
                skipped += 1
                time.sleep(DELAY)
                continue

            # ── Filter to board meeting outcomes only ─────────────────────
            bm_anns = [a for a in announcements if is_board_meeting(a)]
            log.info(f"  {len(bm_anns)} board meeting announcements (of {len(announcements)} total)")

            if not bm_anns:
                skipped += 1
                time.sleep(DELAY)
                continue

            batch_log = []

            for ann in bm_anns:
                pdf_url  = str(ann.get("attchmntFile", "")).strip()
                ann_date = str(ann.get("an_dt", "unknown")).strip()[:10]
                subject  = str(ann.get("subject", "board_meeting")).strip()[:50].replace("/", "-")

                # Some board meeting announcements are just metadata (no PDF)
                # We still log them so you know they exist
                has_pdf = pdf_url and ".pdf" in pdf_url.lower()

                if not has_pdf:
                    log.info(f"  No PDF attachment for {ann_date} — metadata only, logging")
                    batch_log.append({
                        "symbol":   symbol,
                        "date":     ann_date,
                        "subject":  subject,
                        "filename": "NO_PDF",
                        "url":      pdf_url or "none",
                        "desc":     str(ann.get("desc", ""))[:200],
                        "status":   "no_pdf",
                    })
                    continue

                safe_subject = "".join(c if c.isalnum() or c in "-_ " else "" for c in subject)
                filename     = f"{symbol}_{ann_date}_{safe_subject[:40].strip()}.pdf"
                target_path  = bm_dir / filename

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
                    "desc":     str(ann.get("desc", ""))[:200],
                    "status":   status,
                })
                time.sleep(DELAY)

            if batch_log:
                append_log(batch_log)
            time.sleep(DELAY)

    log.info("=" * 60)
    log.info(f"Done.  ✓ {success} downloaded  ✗ {failed} failed  ↷ {skipped} skipped")
    log.info(f"Log: {LOG_FILE}")
    log.info("")
    log.info("NOTE: For full verbatim board minutes, access MCA21 portal:")
    log.info("  https://www.mca.gov.in/mcafoportal/viewCompanyMasterData.do")
    log.info("  File Form MGT-14 filings — ₹100 per document")


if __name__ == "__main__":
    main()