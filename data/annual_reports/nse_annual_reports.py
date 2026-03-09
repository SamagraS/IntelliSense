"""
NSE Annual Report Downloader — CORRECT IMPLEMENTATION
=======================================================
Key facts from official NSE library docs:
  - annual_reports(symbol, segment) → {"data": [{"fromYear": "2023",
                                                  "toYear": "2024",
                                                  "fileName": "https://...zip"}]}
  - fileName points to a ZIP archive, NOT a PDF directly
  - download_document(url) extracts the zip and returns the PDF path
  - Do NOT pass folder= to download_document — it breaks the return path
  - Use shutil.move() to put the PDF into the company subfolder

Requirements:
    pip install "nse[local]" pandas

Usage:
    python nse_annual_reports.py
"""

import pandas as pd
import shutil
import time
import logging
from pathlib import Path
from nse import NSE
from config import RAW_REPORT_DIR

# ─── Configuration ──────────────────────────────────────────────────────────
OUTPUT_DIR    = Path(RAW_REPORT_DIR) / "indian"
LOG_FILE      = Path("download_log_nse.csv")
COMPANIES_CSV = "../structured/companies_financial_scenarios.csv"
DELAY         = 2.0
MIN_YEAR      = 2021
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


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
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    already_done = load_done()

    df = pd.read_csv(COMPANIES_CSV)
    df.columns = df.columns.str.strip()
    symbols = df["SYMBOL"].str.strip().str.upper().dropna().tolist()
    log.info(f"Loaded {len(symbols)} symbols from {COMPANIES_CSV}")

    success = failed = skipped = 0

    # download_folder is where the library saves cookies AND temp downloads.
    # We then move each file into per-company subfolders ourselves.
    with NSE(download_folder=str(OUTPUT_DIR), server=False) as nse:
        for idx, symbol in enumerate(symbols, 1):
            log.info(f"[{idx}/{len(symbols)}] {symbol}")

            company_dir = OUTPUT_DIR / symbol
            company_dir.mkdir(exist_ok=True)

            # ── 1. Get metadata ────────────────────────────────────────────
            # Returns: {"data": [{"fromYear": "2023", "toYear": "2024",
            #           "fileName": "https://archives.nseindia.com/.../AR_XXX.zip"}]}
            try:
                result = nse.annual_reports(symbol=symbol, segment="equities")
            except Exception as e:
                log.warning(f"  API error for {symbol}: {e}")
                skipped += 1
                time.sleep(DELAY)
                continue

            if not isinstance(result, dict) or not result.get("data"):
                log.info(f"  No reports found for {symbol}")
                skipped += 1
                time.sleep(DELAY)
                continue

            reports = result["data"]
            log.info(f"  Found {len(reports)} report(s)")

            batch_log = []

            for report in reports:
                from_year = str(report.get("fromYear", "")).strip()
                to_year   = str(report.get("toYear",   "")).strip()
                zip_url   = str(report.get("fileName", "")).strip()

                if not zip_url:
                    log.warning(f"  Empty fileName for {symbol} {from_year}-{to_year}")
                    continue

                # ── 2. Year filter ─────────────────────────────────────────
                try:
                    if int(to_year or from_year) < MIN_YEAR:
                        log.info(f"  Skipping old report {from_year}-{to_year}")
                        skipped += 1
                        continue
                except (ValueError, TypeError):
                    pass  # Unknown year format — try downloading anyway

                # ── 3. Resume check ────────────────────────────────────────
                target_name = f"{symbol}_{from_year}_{to_year}.pdf"
                target_path = company_dir / target_name

                if target_name in already_done or target_path.exists():
                    log.info(f"  Already downloaded: {target_name}")
                    skipped += 1
                    continue

                log.info(f"  Downloading {from_year}-{to_year} ...")

                # ── 4. Download + extract ──────────────────────────────────
                # IMPORTANT: call download_document WITHOUT folder= argument.
                # The library saves to download_folder (OUTPUT_DIR) and returns
                # the path of the extracted PDF. Then we move it ourselves.
                status = "failed"
                try:
                    extracted = nse.download_document(url=zip_url)

                    if extracted and Path(extracted).exists():
                        shutil.move(str(extracted), str(target_path))
                        size_kb = target_path.stat().st_size // 1024
                        log.info(f"  ✓ Saved: {target_name} ({size_kb} KB)")
                        status = "success"
                        success += 1
                    else:
                        log.warning(f"  download_document returned no file for {zip_url}")
                        failed += 1

                except Exception as e:
                    log.error(f"  Exception for {target_name}: {e}")
                    failed += 1

                batch_log.append({
                    "symbol":    symbol,
                    "from_year": from_year,
                    "to_year":   to_year,
                    "filename":  target_name,
                    "url":       zip_url,
                    "status":    status,
                })
                time.sleep(DELAY)

            if batch_log:
                append_log(batch_log)

            time.sleep(DELAY)

    log.info("=" * 60)
    log.info(f"Done.  OK:{success}  Failed:{failed}  Skipped:{skipped}")
    log.info(f"Log: {LOG_FILE}")


if __name__ == "__main__":
    main()