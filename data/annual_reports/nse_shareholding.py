"""
NSE Shareholding Pattern — Full Pipeline (FY21–FY26)
======================================================
Does everything in one script:

  1. STRUCTURED DATA via nse.shareholding()
     Returns parsed JSON with promoter %, pledged %, institutional %, public %
     for every quarter on record.

  2. SOURCE FILE DOWNLOADS via nse.announcements()
     Downloads the actual Excel/PDF/XBRL filed with NSE per quarter.
     These are the source documents for your OCR pipeline.

  3. ANALYTICS — computed inline, written to CSV:
     a) shareholding_pattern_quarterly.csv   → your DB table
     b) promoter_pledge_analysis.csv         → risk flags + QoQ change + trend
     c) ownership_changes.csv                → significant >2% moves quarter-on-quarter

Risk flag thresholds (per spec):
    < 40%  = normal
    40-60% = caution
    60-75% = high_risk
    > 75%  = critical

Requirements:
    pip install "nse[local]" pandas

Output:
    raw_reports/indian/{SYMBOL}/shareholding/{SYMBOL}_SHP_{YYYY-MM-DD}.xlsx
    shareholding_pattern_quarterly.csv
    promoter_pledge_analysis.csv
    ownership_changes.csv
"""

import uuid
import shutil
import time
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from nse import NSE

try:
    from config import RAW_REPORT_DIR
    OUTPUT_DIR = Path(RAW_REPORT_DIR) / "indian_shareholding"
except ImportError:
    OUTPUT_DIR = Path("annual_reports")

COMPANIES_CSV = "../structured/companies_financial_scenarios.csv"
DELAY         = 2.0
DATE_FROM     = date(2020, 4, 1)   # FY21 start
DATE_TO       = date(2026, 3, 31)  # FY26 end

# Output CSVs — map directly to DB tables
SHP_CSV      = Path("shareholding_pattern_quarterly.csv")
PLEDGE_CSV   = Path("promoter_pledge_analysis.csv")
CHANGES_CSV  = Path("ownership_changes.csv")
DL_LOG_CSV   = Path("download_log_shp.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─── Risk flag calculation ───────────────────────────────────────────────────
def pledge_risk_flag(pledged_pct: float) -> str:
    if pledged_pct < 40:   return "normal"
    if pledged_pct < 60:   return "caution"
    if pledged_pct < 75:   return "high_risk"
    return "critical"


# ─── Shareholding JSON field parser ─────────────────────────────────────────
def parse_shareholding_record(symbol: str, rec: dict) -> dict | None:
    """
    Normalise one quarter's shareholding record from NSE API response.
    NSE returns nested dicts per shareholder category. Field names extracted
    from live API response structure.
    """
    try:
        # NSE API returns these at top level or inside category dicts
        # Try multiple known field name variants (API has changed over versions)
        def get(d, *keys, default=0.0):
            for k in keys:
                v = d.get(k)
                if v is not None:
                    try:
                        return float(str(v).replace(",", "").replace("%", "") or 0)
                    except (ValueError, TypeError):
                        pass
            return default

        filing_date = str(
            rec.get("date") or rec.get("quarter") or rec.get("dateOfFiling", "")
        ).strip()[:10]

        # Promoter category
        promoter_pct = get(rec,
            "promoterAndPromoterGroupShareHolding",
            "promoterHolding",
            "promoter_pct",
            "categoryOnePercentage",
        )

        # Pledged — this field is CRITICAL
        # Can be stored as % of promoter shares pledged OR % of total shares
        # NSE typically stores as % of promoter shares pledged
        pledged_raw = get(rec,
            "pledgedSharesPercentage",
            "promoterSharesPledgedPct",
            "pledgedShares",
            "encumberedShares",
            "percentageOfSharesPledged",
        )

        # Institutional (FII + DII combined)
        institutional_pct = get(rec,
            "institutionalShareHolding",
            "institutionalHolding",
            "diiHolding",
        )
        # Try adding FII + DII separately if combined field absent
        if institutional_pct == 0.0:
            fii = get(rec, "fiiHolding", "foreignPortfolioInvestors", "fiis")
            dii = get(rec, "diiHolding", "mutualFunds", "diis")
            institutional_pct = fii + dii

        # Public / Retail
        public_pct = get(rec,
            "publicShareHolding",
            "publicHolding",
            "retailHolding",
        )

        if not filing_date:
            return None

        return {
            "filing_id":                str(uuid.uuid4()),
            "company_id":               symbol,
            "filing_date":              filing_date,
            "promoter_holding_pct":     round(promoter_pct, 2),
            "promoter_shares_pledged_pct": round(pledged_raw, 2),
            "institutional_holding_pct": round(institutional_pct, 2),
            "public_holding_pct":        round(public_pct, 2),
            "source_document_id":        "",   # filled after download
        }
    except Exception as e:
        log.warning(f"    Parse error for record: {e} — raw: {rec}")
        return None


# ─── Pledge analysis — QoQ change, trend, risk flag ─────────────────────────
def build_pledge_analysis(symbol: str, shp_rows: list[dict]) -> list[dict]:
    """Computes promoter_pledge_analysis rows from a list of quarterly records."""
    rows = sorted(shp_rows, key=lambda r: r["filing_date"])
    analysis = []
    prev_pledged = None
    for row in rows:
        pledged = row["promoter_shares_pledged_pct"]
        qoq     = round(pledged - prev_pledged, 2) if prev_pledged is not None else 0.0

        if prev_pledged is None or abs(qoq) < 0.5:
            trend = "stable"
        elif qoq > 0:
            trend = "increasing"
        else:
            trend = "decreasing"

        analysis.append({
            "company_id":                    symbol,
            "filing_date":                   row["filing_date"],
            "promoter_pledged_pct":          pledged,
            "risk_flag":                     pledge_risk_flag(pledged),
            "qoq_change_percentage_points":  qoq,
            "trend":                         trend,
            "analysis_timestamp":            datetime.utcnow().isoformat(),
        })
        prev_pledged = pledged
    return analysis


# ─── Ownership changes — compare consecutive quarters ───────────────────────
def build_ownership_changes(symbol: str, shp_rows: list[dict]) -> list[dict]:
    """Detects significant (>2%) QoQ changes in each ownership category."""
    rows   = sorted(shp_rows, key=lambda r: r["filing_date"])
    changes = []

    CATEGORIES = [
        ("promoter_holding_pct", "promoter_sale",         "promoter_purchase"),
        ("institutional_holding_pct", "institutional_exit", "institutional_entry"),
        ("public_holding_pct", None, None),
    ]

    for i in range(1, len(rows)):
        cur  = rows[i]
        prev = rows[i - 1]

        for col, neg_type, pos_type in CATEGORIES:
            delta = round(cur[col] - prev[col], 2)
            if abs(delta) < 0.5:   # below noise threshold — skip
                continue

            # Determine change type
            if delta < 0 and neg_type:
                ctype = neg_type
            elif delta > 0 and pos_type:
                ctype = pos_type
            else:
                ctype = "other"

            changes.append({
                "change_id":             str(uuid.uuid4()),
                "company_id":            symbol,
                "filing_date":           cur["filing_date"],
                "change_type":           ctype,
                "entity_name":           symbol,
                "shares_transacted_pct": abs(delta),
                "is_significant_change": abs(delta) >= 2.0,
                "prev_pct":              prev[col],
                "curr_pct":              cur[col],
                "category":              col,
            })

    return changes


# ─── Append to CSV ───────────────────────────────────────────────────────────
def append_csv(rows: list[dict], filepath: Path):
    if not rows:
        return
    df = pd.DataFrame(rows)
    write_header = not filepath.exists()
    df.to_csv(filepath, mode="a", header=write_header, index=False)
    log.info(f"    → {len(rows)} rows written to {filepath.name}")


# ─── SHP announcement download ───────────────────────────────────────────────
def download_shp_files(nse_client, symbol: str, shp_dir: Path, done: set) -> list[dict]:
    """
    Downloads the raw SHP Excel/PDF attachments via announcements() filtered
    to 'Shareholding Pattern' subject. Returns log rows.
    """
    dl_log = []
    try:
        anns = nse_client.announcements(
            index="equities",
            symbol=symbol,
            from_date=datetime.combine(DATE_FROM, datetime.min.time()),
            to_date=datetime.combine(DATE_TO, datetime.min.time()),
        )
        log.info(f"    Announcements API returned {len(anns or [])} results")
    except Exception as e:
        log.warning(f"    announcements() error for {symbol}: {e}")
        return []

    shp_anns = [
        a for a in (anns or [])
        if "shareholding" in str(a.get("subject", "")).lower()
    ]
    log.info(f"    Found {len(shp_anns)} shareholding announcements")

    for ann in shp_anns:
        file_url = str(ann.get("attchmntFile", "")).strip()
        ann_date = str(ann.get("an_dt", "unknown")).strip()[:10]

        if not file_url:
            continue

        ext      = ".pdf" if ".pdf" in file_url.lower() else ".xlsx"
        filename = f"{symbol}_SHP_{ann_date}{ext}"
        dest     = shp_dir / filename

        if filename in done or dest.exists():
            continue

        status = "failed"
        try:
            extracted = nse_client.download_document(url=file_url)
            if extracted and Path(extracted).exists():
                shutil.move(str(extracted), str(dest))
                log.info(f"    ✓ {filename} ({dest.stat().st_size // 1024} KB)")
                status = "success"
        except Exception as e:
            log.error(f"    Download error {filename}: {e}")

        dl_log.append({
            "symbol":   symbol,
            "date":     ann_date,
            "filename": filename,
            "url":      file_url,
            "status":   status,
        })
        time.sleep(1.5)

    return dl_log


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    df_companies = pd.read_csv(COMPANIES_CSV)
    df_companies.columns = df_companies.columns.str.strip()
    symbols = df_companies["SYMBOL"].str.strip().str.upper().dropna().tolist()
    log.info(f"Loaded {len(symbols)} symbols")

    done = set()
    if DL_LOG_CSV.exists():
        done_df = pd.read_csv(DL_LOG_CSV)
        done = set(done_df[done_df["status"] == "success"]["filename"].tolist())

    total_shp = total_pledge = total_changes = 0

    with NSE(download_folder=str(OUTPUT_DIR), server=False) as nse:
        for idx, symbol in enumerate(symbols, 1):
            log.info(f"[{idx}/{len(symbols)}] {symbol}")

            shp_dir = OUTPUT_DIR / symbol / "shareholding"
            shp_dir.mkdir(parents=True, exist_ok=True)

            # ── 1. Get structured shareholding data ────────────────────────
            # nse.shareholding() returns a dict with keys like:
            #   "data": [list of quarterly records]
            # Each record has promoter, pledged, institutional, public fields
            shp_rows     = []
            raw_response = None

            try:
                raw_response = nse.shareholding(symbol=symbol)
            except Exception as e:
                log.warning(f"  shareholding() error: {e}")

            if isinstance(raw_response, dict):
                records = (
                    raw_response.get("data")
                    or raw_response.get("shareholdingPatterns")
                    or raw_response.get("shareholding")
                    or []
                )
            elif isinstance(raw_response, list):
                records = raw_response
            else:
                records = []

            if not records:
                log.info(f"  No structured shareholding data returned")
            else:
                log.info(f"  {len(records)} quarterly records returned")
                for rec in records:
                    parsed = parse_shareholding_record(symbol, rec)
                    if parsed:
                        # Filter to FY21-FY26
                        try:
                            rec_date = datetime.strptime(parsed["filing_date"], "%Y-%m-%d").date()
                            if not (DATE_FROM <= rec_date <= DATE_TO):
                                continue
                        except ValueError:
                            pass
                        shp_rows.append(parsed)

            # ── 2. Write structured CSVs ───────────────────────────────────
            if shp_rows:
                # a) shareholding_pattern_quarterly
                append_csv(shp_rows, SHP_CSV)
                total_shp += len(shp_rows)

                # b) promoter_pledge_analysis
                pledge_rows = build_pledge_analysis(symbol, shp_rows)
                append_csv(pledge_rows, PLEDGE_CSV)
                total_pledge += len(pledge_rows)

                # c) ownership_changes
                change_rows = build_ownership_changes(symbol, shp_rows)
                append_csv(change_rows, CHANGES_CSV)
                total_changes += len(change_rows)

                # Log any critical or high_risk quarters
                for pr in pledge_rows:
                    if pr["risk_flag"] in ("critical", "high_risk"):
                        log.warning(
                            f"  ⚠️  {pr['filing_date']} | Pledged: {pr['promoter_pledged_pct']}% "
                            f"| Flag: {pr['risk_flag'].upper()} | Trend: {pr['trend']}"
                        )
            else:
                log.info(f"  No FY21-FY26 records to write")

            # ── 3. Download raw SHP files (Excel/PDF) ──────────────────────
            log.info(f"  Fetching shareholding file announcements...")
            dl_log = download_shp_files(nse, symbol, shp_dir, done)
            if dl_log:
                append_csv(dl_log, DL_LOG_CSV)
                log.info(f"  📥 Logged {len(dl_log)} download attempts")
            else:
                log.info(f"  No new files to download")

            time.sleep(DELAY)

    # ── Summary ───────────────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info(f"Shareholding rows written:     {total_shp}  → {SHP_CSV}")
    log.info(f"Pledge analysis rows written:  {total_pledge}  → {PLEDGE_CSV}")
    log.info(f"Ownership change rows written: {total_changes}  → {CHANGES_CSV}")
    log.info("")
    log.info("CRITICAL FLAGS SUMMARY:")
    if PLEDGE_CSV.exists():
        pdf = pd.read_csv(PLEDGE_CSV)
        critical = pdf[pdf["risk_flag"].isin(["critical", "high_risk"])][
            ["company_id", "filing_date", "promoter_pledged_pct", "risk_flag", "trend"]
        ].sort_values("promoter_pledged_pct", ascending=False)
        if not critical.empty:
            log.info(f"\n{critical.to_string(index=False)}")
        else:
            log.info("  No critical/high_risk pledge flags found.")


if __name__ == "__main__":
    main()