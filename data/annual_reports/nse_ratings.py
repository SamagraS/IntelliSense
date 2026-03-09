"""
Direct CRA Rationale Scraper — CRISIL + ICRA + CARE
=====================================================
Supplements nse_credit_ratings.py for companies WITHOUT listed debt
(no NSE announcements). Also fills gaps where NSE attachments are missing.

Agency-by-agency source map:
  ┌─────────────────┬─────────────────────────────────────────────────────┐
  │ CRISIL          │ JS-rendered search → Playwright → rationale HTML    │
  │                 │ Rationale pages are PUBLIC (no login)                │
  │                 │ URL: crisilratings.com/mnt/winshare/Ratings/         │
  │                 │      RatingList/RatingDocs/{name}_{date}_RR_{id}.html│
  ├─────────────────┼─────────────────────────────────────────────────────┤
  │ ICRA            │ JS-rendered search → Playwright → extract IDs        │
  │                 │ PDF: icra.in/Rating/GetRationalReportFilePdf?id=<id> │
  │                 │ CONFIRMED PUBLIC — no auth required                  │
  ├─────────────────┼─────────────────────────────────────────────────────┤
  │ CARE/CareEdge   │ JS-rendered search → Playwright → press release PDFs│
  │                 │ URL: careedge.in/ratings/press-release               │
  └─────────────────┴─────────────────────────────────────────────────────┘

Why Playwright (not requests):
  All three sites render their search results via JavaScript/React.
  Plain requests get an empty HTML shell. Playwright runs a real browser
  headlessly so JS executes and results load.

Requirements:
    pip install playwright pandas
    playwright install chromium

Usage:
    python cra_direct_scraper.py
"""

import asyncio
import json
import re
import time
import logging
import requests
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from config import RAW_REPORT_DIR
    OUTPUT_DIR = Path(RAW_REPORT_DIR) / "indian_ratings"
except ImportError:
    OUTPUT_DIR = Path("annual_reports")

LOG_FILE      = Path("download_log_cra_direct.csv")
INDEX_FILE    = Path("cra_direct_index.csv")
COMPANIES_CSV = "../structured/companies_financial_scenarios.csv"
DELAY         = 3.0    # seconds between searches — be respectful

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ── Download helpers ─────────────────────────────────────────────────────────
def download_pdf(url: str, dest: Path, session: requests.Session) -> bool:
    """Download any PDF URL to dest path."""
    if dest.exists():
        log.info(f"    Already exists: {dest.name}")
        return True
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
                dest.unlink()
                log.warning(f"    File too small ({size_kb}KB) — not a valid PDF")
                return False
            log.info(f"    ✓ {dest.name} ({size_kb} KB)")
            return True
        else:
            log.warning(f"    HTTP {r.status_code} / {ct} for {url}")
    except Exception as e:
        log.error(f"    Download error: {e}")
    return False


def download_html_as_text(url: str, dest: Path, session: requests.Session) -> bool:
    """Download CRISIL rationale HTML pages (they serve as plain HTML, not PDF)."""
    if dest.exists():
        return True
    try:
        r = session.get(url, timeout=30)
        if r.status_code == 200 and len(r.text) > 500:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(r.text, encoding="utf-8", errors="replace")
            log.info(f"    ✓ {dest.name} ({len(r.text)//1024} KB HTML)")
            return True
    except Exception as e:
        log.error(f"    HTML download error: {e}")
    return False


def load_done() -> set:
    if LOG_FILE.exists():
        df = pd.read_csv(LOG_FILE)
        return set(df[df["status"] == "success"]["filename"].tolist())
    return set()


def append_log(rows: list, filepath: Path):
    if not rows:
        return
    df = pd.DataFrame(rows)
    write_header = not filepath.exists()
    df.to_csv(filepath, mode="a", header=write_header, index=False)


# ── CRISIL Scraper ───────────────────────────────────────────────────────────
async def scrape_crisil(page, company_name: str) -> list[dict]:
    """
    Search CRISIL rating list for company and return list of rationale links.
    Returns: [{"company": str, "date": str, "rating": str, "url": str}]
    """
    results = []
    try:
        await page.goto(
            "https://www.crisilratings.com/en/home/our-business/ratings/credit-ratings-list.html",
            wait_until="networkidle", timeout=30000
        )
        # Type company name into the search input
        await page.fill('input[placeholder*="Company"], input[placeholder*="company"], input[name*="company"]', company_name)
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(3000)

        # Try to wait for results table/list
        try:
            await page.wait_for_selector("table tr, .rating-list-item, .company-row", timeout=10000)
        except PWTimeout:
            log.warning(f"  CRISIL: No results loaded for {company_name}")
            return []

        # Extract all links pointing to rationale pages
        links = await page.eval_on_selector_all(
            'a[href*="RatingDocs"], a[href*="RR_"], a[href*="rating-rationale"]',
            "els => els.map(e => ({href: e.href, text: e.innerText.trim()}))"
        )

        for link in links:
            url  = link.get("href", "")
            text = link.get("text", "")
            # Extract date and rating from URL or text
            date_match   = re.search(r'(\w+ \d+,? \d{4}|\d{4}-\d{2}-\d{2})', url + " " + text)
            rating_match = re.search(r'CRISIL\s+([A-D]{1,3}[+\-]?(?:\(SO\))?)', text.upper())
            results.append({
                "company": company_name,
                "agency":  "CRISIL",
                "date":    date_match.group(1) if date_match else "unknown",
                "rating":  rating_match.group(1) if rating_match else "",
                "url":     url,
                "type":    "html",  # CRISIL serves rationales as HTML, not PDF
            })

        log.info(f"  CRISIL: Found {len(results)} rationale links for {company_name}")

    except Exception as e:
        log.warning(f"  CRISIL scrape error for {company_name}: {e}")

    return results


# ── ICRA Scraper ─────────────────────────────────────────────────────────────
async def scrape_icra(page, company_name: str) -> list[dict]:
    """
    Search ICRA rating list and extract PDF IDs.
    ICRA PDF endpoint (confirmed working, no auth):
      https://www.icra.in/Rating/GetRationalReportFilePdf?id=<id>
    """
    results = []
    try:
        await page.goto(
            "https://www.icra.in/Rating/Index",
            wait_until="networkidle", timeout=30000
        )
        # Fill search field
        await page.fill(
            'input[placeholder*="Company"], input[id*="search"], input[type="search"], #txtCompanyName',
            company_name
        )
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(3000)

        try:
            await page.wait_for_selector(
                'a[href*="GetRationalReport"], .rationale-link, table.rating-table tr',
                timeout=10000
            )
        except PWTimeout:
            log.warning(f"  ICRA: No results for {company_name}")
            return []

        # Intercept any XHR that returns rating IDs
        # Also look for links directly in DOM
        links = await page.eval_on_selector_all(
            'a[href*="GetRationalReport"], a[href*="id="]',
            "els => els.map(e => ({href: e.href, text: e.innerText.trim()}))"
        )

        for link in links:
            href = link.get("href", "")
            # Extract numeric ID from URL
            id_match = re.search(r'id=(\d+)', href)
            if id_match:
                report_id = id_match.group(1)
                pdf_url   = f"https://www.icra.in/Rating/GetRationalReportFilePdf?id={report_id}"
                results.append({
                    "company": company_name,
                    "agency":  "ICRA",
                    "date":    "unknown",
                    "rating":  "",
                    "url":     pdf_url,
                    "type":    "pdf",
                    "report_id": report_id,
                })

        log.info(f"  ICRA: Found {len(results)} rationale PDFs for {company_name}")

    except Exception as e:
        log.warning(f"  ICRA scrape error for {company_name}: {e}")

    return results


# ── CARE / CareEdge Scraper ──────────────────────────────────────────────────
async def scrape_care(page, company_name: str) -> list[dict]:
    """
    Search CARE (CareEdge) ratings for company and extract press release PDFs.
    """
    results = []
    try:
        await page.goto(
            "https://www.careedge.in/ratings/press-release",
            wait_until="networkidle", timeout=30000
        )
        # Fill search
        await page.fill(
            'input[placeholder*="Company"], input[placeholder*="Search"], input[type="search"]',
            company_name
        )
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(3000)

        try:
            await page.wait_for_selector(
                'a[href*=".pdf"], .press-release-link, .rating-press-release',
                timeout=10000
            )
        except PWTimeout:
            log.warning(f"  CARE: No results for {company_name}")
            return []

        links = await page.eval_on_selector_all(
            'a[href*=".pdf"]',
            "els => els.map(e => ({href: e.href, text: e.innerText.trim()}))"
        )

        for link in links:
            url  = link.get("href", "")
            text = link.get("text", "")
            if "careratings" in url or "careedge" in url:
                results.append({
                    "company": company_name,
                    "agency":  "CARE",
                    "date":    "unknown",
                    "rating":  "",
                    "url":     url,
                    "type":    "pdf",
                })

        log.info(f"  CARE: Found {len(results)} press releases for {company_name}")

    except Exception as e:
        log.warning(f"  CARE scrape error for {company_name}: {e}")

    return results


# ── Main ─────────────────────────────────────────────────────────────────────
async def main():
    already_done = load_done()

    df = pd.read_csv(COMPANIES_CSV)
    df.columns = df.columns.str.strip()
    # Use company name for CRA search (they don't index by NSE symbol)
    # Your CSV should have a COMPANY_NAME column; fall back to SYMBOL
    name_col = "COMPANY_NAME" if "COMPANY_NAME" in df.columns else "SYMBOL"
    companies = df[name_col].str.strip().dropna().tolist()
    symbols   = df["SYMBOL"].str.strip().str.upper().tolist()

    http_session = requests.Session()
    http_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    })

    dl_batch  = []
    idx_batch = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
        )
        page = await context.new_page()

        total = len(companies)
        for i, (company, symbol) in enumerate(zip(companies, symbols), 1):
            log.info(f"[{i}/{total}] {company} ({symbol})")

            cra_dir = OUTPUT_DIR / symbol / "credit_ratings" / "cra_direct"
            cra_dir.mkdir(parents=True, exist_ok=True)

            # Aggregate results from all three agencies
            all_results = []

            crisil_results = await scrape_crisil(page, company)
            all_results.extend(crisil_results)
            await asyncio.sleep(DELAY)

            icra_results = await scrape_icra(page, company)
            all_results.extend(icra_results)
            await asyncio.sleep(DELAY)

            care_results = await scrape_care(page, company)
            all_results.extend(care_results)
            await asyncio.sleep(DELAY)

            log.info(f"  Total: {len(all_results)} rationale documents across all agencies")

            for result in all_results:
                agency    = result["agency"]
                url       = result["url"]
                file_date = result.get("date", "unknown")[:10].replace(" ", "-").replace(",", "")
                file_type = result.get("type", "pdf")
                ext       = ".html" if file_type == "html" else ".pdf"
                filename  = f"{symbol}_{agency}_{file_date}{ext}"
                target    = cra_dir / filename

                # Always log metadata (even if no download)
                idx_batch.append({
                    "symbol":    symbol,
                    "company":   company,
                    "agency":    agency,
                    "date":      file_date,
                    "rating":    result.get("rating", ""),
                    "url":       url,
                    "type":      file_type,
                    "filename":  filename,
                })

                if filename in already_done or target.exists():
                    continue

                # Download
                ok = False
                if file_type == "html":
                    ok = download_html_as_text(url, target, http_session)
                else:
                    ok = download_pdf(url, target, http_session)

                dl_batch.append({
                    "symbol":   symbol,
                    "agency":   agency,
                    "date":     file_date,
                    "filename": filename,
                    "url":      url,
                    "status":   "success" if ok else "failed",
                })
                time.sleep(1)

        await browser.close()

    append_log(dl_batch,  LOG_FILE)
    append_log(idx_batch, INDEX_FILE)

    success = sum(1 for r in dl_batch if r["status"] == "success")
    failed  = len(dl_batch) - success
    log.info("=" * 60)
    log.info(f"Done.  ✓ {success}  ✗ {failed}")
    log.info(f"Metadata index: {INDEX_FILE}")
    log.info(f"Download log:   {LOG_FILE}")


if __name__ == "__main__":
    asyncio.run(main())