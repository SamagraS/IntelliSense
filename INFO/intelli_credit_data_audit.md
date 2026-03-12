# INTELLI-CREDIT — Data Linkage & Validation Audit
**Date:** March 2026 | **Status:** Confidential Internal Technical Document  
**Overall Pipeline Readiness Today:** ~45% signals computable | **With easy fixes (1 week):** ~65%

---

## THE #1 PROBLEM: NO MASTER BRIDGE TABLE

Every single MCA file (mca_company_master, mca_directors, mca_charges_registered, director_company_network) uses `company_cin` as its key. Your core pipeline files (gst_filings, bank_monthly_summary, itr_financials) use `company_id` (COMP_xxxxx) and `case_id` (CAM_xxxxx). **There is no table that connects these two worlds.** Until this bridge exists, your entire Collateral C and most of Character C (director stress, encumbrance) cannot compute.

### Bridge Table to Build First

```
TABLE: entity_master_bridge
┌─────────────┬───────────┬──────────────────────┬──────────────────┬──────────┬──────────┐
│ company_id  │  case_id  │     company_cin       │      gstin       │  symbol  │   pan    │
├─────────────┼───────────┼──────────────────────┼──────────────────┼──────────┼──────────┤
│ COMP_00001  │ CAM_00001 │ L17110GJ1991PLC015846 │ 10SFFPI4264I1Z9  │20MICRONS │ AAAPC...│
└─────────────┴───────────┴──────────────────────┴──────────────────┴──────────┴──────────┘

Source:
  company_id, case_id, gstin, symbol → already in companies_financial_scenario.csv ✅
  company_cin, pan                   → must be fetched from MCA / Screener.in ❌ (missing)
```

---

## SECTION 1 — What's Working (Join Keys Verified)

| File | Keys Present | Join Status |
|------|-------------|-------------|
| gst_filings | company_id + case_id + gstin | ✅ All match master |
| bank_monthly_summary | company_id + case_id | ✅ Match master |
| itr_financials | company_id + case_id | ✅ Match master |
| management_interview | company_id + case_id | ✅ Match master |
| site_visit | company_id + case_id | ✅ Match master |
| promoter_pledge | company_id = SYMBOL/ticker | ✅ Matches SYMBOL in companies_financial |
| shareholding_pattern | company_id = SYMBOL/ticker | ✅ Matches SYMBOL in companies_financial |

---

## SECTION 2 — Computed Signals (Valid Today)

The following signals compute correctly from existing data for COMP_00001 / 20 Microns:

**GST-Bank Reliability Score (Capacity C)**
- GST expected_bank_inflow: ₹9.92 Cr
- Bank total_credits: ₹11.17 Cr  
- Divergence: 11.2% → "Watch" band (5–15%)
- Reliability Score: 0.89 ✅ (gst_reconciliation_match_pct in bank file agrees)

**DSCR Approximation (Capacity C)**
- profit_cr: 27.09 Cr | interest_expense_cr: 4.68 Cr | EMI annualised: 7.79 Cr
- **DSCR = 2.17x** ✅ — adequate coverage

**ITR Divergence (Character C)**
- ITR gross income: ₹172.5 Cr vs Financial revenue: ₹152.0 Cr → 13% divergence
- ⚠️ cross_verification_flag = False but divergence exceeds 10% threshold — flag logic broken (easy fix)

**Director Remuneration (Character C)**
- Remuneration: ₹0.38 Cr / Net Income: ₹25.75 Cr = 1.5% ✅ — well below 30% siphoning threshold

---

## SECTION 3 — 🔴 Critical Issues (8 total)

### C1 — No CIN in companies_financial_scenario
`companies_financial_scenario.csv` has no `company_cin` column. This breaks ALL joins to:
- mca_company_master, mca_directors, mca_charges_registered, director_company_network, ecourts_cases_raw

**Fix:** Add `company_cin` column. Source it from Screener.in or MCA search using the SYMBOL/ISIN that already exists in the file.

### C2 — mca_company_master CIN format is wrong
Sample shows `F01450` — this is not a valid MCA CIN. Valid format: `L/U + 5digits + StateCode + Year + PLC/PTC + 6digits` (e.g. `L17110GJ1991PLC015846`).  
**Fix:** Re-scrape MCA with the correct CIN lookup. The scraper is producing malformed IDs.  
**Also:** `date_of_incorporation`, `authorized_capital_inr`, `paid_up_capital_inr` are all NULL — key fields for Capital C. Re-scrape required.

### C3 — mca_directors has no path back to company_id
`director_din → company_cin → mca_company_master` works internally, but there's no link from `company_cin` → `company_id` (COMP_xxxxx). Graph engine cannot identify which directors belong to which borrower case.  
**Fix:** Build the bridge table (C1 fix resolves this automatically).

### C4 — mca_charges_registered has no company_id link
`company_cin = L01110GJ1991PLC015846` exists but has no mapping to `COMP_xxxxx`. Collateral C encumbrance ratio is entirely blocked.  
**Additional bug:** Sample shows `charge_status = 'satisfied'` — this must be filtered out (only `live` charges count toward encumbrance).

### C5 — judgments.csv: zero linkage, zero text
No `company_id`, no `company_cin`. PDF text not extracted — only metadata (diary number, petitioner, respondent). This file is currently **unusable** for FinBERT or any scoring signal.  
**Fix:** (1) Build entity matching to link petitioner/respondent names to borrower entities. (2) Run OCR on judgment PDFs. Both are medium-term work.

### C6 — promoter_pledge date truncation: '30-JUN-200'
Year is cut off by scraper bug. All QoQ pledge trend calculations are broken — the file's most important signal (increasing pledge = promoter stress) cannot be computed.  
**Fix:** Fix scraper year parsing and re-run for all 2,239 companies. The company_id join itself is fine (uses ticker symbol correctly).

### C7 — shareholding_pattern: all percentages are 0.0
20 Microns is an NSE-listed company — it absolutely has promoter holding data. All fields (`promoter_holding_pct`, `promoter_shares_pledged_pct`, `institutional_holding_pct`, `public_holding_pct`) show 0.0. Total = 0%, which is impossible.  
**Fix:** Scraper is not reading values from the PDF/HTML. Debug extraction logic for the shareholding table format.

### C8 — shareholding_pattern date also truncated: '31-DEC-202'
Same scraper bug as pledge file.

---

## SECTION 4 — 🟠 High Priority Issues (9 total)

### H1 — bank_monthly_summary: large_unexplained_credits is NULL
This is a critical Character C signal (circular trading, unexplained inflows). It's NULL in the sample — either the field isn't being populated by the extraction pipeline or the bank statements don't have clearly identifiable large unexplained credits.  
**Fix:** Investigate extraction logic. If credits exist above a threshold (e.g. >₹10L in a single transaction with no clear counterparty), they should be captured here.

### H2 — itr_financials: only 1 assessment year
A single year of ITR is a snapshot. The divergence trend (is the gap getting worse each year?) is the actual signal. Need AY2021-22, AY2022-23, AY2023-24 per company.

### H3 — Revenue scale mismatch: ITR vs companies_financial
For COMP_00001: ITR implies ₹172.5 Cr annual revenue but companies_financial shows ₹151.97 Cr — 13% divergence. This could be legitimate (different year, gross vs net definition) or a data quality issue. The `itr_vs_financials_profit_divergence` field definition needs documenting.

### H4 — mca_company_master: key fields are NULL
`date_of_incorporation`, `authorized_capital_inr`, `paid_up_capital_inr` are all NULL in sample. These feed Capital C directly.

### H5 — litigation_risk_summary: total_litigation_amount_inr = 0.0
The density score exists (count-based) but the severity dimension (amount at stake) is 0.0/NULL. A company with 5 civil cases worth ₹500Cr is vastly different from one with 5 cases worth ₹5L. Without amount, you have frequency but not severity.

### H6 — company_cases: wrong column used as CIN
`company_cin` column contains `COMP_01986` — this is `company_id` format, not CIN. The column is either mislabeled or the data was written to the wrong field.  
**Easy Fix:** Rename the column to `company_id` in this file, or add a proper `company_cin` field.

### H7 — management_interview: C-category misclassification
`debt_management` topic → `linked_to_c_category = 'Conditions'` — this is wrong. Debt management belongs to **Capacity C**. The audit document flagged this and confirmed it in data. Must audit the full 2,239-company dataset. If >15% of rows have wrong C-category, the entire scorecard signal routing is corrupted.

**Known correct mappings:**
- `debt_management` → Capacity C
- `revenue_trend_explanation` → Capacity C  
- `related_party_transaction_explanation` → Character C
- `governance_concern` → Character C
- `sector_outlook` → Conditions C

### H8 — site_visit: C-category misclassification
`workforce_headcount` → `linked_to_c_category = 'Collateral'` — completely wrong. Workforce headcount is Character C (management stability) or Capacity C (operational capacity). Also: `risk_impact_direction = 'neutral'` for 'High employee turnover' observation is wrong — should be `negative`.

### H9 — news_articles_crawled: company_id NULL
All articles in sample are sector-level (Pharma industry news), not company-specific. These can only feed Conditions C sector signals. Character C company-specific news signals (fraud, DRT, default) cannot be produced until company-specific crawling tags articles with `company_id` at crawl time.

---

## SECTION 5 — 🟡 Medium Issues (4 total)

| # | Issue | Impact |
|---|-------|--------|
| M1 | `scenario_type` in bank_monthly_summary is a data pattern label, not a credit outcome. Do not use as ML training target. | Misleading accuracy metrics if used as ground truth |
| M2 | `litigation_risk_summary.entity_id` uses COMP_ prefix — confirm this = company_id, not a separate ID space. COMP_00045 doesn't appear in the 2-company master sample. | Join ambiguity |
| M3 | `management_interview.score_adjustment_points = 0.77` for `confident_and_consistent` credibility. Spec says this should be ≤ +0.3 for this level. Scale is inflated. | Scorecard miscalibration |
| M4 | `itr_vs_financials_profit_divergence` — is this vs PAT, EBITDA, or PBT? Definition not documented. | Ratio comparability across companies |

---

## SECTION 6 — 🟢 Easy Fixes (This Week)

| # | File | Fix | Effort |
|---|------|-----|--------|
| E1 | mca_charges_registered | Add `WHERE charge_status = 'live'` filter to all encumbrance calculations. 'satisfied' charges must not count. | 10 min |
| E2 | company_cases | Rename `company_cin` column to `company_id` (it already contains COMP_ format values). | 10 min |
| E3 | site_visit | Fix dropdown→score mapping: 'High employee turnover' must have `risk_impact_direction = 'negative'` and `score_adjustment_points = -0.5 to -1.0`. Fix mapping table, not individual rows. | 1 hour |
| E4 | itr_financials | Fix `cross_verification_flag` trigger: auto-set `True` when `itr_vs_financials_profit_divergence / declared_gross_income > 10%`. COMP_00001 should have this flag = True. | 30 min |
| E5 | news_articles_crawled | Tag `company_id` at crawl time: when running `"{company_name} fraud"` query, immediately write the corresponding COMP_id to the `company_id` field of all resulting articles. | 2 hours |
| E6 | management_interview | Build a validation rule table: topic_category → allowed_c_categories. Reject or flag any row where linked_to_c_category doesn't match. Run retroactively on full dataset. | 2 hours |

---

## SECTION 7 — Files Not Yet Seen (Need Schema Validation)

Three files in your structure weren't provided in the samples. These are all **core Layer 1 inputs** and need to be validated before their pipeline components can be built:

- `alm/alm_features.csv` — feeds Capacity C (liquidity risk) and Isolation Forest. What are the column names? Are maturity buckets present?
- `structured/borrowing_profile_synthetic.csv` — core Collateral C input. Is this synthetic data or real? What's the schema vs the spec's `borrowing_profile` table?
- `structured/portfolio_performance.csv` — core NBFC Capacity C signal. Does it have gross_npa_pct, net_npa_pct, provisioning_coverage_ratio as required?

---

## SECTION 8 — Signal Readiness Summary

| Signal | C-Category | Status | Blocker |
|--------|-----------|--------|---------|
| GST-Bank reliability | Capacity | ✅ COMPUTABLE | — |
| DSCR approximation | Capacity | ✅ COMPUTABLE | — |
| GST filing compliance | Character | ✅ COMPUTABLE | — |
| Vendor fraud flag (GST divergence >10%) | Character | ✅ COMPUTABLE | — |
| ITR divergence % | Character | ✅ COMPUTABLE | Fix flag trigger |
| Director remuneration ratio | Character | ✅ COMPUTABLE | — |
| Inflow volatility | Capacity | ✅ COMPUTABLE | — |
| Bounce count / EMI detection | Capacity | ✅ COMPUTABLE | — |
| Litigation density score | Character | ⚠️ PARTIAL | No amount = no severity |
| Large unexplained credits | Character | ⚠️ PARTIAL | NULL in extraction |
| Encumbrance ratio | Collateral | ❌ BLOCKED | No CIN bridge |
| Director stress score | Character | ❌ BLOCKED | No CIN bridge |
| Promoter pledge trend | Character | ❌ BROKEN | Date truncation |
| Shareholding analysis | Character | ❌ BROKEN | All values 0.0 |
| Company-level news signals | Character | ❌ BLOCKED | company_id NULL |
| Judgment text (FinBERT) | Character | ❌ BLOCKED | No OCR, no linkage |
| ALM gap signals | Capacity | ❓ UNKNOWN | Schema not seen |
| Borrowing profile signals | Collateral | ❓ UNKNOWN | Schema not seen |
| Portfolio NPA (NBFC) | Capacity | ❓ UNKNOWN | Schema not seen |

**Computable today: 8/19 signals (42%)**  
**After bridge table + easy fixes: ~13/19 signals (68%)**  
**Full readiness: requires scraper fix + OCR pipeline + CIN mapping**

---

## Recommended Execution Order

1. **Day 1–2:** Build `entity_master_bridge` table (company_id ↔ CIN ↔ GSTIN ↔ PAN ↔ SYMBOL). Use Screener.in ISIN data to get CIN from ticker. This single table unlocks all MCA joins.
2. **Day 2–3:** Fix scraper year-truncation bug. Re-run promoter_pledge and shareholding_pattern scrapers for all 2,239 companies.
3. **Day 3:** Apply easy fixes E1–E6 above.
4. **Day 4–5:** Audit full management_interview and site_visit datasets for C-category misclassification. Build validation rule table.
5. **Week 2:** Share alm_features, borrowing_profile_synthetic, and portfolio_performance schemas for validation.
6. **Week 2–3:** Re-scrape mca_company_master with date_of_incorporation and capital fields populated.
7. **Parallel track:** Build OCR pipeline for judgments.csv and unstructured PDF folders.

---

*Intelli-Credit Data Audit | March 2026 | Based on automated pipeline validation + sample data analysis*
