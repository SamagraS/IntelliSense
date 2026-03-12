# Intelli Credit Validation Rules

Use this as the authoritative ruleset for the `intelli-credit-data-validation` skill.

## 1) Identity and Scope

- Domain: Indian corporate credit appraisal (NBFC / bank lending)
- Trigger point: every layer boundary and before CAM generation
- Goal: prevent invalid data from affecting Five Cs scores and CAM output

### Success criteria

- Zero unresolved CRITICAL violations entering Layer 3 scoring
- Zero CAM sections using data with `extraction_confidence < 75`
- Five Cs scores must be recomputable from stored signals
- CAM citations must map to `document_id` + `page_number`

### Regulatory/format constraints

- PAN: `^[A-Z]{5}[0-9]{4}[A-Z]{1}$`
- CIN: `^[LU][0-9]{5}[A-Z]{2}[0-9]{4}[A-Z]{3}[0-9]{6}$`
- GSTIN: 15 alphanumeric chars; check digit verifiable where available
- Monetary fields: INR, non-negative, max precision DECIMAL(15,2)

## 2) Source Discovery and Reliability

### Required source coverage per case

- CRITICAL (block scoring if missing)
  - 3 years financial statements (`FY2021-22`, `FY2022-23`, `FY2023-24`)
  - 12 months bank transactions
  - 12 months GST filings
  - MCA company master record for CIN
- HIGH (warn)
  - ALM data (required for NBFC)
  - Borrowing profile (at least 1 facility)
  - Shareholding pattern (at least 1 quarter)
  - Litigation risk summary for company and promoter
- STANDARD (note)
  - Rating report(s)
  - Board governance signal(s)
  - Portfolio performance (required for NBFC)
  - Site visit observations
  - Management interview notes
  - At least 10 crawled news articles

### Reliability rules

- `document_classification.validation_status` must be `approved` for any downstream usage.
- `denied` document used downstream -> CRITICAL.
- `pending` document still in pipeline -> WARN.
- Any used field with `extraction_confidence < 75` -> flag/manual review.
- OCR-derived fields must have valid `source_document_id` in document store.

## 3) Structural Validity (Table Rules)

Apply these rules to every case before analysis.

### case_metadata

- `case_id` non-null and unique
- `cin` regex-valid
- `pan` regex-valid
- `sector` in `{Manufacturing, NBFC, Retail, Infrastructure, Pharma, Other}`
- `annual_turnover_cr > 0`, `proposed_amount_inr > 0`, `tenure_months > 0`
- `proposed_interest_rate_pct` in `[0, 50]` if present

### gstfilings

- Unique key: `(case_id, gstin, filing_month)`
- `gstin`: 15-char alphanumeric
- `filing_month`: valid `YYYY-MM`
- Amounts non-negative
- `gstr2a_vs_3b_divergence_pct` in `[-200, 200]`
- `filing_status` in `{filed, missing, late_filed}`
- `filing_month`, `gstin`, `filing_status` non-null

### itrfinancials

- Unique key: `(case_id, assessment_year)`
- AY/FY mapping consistent (example: AY2023-24 -> FY2022-23)
- `declared_gross_income >= declared_net_income`
- `total_tax_paid >= 0`, `director_remuneration_total >= 0`
- income fields non-null

### banktransactions

- `transaction_id` non-null and unique
- `transaction_type` in `{credit, debit}`
- `amount > 0`
- `transaction_date` not in future
- `running_balance` sequence consistent per account; flag unexplained `< -1 Cr`
- required non-nulls: date, amount, type, bank_account_no

### bankmonthlysummary

- Credits must reconcile with banktransactions monthly credits (tolerance +/-0.5%)
- `bounce_count >= 0`, `emi_obligations_detected >= 0`
- Flag negative `average_balance`
- Flag coverage gaps > 2 months without explanation

### alm_data

- Exactly 8 maturity buckets per case:
  - `1-7d`, `8-14d`, `15-30d`, `1-3m`, `3-6m`, `6-12m`, `1-3y`, `>3y`
- Assets/liabilities non-negative
- Recompute cumulative gap and compare (difference <= 1)
- Recompute `gap_as_pct_of_assets` (difference <= 0.1%)
- `negative_gap_flag` true iff liabilities > assets
- `extraction_confidence` in `[0, 100]`

### shareholding_pattern_quarterly

- Holdings in `[0, 100]`
- Sum of promoter + institutional + public in `[99, 101]`
- `promoter_shares_pledged_pct <= promoter_holding_pct`
- quarters roughly 3 months apart
- mandatory non-nulls for promoter fields

### promoter_pledge_analysis

- `pledge_risk_flag` in `{normal, caution, high_risk, critical}`
- Threshold consistency:
  - `normal < 40`, `caution 40-<60`, `high_risk 60-<75`, `critical >= 75`
- Recompute QoQ change
- trend consistency:
  - increasing if `> 0.5`
  - decreasing if `< -0.5`
  - stable otherwise

### borrowing_profile

- `(case_id, lender_name, facility_type)` unique unless distinct source docs
- `facility_type` in `{term_loan, working_capital, cash_credit, letter_of_credit, bank_guarantee, overdraft}`
- sanctioned > 0; outstanding >= 0; outstanding <= sanctioned
- interest in `[0, 50]`; flag `>18`
- track record in `{regular, irregular, arrears, default}`

### portfolio_performance (NBFC required)

- percentages in `[0, 100]`
- `net_npa_pct <= gross_npa_pct`
- `portfolio_outstanding_inr > 0`

### financial_statements_line_items

- At least 3 financial years per case
- `statement_type` in `{PL, BalanceSheet, CashFlow}`
- minimum required line items per statement/year
- `extraction_confidence` in `[0, 100]`, flag `<75`
- key fields non-null

### computed_financial_ratios

- 7 required ratios per year:
  - DSCR, Debt_to_Equity, Interest_Coverage, Current_Ratio,
    EBITDA_Margin, Revenue_CAGR, PAT_Margin
- Recompute formulas and flag relative delta `>1%`

### auditor_notes

- At least one row for each financial year
- `going_concern_flag = true` implies `has_qualification = true`
- emphasis text required when `has_emphasis_of_matter = true`

### mca_company_master

- CIN unique + regex-valid
- status in `{active, strike_off, amalgamated, dissolved, under_liquidation}`
- status not active -> CRITICAL

### mca_directors

- `(director_din, company_cin)` unique for active directorship
- `din_status` in `{active, deactivated, disqualified}`
- `disqualified` -> CRITICAL (Character impact)
- resignation date logic must be valid

### mca_charges_registered

- amount > 0
- status in `{live, satisfied, partially_satisfied}`
- type in `{mortgage, hypothecation, pledge, other}`
- modification date >= creation date when present
- lender cross-check with borrowing profile; mismatch >2 lenders -> flag

### ecourts_cases_raw

- case_number non-null unique
- type in `{DRT, NCLT, civil, criminal, arbitration, consumer_dispute, other}`
- status in `{active, disposed, pending, dismissed}`
- filing date not future; last hearing >= filing

### litigation_risk_summary_entity

- Recompute active counts by type from ecourts
- Recompute density score:
  - `(DRT*10 + NCLT*8 + Criminal*5 + Civil*1) / 10`, cap 10
- stored vs recomputed delta > 0.1 -> flag
- require both `entity_type=company` and `entity_type=promoter`

### news_articles_crawled

- at least 10 articles
- required non-null fields (url, date, query)
- date not future
- `crawl_phase` in `{background_deep_crawl, live_refresh}`
- reject empty full text for scoring

### news_risk_signals

- severity score in `[0,1]`
- `is_high_severity = true` iff score > 0.6
- valid signal category enum
- `article_id` exists in crawled news

### site_visit_observations

- valid category enum
- adjustment in `[-2,2]`
- direction and points must agree
- pending verification older than 7 days -> warn

### management_interview_notes

- valid credibility enum (5-values)
- adjustment in `[-2,2]`
- consistency rules:
  - contradictory_to_documents -> `<= -1.2`
  - evasive -> `[-1.5, -0.5]`
  - confident_and_consistent -> `>= 0`
- pending required verification older than 3 days -> warn

### precognitive_signals

- category in `{velocity_trend, triangulation_flag, sentiment_divergence, network_contagion}`
- severity in `{low, medium, high, critical}`
- score impact in `[-2,2]`
- severity magnitude consistency:
  - low `<0.5`, medium `[0.5,1.0)`, high `[1.0,1.5)`, critical `>=1.5`
- dedupe exact `(case_id, category, description)`
- every data source reference must exist for case

### document_classification

- status in `{pending, approved, denied, edited}`
- no `pending`/`denied` document can be used downstream
- denied requires denial reason
- edited requires human type different from auto type

### schema_mappings

- one record per `(case_id, document_type)` where document approved
- mapping keys must exist in extracted source payload
- required non-nulls present

## 4) Deep Validity Checks

### 4.1 Cross-table triangulation

- `T-1` GST vs Bank vs ITR divergence
  - >25%: medium triangulation signal
  - >40%: critical triangulation signal
- `T-2` MCA live charges vs borrowing profile lender mapping
  - >2 unmatched lenders: high signal
- `T-3` Circular trading pattern in bank transactions
  - 3+ cycles of round credit then 95-100% debit within 3-7 days -> CRITICAL Character signal
- `T-4` Auditor going concern + qualification vs stable rating outlook -> high sentiment divergence
- `T-5` Promoter pledge >60 with overly confident management tone -> medium divergence
- `T-6` Director stress score across connected companies
  - >0.4 high Character signal, >0.7 critical
- `T-7` ALM short-term liquidity stress
  - liabilities >120% of assets (warn)
  - liabilities >150% (critical)
  - 1-3m cumulative gap < -10% total assets (high)
- `T-8` NBFC portfolio quality inconsistency
  - high NPA with rising PAT margin -> high triangulation
  - low PCR + high NPA -> Capital penalty

### 4.2 Statistical validity

- `S-1` Revenue velocity
  - >20% decline in 2 consecutive years -> critical
  - >100% one-year spike without reason -> review flag
- `S-2` Debt velocity
  - debt >30% growth with flat/declining revenue -> high
  - debt doubled in 2 years -> critical
- `S-3` NPA velocity (NBFC)
  - latest increase >2pp medium, >5pp critical
- `S-4` Extreme bounds pre-check for ratios
  - DSCR `[-5,20]`
  - Debt_to_Equity `[0,50]`
  - Interest_Coverage `[-50,100]`
  - Current_Ratio `[0,20]`
  - EBITDA_Margin `[-100,100]`
  - PAT_Margin `[-100,100]`
  - Revenue_CAGR `[-100,500]`
- `S-5` Pledge increase for 3+ consecutive quarters -> high velocity trend

### 4.3 Temporal validity

- `TM-1` Bank data must cover >=12 months from case date; gaps >45 days flagged
- `TM-2` Latest financial year must be within 18 months of case date
- `TM-3` GST months complete for last 24 months; >3 consecutive missing months -> Character penalty
- `TM-4` At least 5 news articles within last 12 months; all >18 months old -> warn
- `TM-5` e-Courts date ordering and no pre-incorporation case dates
- `TM-6` Doc approvals must predate Layer 2 ML signal generation; otherwise rerun affected signals

### 4.4 Label/target validity (Five Cs)

- `L-1` Recompute each C score; delta >0.1 -> BLOCK
- `L-2` Recompute composite with weights; weights sum must be 1.0 (+/-0.001); delta >0.05 -> BLOCK
- `L-3` Decision band consistency:
  - `>7.5 APPROVE`
  - `6.0-7.5 APPROVE_WITH_CONDITIONS`
  - `5.0-6.0 REFER`
  - `<5.0 REJECT`
  - mismatch -> CRITICAL
- `L-4` Override audit trail
  - require who/when/reason
  - if decision band changed, require second approver sign-off
  - missing sign-off -> BLOCK
- `L-5` High/critical precognitive signals must be integrated into target C score

## 5) Severity and Remediation

### Severity actions

- CRITICAL: block pipeline; require human resolution
- HIGH: block CAM export; require credit officer resolution
- MEDIUM: show UI alert; allow CAM with notation
- LOW: log only; include in validation report

### Reference examples by severity

- CRITICAL examples:
  - denied document used downstream
  - going concern in auditor notes
  - MCA status not active
  - director disqualified
  - circular trading true
  - score/decision recomputation mismatch
- HIGH examples:
  - extraction confidence <75 on ratio-critical fields
  - major debt triangulation mismatch
  - severe pledge / director stress / ALM stress
- MEDIUM examples:
  - triangulation divergence 25-40%
  - debt velocity warning
  - missing shareholding quarter
  - evasive interview
- LOW examples:
  - optional source missing
  - confidence in 75-85 amber zone
  - minor temporal gaps

### Remediation policy

- Auto-correct allowed: unit normalization, date normalization, whitespace cleanup
- Clip and return for manual correction: extreme ratio outliers from extraction errors
- Impute only non-critical fields with explicit annotation
- Never impute ratios, flags, or signals used for final decisioning
- Reject unresolved PK null/duplicate records

## 6) Feature and Signal Validation

### Isolation Forest input validity

- Validate matrix completeness and finite numbers
- Confirm sector-specific model selection
- Confirm training data vintage matches sector model
- Missing ratio values may be sector-median imputed only with explicit flag and human acknowledgment before final scoring

### FinBERT validity

- Input chunks 200-500 words (reject empty or <50 words)
- Remove PII before submission (PAN, DIN, account numbers)
- Retain `source_document_id` + `page_number` traceability per chunk

### Graph engine validity

- Ensure DIN exists before graph construction
- Isolated nodes: log only
- Self-loops: data error
- Edge weights/node attributes must be finite numeric values

## 7) CAM Gating

### Pre-CAM gate (must pass all)

- No unresolved CRITICAL violations
- All documents approved
- 7 ratios available for at least 2 of 3 years
- `L-1`, `L-2`, `L-3` pass
- Override audit trail complete
- At least one traceable source per CAM section

### Post-CAM validation

- Every numeric figure in CAM must be traceable to source document/page or approved computed ratio
- SWOT must have >=1 bullet per quadrant
- Each SWOT bullet must map to at least one underlying signal/data point
- Recommendation must match computed decision band
- "What would change this decision" must be non-empty for non-approve outcomes

## 8) Runtime and Drift Monitoring

### Runtime checks per new upload

- Re-run doc classification validation
- Re-run structural checks for new extracted rows
- Re-run triangulation checks `T-1`, `T-2`, `T-4` when related data changes
- Recompute impacted Five Cs and flag decision-band changes

### Weekly drift monitors

- Median extraction confidence by document type (alert if drop >5 points)
- Human edit rate in schema mappings
- Classification denial rate
- Precognitive signal trigger rates by category
- Composite score distribution shift (alert if average shift >0.5)

### Audit triggers

- Cases with decision-band override -> quarterly review
- Cases with manually overridden CRITICAL violations -> escalation
- Isolation Forest retraining when Screener data refreshes or >20% cases cluster near anomaly threshold

## 9) Input/Output Contract

### Inputs

- `case_id` (string)
- `validation_stage` (`layer0` | `layer1` | `layer2` | `layer3` | `pre_cam` | `post_cam` | `full`)
- `strict_mode` (boolean)

### Outputs

```json
{
  "case_id": "STRING",
  "validation_stage": "STRING",
  "run_timestamp": "TIMESTAMP",
  "critical_violations": [{"check_id":"...","table":"...","field":"...","description":"...","value_found":"..."}],
  "high_violations": [],
  "medium_violations": [],
  "low_violations": [],
  "auto_corrections_applied": [{"field":"...","old_value":"...","new_value":"...","rule":"..."}],
  "pipeline_blocked": true,
  "blocking_reasons": ["..."],
  "scores_valid": false,
  "cam_ready": false
}
```

Also produce:

- row-level `validity_annotation` flags
- UI `alert_panel_items`

## 10) Check ID Index

- Structural: table-named checks across all listed entities
- Triangulation: `T-1` to `T-8`
- Statistical: `S-1` to `S-5`
- Temporal: `TM-1` to `TM-6`
- Label/Target: `L-1` to `L-5`