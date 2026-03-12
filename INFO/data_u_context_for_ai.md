# data_u Context Snapshot for AI

Generated: 2026-03-12 12:22:43 +05:30
Workspace: C:\Users\Samagra\Documents\IntelliSense

## 1) Current Dataset State
Total case records (companies_financial_scenarios): 2238
Entity bridge rows (entity_master_bridge): 2238
NBFC cases: 236
ALM rows: 435
NBFC cases missing ALM after latest fix: 0

## 2) Quick Quality Snapshot (Post-fix)
Litigation summary missing at company level: 2112 cases
Promoter litigation summary rows: 0
Site-visit coverage missing: 907 cases
Cases with less than 10 news articles: 2007
Cases with less than 5 news articles in last 12 months: 2175
Duplicate legal case_number groups: 48
Promoter pledge trend/qoq mismatch count: 5052

## 3) Known Missing Rulebook Tables
- structured/financial_statements_line_items.csv
- structured/computed_financial_ratios.csv
- structured/document_classification.csv
- structured/auditor_notes.csv
- structured/schema_mappings.csv
- external intelligence/news_intelligence/news_risk_signals.csv
- structured/precognitive_signals.csv

## 4) File Inventory (CSV)
| rel_path | row_count | col_count | key_fields | count_method |
|---|---:|---:|---|---|
| data_u/alm/alm_features.csv | 435 | 53 | case_id, company_id | import_csv |
| data_u/external intelligence/legal_disputes/company_cases.csv | 746 | 17 | company_id, company_cin | import_csv |
| data_u/external intelligence/legal_disputes/judgments.csv | 47400 | 17 | company_id, company_cin | import_csv |
| data_u/external intelligence/legal_disputes/litigation_risk_summary_entity.csv | 126 | 12 |  | import_csv |
| data_u/external intelligence/mca/director_company_network.csv | 6714 | 7 | case_id, company_id, company_cin | import_csv |
| data_u/external intelligence/mca/mca_charges_live_only.csv | 2238 | 12 | case_id, company_id, company_cin | import_csv |
| data_u/external intelligence/mca/mca_charges_registered.csv | 4476 | 12 | case_id, company_id, company_cin | import_csv |
| data_u/external intelligence/mca/mca_company_master.csv | 2238 | 11 | company_cin | import_csv |
| data_u/external intelligence/mca/mca_directors.csv | 6714 | 9 | case_id, company_id, company_cin | import_csv |
| data_u/external intelligence/news_intelligence/news_articles_crawled.csv | 16662 | 16 | company_id, company_cin | import_csv |
| data_u/primary insights/management_interview_cleaned.csv | 20186 | 17 | case_id, company_id | import_csv |
| data_u/primary insights/management_topic_c_category_rules.csv | 7 | 3 |  | import_csv |
| data_u/primary insights/site_visit_cleaned.csv | 1977 | 12 | case_id, company_id | import_csv |
| data_u/primary insights/site_visit_mapping_rules.csv | 6 | 5 |  | import_csv |
| data_u/structured/bank_monthly_summary.csv | 53712 | 13 | case_id, company_id | import_csv |
| data_u/structured/bank_transactions.csv | 6444912 | 15 | case_id, company_id, source_document_id | line_count_estimate |
| data_u/structured/borrowing_profile_synthetic.csv | 7117 | 63 | case_id, company_id, source_document_id | import_csv |
| data_u/structured/companies_financial_scenarios.csv | 2238 | 22 | case_id, company_id, company_cin, gstin | import_csv |
| data_u/structured/entity_master_bridge.csv | 2238 | 8 | case_id, company_id, company_cin, gstin | import_csv |
| data_u/structured/gst_filings.csv | 53712 | 15 | case_id, company_id, gstin, source_document_id | import_csv |
| data_u/structured/itr_financials.csv | 6714 | 15 | case_id, company_id, source_document_id | import_csv |
| data_u/structured/portfolio_performance.csv | 1652 | 42 | case_id, company_id, source_document_id | import_csv |
| data_u/unstructured/shareholding_pattern/promoter_pledge_analysis.csv | 17904 | 7 | company_id | import_csv |
| data_u/unstructured/shareholding_pattern/shareholding_pattern_quarterly.csv | 17904 | 8 | company_id, source_document_id | import_csv |

## 5) Notes for Another AI
- Use data_u/structured/entity_master_bridge.csv as primary join hub between COMP/CAM and CIN/GST/PAN.
- ALM is wide-format with bucket columns (not long-format maturity bucket rows).
- news_articles_crawled.company_id is populated, but article volume/recency is still sparse for many cases.
- Litigation summary is incomplete for company coverage and has no promoter-level rows yet.
- For strict CAM-grade validation, generate/ingest the missing rulebook tables listed above.
