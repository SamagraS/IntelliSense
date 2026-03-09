import pandas as pd
import numpy as np

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)

print("Loading datasets...")

companies = pd.read_csv("companies_financial_scenarios.csv")
gst = pd.read_csv("gst_filings.csv")
itr = pd.read_csv("itr_financials.csv")
bank_txn = pd.read_csv("bank_transactions.csv")
bank_month = pd.read_csv("bank_monthly_summary.csv")

print("\n================ BASIC DATASET SIZE ================\n")

print("Companies:", len(companies))
print("GST rows:", len(gst))
print("ITR rows:", len(itr))
print("Bank transactions:", len(bank_txn))
print("Bank monthly summaries:", len(bank_month))

print("\n================ COMPANY DISTRIBUTIONS ================\n")

print("Sector distribution:")
print(companies["project_sector"].value_counts(normalize=True))

print("\nScenario distribution:")
print(companies["scenario_type"].value_counts(normalize=True))

print("\nRevenue scale distribution:")
print(companies["revenue_cr"].describe())

print("\n================ GST ANALYSIS ================\n")

gst["tax_ratio"] = gst["gstr3b_tax_paid"] / gst["gstr3b_revenue_declared"]
gst["purchase_ratio"] = gst["gstr2a_reported_purchases"] / gst["gstr3b_revenue_declared"]

print("GST rows:", len(gst))

print("\nGST tax ratio (should be ~0.18):")
print(gst["tax_ratio"].describe())

print("\nPurchase ratio distribution:")
print(gst["purchase_ratio"].describe())

print("\nGST divergence distribution:")
print(gst["gstr2a_vs_3b_divergence_pct"].describe())

print("\nFiling status:")
print(gst["filing_status"].value_counts(normalize=True))

print("\nMissing GST months per company:")
gst_months = gst.groupby("company_id")["filing_month"].count()
print(gst_months.describe())

print("\n================ ITR ANALYSIS ================\n")

itr["tax_ratio"] = itr["total_tax_paid"] / itr["declared_net_income"]
itr["depreciation_ratio"] = itr["depreciation_claimed"] / itr["declared_gross_income"]
itr["director_ratio"] = itr["director_remuneration_total"] / itr["declared_net_income"]

print("ITR rows:", len(itr))

print("\nTax ratio (expected ~0.20–0.25):")
print(itr["tax_ratio"].describe())

print("\nDepreciation ratio:")
print(itr["depreciation_ratio"].describe())

print("\nDirector remuneration ratio:")
print(itr["director_ratio"].describe())

print("\nITR vs Financial divergence:")
print(itr["itr_vs_financials_profit_divergence"].describe())

print("\nCross verification flags:")
print(itr["cross_verification_flag"].value_counts(normalize=True))

print("\n================ BANK TRANSACTION ANALYSIS ================\n")

print("Total transactions:", len(bank_txn))

print("\nTransaction type distribution:")
print(bank_txn["transaction_type"].value_counts(normalize=True))

print("\nCredit amount stats:")
print(bank_txn[bank_txn.transaction_type=="credit"]["amount"].describe())

print("\nDebit amount stats:")
print(bank_txn[bank_txn.transaction_type=="debit"]["amount"].describe())

print("\nRunning balance stats:")
print(bank_txn["running_balance"].describe())

print("\nBounce rate:")
print(bank_txn["is_bounce"].value_counts(normalize=True))

print("\nEMI detection rate:")
print(bank_txn["is_emi_payment"].value_counts(normalize=True))

print("\nRound number transactions:")
print(bank_txn["is_round_number"].value_counts(normalize=True))

print("\nCounterparty extraction coverage:")
print(bank_txn["counterparty_guess"].notna().mean())

print("\n================ MONTHLY BANK SUMMARY ================\n")

print("Rows:", len(bank_month))

print("\nTotal credits stats:")
print(bank_month["total_credits"].describe())

print("\nTotal debits stats:")
print(bank_month["total_debits"].describe())

print("\nAverage balance stats:")
print(bank_month["average_balance"].describe())

print("\nEMI obligations distribution:")
print(bank_month["emi_obligations_detected"].describe())

print("\nBounce counts:")
print(bank_month["bounce_count"].describe())

print("\nGST reconciliation match:")
print(bank_month["gst_reconciliation_match_pct"].describe())

print("\nCircular trading flags:")
print(bank_month["circular_trading_flag"].value_counts(normalize=True))

print("\nInflow volatility:")
print(bank_month["inflow_volatility_stddev"].describe())

print("\n================ CROSS DATASET CHECKS ================\n")

print("\nCompanies missing GST:")
print(set(companies.company_id) - set(gst.company_id))

print("\nCompanies missing ITR:")
print(set(companies.company_id) - set(itr.company_id))

print("\nCompanies missing bank data:")
print(set(companies.company_id) - set(bank_txn.company_id))

print("\n================ GST vs BANK REVENUE CHECK ================\n")

gst_revenue = gst.groupby("company_id")["gstr3b_revenue_declared"].sum()
bank_credits = bank_txn[bank_txn.transaction_type=="credit"].groupby("company_id")["amount"].sum()

merged = pd.DataFrame({
"gst_revenue": gst_revenue,
"bank_credits": bank_credits
}).dropna()

merged["ratio"] = merged["bank_credits"] / merged["gst_revenue"]

print("Bank inflow vs GST revenue ratio:")
print(merged["ratio"].describe())

print("\nCompanies with major mismatch (>40%):")
print((merged["ratio"] > 1.4).sum())

print("\n================ TIME SERIES CHECK ================\n")

txn_counts = bank_txn.groupby(["company_id"]).size()

print("Transactions per company:")
print(txn_counts.describe())

months_per_company = bank_month.groupby("company_id").size()

print("\nMonths per company:")
print(months_per_company.describe())

print("\n================ ANOMALY SCENARIO CHECK ================\n")

print(bank_month["scenario_type"].value_counts(normalize=True))

print("\n================ FINAL DATA HEALTH ================\n")

print("Missing values (GST):")
print(gst.isna().sum())

print("\nMissing values (ITR):")
print(itr.isna().sum())

print("\nMissing values (Bank transactions):")
print(bank_txn.isna().sum())

print("\nMissing values (Bank summary):")
print(bank_month.isna().sum())

print("\n================ AUDIT COMPLETE ================\n")