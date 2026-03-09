import pandas as pd
import numpy as np
import random
from datetime import datetime

df = pd.read_csv("companies_financial_scenarios.csv")

rows = []

months = pd.date_range("2023-01-01", "2024-12-01", freq="MS")

for _, r in df.iterrows():

    annual_revenue = r["revenue_cr"] * 10000000
    monthly_base = annual_revenue / 12

    for m in months:

        season_multiplier = 1

        if r["project_sector"] in ["Retail", "Manufacturing"]:
            if m.month in [10,11,12]:
                season_multiplier = random.uniform(1.2,1.35)
            elif m.month in [1,2,3]:
                season_multiplier = random.uniform(0.7,0.9)

        revenue = monthly_base * season_multiplier * random.uniform(0.92,1.08)

        if r["project_sector"] in ["NBFC","IT"]:
            purchase_ratio = random.uniform(0.2,0.45)
        else:
            purchase_ratio = random.uniform(0.55,0.75)

        base_purchases = revenue * purchase_ratio
        purchases = base_purchases

        # Controlled divergence injection
        divergence_type = random.choices(
            ["perfect","small","large"],
            weights=[0.65,0.25,0.10]
        )[0]

        if divergence_type == "small":
            purchases *= random.uniform(1.05,1.10)

        elif divergence_type == "large":
            purchases *= random.uniform(1.30,1.40)

        # Scenario escalation
        if r["scenario_type"] == "moderate_anomaly":
            purchases *= random.uniform(1.05,1.15)

        if r["scenario_type"] == "extreme_anomaly":
            purchases *= random.uniform(1.30,1.50)

        tax = revenue * random.uniform(0.176,0.182)
        itc = purchases * 0.18

        divergence = ((purchases - base_purchases) / base_purchases) * 100

        if r["scenario_type"] == "normal":
            bank_inflow = revenue * random.uniform(0.95,1.05)

        elif r["scenario_type"] == "moderate_anomaly":
            bank_inflow = revenue * random.uniform(0.70,0.90)

        else:
            bank_inflow = revenue * random.uniform(0.40,0.70)

        if r["scenario_type"] == "extreme_anomaly" and random.random() < 0.10:
            filing_status = "missing"
            delay = None
        else:

            if random.random() < 0.10:
                filing_status = "late_filed"
                delay = random.randint(5,20)
            else:
                filing_status = "filed"
                delay = 0

        rows.append({

            "case_id": r["case_id"],
            "company_id": r["company_id"],
            "gstin": r["gstin"],
            "filing_month": m,

            "gstr3b_revenue_declared": round(revenue,2),
            "gstr3b_tax_paid": round(tax,2),
            "gstr3b_input_tax_credit": round(itc,2),

            "gstr2a_reported_purchases": round(purchases,2),
            "gstr2a_vs_3b_divergence_pct": round(divergence,2),

            "filing_status": filing_status,
            "filing_delay_days": delay,

            "expected_bank_inflow": round(bank_inflow,2),

            "extraction_confidence_score": round(random.uniform(85,99),2),

            "source_document_id": "GST_DOC_" + r["company_id"],

            "processed_timestamp": datetime.now()

        })

gst_df = pd.DataFrame(rows)

gst_df.to_csv("gst_filings.csv", index=False)

print("GST rows generated:", len(gst_df))