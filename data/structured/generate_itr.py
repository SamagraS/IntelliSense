import pandas as pd
import random
from datetime import datetime

df = pd.read_csv("companies_financial_scenarios.csv")

rows=[]

years = [
("FY2021-22","AY2022-23"),
("FY2022-23","AY2023-24"),
("FY2023-24","AY2024-25")
]

for _,r in df.iterrows():

    base_revenue = r["revenue_cr"] * 10000000
    base_profit = r["profit_cr"] * 10000000

    for fy,ay in years:

        # apply trend behaviour
        if r["trend_type"] == "growing":
            growth=random.uniform(1.08,1.18)
        elif r["trend_type"] == "declining":
            growth=random.uniform(0.80,0.92)
        elif r["trend_type"] == "volatile":
            growth=random.uniform(0.75,1.25)
        else:
            growth=random.uniform(0.95,1.05)

        revenue = base_revenue * growth

        financial_pat = base_profit * random.uniform(0.9,1.1)

        # divergence driven by scenario
        if r["scenario_type"] == "normal":
            divergence_case=random.choices(
                ["perfect","small"],
                weights=[0.85,0.15]
            )[0]

        elif r["scenario_type"] == "moderate_anomaly":
            divergence_case=random.choices(
                ["small","large"],
                weights=[0.60,0.40]
            )[0]

        else:
            divergence_case="large"

        declared_pat = financial_pat

        if divergence_case=="small":
            declared_pat*=random.uniform(0.92,1.08)

        if divergence_case=="large":
            declared_pat*=random.uniform(0.70,1.30)

        divergence = declared_pat-financial_pat

        cross_flag = abs(divergence/financial_pat) > 0.10

        # corporate tax logic
        if declared_pat>0:
            tax_paid = declared_pat * random.uniform(0.20,0.25)
        else:
            tax_paid = 0

        depreciation = revenue * random.uniform(0.04,0.12)

        # governance anomaly
        if r["scenario_type"] == "extreme_anomaly":
            director_pay = declared_pat * random.uniform(0.06,0.12)
        else:
            director_pay = declared_pat * random.uniform(0.01,0.05)

        rows.append({

            "case_id": r["case_id"],
            "company_id": r["company_id"],

            "assessment_year": ay,
            "financial_year": fy,

            "declared_gross_income": round(revenue,2),
            "declared_net_income": round(declared_pat,2),

            "total_tax_paid": round(tax_paid,2),

            "depreciation_claimed": round(depreciation,2),

            "director_remuneration_total": round(director_pay,2),

            "itr_vs_financials_profit_divergence": round(divergence,2),

            "cross_verification_flag": cross_flag,

            "extraction_confidence_score": round(random.uniform(85,99),2),

            "source_document_id": "ITR_DOC_" + r["company_id"],

            "processed_timestamp": datetime.now()

        })

itr_df = pd.DataFrame(rows)

itr_df.to_csv("itr_financials.csv", index=False)

print("ITR rows generated:", len(itr_df))