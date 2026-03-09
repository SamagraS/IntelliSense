import pandas as pd
import random
import json
from datetime import timedelta

summary = pd.read_csv("bank_monthly_summary.csv")

summary["month"] = pd.to_datetime(summary["month"])

rows = []

for _, row in summary.iterrows():

    scenario = row["scenario_type"]

    if scenario == "normal":
        prob = 0.005
    elif scenario == "moderate_anomaly":
        prob = 0.03
    else:
        prob = 0.10

    if random.random() < prob:

        n = random.randint(1,2)

        credits = []

        for _ in range(n):

            amount = random.choice([
                5000000,
                10000000,
                15000000,
                20000000,
                30000000
            ])

            day = random.randint(1,25)

            credits.append({
                "amount": amount,
                "date": str((row["month"] + timedelta(days=day)).date()),
                "description": "UNIDENTIFIED RTGS CREDIT"
            })

        row["large_unexplained_credits"] = json.dumps(credits)

    else:
        row["large_unexplained_credits"] = None

    rows.append(row)

updated = pd.DataFrame(rows)

updated.to_csv("bank_monthly_summary.csv", index=False)

print("Updated bank_monthly_summary.csv with unexplained credits")
print("Rows:", len(updated))
print("Injected rows:", updated["large_unexplained_credits"].notna().sum())