import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

companies = pd.read_csv("companies_financial_scenarios.csv")
gst = pd.read_csv("gst_filings.csv")

gst["filing_month"] = pd.to_datetime(gst["filing_month"])

transactions = []
monthly_summary = []

txn_id = 1

def txn_descriptions_credit():
    return [
        "NEFT CUSTOMER PAYMENT",
        "RTGS CLIENT TRANSFER",
        "IMPS RECEIPT",
        "UPI COLLECTION",
        "ONLINE SALES RECEIPT",
        "CUSTOMER INVOICE PAYMENT"
    ]

def txn_descriptions_debit():
    return [
        "NEFT SUPPLIER PAYMENT",
        "VENDOR TRANSFER",
        "SALARY PAYMENT",
        "UTILITY PAYMENT",
        "LOAN EMI PAYMENT",
        "OFFICE EXPENSE"
    ]

def counterparty(desc):

    if "SUPPLIER" in desc:
        return "SUPPLIER"

    if "CUSTOMER" in desc:
        return "CUSTOMER"

    if "CLIENT" in desc:
        return "CLIENT"

    if "VENDOR" in desc:
        return "VENDOR"

    if "UTILITY" in desc:
        return "UTILITY"

    if "SALARY" in desc:
        return "EMPLOYEE"

    return None


for company_id, g in gst.groupby("company_id"):

    case_id = g["case_id"].iloc[0]
    scenario = random.choices(
        ["normal","moderate_anomaly","extreme_anomaly"],
        weights=[0.80,0.15,0.05]
    )[0]

    account_no = f"ACCT{random.randint(1000000000,9999999999)}"

    balance = random.uniform(5e7,2e8)

    for _, row in g.iterrows():

        month = row["filing_month"]
        monthly_revenue = row["gstr3b_revenue_declared"]

        credits_total = 0
        debits_total = 0
        emi_total = 0
        bounce_count = 0

        inflows = []

        txn_count = random.randint(80,160)

        for _ in range(txn_count):

            date = month + timedelta(days=random.randint(0,27))

            if random.random() < 0.45:

                amount = monthly_revenue / (txn_count * 0.45) * random.uniform(0.8,1.2)

                desc = random.choice(txn_descriptions_credit())

                balance += amount
                credits_total += amount
                inflows.append(amount)

                txn_type = "credit"
                emi = False
                bounce = False

            else:

                amount = monthly_revenue / txn_count * random.uniform(0.5,1.2)

                desc = random.choice(txn_descriptions_debit())

                txn_type = "debit"

                emi = "EMI" in desc

                if balance - amount < -5e6:
                    continue

                balance -= amount
                debits_total += amount

                if emi:
                    emi_total += amount

                bounce = random.random() < 0.01

                if bounce:
                    bounce_count += 1

            round_number = random.random() < 0.01

            transactions.append({

                "transaction_id": f"TXN_{txn_id}",
                "case_id": case_id,
                "company_id": company_id,
                "bank_account_no": account_no,
                "transaction_date": date,
                "transaction_type": txn_type,
                "amount": round(amount,2),
                "description": desc,
                "running_balance": round(balance,2),
                "is_emi_payment": emi,
                "is_bounce": bounce,
                "is_round_number": round_number,
                "counterparty_guess": counterparty(desc),
                "extraction_confidence": round(random.uniform(90,99),2),
                "source_document_id": f"BANK_DOC_{company_id}"

            })

            txn_id += 1


        circular = False

        if scenario == "extreme_anomaly":
            circular = random.random() < 0.12

        elif scenario == "moderate_anomaly":
            circular = random.random() < 0.05


        if circular:

            amount = random.choice([500000,1000000,2000000,5000000])

            credit_date = month + timedelta(days=random.randint(1,10))
            debit_date = credit_date + timedelta(days=random.randint(1,3))

            balance += amount

            transactions.append({
                "transaction_id": f"TXN_{txn_id}",
                "case_id": case_id,
                "company_id": company_id,
                "bank_account_no": account_no,
                "transaction_date": credit_date,
                "transaction_type": "credit",
                "amount": amount,
                "description": "RTGS ROUND TRIP CREDIT",
                "running_balance": balance,
                "is_emi_payment": False,
                "is_bounce": False,
                "is_round_number": True,
                "counterparty_guess": "RELATED_PARTY",
                "extraction_confidence": 98,
                "source_document_id": f"BANK_DOC_{company_id}"
            })

            txn_id += 1

            balance -= amount * 0.98

            transactions.append({
                "transaction_id": f"TXN_{txn_id}",
                "case_id": case_id,
                "company_id": company_id,
                "bank_account_no": account_no,
                "transaction_date": debit_date,
                "transaction_type": "debit",
                "amount": amount * 0.98,
                "description": "RTGS ROUND TRIP DEBIT",
                "running_balance": balance,
                "is_emi_payment": False,
                "is_bounce": False,
                "is_round_number": True,
                "counterparty_guess": "RELATED_PARTY",
                "extraction_confidence": 98,
                "source_document_id": f"BANK_DOC_{company_id}"
            })

            txn_id += 1


        volatility = np.std(inflows) if inflows else 0

        gst_match = random.uniform(88,100)

        monthly_summary.append({

            "case_id": case_id,
            "company_id": company_id,
            "month": month,
            "total_credits": round(credits_total,2),
            "total_debits": round(debits_total,2),
            "average_balance": round(balance,2),
            "emi_obligations_detected": round(emi_total,2),
            "bounce_count": bounce_count,
            "large_unexplained_credits": None,
            "gst_reconciliation_match_pct": round(gst_match,2),
            "circular_trading_flag": circular,
            "inflow_volatility_stddev": round(volatility,2),
            "scenario_type": scenario

        })


transactions_df = pd.DataFrame(transactions)
summary_df = pd.DataFrame(monthly_summary)

transactions_df.to_csv("bank_transactions.csv", index=False)
summary_df.to_csv("bank_monthly_summary.csv", index=False)

print("Bank datasets generated")
print("Transactions:", len(transactions_df))
print("Monthly summaries:", len(summary_df))