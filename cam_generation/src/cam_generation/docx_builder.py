from docx import Document
from docx.shared import Pt
import logging
import re


logger = logging.getLogger(__name__)


def clean_text(text: str):
    text = re.sub(r"#{1,6}\s*", "", text)
    text = text.replace("---", "")
    text = text.replace("*", "")
    return text.strip()


def add_key_value_table(doc, title, data: dict):

    doc.add_heading(title, level=2)

    table = doc.add_table(rows=1, cols=2)
    table.style = "Table Grid"

    table.rows[0].cells[0].text = "Item"
    table.rows[0].cells[1].text = "Value"

    for k, v in data.items():
        row = table.add_row().cells
        row[0].text = str(k)
        row[1].text = str(v)


def add_financial_table(doc, financials: dict):

    doc.add_heading("Financial Snapshot", level=2)

    table = doc.add_table(rows=1, cols=2)
    table.style = "Table Grid"

    hdr = table.rows[0].cells
    hdr[0].text = "Metric"
    hdr[1].text = "Value"

    metrics = [
        ("Revenue FY2024", financials.get("revenue_cr", {}).get("FY2024")),
        ("EBITDA FY2024", financials.get("ebitda_cr", {}).get("FY2024")),
        ("Net Profit FY2024", financials.get("net_profit_cr", {}).get("FY2024")),
        ("DSCR", financials.get("dscr")),
        ("Debt / Equity", financials.get("debt_to_equity")),
        ("Current Ratio", financials.get("current_ratio")),
    ]

    for name, val in metrics:
        row = table.add_row().cells
        row[0].text = name
        row[1].text = str(val)


def add_five_cs_table(doc, five_cs: dict):

    doc.add_heading("Five Cs Scorecard", level=2)

    table = doc.add_table(rows=1, cols=3)
    table.style = "Table Grid"

    hdr = table.rows[0].cells
    hdr[0].text = "Credit Dimension"
    hdr[1].text = "Score"
    hdr[2].text = "Weight"

    for c, val in five_cs.items():
        row = table.add_row().cells
        row[0].text = c
        row[1].text = str(val.get("score"))
        row[2].text = str(val.get("c_level_weight"))


def add_swot_matrix(doc, swot_text: str):

    doc.add_heading("SWOT Matrix", level=2)

    strengths = []
    weaknesses = []
    opportunities = []
    threats = []

    lines = swot_text.splitlines()

    for l in lines:

        l = l.lower()

        if "strength" in l:
            strengths.append(l)

        if "weakness" in l:
            weaknesses.append(l)

        if "opportunit" in l:
            opportunities.append(l)

        if "threat" in l:
            threats.append(l)

    table = doc.add_table(rows=2, cols=2)
    table.style = "Table Grid"

    table.rows[0].cells[0].text = "Strengths"
    table.rows[0].cells[1].text = "Weaknesses"
    table.rows[1].cells[0].text = "\n".join(strengths)
    table.rows[1].cells[1].text = "\n".join(weaknesses)

    table = doc.add_table(rows=2, cols=2)
    table.style = "Table Grid"

    table.rows[0].cells[0].text = "Opportunities"
    table.rows[0].cells[1].text = "Threats"
    table.rows[1].cells[0].text = "\n".join(opportunities)
    table.rows[1].cells[1].text = "\n".join(threats)


def build_docx(sections: dict, case_json: dict, output_path):

    try:

        doc = Document()

        title = doc.add_heading("Credit Appraisal Memo", level=0)
        title.alignment = 1

        meta = case_json.get("case_metadata", {})
        decision = case_json.get("final_decision", {})

        dashboard = {
            "Company": meta.get("company_name"),
            "Sector": meta.get("sector"),
            "Requested Loan": decision.get("requested_amount_inr"),
            "Suggested Limit": decision.get("suggested_limit_inr"),
            "Composite Score": decision.get("composite_score"),
            "Decision": decision.get("decision_band"),
            "Risk Premium (bps)": decision.get("risk_premium_bps"),
        }

        add_key_value_table(doc, "Decision Snapshot", dashboard)

        if "financial_summary" in case_json:
            add_financial_table(doc, case_json["financial_summary"])

        if "five_cs_scores" in case_json:
            add_five_cs_table(doc, case_json["five_cs_scores"])

        for section, content in sections.items():

            doc.add_heading(section, level=1)

            cleaned = clean_text(content)

            paragraphs = re.split(r"\n\s*\n", cleaned)

            for para in paragraphs:

                para = para.strip()

                if not para:
                    continue

                doc.add_paragraph(para)

            if section == "SWOT Analysis":
                add_swot_matrix(doc, cleaned)

        doc.save(output_path)

        logger.info(f"DOCX saved: {output_path}")

    except Exception:
        logger.exception("DOCX build failed")
        raise