import os
import re
import json
import uuid
import pandas as pd
from datetime import datetime
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm
import torch

INPUT_DIR = "../processed/cleaned_text"
JSON_OUTPUT = "management_interview_notes.json"
CSV_OUTPUT = "management_interview_notes.csv"

torch.set_grad_enabled(False)
torch.set_num_threads(12)

model = SentenceTransformer("all-MiniLM-L6-v2")

topic_prompts = {
    "revenue_trend_explanation": [
        "revenue decline explanation",
        "sales slowdown explanation",
        "revenue growth drivers"
    ],
    "debt_servicing_plan": [
        "loan repayment plan",
        "debt servicing ability",
        "refinancing strategy"
    ],
    "expansion_capex_plans": [
        "capacity expansion plans",
        "new plant investment",
        "capital expenditure expansion"
    ],
    "customer_concentration_risk": [
        "dependency on few customers",
        "major customer concentration"
    ],
    "supplier_dependencies": [
        "dependency on key suppliers",
        "supply chain risk"
    ],
    "working_capital_cycle": [
        "receivable cycle delays",
        "working capital pressure",
        "inventory cycle"
    ],
    "governance_concern": [
        "corporate governance issues",
        "management transparency concerns"
    ]
}

# Flatten prompts
all_prompts = []
prompt_topic_map = []

for topic, prompts in topic_prompts.items():
    for p in prompts:
        all_prompts.append(p)
        prompt_topic_map.append(topic)

# Encode prompts once
prompt_embeddings = model.encode(all_prompts, convert_to_tensor=True)

def split_sentences(text):
    return re.split(r'(?<=[.!?])\s+', text)


records = []

files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".txt")]

for file in tqdm(files):

    with open(os.path.join(INPUT_DIR, file), "r", encoding="utf8") as f:
        text = f.read()

    sentences = split_sentences(text)

    # Filter short sentences
    sentences = [s.strip() for s in sentences if len(s) > 80]

    if not sentences:
        continue

    # Encode sentences in batch
    sentence_embeddings = model.encode(sentences, convert_to_tensor=True, batch_size=128)

    # Compute similarity matrix
    similarity_matrix = util.cos_sim(sentence_embeddings, prompt_embeddings)

    for i, sentence in enumerate(sentences):

        scores = similarity_matrix[i]

        best_idx = int(scores.argmax())
        best_score = float(scores.max())

        if best_score < 0.45:
            continue

        topic = prompt_topic_map[best_idx]

        record = {
            "interview_id": str(uuid.uuid4()),
            "company_id": "unknown",
            "case_id": "auto_generated",
            "interview_date": datetime.today().date(),
            "interviewer_credit_officer": "auto_generated",
            "interviewee_name": "management",
            "interviewee_designation": "Management",
            "interview_topic_category": topic,
            "note_detail_text": sentence,
            "management_credibility_assessment": "confident_and_consistent",
            "linked_to_c_category": "Capacity",
            "score_adjustment_points": 0.0,
            "requires_document_verification": False,
            "verification_status": "pending",
            "verification_evidence": "",
            "timestamp_created": datetime.now()
        }

        records.append(record)

# Save JSON
with open(JSON_OUTPUT, "w") as f:
    json.dump(records, f, indent=2, default=str)

# Save CSV
df = pd.DataFrame(records)
df.to_csv(CSV_OUTPUT, index=False)

print("Generated records:", len(records))