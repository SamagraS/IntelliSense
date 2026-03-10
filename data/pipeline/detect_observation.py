import os
import json
import torch
from tqdm import tqdm
from sentence_transformers import SentenceTransformer, util

INPUT_DIR = "data/processed/cleaned_text"
OUTPUT = "data/processed/observations/raw_observations.json"

torch.set_grad_enabled(False)
torch.set_num_threads(12)

model = SentenceTransformer("all-MiniLM-L6-v2")

category_prompts = {
    "capacity_utilization": [
        "factory running below capacity",
        "production utilisation low",
        "plant operating normally",
        "factory operating at full capacity"
    ],
    "inventory_condition": [
        "inventory build up",
        "slow moving inventory",
        "high stock levels"
    ],
    "workforce_headcount": [
        "labour shortage",
        "reduced workforce",
        "employee headcount mismatch"
    ],
    "machinery_condition": [
        "equipment maintenance issues",
        "machinery breakdown",
        "production equipment problems"
    ]
}

threshold = 0.45
results = []

# ---------------------------
# Precompute prompt embeddings
# ---------------------------

prompt_texts = []
prompt_categories = []

for category, prompts in category_prompts.items():
    for p in prompts:
        prompt_texts.append(p)
        prompt_categories.append(category)

prompt_embeddings = model.encode(
    prompt_texts,
    convert_to_tensor=True,
    batch_size=32
)

files = os.listdir(INPUT_DIR)

for file in tqdm(files, desc="Processing files", unit="file"):

    with open(os.path.join(INPUT_DIR, file), "r", encoding="utf8") as f:
        text = f.read()

    sentences = [s.strip() for s in text.split(".") if len(s.strip()) > 5]

    if not sentences:
        continue

    # 🔥 Encode ALL sentences in one batch
    sentence_embeddings = model.encode(
        sentences,
        convert_to_tensor=True,
        batch_size=128
    )

    similarities = util.cos_sim(sentence_embeddings, prompt_embeddings)

    for i, sentence in enumerate(sentences):

        best_idx = similarities[i].argmax()
        best_score = float(similarities[i][best_idx])

        if best_score > threshold:
            results.append({
                "source_file": file,
                "category": prompt_categories[best_idx],
                "text": sentence,
                "similarity": best_score
            })

with open(OUTPUT, "w") as f:
    json.dump(results, f, indent=2)

print("Total observations:", len(results))