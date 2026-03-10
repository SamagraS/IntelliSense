import json
import random

INPUT = "data/processed/observations/raw_observations.json"
OUTPUT = "data/processed/observations/synthetic_site_visits.json"

templates = [
    "During the visit, the factory appeared to be {}.",
    "The production facility seems to be {}.",
    "Inspection indicates the plant is {}.",
    "Operations at the site appear to be {}.",
]

capacity_phrases = [
    "operating at very low capacity",
    "running below half of installed capacity",
    "operating normally with active production",
    "operating close to full capacity",
]

with open(INPUT) as f:
    data = json.load(f)

synthetic = []

for obs in data:

    for _ in range(50):

        phrase = random.choice(capacity_phrases)
        template = random.choice(templates)

        synthetic.append({
            "observation_category": obs["category"],
            "text": template.format(phrase)
        })

with open(OUTPUT, "w") as f:
    json.dump(synthetic, f, indent=2)