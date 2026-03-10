import re
import os

INPUT_DIR = "../processed/extracted_text"
OUTPUT_DIR = "../processed/cleaned_text"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def clean_text(text):

    text = re.sub(r"\n+", "\n", text)

    text = re.sub(r"Page \d+", "", text)

    text = re.sub(r"\d+\s+\d+\s+\d+", "", text)

    text = re.sub(r"\s+", " ", text)

    return text


for file in os.listdir(INPUT_DIR):

    with open(os.path.join(INPUT_DIR, file), "r", encoding="utf8") as f:
        text = f.read()

    cleaned = clean_text(text)

    with open(os.path.join(OUTPUT_DIR, file), "w", encoding="utf8") as f:
        f.write(cleaned)