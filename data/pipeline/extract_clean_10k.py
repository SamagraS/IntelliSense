import os
import re
from tqdm import tqdm

INPUT_DIR = "../annual_reports/raw_reports/american"
OUTPUT_DIR = "../processed/cleaned_text"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def extract_sections(text):
    """
    Extract key sections from 10-K:
    Item 1 - Business
    Item 1A - Risk Factors
    Item 7 - Management Discussion & Analysis
    """

    patterns = [
        r"Item\s+1\.\s+Business(.*?)Item\s+1A",
        r"Item\s+1A\.\s+Risk\s+Factors(.*?)Item\s+2",
        r"Item\s+7\.\s+Management.*?Discussion.*?Analysis(.*?)Item\s+8"
    ]

    sections = []

    for pattern in patterns:
        match = re.search(pattern, text, re.I | re.S)
        if match:
            sections.append(match.group(1))

    return "\n".join(sections)


def clean_text(text):

    # Remove HTML tags
    text = re.sub(r"<.*?>", " ", text)

    # Remove URLs
    text = re.sub(r"http\S+", " ", text)

    # Remove large numbers (tables)
    text = re.sub(r"\d{4,}", " ", text)

    # Remove dollar amounts
    text = re.sub(r"\$\s*\d[\d,]*", " ", text)

    # Remove multiple spaces
    text = re.sub(r"\s+", " ", text)

    return text.strip()


# Collect all .txt files recursively
txt_files = []
for root, dirs, files in os.walk(INPUT_DIR):
    for file in files:
        if file.endswith(".txt"):
            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, INPUT_DIR)
            txt_files.append((full_path, relative_path))

print(f"Found {len(txt_files)} .txt files to process")

for full_path, relative_path in tqdm(txt_files):

    with open(full_path, "r", encoding="utf8", errors="ignore") as f:
        text = f.read()

    extracted = extract_sections(text)

    cleaned = clean_text(extracted)

    # Create unique output filename including folder structure
    safe_name = relative_path.replace(os.sep, "_")
    out_path = os.path.join(OUTPUT_DIR, safe_name)

    with open(out_path, "w", encoding="utf8") as f:
        f.write(cleaned)

print("Finished processing", len(txt_files), "10-K files")