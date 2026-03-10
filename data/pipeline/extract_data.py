import pdfplumber
import os
from pathlib import Path

INPUT_DIRS = [
    "../annual_reports/raw_reports/indian_reports",
    "../unstructured/agency_ratings",
    "../unstructured/sanction_letters"
]

OUTPUT_DIR = "../processed/extracted_text"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def extract_pdf_text(pdf_path):
    text = ""

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()

            if page_text:
                text += page_text + "\n"

    return text


successful = 0
failed = 0
skipped_files = []

for directory in INPUT_DIRS:
    if not os.path.exists(directory):
        print(f"Warning: Directory not found: {directory}")
        continue

    # Walk through all subdirectories recursively
    for root, dirs, files in os.walk(directory):
        # If we're in indian_reports, skip folders before AFFORDABLE
        if "indian_reports" in directory:
            # Get the company folder name from the path
            path_parts = root.split(os.sep)
            if "indian_reports" in path_parts:
                idx = path_parts.index("indian_reports")
                if idx + 1 < len(path_parts):
                    company_folder = path_parts[idx + 1]
                    # Skip if alphabetically <= AFFORDABLE
                    if company_folder <= "AFFORDABLE":
                        print(f"⊘ Skipping: {company_folder} (before/at AFFORDABLE)")
                        continue
        
        for file in files:
            if file.endswith(".pdf"):

                pdf_path = os.path.join(root, file)
                
                # Create a unique filename including parent folder to avoid overwrites
                relative_path = os.path.relpath(pdf_path, directory)
                safe_name = relative_path.replace(os.sep, "_").replace(".pdf", ".txt")
                output_file = Path(OUTPUT_DIR) / safe_name

                # Skip if already extracted
                if output_file.exists():
                    print(f"⊙ Already extracted: {relative_path}")
                    continue

                try:
                    text = extract_pdf_text(pdf_path)

                    with open(output_file, "w", encoding="utf8") as f:
                        f.write(text)
                    
                    successful += 1
                    print(f"✓ Extracted: {relative_path}")
                
                except Exception as e:
                    failed += 1
                    skipped_files.append((relative_path, str(e)))
                    print(f"✗ Failed: {relative_path} - {type(e).__name__}")

print(f"\nExtraction complete!")
print(f"Successful: {successful}")
print(f"Failed: {failed}")
if skipped_files:
    print(f"\nSkipped files due to errors:")
    for filename, error in skipped_files:
        print(f"  - {filename}: {error[:50]}...")