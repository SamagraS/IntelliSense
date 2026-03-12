import json
from pathlib import Path
from src.cam_generation.generator import generate_cam_document

INPUT_PATH = Path("input_json/case_data.json")

with open(INPUT_PATH, "r", encoding="utf-8") as f:
    case_json = json.load(f)

result = generate_cam_document(case_json, export_pdf_file=True)

print("CAM generated:")
print(result["docx"])
print(result["pdf"])