import json
import logging
from pathlib import Path


logger = logging.getLogger(__name__)

PROMPT_DIR = Path(__file__).parent / "prompts"


def load_file(path: Path) -> str:

    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def validate_case_data(case_data: dict):

    if "case_metadata" not in case_data:
        raise ValueError("Missing 'case_metadata' in input JSON")

    if "case_id" not in case_data["case_metadata"]:
        raise ValueError("Missing 'case_metadata.case_id'")


def build_prompts(case_data: dict):

    validate_case_data(case_data)

    logger.info("Loading system prompt")

    system_prompt = load_file(PROMPT_DIR / "system_prompt.txt")

    logger.info("Constructing user prompt")

    json_payload = json.dumps(case_data, indent=2)

    user_prompt = f"""
You are generating a Credit Appraisal Memo based strictly on structured data.

Rules:
- Use ONLY the information provided
- Do NOT invent or assume data
- Do NOT use Markdown symbols (###, **, ---)
- Write clear professional banking language
- Maintain the exact CAM section structure

CASE DATA
=========
<case_data>
{json_payload}
</case_data>

Generate the full CAM.
"""

    return system_prompt, user_prompt