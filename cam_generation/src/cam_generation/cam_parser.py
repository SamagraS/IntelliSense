import logging
import re

logger = logging.getLogger(__name__)


EXPECTED_SECTIONS = [
    "Executive Summary",
    "Company and Promoter Profile",
    "Industry and Conditions",
    "Financial Analysis",
    "Five Cs Narrative",
    "Pre-Cognitive Risk Analysis",
    "SWOT Analysis",
    "Proposed Facility and Structure",
    "Recommendation and Rationale",
    "Audit Trail Summary"
]


def normalize_line(line: str) -> str:
    """Normalize line text for reliable section matching."""
    return re.sub(r"[^a-z0-9 ]", "", line.lower()).strip()


def split_sections(text: str) -> dict:
    sections = {}
    current_section = None

    lines = text.splitlines()

    for line in lines:

        normalized = normalize_line(line)

        matched_section = None

        for sec in EXPECTED_SECTIONS:
            if normalize_line(sec) in normalized:
                matched_section = sec
                break

        if matched_section:
            current_section = matched_section
            sections[current_section] = []
            continue

        if current_section:
            sections[current_section].append(line)

    # Convert lists to clean text
    for k in sections:
        sections[k] = "\n".join(sections[k]).strip()

    return sections


def validate_sections(sections: dict):

    missing = [s for s in EXPECTED_SECTIONS if s not in sections]

    if missing:
        logger.warning(f"Missing sections detected: {missing}")

    if len(sections) < 5:
        raise RuntimeError(
            "LLM output structure invalid — too few sections parsed."
        )