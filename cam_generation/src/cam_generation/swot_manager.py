import logging

logger = logging.getLogger(__name__)


VALID_CATEGORIES = [
    "strengths",
    "weaknesses",
    "opportunities",
    "threats"
]


def initialize_swot(swot_data: dict):

    structure = {c: [] for c in VALID_CATEGORIES}

    for category in VALID_CATEGORIES:

        if category in swot_data:

            structure[category] = [
                {
                    "text": item,
                    "source": "ai",
                    "edited": False
                }
                for item in swot_data[category]
            ]

    return structure


def add_user_swot(swot: dict, category: str, text: str):

    if category not in VALID_CATEGORIES:
        raise ValueError(f"Invalid SWOT category: {category}")

    swot[category].append({
        "text": text,
        "source": "user",
        "edited": False
    })

    logger.info(f"User added SWOT item to {category}")


def edit_swot(swot: dict, category: str, index: int, new_text: str):

    if category not in VALID_CATEGORIES:
        raise ValueError(f"Invalid SWOT category: {category}")

    if index >= len(swot[category]):
        raise IndexError("SWOT item index out of range")

    swot[category][index]["text"] = new_text
    swot[category][index]["edited"] = True

    logger.info(f"SWOT item edited in {category} at index {index}")