import os
import logging
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


api_key = os.getenv("OPENROUTER_API_KEY")

if not api_key:
    raise RuntimeError("OPENROUTER_API_KEY not found in environment variables")


client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=api_key,
)


MODEL_NAME = "openrouter/auto"


def generate_cam_text(system_prompt: str, user_prompt: str) -> str:

    try:

        logger.info(f"Generating CAM using model: {MODEL_NAME}")

        response = client.chat.completions.create(
            model=MODEL_NAME,
            temperature=0.2,
            max_tokens=2500,
            timeout=60,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )

        if not response.choices:
            raise RuntimeError("LLM returned empty response")

        content = response.choices[0].message.content

        if not content:
            raise RuntimeError("LLM returned empty content")

        return content

    except Exception as e:
        logger.exception("LLM call failed")
        raise RuntimeError(f"LLM generation error: {str(e)}")