import logging
import time
from pathlib import Path

from .prompt_builder import build_prompts
from .llm_client import generate_cam_text
from .cam_parser import split_sections, validate_sections
from .docx_builder import build_docx
from .pdf_exporter import export_pdf


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


OUTPUT_DIR = Path("generated_cams")
OUTPUT_DIR.mkdir(exist_ok=True)


def _validate_input(case_json: dict):
    if "case_metadata" not in case_json:
        raise ValueError("Missing 'case_metadata' in input JSON")

    if "case_id" not in case_json["case_metadata"]:
        raise ValueError("Missing 'case_metadata.case_id'")


def _sanitize_case_id(case_id: str):
    return "".join(c for c in case_id if c.isalnum() or c in ("_", "-"))


def generate_cam_document(case_json: dict, export_pdf_file: bool = False) -> dict:
    try:

        start_time = time.time()

        _validate_input(case_json)

        case_id = _sanitize_case_id(case_json["case_metadata"]["case_id"])

        logger.info("Building prompts...")
        system_prompt, user_prompt = build_prompts(case_json)

        logger.info("Calling LLM...")

        llm_start = time.time()

        cam_text = None

        for attempt in range(2):
            try:
                cam_text = generate_cam_text(system_prompt, user_prompt)
                break
            except Exception as e:
                logger.warning(f"LLM attempt {attempt+1} failed: {e}")
                if attempt == 1:
                    raise

        logger.info(f"LLM generation completed in {time.time()-llm_start:.2f}s")

        # Save raw LLM output for debugging
        raw_path = OUTPUT_DIR / f"{case_id}_raw.txt"
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(cam_text)

        logger.info("Parsing CAM sections...")
        sections = split_sections(cam_text)

        validate_sections(sections)

        docx_path = OUTPUT_DIR / f"{case_id}_CAM.docx"

        logger.info("Generating DOCX...")
        build_docx(sections, case_json, docx_path)

        pdf_path = None
        if export_pdf_file:
            logger.info("Exporting PDF...")
            pdf_path = export_pdf(docx_path)

        total_time = time.time() - start_time

        logger.info(f"CAM generation completed in {total_time:.2f}s")

        return {
            "docx": str(docx_path),
            "pdf": str(pdf_path) if pdf_path else None,
            "sections": list(sections.keys()),
            "raw_output": str(raw_path),
            "generation_time_seconds": round(total_time, 2)
        }

    except Exception as e:
        logger.exception("CAM generation failed")
        raise RuntimeError(f"CAM generation error: {str(e)}")