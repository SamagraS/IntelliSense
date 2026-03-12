from docx2pdf import convert
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def export_pdf(docx_path):

    try:

        docx_path = Path(docx_path).resolve()

        if not docx_path.exists():
            raise FileNotFoundError(f"DOCX file not found: {docx_path}")

        pdf_path = docx_path.with_suffix(".pdf")

        logger.info(f"Converting DOCX → PDF: {docx_path.name}")

        convert(str(docx_path), str(pdf_path))

        logger.info(f"PDF exported: {pdf_path}")

        return str(pdf_path)

    except Exception as e:
        logger.exception("PDF export failed")
        raise