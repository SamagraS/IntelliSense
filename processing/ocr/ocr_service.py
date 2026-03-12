"""
ocr_service.py
==============
Reusable OCR pipeline for financial PDF documents.

Pipeline
--------
PDF → pdf2image → OpenCV preprocessing → Tesseract OCR → line/word output
                                       → Camelot (lattice) → table output
                                                ↓ (on failure)
                                          pdfplumber (stream) → table output

Public API
----------
    extract_from_pdf(pdf_path, case_id, document_type) -> dict

Dependencies
------------
    pip install pytesseract opencv-python-headless pdf2image camelot-py[cv] \
                pdfplumber pillow numpy pandas
    Tesseract binary must be installed and on PATH (or set TESSERACT_CMD env var).
    On Debian/Ubuntu: sudo apt-get install tesseract-ocr poppler-utils
"""

from __future__ import annotations

import io
import logging
import math
import os
import re
import uuid
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import camelot
import cv2
import pymupdf as fitz  # PyMuPDF — native text extraction for born-digital PDFs
import numpy as np
import pandas as pd
import pdfplumber
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ---------------------------------------------------------------------------
# Optional: override Tesseract binary path via env var
# ---------------------------------------------------------------------------
_TESSERACT_CMD = os.environ.get("TESSERACT_CMD", "")
if _TESSERACT_CMD:
    pytesseract.pytesseract.tesseract_cmd = _TESSERACT_CMD

# ---------------------------------------------------------------------------
# Constants & tunables (override via environment variables)
# ---------------------------------------------------------------------------
# DPI for pdf2image rendering — 300 is the sweet spot for OCR quality
PDF_RENDER_DPI: int = int(os.environ.get("OCR_DPI", "300"))

# Tesseract page-segmentation mode:
#   PSM 3  = Fully automatic page segmentation (default)
#   PSM 6  = Assume a single uniform block of text
#   PSM 11 = Sparse text — good for noisy financial docs with mixed layout
TESSERACT_PSM: int = int(os.environ.get("TESSERACT_PSM", "3"))

# Multi-PSM strategy: try these modes and pick the one with best avg confidence.
# PSM 3 handles mixed layouts; PSM 4 handles single-column financial statements.
# PSM 11 handles sparse text (stamps, letterheads, scattered text).
TESSERACT_PSM_CANDIDATES: list[int] = [3, 4]

# Confidence threshold below which a word is flagged needs_review
CONFIDENCE_THRESHOLD: float = float(os.environ.get("OCR_CONF_THRESHOLD", "75.0"))

# Minimum confidence for a table cell to contribute to table-level confidence
TABLE_CELL_CONF_FLOOR: float = 0.0

# Maximum skew angle (degrees) to attempt correction — beyond this the image
# is likely a multi-column spread or rotated 90°, handled separately.
MAX_DESKEW_ANGLE: float = float(os.environ.get("MAX_DESKEW_ANGLE", "45.0"))

# Camelot edge tolerance settings — increased for thin ruling lines in financial PDFs
CAMELOT_LINE_SCALE: int = int(os.environ.get("CAMELOT_LINE_SCALE", "40"))
CAMELOT_COPY_TEXT: list[str] = ["v"]   # copy spanning cell text vertically

# Tesseract character whitelist for financial documents
TESSERACT_CHAR_WHITELIST: str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.,%-/():₹"

# ---------------------------------------------------------------------------
# Native text extraction (born-digital PDFs)
# ---------------------------------------------------------------------------
# A page qualifies for native text extraction when PyMuPDF can pull at least
# this many non-whitespace characters from it.  Pages below this threshold
# are treated as scanned images and go through the full OCR pipeline.
# Most born-digital financial PDFs yield 200–2000 chars per page.
NATIVE_TEXT_MIN_CHARS: int = int(os.environ.get("NATIVE_TEXT_MIN_CHARS", "50"))

# Synthetic confidence assigned to natively extracted words (text is exact —
# no recognition uncertainty).  Set < 100 to distinguish from OCR in logs.
NATIVE_TEXT_CONFIDENCE: float = 99.0

# ---------------------------------------------------------------------------
# Upscaling parameters
# ---------------------------------------------------------------------------
# If the rendered page image is narrower than this many pixels, it was likely
# rendered at an effective DPI < 200.  We upscale it to TARGET_WIDTH before
# OCR.  Tesseract accuracy drops sharply below ~200 DPI.
UPSCALE_MIN_WIDTH_PX: int = int(os.environ.get("UPSCALE_MIN_WIDTH_PX", "1400"))
UPSCALE_INTERPOLATION: int = cv2.INTER_LANCZOS4  # best quality for upscaling text

# ---------------------------------------------------------------------------
# Internal data structures (plain dataclasses → serialise to dict easily)
# ---------------------------------------------------------------------------

@dataclass
class BBox:
    """Bounding box in pixel coordinates."""
    left: int
    top: int
    width: int
    height: int

    def to_dict(self) -> dict[str, int]:
        return {"left": self.left, "top": self.top,
                "width": self.width, "height": self.height}


@dataclass
class WordToken:
    """Single word extracted by Tesseract."""
    text: str
    confidence: float
    bbox: BBox
    block_num: int
    line_num: int
    word_num: int
    needs_review: bool = False


@dataclass
class LineResult:
    """Aggregated line — all words on the same (block, line)."""
    text: str
    confidence: float       # mean word confidence for the line
    bbox: BBox
    needs_review: bool      # True if any word < CONFIDENCE_THRESHOLD

    def to_dict(self) -> dict[str, Any]:
        return {
            "text": self.text,
            "confidence": round(self.confidence, 2),
            "bbox": self.bbox.to_dict(),
            "needs_review": self.needs_review,
        }


@dataclass
class TableResult:
    """Extracted table with rows and a heuristic confidence score."""
    table_id: str
    rows: list[dict[str, Any]]
    confidence: float       # heuristic: fraction of non-empty cells
    extraction_method: str  # "camelot_lattice" | "camelot_stream" | "pdfplumber"
    page_number: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
            "rows": self.rows,
            "confidence": round(self.confidence, 4),
            "extraction_method": self.extraction_method,
            "page_number": self.page_number,
        }


@dataclass
class PageResult:
    """All OCR and table data for a single PDF page."""
    page_number: int
    lines: list[LineResult] = field(default_factory=list)
    tables: list[TableResult] = field(default_factory=list)
    extraction_method: str = "ocr"   # "native_text" | "ocr"

    def to_dict(self) -> dict[str, Any]:
        return {
            "page_number": self.page_number,
            "lines": [ln.to_dict() for ln in self.lines],
            "tables": [tbl.to_dict() for tbl in self.tables],
            "extraction_method": self.extraction_method,
        }


# ---------------------------------------------------------------------------
# Image preprocessing
# ---------------------------------------------------------------------------

def _compute_skew_angle(image: np.ndarray) -> float:
    """
    Estimate the skew angle of a binarised image using the minimum-area
    rectangle around the largest cluster of foreground pixels.

    Returns angle in degrees; positive = clockwise tilt.
    Returns 0.0 if no reliable angle can be estimated.
    """
    # Work on a copy to avoid mutating the caller's array
    gray = image if image.ndim == 2 else cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Threshold to get foreground mask
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # Find all non-zero (foreground) pixel coordinates
    coords = np.column_stack(np.where(binary > 0))
    if coords.shape[0] < 50:
        logger.debug("Too few foreground pixels to estimate skew — skipping.")
        return 0.0

    # minAreaRect returns ((cx,cy), (w,h), angle)
    rect = cv2.minAreaRect(coords)
    angle = rect[-1]

    # OpenCV returns angles in [-90, 0); normalise to [-45, 45]
    if angle < -45:
        angle = 90 + angle

    return float(angle)


def _deskew(image: np.ndarray, angle: float) -> np.ndarray:
    """
    Rotate *image* by *-angle* degrees around its centre to straighten text.
    Uses INTER_CUBIC resampling and fills borders with white (255).
    """
    (h, w) = image.shape[:2]
    centre = (w // 2, h // 2)
    M = cv2.getRotationMatrix2D(centre, -angle, scale=1.0)

    # Expand canvas to avoid clipping after rotation
    cos_a = abs(M[0, 0])
    sin_a = abs(M[0, 1])
    new_w = int(h * sin_a + w * cos_a)
    new_h = int(h * cos_a + w * sin_a)
    M[0, 2] += (new_w - w) / 2
    M[1, 2] += (new_h - h) / 2

    rotated = cv2.warpAffine(
        image, M, (new_w, new_h),
        flags=cv2.INTER_CUBIC,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=255,
    )
    return rotated


def _upscale_if_needed(image: np.ndarray) -> tuple[np.ndarray, float]:
    """
    Upscale *image* with Lanczos4 if its width is below UPSCALE_MIN_WIDTH_PX.

    Tesseract accuracy degrades noticeably when character height is below
    ~20 pixels.  A standard A4 page at 300 DPI is ~2480×3508 px; pages
    narrower than UPSCALE_MIN_WIDTH_PX (default 1400 px) were likely rendered
    at a low effective DPI and benefit from upscaling before binarization.

    Returns
    -------
    (upscaled_image, scale_factor)
        scale_factor == 1.0 when no upscaling was applied.
    """
    h, w = image.shape[:2]
    if w >= UPSCALE_MIN_WIDTH_PX:
        return image, 1.0

    scale = UPSCALE_MIN_WIDTH_PX / w
    new_w = int(w * scale)
    new_h = int(h * scale)
    upscaled = cv2.resize(image, (new_w, new_h), interpolation=UPSCALE_INTERPOLATION)
    logger.debug("Upscaled from %dx%d → %dx%d (×%.2f)", w, h, new_w, new_h, scale)
    return upscaled, scale


def _remove_background_shadows(gray: np.ndarray) -> np.ndarray:
    """
    Remove uneven lighting / scanner shadows using morphological background
    estimation and top-hat correction.

    Works by estimating the large-scale background (non-text) via a dilated
    morphological kernel, then subtracting it from the image and normalising.
    This handles documents with gradient shading, coffee-stain artefacts, or
    scanner bed shadows without touching the fine text strokes.

    Parameters
    ----------
    gray : np.ndarray
        Single-channel uint8 image (already grayscale).

    Returns
    -------
    np.ndarray
        Background-corrected grayscale image; same dtype/shape as input.
    """
    # Adaptive kernel size: 101x101 for normal pages, 51x51 for small pages
    # Oversized kernels bleed text strokes into background estimate
    h, w = gray.shape
    kernel_size = 51 if w < 1000 else 101
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (kernel_size, kernel_size))
    background = cv2.dilate(gray, kernel)
    # Subtract: bright areas disappear, text (dark on light) remains
    corrected = cv2.subtract(background, gray)
    # Normalise to full 0-255 range
    corrected = cv2.normalize(corrected, None, 0, 255, cv2.NORM_MINMAX)  # type: ignore[call-overload]
    return corrected


def preprocess_image(image: np.ndarray) -> np.ndarray:
    """
    Apply an improved preprocessing pipeline to maximise Tesseract confidence
    on financial document pages (scanned or low-quality).

    Pipeline (in order)
    -------------------
    1.  Grayscale conversion.
    2.  Upscale if the image is below the minimum width threshold
        (catches low-DPI renders — the single biggest cause of <70 % conf).
    3.  Background shadow removal via morphological top-hat correction
        (handles scanner gradients and uneven lighting).
    4.  CLAHE (Contrast Limited Adaptive Histogram Equalisation)
        — boosts local contrast in low-contrast regions before denoising.
    5.  Non-local means denoising with adaptive strength
        (h scales with estimated noise level so we don't over-smooth).
    6.  Unsharp mask (Gaussian-based) to sharpen character edges.
    7.  Otsu binarisation on the sharpened image.
    8.  Deskew using minAreaRect angle estimation.
    9.  Border crop (remove thin scanner borders).

    Parameters
    ----------
    image : np.ndarray
        Input image array (BGR or grayscale, uint8).

    Returns
    -------
    np.ndarray
        Preprocessed single-channel (grayscale) uint8 image ready for
        Tesseract ingestion.
    """
    # ------------------------------------------------------------------
    # Step 1: Grayscale
    # ------------------------------------------------------------------
    if image.ndim == 3:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        gray = image.copy()

    logger.debug("Step 1 — grayscale: shape=%s", gray.shape)

    # ------------------------------------------------------------------
    # Step 2: Upscale if needed
    # Low-DPI pages are the #1 cause of sub-70% Tesseract confidence.
    # Lanczos4 preserves character edge quality better than bilinear.
    # ------------------------------------------------------------------
    gray, _scale = _upscale_if_needed(gray)
    logger.debug("Step 2 — upscale: shape=%s (scale=%.2f)", gray.shape, _scale)

    # ------------------------------------------------------------------
    # Step 3: Background shadow removal
    # Scanned PDFs often have gradient shading from scanner lids / page curl.
    # Skip on very small images where the kernel would dominate the image.
    # ------------------------------------------------------------------
    h, w = gray.shape
    if min(h, w) > 150:
        gray = _remove_background_shadows(gray)
        logger.debug("Step 3 — shadow removal applied")
    else:
        logger.debug("Step 3 — shadow removal skipped (image too small)")

    # ------------------------------------------------------------------
    # Step 4: CLAHE — adaptive contrast enhancement
    # Applied BEFORE denoising: better contrast → denoiser can distinguish
    # signal (text edges) from noise more accurately.
    # clipLimit=2.0 prevents over-amplifying noise in uniform regions.
    # tileGridSize=(8,8) gives per-region adaptation at paragraph scale.
    # ------------------------------------------------------------------
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    gray = clahe.apply(gray)
    logger.debug("Step 4 — CLAHE applied")

    # ------------------------------------------------------------------
    # Step 5: Adaptive denoising
    # Estimate noise level from local std-dev; scale h parameter accordingly.
    # h=5  → light denoising (clean scans)
    # h=15 → heavy denoising (aged/photocopied documents)
    # ------------------------------------------------------------------
    noise_estimate = float(gray.std())
    h_param = int(np.clip(noise_estimate * 0.4, 5, 20))
    denoised = cv2.fastNlMeansDenoising(
        gray, h=h_param, templateWindowSize=7, searchWindowSize=21
    )
    logger.debug("Step 5 — denoised (h=%d, noise_est=%.1f)", h_param, noise_estimate)

    # ------------------------------------------------------------------
    # Step 6: Unsharp mask
    # Sharpens character stroke edges so Tesseract can cleanly separate
    # strokes from inter-character spacing.
    # Formula: sharpened = original × (1 + amount) − blurred × amount
    # amount=0.5 is conservative; raise to 1.0 for very blurry scans.
    # ------------------------------------------------------------------
    blurred = cv2.GaussianBlur(denoised, (0, 0), sigmaX=2.0)
    sharpened = cv2.addWeighted(denoised, 1.5, blurred, -0.5, 0)
    logger.debug("Step 6 — unsharp mask applied")

    # ------------------------------------------------------------------
    # Step 7: Binarise — Otsu on the sharpened image
    # Otsu automatically finds the optimal threshold between text and
    # background from the bimodal histogram.
    # ------------------------------------------------------------------
    _, binary = cv2.threshold(sharpened, 0, 255,
                              cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    logger.debug("Step 7 — Otsu binarised")

    # ------------------------------------------------------------------
    # Step 8: Deskew
    # ------------------------------------------------------------------
    angle = _compute_skew_angle(binary)
    logger.debug("Step 8 — detected skew angle: %.2f°", angle)

    if abs(angle) < 0.5:
        deskewed = binary
    elif abs(angle) > MAX_DESKEW_ANGLE:
        logger.warning(
            "Skew angle %.2f° exceeds MAX_DESKEW_ANGLE=%.1f° — "
            "page may be rotated 90°; skipping deskew.",
            angle, MAX_DESKEW_ANGLE,
        )
        deskewed = binary
    else:
        deskewed = _deskew(binary, angle)
        logger.debug("Step 8 — deskewed by %.2f°", angle)

    # ------------------------------------------------------------------
    # Step 9: Border crop
    # Remove thin black borders introduced by scanning that confuse
    # Tesseract's layout analysis.
    # ------------------------------------------------------------------
    inverted = cv2.bitwise_not(deskewed)
    coords = cv2.findNonZero(inverted)

    if coords is not None:
        x, y, bw, bh = cv2.boundingRect(coords)
        margin = 10
        x = max(0, x - margin)
        y = max(0, y - margin)
        x2 = min(deskewed.shape[1], x + bw + margin * 2)
        y2 = min(deskewed.shape[0], y + bh + margin * 2)
        cropped = deskewed[y:y2, x:x2]
        logger.debug("Step 9 — border-cropped to (%d,%d,%d,%d)", x, y, x2, y2)
    else:
        cropped = deskewed
        logger.debug("Step 9 — no border crop needed")

    return cropped


# ---------------------------------------------------------------------------
# Tesseract OCR helpers
# ---------------------------------------------------------------------------

def _pil_to_cv(pil_image: Image.Image) -> np.ndarray:
    """Convert a PIL RGBA/RGB image to a BGR numpy array for OpenCV."""
    img = pil_image.convert("RGB")
    return cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)


def _run_tesseract(image: np.ndarray) -> pd.DataFrame:
    """
    Run Tesseract on *image* using a multi-PSM winner strategy.

    Strategy
    --------
    We run Tesseract for each PSM in TESSERACT_PSM_CANDIDATES and return the
    result with the highest mean confidence.  This handles:
      - PSM 3 (auto): best for mixed-layout pages (narrative + tables).
      - PSM 4 (single column): best for financial statement pages with
        uniform column structure.

    The winning PSM is logged so it can be inspected in production.

    Confidence filtering
    --------------------
    Rows with conf == -1 are non-word layout elements (paragraphs, lines,
    blocks) — Tesseract emits them when using image_to_data.  We drop them.
    Rows with empty text are also dropped.  Rows with conf == 0 are
    genuine low-confidence detections and are KEPT — they represent real
    text Tesseract couldn't read confidently; excluding them would hide
    the true quality of the document.

    Returns
    -------
    pd.DataFrame
        Columns: level, page_num, block_num, par_num, line_num, word_num,
                 left, top, width, height, conf, text.
        All rows have conf > -1 and non-empty text.
    """
    best_df: pd.DataFrame | None = None
    best_conf: float = -1.0
    best_psm: int = TESSERACT_PSM_CANDIDATES[0]

    for psm in TESSERACT_PSM_CANDIDATES:
        # Add character whitelist for financial documents to reduce hallucinations
        config = f"--psm {psm} --oem 1 -c tessedit_char_whitelist={TESSERACT_CHAR_WHITELIST}"
        try:
            data = pytesseract.image_to_data(
                image,
                config=config,
                output_type=pytesseract.Output.DATAFRAME,
                lang="eng",
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Tesseract PSM %d failed: %s", psm, exc)
            continue

        # Drop layout-level rows (conf == -1), noise detections (conf == 0), and empty-text rows
        # conf > 0 prevents zero-confidence noise tokens (table borders, stamps) from
        # entering _aggregate_lines and polluting the confidence metrics
        filtered = data[
            (data["conf"] > 0) & (data["text"].str.strip() != "")
        ].copy()
        filtered["conf"] = filtered["conf"].astype(float)

        if filtered.empty:
            continue

        # Evaluate: mean confidence of all detected word tokens
        # Filter out conf == 0 (noise detections) from the metric
        valid_confs = filtered[filtered["conf"] > 0]["conf"]
        if len(valid_confs) == 0:
            mean_conf = 0.0
        else:
            mean_conf = float(valid_confs.mean())
        logger.debug("PSM %d → %d words, avg conf %.1f%%", psm, len(filtered), mean_conf)

        if mean_conf > best_conf:
            best_conf = mean_conf
            best_df = filtered
            best_psm = psm
    
    # If both PSM 3 and 4 scored below 75%, try PSM 11 (sparse text)
    if best_conf < 75.0 and 11 not in TESSERACT_PSM_CANDIDATES:
        logger.debug("PSM 3 and 4 both below 75%%, trying PSM 11 (sparse text)")
        config = f"--psm 11 --oem 1 -c tessedit_char_whitelist={TESSERACT_CHAR_WHITELIST}"
        try:
            data = pytesseract.image_to_data(
                image,
                config=config,
                output_type=pytesseract.Output.DATAFRAME,
                lang="eng",
            )
            filtered = data[
                (data["conf"] > 0) & (data["text"].str.strip() != "")
            ].copy()
            filtered["conf"] = filtered["conf"].astype(float)
            
            if not filtered.empty:
                valid_confs = filtered[filtered["conf"] > 0]["conf"]
                if len(valid_confs) > 0:
                    mean_conf_11 = float(valid_confs.mean())
                    logger.debug("PSM 11 → %d words, avg conf %.1f%%", len(filtered), mean_conf_11)
                    
                    if mean_conf_11 > best_conf:
                        best_conf = mean_conf_11
                        best_df = filtered
                        best_psm = 11
        except Exception as exc:
            logger.warning("Tesseract PSM 11 failed: %s", exc)

    if best_df is None:
        logger.warning("All Tesseract PSM candidates returned empty results")
        return pd.DataFrame(
            columns=["level","page_num","block_num","par_num","line_num",
                     "word_num","left","top","width","height","conf","text"]
        )

    logger.info("  Tesseract winner: PSM %d (avg conf %.1f%%)", best_psm, best_conf)
    return best_df.reset_index(drop=True)


def _aggregate_lines(word_df: pd.DataFrame) -> list[LineResult]:
    """
    Group Tesseract word-level output by (block_num, par_num, line_num) and
    reconstruct logical text lines with aggregate confidence and bounding box.

    Words within a line are sorted by their left-pixel coordinate.
    """
    if word_df.empty:
        return []

    lines: list[LineResult] = []
    group_cols = ["block_num", "par_num", "line_num"]

    for _, line_words in word_df.groupby(group_cols, sort=True):
        # Sort words left-to-right within the line
        line_words = line_words.sort_values("left")

        texts = line_words["text"].tolist()
        confs = line_words["conf"].tolist()

        # Combine words into a single line string
        line_text = " ".join(str(t) for t in texts).strip()
        if not line_text:
            continue

        # Filter out conf == 0 (noise) from line confidence calculation
        valid_confs = [c for c in confs if c > 0]
        mean_conf = float(np.mean(valid_confs)) if valid_confs else 0.0
        needs_review = any(c < CONFIDENCE_THRESHOLD for c in confs)

        # Bounding box = union of all word boxes in the line
        left = int(line_words["left"].min())
        top = int(line_words["top"].min())
        right = int((line_words["left"] + line_words["width"]).max())
        bottom = int((line_words["top"] + line_words["height"]).max())

        line_bbox = BBox(
            left=left,
            top=top,
            width=right - left,
            height=bottom - top,
        )

        lines.append(LineResult(
            text=line_text,
            confidence=mean_conf,
            bbox=line_bbox,
            needs_review=needs_review,
        ))

    return lines


# ---------------------------------------------------------------------------
# Table extraction helpers
# ---------------------------------------------------------------------------

def _table_confidence(df: pd.DataFrame) -> float:
    """
    Structural table confidence score based on:
    1. Column consistency: reward tables where rows have similar column counts
    2. Reasonable fill rate: penalize if >40% of cells are empty
    
    This replaces the simple fill-rate heuristic which penalizes tables with
    merged/spanning cells (legal in financial tables).
    """
    if df.empty:
        return 0.0
    
    # Column consistency score
    row_col_counts = df.notna().sum(axis=1)
    if len(row_col_counts) > 0:
        col_variance = row_col_counts.std() / (row_col_counts.mean() + 1e-6)
        consistency_score = max(0.0, 1.0 - col_variance)
    else:
        consistency_score = 0.0
    
    # Fill rate score with threshold
    total = df.size
    non_empty = df.map(
        lambda v: bool(str(v).strip()) if v is not None and str(v).strip() not in ['nan', 'None'] else False
    ).values.sum()
    fill_rate = float(non_empty) / float(total) if total > 0 else 0.0
    
    # Penalize if >40% empty, otherwise give full credit
    fill_score = min(1.0, fill_rate / 0.6) if fill_rate < 0.6 else 1.0
    
    # Weighted combination: 60% consistency, 40% fill rate
    return 0.6 * consistency_score + 0.4 * fill_score


def _dataframe_to_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert a Camelot/pdfplumber DataFrame to a list of row dicts.

    The first row is treated as the header if it looks like a header
    (all values are non-numeric strings).  Otherwise column indices
    (col_0, col_1, …) are used as keys.
    """
    if df.empty:
        return []

    df = df.copy().reset_index(drop=True)

    # Attempt to promote first row to header
    first_row = df.iloc[0].tolist()
    is_header = all(
        isinstance(v, str) and not _is_numeric(v) for v in first_row
    )

    if is_header:
        df.columns = [str(v).strip() or f"col_{i}"
                      for i, v in enumerate(first_row)]
        df = df.iloc[1:].reset_index(drop=True)
    else:
        df.columns = [f"col_{i}" for i in range(len(df.columns))]

    # Replace NaN / None with empty string BEFORE converting to string
    # This prevents 'nan' strings from appearing in the output
    df = df.fillna("").astype(str)
    return df.to_dict(orient="records")


def _is_numeric(value: str) -> bool:
    """Return True if *value* can be parsed as a number (int or float)."""
    try:
        float(value.replace(",", "").replace("₹", "").strip())
        return True
    except (ValueError, AttributeError):
        return False


def _merge_split_tables(tables: list[TableResult]) -> list[TableResult]:
    """
    Merge consecutive tables that appear to be fragments of the same logical table.
    
    Camelot frequently splits one table at page header breaks. This function
    merges tables that:
    1. Have the same number of columns
    2. Are on the same page
    3. Would be within 20px vertically if bbox data were available
    
    Since TableResult doesn't store bbox, we merge based on column count matching.
    """
    if len(tables) <= 1:
        return tables
    
    merged: list[TableResult] = []
    i = 0
    
    while i < len(tables):
        current = tables[i]
        
        # Look ahead to see if next table should be merged
        if i + 1 < len(tables):
            next_table = tables[i + 1]
            
            # Check if tables should be merged:
            # 1. Same page
            # 2. Same column count in their rows
            if (current.page_number == next_table.page_number and 
                current.rows and next_table.rows):
                
                current_cols = len(current.rows[0]) if current.rows else 0
                next_cols = len(next_table.rows[0]) if next_table.rows else 0
                
                if current_cols == next_cols and current_cols > 0:
                    # Merge: combine rows
                    merged_rows = current.rows + next_table.rows
                    merged_conf = (current.confidence + next_table.confidence) / 2.0
                    
                    merged_table = TableResult(
                        table_id=current.table_id,  # Keep first table's ID
                        rows=merged_rows,
                        confidence=merged_conf,
                        extraction_method=current.extraction_method,
                        page_number=current.page_number,
                    )
                    merged.append(merged_table)
                    i += 2  # Skip both tables
                    logger.debug(f"Merged tables {current.table_id} and {next_table.table_id}")
                    continue
        
        # No merge, keep current table
        merged.append(current)
        i += 1
    
    return merged


def _detect_table_lines_opencv(
    image: np.ndarray,
    min_line_length: int = 50
) -> tuple[list[tuple[int, int, int, int]], list[tuple[int, int, int, int]]]:
    """
    Detect horizontal and vertical line segments in *image* using OpenCV HoughLinesP.
    Returns (horizontal_lines, vertical_lines) as lists of (x1, y1, x2, y2) tuples.
    
    This is used to find table boundaries in scanned financial PDFs where Camelot's
    vector-based line detection fails because the PDF contains only raster images.
    
    Parameters
    ----------
    image : np.ndarray
        Grayscale or BGR image
    min_line_length : int
        Minimum line length in pixels (default 50)
    
    Returns
    -------
    tuple[list, list]
        (horizontal_lines, vertical_lines) where each line is (x1, y1, x2, y2)
    """
    # Ensure grayscale
    if len(image.shape) == 3:
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    else:
        gray = image.copy()
    
    # Apply adaptive threshold for robust line detection
    binary = cv2.adaptiveThreshold(
        gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
    )
    
    # Detect horizontal lines
    horizontal_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (min_line_length, 1))
    horizontal_mask = cv2.morphologyEx(binary, cv2.MORPH_OPEN, horizontal_kernel, iterations=2)
    
    # Detect vertical lines
    vertical_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, min_line_length))
    vertical_mask = cv2.morphologyEx(binary, cv2.MORPH_OPEN, vertical_kernel, iterations=2)
    
    # Extract line coordinates using HoughLinesP
    horizontal_lines = []
    vertical_lines = []
    
    # Horizontal line segments
    h_lines = cv2.HoughLinesP(
        horizontal_mask, rho=1, theta=np.pi/180, threshold=100,
        minLineLength=min_line_length, maxLineGap=10
    )
    if h_lines is not None:
        for line in h_lines:
            x1, y1, x2, y2 = line[0]
            horizontal_lines.append((x1, y1, x2, y2))
    
    # Vertical line segments
    v_lines = cv2.HoughLinesP(
        vertical_mask, rho=1, theta=np.pi/180, threshold=100,
        minLineLength=min_line_length, maxLineGap=10
    )
    if v_lines is not None:
        for line in v_lines:
            x1, y1, x2, y2 = line[0]
            vertical_lines.append((x1, y1, x2, y2))
    
    logger.debug(
        "OpenCV line detection: %d horizontal, %d vertical",
        len(horizontal_lines), len(vertical_lines)
    )
    
    return horizontal_lines, vertical_lines


def _extract_tables_regex_columnar(
    lines: list[LineResult],
    page_number: int
) -> list[TableResult]:
    """
    Fallback parser for financial result PDFs that are scanned images.
    
    Financial results follow predictable columnar patterns like:
        Particulars | Q2FY24 | Q2FY23 | H1FY24 | H1FY23
        Revenue     | 1234   | 1100   | 2500   | 2200
    
    This function uses regex to detect column headers and parse tabular data
    from the extracted text lines when Camelot returns 0 tables.
    
    Parameters
    ----------
    lines : list[LineResult]
        Text lines extracted via OCR
    page_number : int
        Page number for table ID generation
    
    Returns
    -------
    list[TableResult]
        Extracted tables (may be empty if no patterns matched)
    """
    if len(lines) < 5:
        # Need at least header + a few data rows
        return []
    
    # Common financial result column headers (case-insensitive patterns)
    header_patterns = [
        r"particulars.*(?:q[1-4]fy\d{2}|fy\d{4}|current|previous)",
        r"description.*(?:year|quarter|period)",
        r"items.*(?:amount|value)",
    ]
    
    # Find header row
    header_idx = -1
    for idx, line in enumerate(lines):
        text_lower = line.text.lower()
        for pattern in header_patterns:
            if re.search(pattern, text_lower):
                header_idx = idx
                break
        if header_idx >= 0:
            break
    
    if header_idx < 0:
        logger.debug("No financial result header pattern found on page %d", page_number)
        return []
    
    # Extract header columns by splitting on 2+ spaces or tabs
    header_text = lines[header_idx].text
    columns = [col.strip() for col in re.split(r"\s{2,}|\t", header_text) if col.strip()]
    
    if len(columns) < 2:
        logger.debug("Header row has <2 columns, skipping regex parser on page %d", page_number)
        return []
    
    logger.info(
        "Regex columnar parser: detected %d-column table on page %d (header: %s)",
        len(columns), page_number, columns[:3]
    )
    
    # Parse subsequent rows until confidence drops or empty line
    rows_data = [columns]  # First row is header
    
    for line in lines[header_idx + 1:]:
        text = line.text.strip()
        if not text:
            break  # Empty line signals end of table
        
        # Split row into columns
        cells = [cell.strip() for cell in re.split(r"\s{2,}|\t", text) if cell.strip()]
        
        # Only add if column count matches (allow ±1 for merged cells)
        if abs(len(cells) - len(columns)) <= 1:
            # Pad or trim to match column count
            while len(cells) < len(columns):
                cells.append("")
            cells = cells[:len(columns)]
            rows_data.append(cells)
        
        # Stop after 50 rows or low confidence (likely noise)
        if len(rows_data) > 50 or line.confidence < 50.0:
            break
    
    if len(rows_data) < 2:
        # Header only, no data
        return []
    
    # Convert to TableResult
    df = pd.DataFrame(rows_data[1:], columns=rows_data[0])
    confidence = _table_confidence(df)
    rows_dict = _dataframe_to_rows(df)
    
    return [TableResult(
        table_id=f"p{page_number}_t1_regex",
        rows=rows_dict,
        confidence=confidence,
        extraction_method="regex_columnar",
        page_number=page_number,
    )]


def _extract_tables_camelot_lattice(
    pdf_path: str, page_number: int
) -> list[TableResult]:
    """
    Extract tables from *page_number* of *pdf_path* using Camelot in lattice
    mode (ruled-line detection).  Returns an empty list on any error.
    """
    results: list[TableResult] = []
    try:
        # Camelot page numbers are 1-indexed and accept a string
        # edge_tol and joint_tol help with imperfect scans and thin lines
        tables = camelot.read_pdf(
            pdf_path,
            pages=str(page_number),
            flavor="lattice",
            line_scale=CAMELOT_LINE_SCALE,
            copy_text=CAMELOT_COPY_TEXT,
            edge_tol=500,
            joint_tol=5,
            suppress_stdout=True,
        )

        for idx, table in enumerate(tables):
            df: pd.DataFrame = table.df
            conf = _table_confidence(df)
            rows = _dataframe_to_rows(df)
            results.append(TableResult(
                table_id=f"p{page_number}_t{idx + 1}",
                rows=rows,
                confidence=conf,
                extraction_method="camelot_lattice",
                page_number=page_number,
            ))

        if results:
            logger.info(
                "Camelot lattice: page %d → %d table(s)", page_number, len(results)
            )
    except Exception as exc:  # pylint: disable=broad-except
        logger.debug("Camelot lattice failed on page %d: %s", page_number, exc)

    return results


def _extract_tables_camelot_stream(
    pdf_path: str, page_number: int
) -> list[TableResult]:
    """
    Fallback: Camelot in stream mode (whitespace-delimited columns).
    row_tol and column_tol tuned for dense financial tables.
    """
    results: list[TableResult] = []
    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=str(page_number),
            flavor="stream",
            row_tol=10,
            column_tol=0,
            suppress_stdout=True,
        )

        for idx, table in enumerate(tables):
            df: pd.DataFrame = table.df
            conf = _table_confidence(df)
            rows = _dataframe_to_rows(df)
            results.append(TableResult(
                table_id=f"p{page_number}_t{idx + 1}",
                rows=rows,
                confidence=conf,
                extraction_method="camelot_stream",
                page_number=page_number,
            ))

        if results:
            logger.info(
                "Camelot stream: page %d → %d table(s)", page_number, len(results)
            )
    except Exception as exc:  # pylint: disable=broad-except
        logger.debug("Camelot stream failed on page %d: %s", page_number, exc)

    return results


def _extract_tables_pdfplumber(
    pdf_path: str, page_number: int
) -> list[TableResult]:
    """
    Final fallback: pdfplumber for stream-style tables (no visible ruling
    lines). Uses strict line detection strategies for better accuracy.
    """
    results: list[TableResult] = []
    try:
        # Settings optimized for ruled-line financial PDFs
        table_settings = {
            "vertical_strategy": "lines_strict",
            "horizontal_strategy": "lines_strict",
            "text_x_tolerance": 5,
            "text_y_tolerance": 5,
        }
        
        with pdfplumber.open(pdf_path) as pdf:
            # pdfplumber is 0-indexed; our page_number is 1-indexed
            page_idx = page_number - 1
            if page_idx < 0 or page_idx >= len(pdf.pages):
                return results

            page = pdf.pages[page_idx]
            raw_tables = page.extract_tables(table_settings=table_settings)

            for idx, raw_table in enumerate(raw_tables):
                if not raw_table:
                    continue
                df = pd.DataFrame(raw_table)
                conf = _table_confidence(df)
                rows = _dataframe_to_rows(df)
                results.append(TableResult(
                    table_id=f"p{page_number}_t{idx + 1}",
                    rows=rows,
                    confidence=conf,
                    extraction_method="pdfplumber",
                    page_number=page_number,
                ))

        if results:
            logger.info(
                "pdfplumber: page %d → %d table(s)", page_number, len(results)
            )
    except Exception as exc:  # pylint: disable=broad-except
        logger.warning("pdfplumber failed on page %d: %s", page_number, exc)

    return results


def _extract_tables_for_page(
    pdf_path: str,
    page_number: int,
    text_lines: list[LineResult] | None = None
) -> list[TableResult]:
    """
    Orchestrate table extraction for a single page using the cascade:
      1. Camelot lattice   (best for ruled/grid tables)
      2. Camelot stream    (whitespace tables)
      3. pdfplumber        (catch-all)
      4. Regex columnar    (for scanned financial results with >30 text lines)

    A result set is considered successful when at least one table is found
    with confidence > 0.3 (at least 30% of cells are non-empty).
    
    Parameters
    ----------
    pdf_path : str
        Path to PDF file
    page_number : int
        1-indexed page number
    text_lines : list[LineResult], optional
        Extracted text lines (if available). Used for regex fallback when
        Camelot returns 0 tables on scanned financial documents.
    """
    MIN_USEFUL_CONFIDENCE = 0.30

    # --- Attempt 1: Camelot lattice ---
    tables = _extract_tables_camelot_lattice(pdf_path, page_number)
    tables = _merge_split_tables(tables)  # Merge split tables
    if tables and any(t.confidence > MIN_USEFUL_CONFIDENCE for t in tables):
        return tables

    # --- Attempt 2: Camelot stream ---
    tables = _extract_tables_camelot_stream(pdf_path, page_number)
    tables = _merge_split_tables(tables)  # Merge split tables
    if tables and any(t.confidence > MIN_USEFUL_CONFIDENCE for t in tables):
        return tables

    # --- Attempt 3: pdfplumber ---
    tables = _extract_tables_pdfplumber(pdf_path, page_number)
    tables = _merge_split_tables(tables)  # Merge split tables
    if tables:
        return tables
    
    # --- Attempt 4: Regex columnar parser (scanned financial results) ---
    # If Camelot/pdfplumber returned 0 tables AND we have >30 text lines,
    # try the regex-based columnar parser for financial results.
    # This handles BSE/NSE scanned PDFs where Camelot fails because the PDF
    # contains only raster images with no vector lines or selectable text.
    if text_lines and len(text_lines) > 30:
        logger.info(
            "  Page %d: Camelot/pdfplumber returned 0 tables but page has %d text lines. "
            "Trying regex columnar parser for scanned financial results...",
            page_number, len(text_lines)
        )
        tables = _extract_tables_regex_columnar(text_lines, page_number)
        if tables:
            logger.info(
                "  Page %d: Regex columnar parser extracted %d table(s)",
                page_number, len(tables)
            )
            return tables
    
    return []


# ---------------------------------------------------------------------------
# PDF-level helpers
# ---------------------------------------------------------------------------

def _get_page_count(pdf_path: str) -> int:
    """
    Return the total number of pages in *pdf_path* via pdfplumber.
    
    Returns 0 if the PDF cannot be opened (e.g., password-protected,
    corrupt file, or unsupported PDF 2.0 features).
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return len(pdf.pages)
    except Exception as exc:
        logger.error(
            "Failed to open PDF '%s' for page count: %s. "
            "Common causes: password-protected (BSE/NSE filings often have owner passwords), "
            "corrupt file, or PDF 2.0 features unsupported by installed poppler version.",
            pdf_path, exc
        )
        return 0


# ---------------------------------------------------------------------------
# Native text extraction (born-digital PDFs) via PyMuPDF
# ---------------------------------------------------------------------------

def _count_native_chars(pdf_path: str, page_number: int) -> int:
    """
    Return the count of non-whitespace characters PyMuPDF can extract from
    *page_number* (1-indexed) of *pdf_path*.

    A high count (>= NATIVE_TEXT_MIN_CHARS) means the page is born-digital
    and we can skip OCR entirely.  A low count means the page is a raster
    image (scanned) — OCR is required.
    """
    try:
        doc = fitz.open(pdf_path)
        page = doc[page_number - 1]   # fitz is 0-indexed
        text = page.get_text("text")
        doc.close()
        return len(text.replace(" ", "").replace("\n", "").replace("\t", ""))
    except Exception as exc:
        logger.warning(
            "PyMuPDF failed to inspect page %d of '%s': %s",
            page_number, pdf_path, exc,
        )
        return 0


def _extract_native_text_lines(pdf_path: str, page_number: int) -> list[LineResult]:
    """
    Extract text lines from a born-digital PDF page using PyMuPDF's block-level
    layout API.  Returns a list of :class:`LineResult` with synthetic
    confidence = NATIVE_TEXT_CONFIDENCE (99.0).

    PyMuPDF preserves the visual layout: each block corresponds to a paragraph
    or table cell region, and each line within a block maps cleanly to a
    visual text line.

    Parameters
    ----------
    pdf_path : str
        Path to the PDF.
    page_number : int
        1-indexed page number.

    Returns
    -------
    list[LineResult]
        Ordered top-to-bottom, left-to-right as in the original document.
    """
    doc = fitz.open(pdf_path)
    page = doc[page_number - 1]

    # get_text("dict") returns a nested dict:
    #   page → blocks → lines → spans → chars
    # We work at the "lines" level to match our LineResult model.
    page_dict = page.get_text("dict", sort=True)  # sort=True → reading order
    page_width  = int(page.rect.width)
    page_height = int(page.rect.height)
    doc.close()

    lines: list[LineResult] = []

    for block in page_dict.get("blocks", []):
        # Skip image-only blocks (no text content)
        if block.get("type") != 0:
            continue

        for line in block.get("lines", []):
            # Concatenate all span texts in the line
            line_text = " ".join(
                span.get("text", "").strip()
                for span in line.get("spans", [])
            ).strip()

            if not line_text:
                continue

            # PyMuPDF bbox is in PDF points (72 dpi); keep as-is for now —
            # downstream consumers that need pixel coords should apply
            # (page_width / page.rect.width) scaling.
            bbox_raw = line.get("bbox", (0, 0, 0, 0))
            bbox = BBox(
                left=int(bbox_raw[0]),
                top=int(bbox_raw[1]),
                width=int(bbox_raw[2] - bbox_raw[0]),
                height=int(bbox_raw[3] - bbox_raw[1]),
            )

            lines.append(LineResult(
                text=line_text,
                confidence=NATIVE_TEXT_CONFIDENCE,
                bbox=bbox,
                needs_review=False,  # native text is exact — no review needed
            ))

    logger.debug(
        "Native text extraction: page %d → %d lines", page_number, len(lines)
    )
    return lines


def _render_page_to_pil(pdf_path: str, page_number: int) -> Image.Image:
    """
    Render a single PDF page to a PIL Image at PDF_RENDER_DPI resolution.
    page_number is 1-indexed.
    """
    images = convert_from_path(
        pdf_path,
        dpi=PDF_RENDER_DPI,
        first_page=page_number,
        last_page=page_number,
    )
    if not images:
        raise RuntimeError(
            f"pdf2image returned no images for page {page_number} of '{pdf_path}'"
        )
    return images[0]


# ---------------------------------------------------------------------------
# Core processing function
# ---------------------------------------------------------------------------

def _process_page(
    pdf_path: str,
    page_number: int,
    pil_image: Image.Image,
) -> PageResult:
    """
    Process a single PDF page, automatically choosing the best extraction path.

    Extraction strategy
    -------------------
    1. **Native text path** (born-digital PDFs):
       If PyMuPDF can extract >= NATIVE_TEXT_MIN_CHARS non-whitespace
       characters from the page, we use PyMuPDF's layout-aware text
       extraction directly.  This yields NATIVE_TEXT_CONFIDENCE (99%) for
       all lines and is 10–50× faster than OCR.

       This is the dominant case for: annual reports, rating reports, sanction
       letters, GSTR filings, and most NBFC financial statements — all of
       which are typically generated by document software, not scanners.

    2. **OCR path** (scanned / image-only pages):
       If native text is insufficient, the page image goes through the full
       OpenCV preprocessing pipeline (upscale → shadow removal → CLAHE →
       denoise → sharpen → binarise → deskew) followed by multi-PSM
       Tesseract OCR.

    Both paths end with a Camelot / pdfplumber table extraction pass, which
    works directly on the PDF geometry and is independent of the text path.

    Parameters
    ----------
    pdf_path : str
        Path to the source PDF (needed for native extraction and table pass).
    page_number : int
        1-indexed page number.
    pil_image : Image.Image
        Rendered PIL image of the page (used only on the OCR path).

    Returns
    -------
    PageResult
        With extraction_method set to "native_text" or "ocr".
    """
    logger.info("Processing page %d ...", page_number)

    # ------------------------------------------------------------------
    # Decision: native text or OCR?
    # ------------------------------------------------------------------
    native_char_count = _count_native_chars(pdf_path, page_number)
    use_native = native_char_count >= NATIVE_TEXT_MIN_CHARS

    if use_native:
        # ----------------------------------------------------------------
        # Path A — Native text extraction (born-digital PDF)
        # ----------------------------------------------------------------
        logger.info(
            "  Page %d → native text path (%d chars available)",
            page_number, native_char_count,
        )
        lines = _extract_native_text_lines(pdf_path, page_number)
        extraction_method = "native_text"
        logger.info(
            "  Page %d → %d line(s) extracted natively", page_number, len(lines)
        )
    else:
        # ----------------------------------------------------------------
        # Path B — Full OCR pipeline (scanned / image-only page)
        # ----------------------------------------------------------------
        logger.info(
            "  Page %d → OCR path (only %d native chars — below threshold %d)",
            page_number, native_char_count, NATIVE_TEXT_MIN_CHARS,
        )
        cv_image = _pil_to_cv(pil_image)
        preprocessed = preprocess_image(cv_image)
        word_df = _run_tesseract(preprocessed)
        lines = _aggregate_lines(word_df)
        extraction_method = "ocr"
        logger.info(
            "  Page %d → %d line(s), %d word token(s)",
            page_number, len(lines), len(word_df),
        )

    # ------------------------------------------------------------------
    # Table extraction (always via Camelot/pdfplumber — independent of
    # whether text was extracted natively or via OCR)
    # Pass text lines for regex fallback on scanned financial documents.
    # ------------------------------------------------------------------
    tables = _extract_tables_for_page(pdf_path, page_number, text_lines=lines)
    logger.info("  Page %d → %d table(s)", page_number, len(tables))

    return PageResult(
        page_number=page_number,
        lines=lines,
        tables=tables,
        extraction_method=extraction_method,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_from_pdf(
    pdf_path: str,
    case_id: str,
    document_type: str,
) -> dict[str, Any]:
    """
    Main entry point for the OCR pipeline.

    Converts every page of *pdf_path* to an image, preprocesses it with
    OpenCV, runs Tesseract for text extraction, and applies a Camelot →
    pdfplumber cascade for table extraction.

    Parameters
    ----------
    pdf_path : str
        Absolute or relative path to the PDF file.
    case_id : str
        Unique identifier for the lending case / entity this document belongs
        to.  Stored verbatim in the output dict.
    document_type : str
        One of the system's document-type enumerations, e.g.:
        "ALM", "BANK_STMT", "GSTR_3B", "RATING_REPORT", "SANCTION",
        "BOARD_MINUTES", "FINANCIAL_RESULTS", "ANNUAL_REPORT", etc.

    Returns
    -------
    dict with keys:
        ocr_run_id    : str      — UUIDv4 uniquely identifying this run
        case_id       : str      — from input
        document_type : str      — from input
        pdf_path      : str      — absolute resolved path
        page_count    : int      — total pages processed
        pages         : list[dict]  — one entry per page:
            page_number : int
            lines       : list[dict]
                text         : str
                confidence   : float   (0–100)
                bbox         : dict {left, top, width, height} in pixels
                needs_review : bool
            tables      : list[dict]
                table_id          : str
                rows              : list[dict]  (pandas to_dict records)
                confidence        : float       (0–1 fill-rate heuristic)
                extraction_method : str
                page_number       : int
        metadata : dict
            ocr_engine        : str
            ocr_dpi           : int
            tesseract_psm     : int
            confidence_threshold : float

    Raises
    ------
    FileNotFoundError
        If *pdf_path* does not exist.
    RuntimeError
        If the PDF cannot be rendered (e.g., corrupted file or missing
        poppler installation).
    """
    # Validate path is not empty and points to an existing file
    if not pdf_path or not pdf_path.strip():
        raise FileNotFoundError(f"PDF not found: '{pdf_path}'")
    
    pdf_path_obj = Path(pdf_path).resolve()
    if not pdf_path_obj.exists():
        raise FileNotFoundError(f"PDF not found: '{pdf_path}'")
    
    if not pdf_path_obj.is_file():
        raise FileNotFoundError(f"Path is not a file: '{pdf_path}'")

    pdf_path_str = str(pdf_path_obj)
    ocr_run_id = str(uuid.uuid4())

    logger.info(
        "=== OCR run %s | case=%s | type=%s | file=%s ===",
        ocr_run_id, case_id, document_type, pdf_path_str,
    )

    page_count = _get_page_count(pdf_path_str)
    if page_count == 0:
        raise RuntimeError(
            f"Failed to open PDF '{pdf_path_str}'. "
            "Common causes: password-protected (BSE/NSE filings often have owner passwords), "
            "corrupt file, zero-byte download, or PDF 2.0 features unsupported by poppler. "
            "Check the logs above for specific error details."
        )
    logger.info("Total pages: %d", page_count)

    page_results: list[PageResult] = []

    for page_num in range(1, page_count + 1):
        try:
            pil_image = _render_page_to_pil(pdf_path_str, page_num)
            page_result = _process_page(pdf_path_str, page_num, pil_image)
            page_results.append(page_result)
        except Exception as exc:  # pylint: disable=broad-except
            # Never abort the whole run for a single bad page; record empty
            logger.error(
                "Failed to process page %d: %s", page_num, exc, exc_info=True
            )
            page_results.append(PageResult(page_number=page_num))

    # Summarise extraction methods used across pages
    native_pages = sum(1 for p in page_results if p.extraction_method == "native_text")
    ocr_pages    = sum(1 for p in page_results if p.extraction_method == "ocr")

    result: dict[str, Any] = {
        "ocr_run_id": ocr_run_id,
        "case_id": case_id,
        "document_type": document_type,
        "pdf_path": pdf_path_str,
        "page_count": page_count,
        "pages": [p.to_dict() for p in page_results],
        "metadata": {
            "ocr_engine": f"tesseract/{pytesseract.get_tesseract_version()}",
            "pdf_engine": f"pymupdf/{getattr(fitz, '__version__', None) or getattr(fitz, 'VersionBind', 'unknown')}",
            "ocr_dpi": PDF_RENDER_DPI,
            "tesseract_psm_candidates": TESSERACT_PSM_CANDIDATES,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "native_text_min_chars": NATIVE_TEXT_MIN_CHARS,
            "upscale_min_width_px": UPSCALE_MIN_WIDTH_PX,
            "pages_native_text": native_pages,
            "pages_ocr": ocr_pages,
        },
    }

    logger.info(
        "=== OCR run %s complete — %d page(s) processed ===",
        ocr_run_id, len(page_results),
    )

    return result


# ---------------------------------------------------------------------------
# CLI entry point (for quick smoke-testing)
# ---------------------------------------------------------------------------

def _main() -> None:
    """Minimal CLI: python ocr_service.py <pdf_path> <case_id> <doc_type>"""
    import json
    import sys

    if len(sys.argv) < 4:
        print(
            "Usage: python ocr_service.py <pdf_path> <case_id> <document_type>",
            file=sys.stderr,
        )
        sys.exit(1)

    pdf_path, case_id, document_type = sys.argv[1], sys.argv[2], sys.argv[3]
    result = extract_from_pdf(pdf_path, case_id, document_type)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    _main()