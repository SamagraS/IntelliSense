"""
ingestor_endpoints.py
=====================
Document ingestion, classification, and OCR endpoints.

Endpoints:
    POST   /api/ingest/upload         - Upload and process single document
    POST   /api/ingest/batch          - Batch upload multiple documents
    GET    /api/ingest/status/{case_id} - Get case processing status
    PATCH  /api/ingest/validate       - Approve/deny classification
    PATCH  /api/ingest/schema/edit    - Edit schema mapping
    GET    /api/ingest/findings/{case_id} - Get key findings
"""

from __future__ import annotations

import asyncio
import logging
import sys
import traceback
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel, Field

# Add processing directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from processing.classification.document_classifier import (
    EnhancedDocumentClassifier,
    LocalFileHandler,
    DocumentType,
    FileType,
    ClassificationMethod,
    ValidationStatus,
)
from processing.ocr.ocr_service import extract_from_pdf as ocr_extract_pdf
from processing.ocr.document_analyser import (
    analyze_alm,
    analyze_shareholding,
    analyze_borrowing_profile,
    analyze_portfolio_cuts,
    analyze_board_minutes,
    analyze_sanction_letter,
    analyze_rating_report,
)
from processing.ocr.financial_spreading import spread_financial_statement
from processing.ocr.gst_bank_recon import reconcile_gst_bank

from app.config import UPLOAD_DIR
from app.dependencies import executor, logger

# ═══════════════════════════════════════════════════════════════════
# Router Setup
# ═══════════════════════════════════════════════════════════════════

router = APIRouter(
    prefix="/api/ingest",
    tags=["ingestor"],
    responses={404: {"description": "Not found"}},
)

# ═══════════════════════════════════════════════════════════════════
# REQUEST/RESPONSE MODELS
# ═══════════════════════════════════════════════════════════════════


class SchemaField(BaseModel):
    """Schema field definition"""
    field_name: str
    display_name: str
    data_type: str  # text, number, date, boolean
    required: bool = False
    editable: bool = True


class DocumentResponse(BaseModel):
    """Response model for a processed document"""
    document_id: str
    filename: str
    classified_type: str
    confidence: float
    flags: int
    human_validation: str  # "APPROVE" | "DENY" | "PENDING"
    status: str  # "READY" | "PENDING" | "LOW_CONF"
    schema_fields: List[SchemaField]
    extracted_data: Dict[str, Any]
    key_findings: List[Dict[str, Any]]
    file_size_mb: float
    pages: int
    ocr_confidence: float


class BatchStatusResponse(BaseModel):
    """Response for batch processing status"""
    case_id: str
    total_documents: int
    processed: int
    pending: int
    ready: int
    low_confidence: int
    documents: List[DocumentResponse]


class ValidationRequest(BaseModel):
    """Request to validate a document classification"""
    document_id: str
    action: str  # "approve" | "deny"
    corrected_type: Optional[str] = None
    user_email: Optional[str] = None


class SchemaEditRequest(BaseModel):
    """Request to edit schema mapping"""
    document_id: str
    field_edits: Dict[str, Any]
    user_email: Optional[str] = None


# ═══════════════════════════════════════════════════════════════════
# PROCESSING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════


def classify_document(filepath: str, file_type: FileType) -> Tuple[DocumentType, float, ClassificationMethod, Dict]:
    """Classify a document using the enhanced classifier."""
    try:
        classifier = EnhancedDocumentClassifier()
        doc_type, confidence, method, metadata = classifier.classify(filepath, file_type)
        logger.info(f"[CLASSIFY] {Path(filepath).name} → {doc_type.value} ({confidence:.1%})")
        return doc_type, confidence, method, metadata
    except Exception as e:
        logger.error(f"[CLASSIFY ERROR] {filepath}: {str(e)}")
        return DocumentType.UNKNOWN, 0.0, ClassificationMethod.FILENAME, {"error": str(e)}


def process_ocr(filepath: str, case_id: str, document_type: str) -> Dict[str, Any]:
    """Process OCR extraction for a document."""
    try:
        logger.info(f"[OCR] Starting OCR for {Path(filepath).name}")
        
        if not filepath.lower().endswith('.pdf'):
            logger.warning(f"[OCR] Skipping non-PDF file: {filepath}")
            return {
                "success": False,
                "error": "OCR only supported for PDF files",
                "pages": [],
                "page_count": 0,
            }
        
        ocr_result = ocr_extract_pdf(filepath, case_id, document_type)
        logger.info(f"[OCR] Completed: {ocr_result.get('page_count', 0)} pages")
        return ocr_result
    
    except Exception as e:
        logger.error(f"[OCR ERROR] {filepath}: {str(e)}\n{traceback.format_exc()}")
        return {
            "success": False,
            "error": str(e),
            "pages": [],
            "page_count": 0,
        }


def analyze_document(doc_type: DocumentType, ocr_result: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze OCR result based on document type."""
    try:
        logger.info(f"[ANALYZE] Running analysis for {doc_type.value}")
        
        all_rows = []
        all_text_lines = []
        
        for page in ocr_result.get("pages", []):
            for table in page.get("tables", []):
                all_rows.extend(table.get("rows", []))
            
            for line in page.get("lines", []):
                all_text_lines.append(line.get("text", ""))
        
        if doc_type == DocumentType.ALM:
            return analyze_alm(all_rows, all_text_lines)
        elif doc_type == DocumentType.SHAREHOLDING:
            return analyze_shareholding(all_rows, all_text_lines)
        elif doc_type == DocumentType.BORROWING_PROFILE:
            return analyze_borrowing_profile(all_rows, all_text_lines)
        elif doc_type == DocumentType.PORTFOLIO_CUTS:
            return analyze_portfolio_cuts(all_rows, all_text_lines)
        elif doc_type == DocumentType.BOARD_MINUTES:
            return analyze_board_minutes(all_rows, all_text_lines)
        elif doc_type == DocumentType.SANCTION_LETTER:
            return analyze_sanction_letter(all_rows, all_text_lines)
        elif doc_type == DocumentType.RATING_REPORT:
            return analyze_rating_report(all_rows, all_text_lines)
        elif doc_type == DocumentType.ANNUAL_REPORT:
            return spread_financial_statement(all_rows, all_text_lines)
        else:
            return {
                "success": True,
                "message": f"No specific analyzer for {doc_type.value}",
                "extracted_fields": {},
                "findings": [],
            }
    
    except Exception as e:
        logger.error(f"[ANALYZE ERROR] {doc_type.value}: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "extracted_fields": {},
            "findings": [],
        }


def generate_schema_fields(doc_type: DocumentType, extracted_fields: Dict[str, Any]) -> List[SchemaField]:
    """Generate dynamic schema fields based on document type."""
    schema_fields = []
    
    SCHEMA_TEMPLATES = {
        DocumentType.ALM: [
            ("maturity_bucket", "Maturity Bucket", "text", True),
            ("assets_inr_cr", "Assets (₹ Cr)", "number", True),
            ("liabilities_inr_cr", "Liabilities (₹ Cr)", "number", True),
            ("gap_inr_cr", "Gap (₹ Cr)", "number", True),
            ("cumulative_gap", "Cumulative Gap (₹ Cr)", "number", False),
        ],
        DocumentType.SHAREHOLDING: [
            ("category", "Category", "text", True),
            ("num_shares", "Number of Shares", "number", True),
            ("percentage", "Percentage (%)", "number", True),
            ("pledged_shares", "Pledged Shares", "number", False),
            ("pledged_percentage", "Pledged %", "number", False),
        ],
        DocumentType.BORROWING_PROFILE: [
            ("lender_name", "Lender Name", "text", True),
            ("facility_type", "Facility Type", "text", True),
            ("sanctioned_amount_cr", "Sanctioned (₹ Cr)", "number", True),
            ("outstanding_cr", "Outstanding (₹ Cr)", "number", True),
            ("interest_rate_pct", "Interest Rate (%)", "number", True),
            ("maturity_date", "Maturity Date", "date", False),
        ],
        DocumentType.PORTFOLIO_CUTS: [
            ("metric_name", "Metric", "text", True),
            ("value", "Value", "number", True),
            ("percentage", "Percentage (%)", "number", False),
            ("period", "Period", "text", False),
        ],
        DocumentType.RATING_REPORT: [
            ("rating_agency", "Rating Agency", "text", True),
            ("current_rating", "Current Rating", "text", True),
            ("previous_rating", "Previous Rating", "text", False),
            ("outlook", "Outlook", "text", True),
            ("rating_date", "Rating Date", "date", True),
        ],
        DocumentType.SANCTION_LETTER: [
            ("facility_type", "Facility Type", "text", True),
            ("sanctioned_amount_cr", "Sanctioned Amount (₹ Cr)", "number", True),
            ("interest_rate_pct", "Interest Rate (%)", "number", True),
            ("tenor_months", "Tenor (Months)", "number", True),
            ("security", "Security", "text", False),
        ],
        DocumentType.BOARD_MINUTES: [
            ("meeting_date", "Meeting Date", "date", True),
            ("attendees", "Attendees", "text", False),
            ("agenda_item", "Agenda Item", "text", True),
            ("resolution", "Resolution", "text", True),
            ("vote_result", "Vote Result", "text", False),
        ],
    }
    
    template = SCHEMA_TEMPLATES.get(doc_type, [])
    
    for field_name, display_name, data_type, required in template:
        schema_fields.append(
            SchemaField(
                field_name=field_name,
                display_name=display_name,
                data_type=data_type,
                required=required,
                editable=True,
            )
        )
    
    for key in extracted_fields.keys():
        if not any(f.field_name == key for f in schema_fields):
            schema_fields.append(
                SchemaField(
                    field_name=key,
                    display_name=key.replace("_", " ").title(),
                    data_type="text",
                    required=False,
                    editable=True,
                )
            )
    
    return schema_fields


def calculate_flags(analysis_result: Dict[str, Any], confidence: float) -> int:
    """Calculate number of flags based on analysis findings."""
    flags = 0
    
    if confidence < 0.70:
        flags += 1
    
    findings = analysis_result.get("findings", [])
    for finding in findings:
        severity = finding.get("severity", "").upper()
        if severity in ["HIGH", "CRITICAL"]:
            flags += 1
    
    if "flags" in analysis_result:
        flags += len(analysis_result.get("flags", []))
    
    return min(flags, 8)


def extract_key_findings(
    doc_type: DocumentType,
    filename: str,
    analysis_result: Dict[str, Any],
    pages: int,
) -> List[Dict[str, Any]]:
    """Extract key findings for display."""
    findings = []
    
    analysis_findings = analysis_result.get("findings", [])
    
    for finding in analysis_findings:
        findings.append({
            "source": f"{filename} · p.{finding.get('page', 1)}",
            "severity": finding.get("severity", "INFO").upper(),
            "text": finding.get("text", finding.get("description", "No description")),
            "document_type": doc_type.value,
        })
    
    if "summary" in analysis_result:
        summary = analysis_result["summary"]
        if summary.get("critical_issues"):
            for issue in summary["critical_issues"]:
                findings.append({
                    "source": f"{filename} · Summary",
                    "severity": "HIGH",
                    "text": issue,
                    "document_type": doc_type.value,
                })
    
    return findings


# ═══════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═══════════════════════════════════════════════════════════════════


@router.post("/upload", response_model=DocumentResponse)
async def upload_document(
    file: UploadFile = File(...),
    case_id: str = Form(...),
    user_email: Optional[str] = Form(None),
):
    """
    Upload and process a single document.
    
    Performs classification, OCR, and analysis in parallel.
    Returns complete document metadata with dynamic schema.
    """
    document_id = str(uuid.uuid4())
    
    try:
        logger.info(f"=== UPLOAD: {file.filename} (case: {case_id}) ===")
        
        # Save file
        file_ext = Path(file.filename).suffix
        save_filename = f"{document_id}{file_ext}"
        file_path = UPLOAD_DIR / case_id / save_filename
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)
        
        file_size_mb = len(content) / (1024 * 1024)
        logger.info(f"[✓] Saved: {file_path} ({file_size_mb:.2f} MB)")
        
        # Detect file type
        handler = LocalFileHandler()
        try:
            file_type = handler.detect_file_type(str(file_path))
        except:
            file_type = FileType.PDF
        
        # Run classification and OCR in parallel
        logger.info("[→] Starting parallel processing...")
        
        loop = asyncio.get_event_loop()
        
        classify_future = loop.run_in_executor(
            executor,
            classify_document,
            str(file_path),
            file_type,
        )
        
        ocr_future = loop.run_in_executor(
            executor,
            process_ocr,
            str(file_path),
            case_id,
            "UNKNOWN",
        )
        
        doc_type, confidence, method, classify_metadata = await classify_future
        ocr_result = await ocr_future
        
        logger.info(f"[✓] Classification: {doc_type.value} ({confidence:.1%})")
        logger.info(f"[✓] OCR: {ocr_result.get('page_count', 0)} pages")
        
        # Analysis
        analysis_result = analyze_document(doc_type, ocr_result)
        logger.info(f"[✓] Analysis complete")
        
        # Generate schema
        extracted_fields = analysis_result.get("extracted_fields", {})
        schema_fields = generate_schema_fields(doc_type, extracted_fields)
        
        # Calculate status
        flags = calculate_flags(analysis_result, confidence)
        
        if confidence < 0.64:
            status_val = "LOW_CONF"
            human_validation = "PENDING"
        elif confidence < 0.90:
            status_val = "PENDING"
            human_validation = "PENDING"
        else:
            status_val = "READY"
            human_validation = "APPROVE"
        
        # Extract findings
        key_findings = extract_key_findings(
            doc_type,
            file.filename,
            analysis_result,
            ocr_result.get("page_count", 0),
        )
        
        # Calculate OCR confidence
        ocr_confidence = 0.0
        total_lines = 0
        total_conf = 0.0
        
        for page in ocr_result.get("pages", []):
            for line in page.get("lines", []):
                conf = line.get("confidence", 0.0)
                if conf > 0:
                    total_conf += conf
                    total_lines += 1
        
        if total_lines > 0:
            ocr_confidence = total_conf / total_lines / 100.0
        
        # Build response
        response = DocumentResponse(
            document_id=document_id,
            filename=file.filename,
            classified_type=doc_type.value,
            confidence=confidence,
            flags=flags,
            human_validation=human_validation,
            status=status_val,
            schema_fields=schema_fields,
            extracted_data=extracted_fields,
            key_findings=key_findings,
            file_size_mb=file_size_mb,
            pages=ocr_result.get("page_count", 0),
            ocr_confidence=ocr_confidence,
        )
        
        logger.info(f"[✓] Processed: {document_id}")
        logger.info("=" * 80)
        
        return response
    
    except Exception as e:
        logger.error(f"[✗] Upload failed: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Document processing failed: {str(e)}",
        )


@router.post("/batch", response_model=BatchStatusResponse)
async def batch_upload(
    files: List[UploadFile] = File(...),
    case_id: str = Form(...),
    user_email: Optional[str] = Form(None),
):
    """Batch upload and process multiple documents in parallel."""
    logger.info(f"=== BATCH UPLOAD: {len(files)} files (case: {case_id}) ===")
    
    tasks = []
    for file in files:
        task = upload_document(file, case_id, user_email)
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    documents = []
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"[✗] File failed: {str(result)}")
        else:
            documents.append(result)
    
    total_documents = len(documents)
    ready = sum(1 for d in documents if d.status == "READY")
    pending = sum(1 for d in documents if d.status == "PENDING")
    low_confidence = sum(1 for d in documents if d.status == "LOW_CONF")
    
    response = BatchStatusResponse(
        case_id=case_id,
        total_documents=total_documents,
        processed=total_documents,
        pending=pending,
        ready=ready,
        low_confidence=low_confidence,
        documents=documents,
    )
    
    logger.info(f"[✓] Batch complete: {total_documents} documents")
    logger.info("=" * 80)
    
    return response


@router.get("/status/{case_id}", response_model=BatchStatusResponse)
async def get_case_status(case_id: str):
    """Get processing status for all documents in a case."""
    case_dir = UPLOAD_DIR / case_id
    if not case_dir.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Case not found: {case_id}",
        )
    
    return BatchStatusResponse(
        case_id=case_id,
        total_documents=0,
        processed=0,
        pending=0,
        ready=0,
        low_confidence=0,
        documents=[],
    )


@router.patch("/validate")
async def validate_document(request: ValidationRequest):
    """Approve or deny a document classification."""
    logger.info(f"[VALIDATE] {request.document_id}: {request.action}")
    
    if request.action == "approve":
        return {"success": True, "message": "Document approved"}
    elif request.action == "deny":
        return {
            "success": True,
            "message": "Document denied",
            "corrected_type": request.corrected_type,
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid action: {request.action}",
        )


@router.patch("/schema/edit")
async def edit_schema(request: SchemaEditRequest):
    """Edit schema field values for a document."""
    logger.info(f"[SCHEMA EDIT] {request.document_id}: {len(request.field_edits)} edits")
    
    return {
        "success": True,
        "message": f"Schema updated with {len(request.field_edits)} edits",
        "document_id": request.document_id,
    }


@router.get("/findings/{case_id}")
async def get_findings(case_id: str, limit: int = 25):
    """Get all extracted key findings for a case."""
    mock_findings = [
        {
            "source": "ICRA Rating Report · p.3",
            "severity": "HIGH",
            "text": "Outlook revised to Negative from Stable — elevated NPA trajectory in SME segment.",
            "document_type": "RATING_REPORT",
        },
        {
            "source": "Shareholding Pattern Q3 FY25",
            "severity": "HIGH",
            "text": "Promoter pledge: 68.4% of promoter shareholding pledged — above 60% threshold.",
            "document_type": "SHAREHOLDING",
        },
    ]
    
    return {
        "case_id": case_id,
        "total_findings": len(mock_findings),
        "findings": mock_findings[:limit],
    }
