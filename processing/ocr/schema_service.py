"""
schema_service.py
=================
Dynamic schema configuration layer for OCR-extracted financial documents.

Responsibilities
----------------
- Maintain per-(case_id, document_type) schema templates with versioning.
- Accept column→field mappings from the review UI.
- Record manual cell-level edits with full audit trail.
- Validate & persist confirmed mappings to the schema_mappings SQLite database.

Storage
-------
By default, uses SQLite database (schema.db) for persistent storage.
For testing, set environment variable SCHEMA_USE_SQLITE=false to use in-memory storage.

Run locally
-----------
    pip install fastapi uvicorn[standard] pydantic sqlalchemy
    uvicorn schema_service:app --reload

API base path: http://localhost:8000
Interactive docs: http://localhost:8000/docs

Database location: ./schema.db (created automatically)
"""

from __future__ import annotations

import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, Protocol

from fastapi import FastAPI, HTTPException, Path, Body, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class DataType(str, Enum):
    TEXT = "text"
    NUMBER = "number"
    DATE = "date"
    BOOLEAN = "boolean"


class FieldSource(str, Enum):
    """Whether the field originated from a system template or was user-added."""
    TEMPLATE = "template"
    CUSTOM = "custom"


class SchemaOperation(str, Enum):
    ADD = "add"
    RENAME = "rename"
    CHANGE_TYPE = "change_type"
    MARK_REQUIRED = "mark_required"
    MARK_OPTIONAL = "mark_optional"
    REMOVE = "remove"


class DocumentType(str, Enum):
    ALM = "ALM"
    BANK_STMT = "BANK_STMT"
    GSTR_3B = "GSTR_3B"
    GSTR_2A = "GSTR_2A"
    RATING_REPORT = "RATING_REPORT"
    SANCTION = "SANCTION"
    BOARD_MINUTES = "BOARD_MINUTES"
    FINANCIAL_RESULTS = "FINANCIAL_RESULTS"
    ANNUAL_REPORT = "ANNUAL_REPORT"
    SHAREHOLDING = "SHAREHOLDING"
    BORROWING_PROFILE = "BORROWING_PROFILE"
    PORTFOLIO = "PORTFOLIO"
    ITR = "ITR"


# ---------------------------------------------------------------------------
# Core domain models
# ---------------------------------------------------------------------------

class SchemaField(BaseModel):
    """
    A single field in a document's schema template.

    Attributes
    ----------
    field_name:
        Snake-case identifier used as the key in normalized tables
        (e.g., ``assets_bucket_inr``).
    display_name:
        Human-readable label shown in the review UI.
    data_type:
        How the raw string value should be coerced after extraction.
    required:
        Whether validation should fail if this field is absent.
    source:
        ``template`` = shipped with the system; ``custom`` = user-added.
    description:
        Optional guidance for reviewers.
    validation_regex:
        Optional regex applied after type coercion to catch format errors.
    """
    field_name: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9_]{0,63}$",
        description="Snake-case field identifier, max 64 chars",
    )
    display_name: str = Field(..., min_length=1, max_length=128)
    data_type: DataType
    required: bool = False
    source: FieldSource = FieldSource.TEMPLATE
    description: Optional[str] = Field(None, max_length=512)
    validation_regex: Optional[str] = Field(None, max_length=256)

    model_config = {"use_enum_values": True}


class ManualEdit(BaseModel):
    """
    Represents a single reviewer-applied cell correction.

    Attributes
    ----------
    edit_id:
        Auto-generated UUID for this edit record.
    row_index:
        Zero-based row index in the extracted table.
    column_name:
        The raw OCR column header (before mapping).
    old_value:
        The value as extracted by OCR (pre-edit).
    new_value:
        The reviewer's corrected value.
    edited_by:
        Username or system token of the reviewer.
    edited_at:
        UTC timestamp of when the edit was recorded.
    reason:
        Optional free-text reason (e.g., "OCR misread ₹ as T").
    """
    edit_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    row_index: int = Field(..., ge=0)
    column_name: str = Field(..., min_length=1)
    old_value: str
    new_value: str
    edited_by: str = Field(..., min_length=1)
    edited_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reason: Optional[str] = Field(None, max_length=512)


class SchemaMappingRecord(BaseModel):
    """
    Persisted record for a fully configured (case_id, document_type) pair.

    This is the main entity stored in the schema_mappings Delta table.
    The in-memory store uses mapping_id as the primary key.

    Attributes
    ----------
    mapping_id:
        UUIDv4 primary key.
    case_id:
        Lending case / borrower entity identifier.
    document_type:
        One of the DocumentType enum values.
    schema_template_version:
        Semver string of the base template this mapping was created from.
    schema_fields:
        Current state of all fields (template + custom) for this record.
    field_mappings:
        Raw OCR column header → schema field_name.
        e.g. ``{"Assets (₹Cr)": "assets_bucket_inr"}``.
    custom_fields_added:
        Fields added by the user beyond the base template.
    manual_edits_applied:
        All cell-level corrections made by reviewers.
    validated_by:
        Username of the person who triggered Validate & Save.
    validation_timestamp:
        UTC datetime of the last successful validation.
    created_at:
        UTC datetime when this mapping record was first created.
    updated_at:
        UTC datetime of the most recent modification.
    """
    mapping_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    case_id: str = Field(..., min_length=1, max_length=128)
    document_type: str
    schema_template_version: str = Field(default="1.0.0")
    schema_fields: list[SchemaField] = Field(default_factory=list)
    field_mappings: dict[str, str] = Field(default_factory=dict)
    custom_fields_added: list[SchemaField] = Field(default_factory=list)
    manual_edits_applied: list[ManualEdit] = Field(default_factory=list)
    validated_by: Optional[str] = None
    validation_timestamp: Optional[datetime] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

# ---- GET schema response ---------------------------------------------------

class ExtractedColumnSample(BaseModel):
    """Sample data from one OCR-extracted column, used in the review UI."""
    column_name: str
    sample_values: list[str] = Field(
        description="Up to 5 non-null sample cell values from this column"
    )
    avg_confidence: float = Field(ge=0.0, le=100.0)
    low_confidence_row_count: int = Field(
        ge=0,
        description="Number of rows in this column below the confidence threshold",
    )
    already_mapped_to: Optional[str] = Field(
        None,
        description="schema field_name if already mapped, else null",
    )


class GetSchemaResponse(BaseModel):
    case_id: str
    document_type: str
    mapping_id: Optional[str] = Field(
        None,
        description="Null when no mapping has been saved yet for this pair",
    )
    schema_template_version: str
    schema_fields: list[SchemaField]
    extracted_columns: list[ExtractedColumnSample]
    is_validated: bool
    validation_timestamp: Optional[datetime] = None


# ---- POST /update ----------------------------------------------------------

class SchemaUpdateOperation(BaseModel):
    """
    A single schema mutation operation.

    Only the fields relevant to the chosen ``operation`` need to be provided.
    The API validates that the required supporting fields are present.
    """
    operation: SchemaOperation
    # For ADD: provide the full field definition
    field: Optional[SchemaField] = None
    # For RENAME / CHANGE_TYPE / MARK_REQUIRED / MARK_OPTIONAL / REMOVE
    field_name: Optional[str] = Field(
        None,
        pattern=r"^[a-z][a-z0-9_]{0,63}$",
        description="Existing field_name to target",
    )
    # For RENAME: new display name
    new_display_name: Optional[str] = Field(None, min_length=1, max_length=128)
    # For CHANGE_TYPE
    new_data_type: Optional[DataType] = None

    @model_validator(mode="after")
    def check_required_payload(self) -> "SchemaUpdateOperation":
        op = self.operation
        if op == SchemaOperation.ADD and self.field is None:
            raise ValueError("'field' is required for ADD operation")
        if op in (
            SchemaOperation.RENAME,
            SchemaOperation.CHANGE_TYPE,
            SchemaOperation.MARK_REQUIRED,
            SchemaOperation.MARK_OPTIONAL,
            SchemaOperation.REMOVE,
        ) and not self.field_name:
            raise ValueError(f"'field_name' is required for {op} operation")
        if op == SchemaOperation.RENAME and not self.new_display_name:
            raise ValueError("'new_display_name' is required for RENAME operation")
        if op == SchemaOperation.CHANGE_TYPE and not self.new_data_type:
            raise ValueError("'new_data_type' is required for CHANGE_TYPE operation")
        return self


class UpdateSchemaRequest(BaseModel):
    operations: list[SchemaUpdateOperation] = Field(..., min_length=1)


class UpdateSchemaResponse(BaseModel):
    case_id: str
    document_type: str
    applied_operations: int
    schema_fields: list[SchemaField]
    skipped: list[dict[str, str]] = Field(
        default_factory=list,
        description="Operations that were skipped with reason",
    )


# ---- POST /mapping ---------------------------------------------------------

class ColumnMappingRequest(BaseModel):
    """
    Map raw OCR column headers to canonical schema field names.

    field_mappings:
        Keys are the raw column headers exactly as extracted (e.g.
        ``"Assets (₹Cr)"``).  Values are schema ``field_name`` identifiers.
    unmapped_action:
        What to do with columns that are NOT in this dict.
        ``ignore`` (default) = leave them unmapped.
        ``auto_name`` = derive a snake_case field_name automatically.
    submitted_by:
        Username of the analyst performing the mapping.
    """
    field_mappings: dict[str, str] = Field(..., min_length=1)
    unmapped_action: str = Field(
        default="ignore",
        pattern=r"^(ignore|auto_name)$",
    )
    submitted_by: str = Field(..., min_length=1)

    @field_validator("field_mappings")
    @classmethod
    def validate_target_field_names(cls, v: dict[str, str]) -> dict[str, str]:
        import re
        pattern = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
        for raw_col, field_name in v.items():
            if not pattern.match(field_name):
                raise ValueError(
                    f"Target field_name '{field_name}' for column '{raw_col}' "
                    "must be snake_case [a-z][a-z0-9_]{{0,63}}"
                )
        return v


class ColumnMappingResponse(BaseModel):
    mapping_id: str
    case_id: str
    document_type: str
    mapped_count: int
    unmapped_columns: list[str]
    field_mappings: dict[str, str]
    created_at: datetime


# ---- POST /edits -----------------------------------------------------------

class CellEditRequest(BaseModel):
    """A reviewer-submitted correction for a single OCR cell."""
    row_index: int = Field(..., ge=0)
    column_name: str = Field(..., min_length=1)
    old_value: str
    new_value: str
    edited_by: str = Field(..., min_length=1)
    reason: Optional[str] = Field(None, max_length=512)


class ApplyEditsRequest(BaseModel):
    edits: list[CellEditRequest] = Field(..., min_length=1)
    mapping_id: str = Field(..., description="mapping_id returned by POST /mapping")


class ApplyEditsResponse(BaseModel):
    mapping_id: str
    edits_applied: int
    total_edits_on_record: int
    updated_at: datetime


# ---- POST /validate --------------------------------------------------------

class ValidateRequest(BaseModel):
    mapping_id: str
    validated_by: str = Field(..., min_length=1)


class ValidationError(BaseModel):
    field_name: str
    error_type: str  # "missing_required" | "unmapped_required" | "type_mismatch"
    detail: str


class ValidateResponse(BaseModel):
    mapping_id: str
    case_id: str
    document_type: str
    is_valid: bool
    errors: list[ValidationError] = Field(default_factory=list)
    validated_by: Optional[str] = None
    validation_timestamp: Optional[datetime] = None
    schema_template_version: str


# ---------------------------------------------------------------------------
# In-memory persistence layer
# ---------------------------------------------------------------------------
# Replace this section with a real repository (Delta tables via PySpark,
# PostgreSQL via SQLAlchemy, etc.) without changing the API layer.
# The repository exposes a clean interface so the FastAPI handlers never
# touch raw dicts directly.

class InMemorySchemaRepository:
    """
    Thread-unsafe in-memory store for development and testing.

    Real implementations should implement the same public method signatures
    backed by Delta MERGE INTO, PostgreSQL, or DynamoDB as appropriate.

    Internal layout
    ---------------
    _by_mapping_id : dict[mapping_id → SchemaMappingRecord]
    _index         : dict[(case_id, document_type) → mapping_id]
                     One active mapping per (case_id, document_type).
    """

    def __init__(self) -> None:
        self._by_mapping_id: dict[str, SchemaMappingRecord] = {}
        # (case_id, document_type) → mapping_id
        self._index: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_by_case_and_type(
        self, case_id: str, document_type: str
    ) -> Optional[SchemaMappingRecord]:
        key = (case_id, document_type)
        mid = self._index.get(key)
        return self._by_mapping_id.get(mid) if mid else None

    def get_by_mapping_id(self, mapping_id: str) -> Optional[SchemaMappingRecord]:
        return self._by_mapping_id.get(mapping_id)

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def upsert(self, record: SchemaMappingRecord) -> SchemaMappingRecord:
        """Insert or replace the mapping record.  Updates the index."""
        record.updated_at = datetime.now(timezone.utc)
        self._by_mapping_id[record.mapping_id] = record
        self._index[(record.case_id, record.document_type)] = record.mapping_id
        return record

    def create_for_case(
        self, case_id: str, document_type: str
    ) -> SchemaMappingRecord:
        """Bootstrap a new empty record with the base template pre-loaded."""
        fields = deepcopy(SCHEMA_TEMPLATES.get(document_type, []))
        record = SchemaMappingRecord(
            case_id=case_id,
            document_type=document_type,
            schema_fields=fields,
            schema_template_version=TEMPLATE_VERSION,
        )
        return self.upsert(record)


# ---------------------------------------------------------------------------
# Schema templates
# ---------------------------------------------------------------------------
# These are the canonical "out-of-the-box" field definitions per document
# type.  Users can extend them via POST /update (add/rename/change_type).
# Keep template field ``source = FieldSource.TEMPLATE`` so the UI can
# distinguish them from user-added fields.

TEMPLATE_VERSION = "1.0.0"

SCHEMA_TEMPLATES: dict[str, list[SchemaField]] = {

    DocumentType.ALM: [
        SchemaField(field_name="maturity_bucket", display_name="Maturity Bucket",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="assets_inr", display_name="Assets (₹Cr)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="liabilities_inr", display_name="Liabilities (₹Cr)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="gap_inr", display_name="Gap (₹Cr)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="cumulative_gap_inr", display_name="Cumulative Gap (₹Cr)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="is_negative_short_term_gap",
                    display_name="Negative Short-Term Gap Flag",
                    data_type=DataType.BOOLEAN, required=False),
    ],

    DocumentType.BANK_STMT: [
        SchemaField(field_name="txn_date", display_name="Transaction Date",
                    data_type=DataType.DATE, required=True),
        SchemaField(field_name="description", display_name="Description",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="credit_inr", display_name="Credit (₹)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="debit_inr", display_name="Debit (₹)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="balance_inr", display_name="Balance (₹)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="txn_reference", display_name="Reference",
                    data_type=DataType.TEXT, required=False),
    ],

    DocumentType.GSTR_3B: [
        SchemaField(field_name="filing_period", display_name="Filing Period",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="gross_turnover_inr", display_name="Gross Turnover (₹)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="taxable_turnover_inr",
                    display_name="Taxable Turnover (₹)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="itc_claimed_inr", display_name="ITC Claimed (₹)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="tax_paid_inr", display_name="Tax Paid (₹)",
                    data_type=DataType.NUMBER, required=True),
    ],

    DocumentType.RATING_REPORT: [
        SchemaField(field_name="current_rating", display_name="Current Rating",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="rating_outlook", display_name="Outlook",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="last_change_date", display_name="Last Change Date",
                    data_type=DataType.DATE, required=False),
        SchemaField(field_name="rating_direction",
                    display_name="Change Direction (Upgrade/Downgrade/Reaffirm)",
                    data_type=DataType.TEXT, required=False),
        SchemaField(field_name="key_risk_factors", display_name="Key Risk Factors",
                    data_type=DataType.TEXT, required=False),
    ],

    DocumentType.SANCTION: [
        SchemaField(field_name="loan_amount_inr", display_name="Loan Amount (₹)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="interest_rate_pct", display_name="Interest Rate (%)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="tenor_months", display_name="Tenor (Months)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="collateral_description",
                    display_name="Collateral Description",
                    data_type=DataType.TEXT, required=False),
        SchemaField(field_name="covenants_text", display_name="Covenants",
                    data_type=DataType.TEXT, required=False),
        SchemaField(field_name="has_restructuring_mention",
                    display_name="Restructuring Mention",
                    data_type=DataType.BOOLEAN, required=False),
    ],

    DocumentType.BOARD_MINUTES: [
        SchemaField(field_name="meeting_date", display_name="Meeting Date",
                    data_type=DataType.DATE, required=True),
        SchemaField(field_name="agenda_items", display_name="Agenda Items",
                    data_type=DataType.TEXT, required=False),
        SchemaField(field_name="governance_signal_type",
                    display_name="Governance Signal",
                    data_type=DataType.TEXT, required=False),
        SchemaField(field_name="signal_context_text",
                    display_name="Signal Context (±2 sentences)",
                    data_type=DataType.TEXT, required=False),
    ],

    DocumentType.FINANCIAL_RESULTS: [
        SchemaField(field_name="period_label", display_name="Period",
                    data_type=DataType.TEXT, required=True),
        SchemaField(field_name="revenue_inr", display_name="Revenue (₹Cr)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="ebitda_inr", display_name="EBITDA (₹Cr)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="pat_inr", display_name="PAT (₹Cr)",
                    data_type=DataType.NUMBER, required=True),
        SchemaField(field_name="total_debt_inr", display_name="Total Debt (₹Cr)",
                    data_type=DataType.NUMBER, required=False),
        SchemaField(field_name="net_worth_inr", display_name="Net Worth (₹Cr)",
                    data_type=DataType.NUMBER, required=False),
    ],
}

# Fallback for document types not explicitly templated
_EMPTY_TEMPLATE: list[SchemaField] = []

# ---------------------------------------------------------------------------
# Simulated OCR column data
# ---------------------------------------------------------------------------
# In production this comes from the ocr_extracted Delta table.
# Keyed by document_type for plausible demo shapes.

_MOCK_OCR_COLUMNS: dict[str, list[dict[str, Any]]] = {
    DocumentType.ALM: [
        {"column_name": "Maturity Bucket", "sample_values": ["0-1M","1-3M","3-6M","6-12M","1-3Y"],
         "avg_confidence": 94.1, "low_confidence_row_count": 0},
        {"column_name": "Assets (₹Cr)", "sample_values": ["1200.50","890.00","2340.75","500.20","3400.00"],
         "avg_confidence": 88.3, "low_confidence_row_count": 1},
        {"column_name": "Liabilities (₹Cr)", "sample_values": ["1100.00","920.50","2100.00","480.00","3200.00"],
         "avg_confidence": 87.6, "low_confidence_row_count": 2},
        {"column_name": "Gap", "sample_values": ["100.50","-30.50","240.75","20.20","200.00"],
         "avg_confidence": 72.1, "low_confidence_row_count": 4},
    ],
    DocumentType.BANK_STMT: [
        {"column_name": "Date", "sample_values": ["01/04/2024","02/04/2024","03/04/2024"],
         "avg_confidence": 96.0, "low_confidence_row_count": 0},
        {"column_name": "Narration", "sample_values": ["NEFT-HDFC","UPI-GPAY","CHQ-000123"],
         "avg_confidence": 82.5, "low_confidence_row_count": 3},
        {"column_name": "Credit", "sample_values": ["50000","","120000","","75000"],
         "avg_confidence": 91.2, "low_confidence_row_count": 0},
        {"column_name": "Debit", "sample_values": ["","30000","","15000",""],
         "avg_confidence": 90.8, "low_confidence_row_count": 0},
        {"column_name": "Balance", "sample_values": ["500000","470000","590000","575000","650000"],
         "avg_confidence": 93.4, "low_confidence_row_count": 0},
    ],
}
_DEFAULT_OCR_COLUMNS: list[dict[str, Any]] = [
    {"column_name": "Column A", "sample_values": ["val1","val2","val3"],
     "avg_confidence": 85.0, "low_confidence_row_count": 1},
    {"column_name": "Column B", "sample_values": ["100","200","300"],
     "avg_confidence": 90.0, "low_confidence_row_count": 0},
]


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _to_snake_case(raw: str) -> str:
    """
    Convert a raw OCR column header to a snake_case field name.
    Strips special characters, lowercases, replaces spaces/hyphens with
    underscores, and truncates to 64 characters.

    Examples
    --------
    "Assets (₹Cr)"  → "assets_cr"
    "Date of Birth" → "date_of_birth"
    """
    import re
    s = raw.strip().lower()
    s = re.sub(r"[₹$€£%()'\",]", "", s)       # remove currency/punctuation
    s = re.sub(r"[\s\-/]+", "_", s)             # spaces/hyphens → underscores
    s = re.sub(r"[^a-z0-9_]", "", s)            # remove any remaining non-alnum
    s = re.sub(r"_+", "_", s).strip("_")        # collapse and trim underscores
    if not s or not s[0].isalpha():
        s = "field_" + s                         # ensure starts with a letter
    return s[:64]


# ---------------------------------------------------------------------------
# Repository Protocol (allows both in-memory and SQLite implementations)
# ---------------------------------------------------------------------------

class SchemaRepositoryProtocol(Protocol):
    """Protocol defining the repository interface for schema storage."""
    
    def get_by_case_and_type(
        self, case_id: str, document_type: str
    ) -> Optional[SchemaMappingRecord]: ...
    
    def get_by_mapping_id(self, mapping_id: str) -> Optional[SchemaMappingRecord]: ...
    
    def upsert(self, record: SchemaMappingRecord) -> SchemaMappingRecord: ...
    
    def create_for_case(
        self, case_id: str, document_type: str
    ) -> SchemaMappingRecord: ...


def _get_or_create_record(
    repo: SchemaRepositoryProtocol,
    case_id: str,
    document_type: str,
) -> SchemaMappingRecord:
    """Return existing record or bootstrap one from the template."""
    record = repo.get_by_case_and_type(case_id, document_type)
    if record is None:
        record = repo.create_for_case(case_id, document_type)
    return record


def _require_mapping(
    repo: SchemaRepositoryProtocol, mapping_id: str
) -> SchemaMappingRecord:
    """Raise 404 if mapping_id doesn't exist."""
    record = repo.get_by_mapping_id(mapping_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No mapping found with mapping_id='{mapping_id}'",
        )
    return record


def _require_mapping_for_case(
    repo: SchemaRepositoryProtocol, case_id: str, document_type: str
) -> SchemaMappingRecord:
    """Raise 404 if no mapping exists for (case_id, document_type)."""
    record = repo.get_by_case_and_type(case_id, document_type)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No mapping found for case_id='{case_id}' "
                f"and document_type='{document_type}'. "
                "Call POST /mapping first."
            ),
        )
    return record


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app(use_sqlite: bool = True, db_path: str = "schema.db") -> FastAPI:
    """
    Create FastAPI application with schema service endpoints.
    
    Parameters
    ----------
    use_sqlite : bool
        If True (default), use SQLite persistence. If False, use in-memory store.
    db_path : str
        Path to SQLite database file (only used if use_sqlite=True).
        Default: "schema.db" in current directory.
    """
    application = FastAPI(
        title="OCR Schema Configuration Service",
        description=(
            "Dynamic schema template management and column-mapping layer for "
            "OCR-extracted financial documents."
        ),
        version="1.0.0",
    )

    # Choose repository implementation
    if use_sqlite:
        from processing.ocr.schema_repository_sqlite import SQLiteSchemaRepository
        repo = SQLiteSchemaRepository(db_path=db_path, echo=False)
        print(f"✓ Using SQLite repository: {db_path}")
    else:
        repo = InMemorySchemaRepository()
        print("⚠ Using in-memory repository (data will be lost on restart)")

    # ------------------------------------------------------------------ #
    # GET /cases/{case_id}/schema/{document_type}                         #
    # ------------------------------------------------------------------ #
    @application.get(
        "/cases/{case_id}/schema/{document_type}",
        response_model=GetSchemaResponse,
        summary="Get current schema and extracted column preview",
        tags=["Schema"],
    )
    async def get_schema(
        case_id: str = Path(..., description="Lending case identifier"),
        document_type: str = Path(..., description="Document type enum value"),
    ) -> GetSchemaResponse:
        """
        Return the current schema template fields for this
        (case_id, document_type) pair together with a preview of the columns
        extracted by OCR and any existing field mapping.

        If no mapping record exists yet, the response is bootstrapped from the
        system template — nothing is persisted until POST /mapping is called.
        """
        record = _get_or_create_record(repo, case_id, document_type)

        # Build extracted-column preview, injecting already-mapped field names
        raw_columns = _MOCK_OCR_COLUMNS.get(document_type, _DEFAULT_OCR_COLUMNS)
        extracted_columns = [
            ExtractedColumnSample(
                **col,
                already_mapped_to=record.field_mappings.get(col["column_name"]),
            )
            for col in raw_columns
        ]

        return GetSchemaResponse(
            case_id=case_id,
            document_type=document_type,
            mapping_id=record.mapping_id,
            schema_template_version=record.schema_template_version,
            schema_fields=record.schema_fields,
            extracted_columns=extracted_columns,
            is_validated=record.validation_timestamp is not None,
            validation_timestamp=record.validation_timestamp,
        )

    # ------------------------------------------------------------------ #
    # POST /cases/{case_id}/schema/{document_type}/update                 #
    # ------------------------------------------------------------------ #
    @application.post(
        "/cases/{case_id}/schema/{document_type}/update",
        response_model=UpdateSchemaResponse,
        summary="Apply schema mutations (add/rename/change_type/mark_required/remove)",
        tags=["Schema"],
    )
    async def update_schema(
        case_id: str = Path(...),
        document_type: str = Path(...),
        body: UpdateSchemaRequest = Body(...),
    ) -> UpdateSchemaResponse:
        """
        Apply an ordered list of schema operations to the current template.

        Operations are applied sequentially; later operations see the state
        left by earlier ones.  Unknown ``field_name`` targets are silently
        skipped and reported in the ``skipped`` list.

        Operations:
        - **add** — append a new field (``field`` payload required).
        - **rename** — update ``display_name`` of an existing field.
        - **change_type** — change the ``data_type`` of an existing field.
        - **mark_required** / **mark_optional** — toggle the ``required`` flag.
        - **remove** — delete a field (only allowed for ``source=custom`` fields).
        """
        record = _get_or_create_record(repo, case_id, document_type)
        fields: list[SchemaField] = list(record.schema_fields)
        skipped: list[dict[str, str]] = []
        applied = 0

        for op in body.operations:
            if op.operation == SchemaOperation.ADD:
                new_field = op.field
                # Reject duplicate field_name
                if any(f.field_name == new_field.field_name for f in fields):
                    skipped.append({
                        "operation": op.operation,
                        "field_name": new_field.field_name,
                        "reason": "field_name already exists",
                    })
                    continue
                # Force source = custom for user-added fields
                custom = new_field.model_copy(
                    update={"source": FieldSource.CUSTOM}
                )
                fields.append(custom)
                record.custom_fields_added.append(custom)
                applied += 1

            else:
                target_name = op.field_name
                idx = next(
                    (i for i, f in enumerate(fields)
                     if f.field_name == target_name), None
                )
                if idx is None:
                    skipped.append({
                        "operation": op.operation,
                        "field_name": target_name,
                        "reason": "field_name not found",
                    })
                    continue

                target = fields[idx]

                if op.operation == SchemaOperation.RENAME:
                    fields[idx] = target.model_copy(
                        update={"display_name": op.new_display_name}
                    )
                    applied += 1

                elif op.operation == SchemaOperation.CHANGE_TYPE:
                    fields[idx] = target.model_copy(
                        update={"data_type": op.new_data_type}
                    )
                    applied += 1

                elif op.operation == SchemaOperation.MARK_REQUIRED:
                    fields[idx] = target.model_copy(update={"required": True})
                    applied += 1

                elif op.operation == SchemaOperation.MARK_OPTIONAL:
                    fields[idx] = target.model_copy(update={"required": False})
                    applied += 1

                elif op.operation == SchemaOperation.REMOVE:
                    if target.source == FieldSource.TEMPLATE:
                        skipped.append({
                            "operation": op.operation,
                            "field_name": target_name,
                            "reason": "cannot remove template fields; mark optional instead",
                        })
                        continue
                    fields.pop(idx)
                    # Also remove from custom_fields_added tracking
                    record.custom_fields_added = [
                        f for f in record.custom_fields_added
                        if f.field_name != target_name
                    ]
                    applied += 1

        record.schema_fields = fields
        # Reset validation whenever schema changes
        if applied > 0:
            record.validated_by = None
            record.validation_timestamp = None
        repo.upsert(record)

        return UpdateSchemaResponse(
            case_id=case_id,
            document_type=document_type,
            applied_operations=applied,
            schema_fields=fields,
            skipped=skipped,
        )

    # ------------------------------------------------------------------ #
    # POST /cases/{case_id}/schema/{document_type}/mapping                #
    # ------------------------------------------------------------------ #
    @application.post(
        "/cases/{case_id}/schema/{document_type}/mapping",
        response_model=ColumnMappingResponse,
        status_code=status.HTTP_201_CREATED,
        summary="Submit column → field mappings",
        tags=["Mapping"],
    )
    async def save_mapping(
        case_id: str = Path(...),
        document_type: str = Path(...),
        body: ColumnMappingRequest = Body(...),
    ) -> ColumnMappingResponse:
        """
        Persist the analyst's raw-column → schema-field mapping.

        - **field_mappings**: ``{"Assets (₹Cr)": "assets_inr"}``
        - **unmapped_action**: ``ignore`` (default) or ``auto_name``
          (auto-derive snake_case names for unmapped columns).
        - Returns a ``mapping_id`` which is required by the edits and
          validate endpoints.

        If a mapping already exists for this (case_id, document_type), it is
        updated in-place (same mapping_id preserved).  Calling this endpoint
        resets any prior validation.
        """
        record = _get_or_create_record(repo, case_id, document_type)

        # Validate that all target field names exist in the current schema
        known_field_names = {f.field_name for f in record.schema_fields}
        bad_targets = {
            raw: tgt for raw, tgt in body.field_mappings.items()
            if tgt not in known_field_names
        }
        if bad_targets:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "Some target field_names are not in the current schema. "
                               "Add them via POST /update first.",
                    "unknown_targets": bad_targets,
                },
            )

        # Determine unmapped columns
        raw_columns = _MOCK_OCR_COLUMNS.get(document_type, _DEFAULT_OCR_COLUMNS)
        all_raw_names = {col["column_name"] for col in raw_columns}
        mapped_raw = set(body.field_mappings.keys())
        unmapped = sorted(all_raw_names - mapped_raw)

        # Auto-name if requested
        final_mappings = dict(record.field_mappings)   # preserve any prior
        final_mappings.update(body.field_mappings)

        if body.unmapped_action == "auto_name":
            for raw_col in unmapped:
                auto_name = _to_snake_case(raw_col)
                # Don't overwrite existing mappings or schema fields
                if (raw_col not in final_mappings
                        and auto_name not in known_field_names):
                    final_mappings[raw_col] = auto_name

        record.field_mappings = final_mappings
        record.validated_by = None
        record.validation_timestamp = None
        repo.upsert(record)

        return ColumnMappingResponse(
            mapping_id=record.mapping_id,
            case_id=case_id,
            document_type=document_type,
            mapped_count=len(final_mappings),
            unmapped_columns=unmapped,
            field_mappings=final_mappings,
            created_at=record.created_at,
        )

    # ------------------------------------------------------------------ #
    # POST /cases/{case_id}/schema/{document_type}/edits                  #
    # ------------------------------------------------------------------ #
    @application.post(
        "/cases/{case_id}/schema/{document_type}/edits",
        response_model=ApplyEditsResponse,
        summary="Record manual cell-level corrections",
        tags=["Edits"],
    )
    async def apply_edits(
        case_id: str = Path(...),
        document_type: str = Path(...),
        body: ApplyEditsRequest = Body(...),
    ) -> ApplyEditsResponse:
        """
        Append reviewer cell-level edits to the mapping's audit trail.

        Edits are **additive** — earlier edits are preserved.  If the same
        (row_index, column_name) is edited twice, both records are kept so
        the full correction chain is auditable.

        Requires a valid ``mapping_id`` (returned by POST /mapping).
        """
        record = _require_mapping(repo, body.mapping_id)

        # Guard: mapping_id must belong to this (case_id, document_type)
        if record.case_id != case_id or record.document_type != document_type:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"mapping_id '{body.mapping_id}' belongs to "
                    f"case_id='{record.case_id}' / "
                    f"document_type='{record.document_type}', "
                    f"not the requested case/type."
                ),
            )

        new_edits: list[ManualEdit] = []
        for edit_req in body.edits:
            new_edits.append(ManualEdit(
                row_index=edit_req.row_index,
                column_name=edit_req.column_name,
                old_value=edit_req.old_value,
                new_value=edit_req.new_value,
                edited_by=edit_req.edited_by,
                reason=edit_req.reason,
            ))

        record.manual_edits_applied.extend(new_edits)
        # Edits invalidate previous validation — reviewer must re-validate
        record.validated_by = None
        record.validation_timestamp = None
        repo.upsert(record)

        return ApplyEditsResponse(
            mapping_id=record.mapping_id,
            edits_applied=len(new_edits),
            total_edits_on_record=len(record.manual_edits_applied),
            updated_at=record.updated_at,
        )

    # ------------------------------------------------------------------ #
    # POST /cases/{case_id}/schema/{document_type}/validate               #
    # ------------------------------------------------------------------ #
    @application.post(
        "/cases/{case_id}/schema/{document_type}/validate",
        response_model=ValidateResponse,
        summary="Validate schema completeness and persist mapping",
        tags=["Validation"],
    )
    async def validate_and_save(
        case_id: str = Path(...),
        document_type: str = Path(...),
        body: ValidateRequest = Body(...),
    ) -> ValidateResponse:
        """
        Run validation checks then stamp ``validated_by`` and
        ``validation_timestamp`` on a successful pass.

        Validation rules
        ----------------
        1. **All required fields are mapped** — every field with
           ``required=True`` must have at least one raw OCR column pointing
           to it in ``field_mappings``.
        2. **No orphaned mappings** — every value in ``field_mappings`` must
           correspond to a known field in ``schema_fields``.
        3. **No empty mapping** — at least one column must be mapped.

        If validation **fails**, the record is NOT stamped and a list of
        ``ValidationError`` objects is returned with ``is_valid=false``.
        The caller can fix issues via /update or /mapping and retry.

        Returns 200 in both valid and invalid cases (HTTP status is not used
        to signal business validation — use ``is_valid`` flag instead).
        """
        record = _require_mapping(repo, body.mapping_id)

        if record.case_id != case_id or record.document_type != document_type:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"mapping_id '{body.mapping_id}' does not match the "
                       "requested case_id / document_type.",
            )

        errors: list[ValidationError] = []

        # Rule 1: required fields must be mapped
        mapped_target_fields = set(record.field_mappings.values())
        for field in record.schema_fields:
            if field.required and field.field_name not in mapped_target_fields:
                errors.append(ValidationError(
                    field_name=field.field_name,
                    error_type="missing_required",
                    detail=(
                        f"Required field '{field.field_name}' "
                        f"('{field.display_name}') has no column mapped to it."
                    ),
                ))

        # Rule 2: no orphaned mapping targets
        known_field_names = {f.field_name for f in record.schema_fields}
        for raw_col, target in record.field_mappings.items():
            if target not in known_field_names:
                errors.append(ValidationError(
                    field_name=target,
                    error_type="unmapped_required",
                    detail=(
                        f"Column '{raw_col}' is mapped to '{target}' "
                        "which does not exist in the current schema."
                    ),
                ))

        # Rule 3: at least one mapping must exist
        if not record.field_mappings:
            errors.append(ValidationError(
                field_name="__global__",
                error_type="missing_required",
                detail="No column mappings have been submitted yet.",
            ))

        is_valid = len(errors) == 0

        if is_valid:
            record.validated_by = body.validated_by
            record.validation_timestamp = datetime.now(timezone.utc)
            repo.upsert(record)

        return ValidateResponse(
            mapping_id=record.mapping_id,
            case_id=case_id,
            document_type=document_type,
            is_valid=is_valid,
            errors=errors,
            validated_by=record.validated_by,
            validation_timestamp=record.validation_timestamp,
            schema_template_version=record.schema_template_version,
        )

    return application


# ---------------------------------------------------------------------------
# Instantiate app
# ---------------------------------------------------------------------------

# Read configuration from environment variables
USE_SQLITE = os.environ.get("SCHEMA_USE_SQLITE", "true").lower() in ("true", "1", "yes")
DB_PATH = os.environ.get("SCHEMA_DB_PATH", "schema.db")

# Create default app instance (can be imported by ASGI servers)
# For production: set SCHEMA_USE_SQLITE=true
# For tests: apps are created via create_app() in test fixtures
app = None

# Only create app automatically if not in test environment
if not os.environ.get("PYTEST_CURRENT_TEST"):
    try:
        app = create_app(use_sqlite=USE_SQLITE, db_path=DB_PATH)
    except (ImportError, ModuleNotFoundError) as e:
        # If SQLite module not available, create in-memory app
        print(f"Warning: Could not load SQLite ({e}), using in-memory storage")
        app = create_app(use_sqlite=False)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    # Ensure app is created for CLI
    if app is None:
        app = create_app(use_sqlite=USE_SQLITE, db_path=DB_PATH)
    
    print(f"\nStarting Schema Service on http://0.0.0.0:8000")
    print(f"API Docs: http://0.0.0.0:8000/docs")
    print(f"Storage: {'SQLite' if USE_SQLITE else 'In-Memory'}")
    if USE_SQLITE:
        print(f"Database: {DB_PATH}\n")
    uvicorn.run("processing.ocr.schema_service:app", host="0.0.0.0", port=8000, reload=True)