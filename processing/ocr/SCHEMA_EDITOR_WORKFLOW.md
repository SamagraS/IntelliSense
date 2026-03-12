# Schema Editor Workflow

## Overview

The Schema Editor allows users to review OCR-extracted data, configure field mappings, correct errors, and validate the schema before database ingestion.

## Architecture

```
┌─────────────────┐
│   OCR Service   │ Extracts data from PDF/CSV
└────────┬────────┘
         │
         ▼ POST /cases/{case_id}/ocr/{document_type}/extracted-data
┌─────────────────────────────────────────────────────────┐
│              Schema Service (FastAPI)                    │
│                                                           │
│  ┌───────────────────┐    ┌──────────────────────────┐  │
│  │ SQLite Repository │◄───┤ SchemaMappingRecord      │  │
│  │  (schema.db)      │    │ - schema_fields          │  │
│  │                   │    │ - field_mappings         │  │
│  │                   │    │ - extracted_data         │  │
│  │                   │    │ - manual_edits_applied   │  │
│  └───────────────────┘    └──────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │
         ▼ GET /cases/{case_id}/schema/{document_type}
┌─────────────────┐
│  Frontend UI    │ Schema Editor Interface
│  (Ingestor Page)│
└─────────────────┘
```

## Complete Workflow

### Step 1: OCR Processing

After the OCR service processes a document, it submits the extracted data:

**POST** `/cases/{case_id}/ocr/{document_type}/extracted-data`

```json
{
  "ocr_run_id": "CASE_001_ALM_20260312",
  "columns": ["Maturity Bucket", "Assets (₹Cr)", "Liabilities (₹Cr)", "Gap"],
  "rows": [
    {
      "cells": [
        {"value": "0-1M", "confidence": 94.2},
        {"value": "1200.50", "confidence": 88.3},
        {"value": "1100.00", "confidence": 87.6},
        {"value": "100.50", "confidence": 72.1}
      ]
    },
    {
      "cells": [
        {"value": "1-3M", "confidence": 93.8},
        {"value": "890.00", "confidence": 85.2},
        {"value": "920.50", "confidence": 86.4},
        {"value": "-30.50", "confidence": 68.5}
      ]
    }
  ]
}
```

**Response:**
```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "case_id": "CASE_001",
  "document_type": "ALM",
  "ocr_run_id": "CASE_001_ALM_20260312",
  "rows_received": 2,
  "columns_received": 4,
  "extraction_timestamp": "2026-03-12T10:30:00Z"
}
```

### Step 2: Load Schema Editor

User opens the Ingestor page, frontend calls:

**GET** `/cases/{case_id}/schema/{document_type}`

**Response:**
```json
{
  "case_id": "CASE_001",
  "document_type": "ALM",
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "schema_template_version": "1.0.0",
  "schema_fields": [
    {
      "field_name": "maturity_bucket",
      "display_name": "Maturity Bucket",
      "data_type": "text",
      "required": true,
      "source": "template"
    },
    {
      "field_name": "assets_inr",
      "display_name": "Assets (₹Cr)",
      "data_type": "number",
      "required": true,
      "source": "template"
    }
  ],
  "extracted_columns": [
    {
      "column_name": "Maturity Bucket",
      "sample_values": ["0-1M", "1-3M"],
      "avg_confidence": 94.0,
      "low_confidence_row_count": 0,
      "already_mapped_to": null
    },
    {
      "column_name": "Assets (₹Cr)",
      "sample_values": ["1200.50", "890.00"],
      "avg_confidence": 86.75,
      "low_confidence_row_count": 0,
      "already_mapped_to": null
    }
  ],
  "extracted_data": {
    "ocr_run_id": "CASE_001_ALM_20260312",
    "columns": ["Maturity Bucket", "Assets (₹Cr)", "Liabilities (₹Cr)", "Gap"],
    "rows": [
      {
        "cells": [
          {"value": "0-1M", "confidence": 94.2},
          {"value": "1200.50", "confidence": 88.3}
        ]
      }
    ]
  },
  "is_validated": false,
  "validation_timestamp": null
}
```

### Step 3: UI Display

#### Left Panel: Field Configuration
- Shows `schema_fields` array
- Pre-defined template fields (source="template")
- User can:
  - Add custom fields (POST /update with "add" operation)
  - Rename fields (POST /update with "rename" operation)
  - Change data types (POST /update with "change_type" operation)
  - Mark required/optional (POST /update with "mark_required/mark_optional")

#### Right Panel: Extracted Data Table
- Shows `extracted_data.rows` in table format
- Color-code cells by confidence:
  - **Green** (>85%): High confidence
  - **Amber** (75-85%): Medium confidence
  - **Red** (<75%): Low confidence - needs review
- Cells are editable (POST /edits to submit corrections)

### Step 4: Schema Modifications (Optional)

**POST** `/cases/{case_id}/schema/{document_type}/update`

```json
{
  "operations": [
    {
      "operation": "add",
      "field": {
        "field_name": "liquidity_ratio",
        "display_name": "Liquidity Ratio",
        "data_type": "number",
        "required": false,
        "source": "custom"
      }
    },
    {
      "operation": "rename",
      "field_name": "gap_inr",
      "new_display_name": "Net Gap (₹Cr)"
    }
  ]
}
```

### Step 5: Column Mapping

User drags extracted columns to schema fields:

**POST** `/cases/{case_id}/schema/{document_type}/mapping`

```json
{
  "field_mappings": {
    "Maturity Bucket": "maturity_bucket",
    "Assets (₹Cr)": "assets_inr",
    "Liabilities (₹Cr)": "liabilities_inr",
    "Gap": "gap_inr"
  },
  "unmapped_action": "ignore",
  "submitted_by": "analyst@company.com"
}
```

**Response:**
```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "case_id": "CASE_001",
  "document_type": "ALM",
  "mapped_count": 4,
  "unmapped_columns": [],
  "field_mappings": {
    "Maturity Bucket": "maturity_bucket",
    "Assets (₹Cr)": "assets_inr",
    "Liabilities (₹Cr)": "liabilities_inr",
    "Gap": "gap_inr"
  },
  "created_at": "2026-03-12T10:35:00Z"
}
```

### Step 6: Manual Edits (Optional)

User corrects low-confidence cells:

**POST** `/cases/{case_id}/schema/{document_type}/edits`

```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "edits": [
    {
      "row_index": 1,
      "column_name": "Gap",
      "old_value": "-30.50",
      "new_value": "-31.50",
      "edited_by": "analyst@company.com",
      "reason": "OCR misread decimal point"
    }
  ]
}
```

### Step 7: Validation

User clicks "Validate & Save":

**POST** `/cases/{case_id}/schema/{document_type}/validate`

```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "validated_by": "analyst@company.com"
}
```

**Response (Success):**
```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "case_id": "CASE_001",
  "document_type": "ALM",
  "is_valid": true,
  "errors": [],
  "validated_by": "analyst@company.com",
  "validation_timestamp": "2026-03-12T10:40:00Z",
  "schema_template_version": "1.0.0"
}
```

**Response (Failure with errors):**
```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "case_id": "CASE_001",
  "document_type": "ALM",
  "is_valid": false,
  "errors": [
    {
      "field_name": "assets_inr",
      "error_type": "missing_required",
      "detail": "Required field 'assets_inr' ('Assets (₹Cr)') has no column mapped to it."
    }
  ],
  "validated_by": null,
  "validation_timestamp": null,
  "schema_template_version": "1.0.0"
}
```

### Step 8: Retrieve Validated Output

ETL pipeline fetches the final validated data:

**GET** `/cases/{case_id}/schema/{document_type}/validated-output`

**Response:**
```json
{
  "mapping_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "case_id": "CASE_001",
  "document_type": "ALM",
  "schema_template_version": "1.0.0",
  "fields": [
    {
      "field_name": "maturity_bucket",
      "display_name": "Maturity Bucket",
      "data_type": "text",
      "required": true,
      "source": "template"
    },
    {
      "field_name": "assets_inr",
      "display_name": "Assets (₹Cr)",
      "data_type": "number",
      "required": true,
      "source": "template"
    }
  ],
  "field_mappings": {
    "Maturity Bucket": "maturity_bucket",
    "Assets (₹Cr)": "assets_inr",
    "Liabilities (₹Cr)": "liabilities_inr",
    "Gap": "gap_inr"
  },
  "extracted_data": {
    "ocr_run_id": "CASE_001_ALM_20260312",
    "columns": ["Maturity Bucket", "Assets (₹Cr)", "Liabilities (₹Cr)", "Gap"],
    "rows": [
      {
        "cells": [
          {"value": "0-1M", "confidence": 94.2},
          {"value": "1200.50", "confidence": 88.3},
          {"value": "1100.00", "confidence": 87.6},
          {"value": "100.50", "confidence": 72.1}
        ]
      }
    ]
  },
  "manual_edits_applied": [
    {
      "edit_id": "a8f7e456-1234-5678-abcd-ef0123456789",
      "row_index": 1,
      "column_name": "Gap",
      "old_value": "-30.50",
      "new_value": "-31.50",
      "edited_by": "analyst@company.com",
      "edited_at": "2026-03-12T10:38:00Z",
      "reason": "OCR misread decimal point"
    }
  ],
  "validated_by": "analyst@company.com",
  "validation_timestamp": "2026-03-12T10:40:00Z"
}
```

## Database Schema

The SQLite database (`schema.db`) stores all mapping records:

| Column | Type | Description |
|--------|------|-------------|
| `mapping_id` | TEXT (PK) | UUID primary key |
| `case_id` | TEXT | Lending case identifier |
| `document_type` | TEXT | Document type enum (ALM, BANK_STMT, etc.) |
| `schema_template_version` | TEXT | Schema version (e.g., "1.0.0") |
| `schema_fields_json` | TEXT | JSON array of SchemaField objects |
| `field_mappings_json` | TEXT | JSON dict: {extracted_col: schema_field} |
| `custom_fields_json` | TEXT | JSON array of user-added fields |
| `manual_edits_json` | TEXT | JSON array of ManualEdit objects |
| `extracted_data_json` | TEXT | JSON object with OCR results |
| `validated_by` | TEXT | Username who validated |
| `validation_timestamp` | DATETIME | When validated |
| `created_at` | DATETIME | Record creation time |
| `updated_at` | DATETIME | Last modification time |

**Unique constraint:** `(case_id, document_type)` - One active mapping per case-document pair

## Running the Service

### Installation

```bash
pip install fastapi uvicorn[standard] pydantic sqlalchemy
```

### Start Server

```bash
cd processing/ocr
python schema_service.py
```

The service starts on `http://localhost:8000`

### API Documentation

Interactive docs available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### Storage Configuration

**SQLite (Production):**
```bash
export SCHEMA_USE_SQLITE=true
export SCHEMA_DB_PATH=/data/schema.db
python schema_service.py
```

**In-Memory (Testing):**
```bash
export SCHEMA_USE_SQLITE=false
python schema_service.py
```

## Testing

See `test/test_schema_service_integration.py` for integration tests demonstrating the complete workflow.

## Confidence-Based Color Coding

Frontend should apply these CSS classes based on cell confidence:

```css
.cell-high-confidence {    /* confidence > 85% */
  background-color: #d4edda;  /* Light green */
  color: #155724;
}

.cell-medium-confidence {  /* 75% <= confidence <= 85% */
  background-color: #fff3cd;  /* Light amber */
  color: #856404;
}

.cell-low-confidence {     /* confidence < 75% */
  background-color: #f8d7da;  /* Light red */
  color: #721c24;
  border: 2px solid #f5c6cb;  /* Highlight for reviewer attention */
}
```

## Error Handling

### HTTP Status Codes

- `200 OK` - Successful GET/POST operations
- `201 Created` - Resource created (mapping, OCR data)
- `404 Not Found` - Mapping or case not found
- `422 Unprocessable Entity` - Validation failed (check `errors` array)
- `409 Conflict` - mapping_id mismatch for case/document

### Validation Failures

When validation fails (`is_valid: false`), fix issues and retry:

1. **Missing required field mapping**: Add mapping via POST /mapping
2. **Orphaned mapping target**: Remove mapping or add field via POST /update
3. **No mappings submitted**: Submit at least one mapping via POST /mapping

## Pre-defined Templates

The service ships with templates for:

- `ALM` - Asset Liability Management
- `BANK_STMT` - Bank Statement
- `GSTR_3B` - GST Return 3B
- `RATING_REPORT` - Credit Rating Report
- `SANCTION` - Loan Sanction Letter
- `BOARD_MINUTES` - Board Meeting Minutes
- `FINANCIAL_RESULTS` - Quarterly/Annual Financial Results
- `ANNUAL_REPORT` - Annual Report
- `SHAREHOLDING` - Shareholding Pattern
- `BORROWING_PROFILE` - Borrowing Profile
- `PORTFOLIO` - Portfolio Performance
- `ITR` - Income Tax Return

New templates can be added to `SCHEMA_TEMPLATES` dict in [schema_service.py](schema_service.py).
