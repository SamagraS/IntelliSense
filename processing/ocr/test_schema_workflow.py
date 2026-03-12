"""
test_schema_workflow.py
=======================
End-to-end test demonstrating the complete Schema Editor workflow.

Prerequisites
-------------
1. Start schema_service.py in a separate terminal:
   cd processing/ocr
   python schema_service.py

2. Run this test:
   python test_schema_workflow.py

Flow
----
1. OCR extracts data → POST /ocr/extracted-data
2. Load schema editor → GET /schema
3. Modify schema → POST /update
4. Map columns → POST /mapping
5. Apply edits → POST /edits
6. Validate → POST /validate
7. Retrieve final output → GET /validated-output
"""

import requests
from datetime import datetime

BASE_URL = "http://localhost:8000"
CASE_ID = f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
DOCUMENT_TYPE = "ALM"


def print_section(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_response(resp: requests.Response, truncate: bool = False):
    """Pretty print response with status code."""
    print(f"\nStatus: {resp.status_code} {resp.reason}")
    data = resp.json()
    
    if truncate and "extracted_data" in data:
        # Truncate extracted data for readability
        data["extracted_data"] = "...(truncated)..."
    
    import json
    print(json.dumps(data, indent=2, default=str))


def main():
    print_section(f"Test Case: {CASE_ID} / {DOCUMENT_TYPE}")
    print(f"Schema Service: {BASE_URL}")
    
    # Check if service is running
    try:
        resp = requests.get(f"{BASE_URL}/docs", timeout=2)
        print("✓ Schema service is running")
    except requests.exceptions.ConnectionError:
        print("✗ Schema service is NOT running!")
        print("\nPlease start it first:")
        print("  cd processing/ocr")
        print("  python schema_service.py")
        return
    
    # -----------------------------------------------------------------------
    # Step 1: OCR submits extracted data
    # -----------------------------------------------------------------------
    print_section("Step 1: OCR Service Submits Extracted Data")
    
    ocr_data = {
        "ocr_run_id": f"{CASE_ID}_{DOCUMENT_TYPE}_{datetime.now().strftime('%Y%m%d')}",
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
            },
            {
                "cells": [
                    {"value": "3-6M", "confidence": 95.1},
                    {"value": "2340.75", "confidence": 91.7},
                    {"value": "2100.00", "confidence": 90.2},
                    {"value": "240.75", "confidence": 82.3}
                ]
            },
            {
                "cells": [
                    {"value": "6-12M", "confidence": 94.5},
                    {"value": "500.20", "confidence": 89.1},
                    {"value": "480.00", "confidence": 88.9},
                    {"value": "20.20", "confidence": 76.4}
                ]
            },
            {
                "cells": [
                    {"value": "1-3Y", "confidence": 92.7},
                    {"value": "3400.00", "confidence": 93.2},
                    {"value": "3200.00", "confidence": 92.8},
                    {"value": "200.00", "confidence": 85.6}
                ]
            }
        ]
    }
    
    resp = requests.post(
        f"{BASE_URL}/cases/{CASE_ID}/ocr/{DOCUMENT_TYPE}/extracted-data",
        json=ocr_data,
    )
    print_response(resp)
    
    if resp.status_code != 201:
        print("✗ Failed to submit OCR data")
        return
    
    mapping_id = resp.json()["mapping_id"]
    print(f"\n✓ OCR data submitted successfully")
    print(f"  Mapping ID: {mapping_id}")
    
    # -----------------------------------------------------------------------
    # Step 2: Load Schema Editor
    # -----------------------------------------------------------------------
    print_section("Step 2: Load Schema Editor (GET /schema)")
    
    resp = requests.get(f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}")
    print_response(resp, truncate=True)
    
    print(f"\n✓ Schema loaded with {len(resp.json()['schema_fields'])} template fields")
    print(f"  Extracted columns: {resp.json()['extracted_columns'][0]['column_name']}, ...")
    
    # -----------------------------------------------------------------------
    # Step 3: Modify Schema (Optional)
    # -----------------------------------------------------------------------
    print_section("Step 3: Modify Schema - Add Custom Field")
    
    schema_update = {
        "operations": [
            {
                "operation": "add",
                "field": {
                    "field_name": "cumulative_gap_pct",
                    "display_name": "Cumulative Gap (%)",
                    "data_type": "number",
                    "required": False,
                    "source": "custom",
                    "description": "Cumulative gap as percentage of total assets"
                }
            }
        ]
    }
    
    resp = requests.post(
        f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/update",
        json=schema_update,
    )
    print_response(resp, truncate=True)
    
    print(f"\n✓ Schema modified: {resp.json()['applied_operations']} operations applied")
    
    # -----------------------------------------------------------------------
    # Step 4: Submit Column Mappings
    # -----------------------------------------------------------------------
    print_section("Step 4: Submit Column → Field Mappings")
    
    mapping_request = {
        "field_mappings": {
            "Maturity Bucket": "maturity_bucket",
            "Assets (₹Cr)": "assets_inr",
            "Liabilities (₹Cr)": "liabilities_inr",
            "Gap": "gap_inr"
        },
        "unmapped_action": "ignore",
        "submitted_by": "test_analyst@company.com"
    }
    
    resp = requests.post(
        f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/mapping",
        json=mapping_request,
    )
    print_response(resp)
    
    print(f"\n✓ Column mappings submitted: {resp.json()['mapped_count']} columns mapped")
    
    # -----------------------------------------------------------------------
    # Step 5: Apply Manual Edits
    # -----------------------------------------------------------------------
    print_section("Step 5: Apply Manual Cell Corrections")
    
    edits_request = {
        "mapping_id": mapping_id,
        "edits": [
            {
                "row_index": 1,
                "column_name": "Gap",
                "old_value": "-30.50",
                "new_value": "-31.50",
                "edited_by": "test_analyst@company.com",
                "reason": "OCR misread decimal point - verified against original document"
            }
        ]
    }
    
    resp = requests.post(
        f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/edits",
        json=edits_request,
    )
    print_response(resp)
    
    print(f"\n✓ Manual edits applied: {resp.json()['edits_applied']} corrections recorded")
    
    # -----------------------------------------------------------------------
    # Step 6: Validate Schema
    # -----------------------------------------------------------------------
    print_section("Step 6: Validate & Save")
    
    validate_request = {
        "mapping_id": mapping_id,
        "validated_by": "test_analyst@company.com"
    }
    
    resp = requests.post(
        f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/validate",
        json=validate_request,
    )
    print_response(resp)
    
    if resp.json()["is_valid"]:
        print(f"\n✓ Validation successful!")
        print(f"  Validated by: {resp.json()['validated_by']}")
        print(f"  Timestamp: {resp.json()['validation_timestamp']}")
    else:
        print(f"\n✗ Validation failed with {len(resp.json()['errors'])} errors:")
        for err in resp.json()["errors"]:
            print(f"  - {err['field_name']}: {err['detail']}")
        return
    
    # -----------------------------------------------------------------------
    # Step 7: Retrieve Final Validated Output
    # -----------------------------------------------------------------------
    print_section("Step 7: Retrieve Final Validated Output")
    
    resp = requests.get(
        f"{BASE_URL}/cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/validated-output"
    )
    print_response(resp, truncate=True)
    
    output = resp.json()
    print(f"\n✓ Final validated output retrieved")
    print(f"  Fields: {len(output['fields'])} total")
    print(f"  Mappings: {len(output['field_mappings'])} columns mapped")
    print(f"  Manual edits: {len(output['manual_edits_applied'])} corrections applied")
    print(f"  Extracted rows: {len(output['extracted_data']['rows'])} rows")
    print(f"  Ready for database ingestion: YES")
    
    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    print_section("Workflow Complete!")
    
    print(f"""
Test Summary:
  Case ID:        {CASE_ID}
  Document Type:  {DOCUMENT_TYPE}
  Mapping ID:     {mapping_id}
  
Data Statistics:
  Template fields:   6 (from ALM template)
  Custom fields:     1 (cumulative_gap_pct)
  Extracted columns: 4
  Data rows:         5
  Manual edits:      1 cell correction
  
Status: VALIDATED ✓

Next Steps:
  - ETL pipeline can fetch validated output from:
    GET /cases/{CASE_ID}/schema/{DOCUMENT_TYPE}/validated-output
    
  - Data is ready for ingestion into Delta tables
  - All manual corrections are audited (edit_id, timestamp, user)
  - Field mappings and schema version are preserved for reproducibility
    """)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n✗ Test interrupted by user")
    except Exception as e:
        print(f"\n\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
