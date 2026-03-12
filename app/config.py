"""
config.py
=========
Shared configuration for the IntelliCredit API application.
"""

import os
from pathlib import Path

# ═══════════════════════════════════════════════════════════════════
# Database Configuration
# ═══════════════════════════════════════════════════════════════════

DB_PATH = os.environ.get("INGESTOR_DB_PATH", "intelli_credit.db")
DB_URL = f"sqlite:///{DB_PATH}"
DB_CONNECT_ARGS = {"check_same_thread": False}

# ═══════════════════════════════════════════════════════════════════
# Upload Configuration
# ═══════════════════════════════════════════════════════════════════

UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "./UPLOADS"))
UPLOAD_DIR.mkdir(exist_ok=True)

# ═══════════════════════════════════════════════════════════════════
# API Configuration
# ═══════════════════════════════════════════════════════════════════

API_TITLE = "IntelliCredit Platform API"
API_DESCRIPTION = """
IntelliCredit Platform - Comprehensive lending intelligence and credit risk assessment.

## Features
- 📄 Document ingestion and classification
- 🔍 OCR and data extraction
- 📊 Financial analysis and spreading
- 🎯 Credit scoring and risk assessment
- 📈 Portfolio analytics
- 🔐 Secure multi-user workspace

## Modules
- **Ingestor**: Document upload, classification, and OCR processing
- **Research**: Coming soon
- **Analytics**: Coming soon
- **Scoring**: Coming soon
"""
API_VERSION = "2.0.0"

# CORS Origins (for production, specify exact domains)
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "*").split(",")

# ═══════════════════════════════════════════════════════════════════
# Logging Configuration
# ═══════════════════════════════════════════════════════════════════

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
LOG_FILE = os.environ.get("LOG_FILE", "intellicredit_api.log")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

# ═══════════════════════════════════════════════════════════════════
# Processing Configuration
# ═══════════════════════════════════════════════════════════════════

# Thread pool for parallel processing
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "4"))

# OCR settings
OCR_DPI = int(os.environ.get("OCR_DPI", "300"))
TESSERACT_PSM = int(os.environ.get("TESSERACT_PSM", "3"))
OCR_CONF_THRESHOLD = float(os.environ.get("OCR_CONF_THRESHOLD", "75.0"))

# API timeouts
API_TIMEOUT = float(os.environ.get("API_TIMEOUT", "60.0"))

# ═══════════════════════════════════════════════════════════════════
# Feature Flags
# ═══════════════════════════════════════════════════════════════════

ENABLE_SEMANTIC_CLASSIFICATION = os.environ.get("ENABLE_SEMANTIC", "true").lower() == "true"
ENABLE_BACKGROUND_TASKS = os.environ.get("ENABLE_BACKGROUND_TASKS", "false").lower() == "true"
ENABLE_CACHING = os.environ.get("ENABLE_CACHING", "false").lower() == "true"
