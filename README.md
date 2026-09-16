# IntelliSense (Intelli-Credit)
### *Autonomous AI Underwriting & Digital Credit Appraisal Engine for Institutional Lending*

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![PyTorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=for-the-badge&logo=pytorch&logoColor=white)](https://pytorch.org/)
[![HuggingFace](https://img.shields.io/badge/HuggingFace-FinBERT-FFD21E?style=for-the-badge&logo=huggingface&logoColor=black)](https://huggingface.co/ProsusAI/finbert)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)](https://www.sqlite.org/)
[![OpenAI / OpenRouter](https://img.shields.io/badge/LLM-OpenRouter%20API-412991?style=for-the-badge&logo=openai&logoColor=white)](https://openrouter.ai/)

---

## Executive Overview

**IntelliSense (Intelli-Credit)** is an institutional-grade, AI-driven **“Digital Credit Manager”** engineered for Indian Banks, Non-Banking Financial Companies (NBFCs), and SME lenders. 

In traditional commercial credit underwriting, credit teams spend days manually cross-referencing multi-page bank statements, GST filings (GSTR-1, GSTR-3B), audited financial statements (P&L and Balance Sheet), MCA-21 company registry records, e-Courts litigation data, and qualitative site-visit reports. 

**IntelliSense automates this entire pipeline end-to-end:**
1. **Ingests & Normalizes Heterogeneous Documents:** High-precision OCR, multi-tier document classification, and schema extraction.
2. **Detects Fraud & Circular Trading:** Cross-reconciles reported GST sales against actual banking inflows.
3. **Applies Multi-Modal Machine Learning Risk Intelligence:**
   - **FinBERT** for sentiment and governance risk extraction from qualitative texts.
   - **Isolation Forest** for cash flow and transactional anomaly detection.
   - **NetworkX Graph Engine** for promoter networks, shell company interlocks, and charge encumbrances.
4. **Computes "Five Cs of Credit":** Aggregates quantitative ratios and qualitative ML signals into an explainable composite credit scorecard.
5. **Generates Audit-Ready Credit Appraisal Memos (CAM):** Synthesizes credit limits, debt sizing, pricing recommendations, and SWOT analysis into standardized institutional **DOCX** and **PDF** formats.

---

## System Architecture

```
                                  [ RAW BORROWER ARTIFACTS ]
    Bank Statements (PDF/CSV) │ GST Filings (GSTR-1/3B) │ Audited P&L/BS │ MCA Filings │ Due Diligence Notes
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               1. DOCUMENT PROCESSING & INGESTION                                       │
│  • Multi-Tier Document Classifier (Rule-based & keyword pattern matching)                              │
│  • Hybrid OCR Extraction Engine: PyMuPDF (fitz) + pdfplumber + Camelot + Tesseract                     │
│  • Dynamic Schema Repository (SQLite): Field extraction, verification flags, and manual override UI   │
└───────────────────────────────────┬───────────────────────────────────┬────────────────────────────────┘
                                    │                                   │
                                    ▼                                   ▼
┌──────────────────────────────────────────────┐    ┌────────────────────────────────────────────────────┐
│      2. FINANCIAL SPREADING & RECON          │    │             3. ML RISK INTELLIGENCE ENGINE         │
│  • GST vs Bank Credit Reconciliation         │    │  • NLP Governance: ProsusAI/FinBERT chunk scoring  │
│  • Circular Trading & Revenue Inflation Check│    │  • Anomaly Detection: Isolation Forest cash flows  │
│  • Ratios: DSCR, Current Ratio, Debt/EBITDA  │    │  • Network Risk: Graph analysis of director links  │
│  • ALM (Asset Liability Management) Buckets  │    │  • Pre-Cognitive Early Warning Signals             │
└──────────────────────┬───────────────────────┘    └─────────────────────┬──────────────────────────────┘
                       │                                                  │
                       └────────────────────────┬─────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              4. FIVE Cs SCORECARD AGGREGATOR                                           │
│       Character (20%)  │  Capacity (30%)  │  Capital (20%)  │  Collateral (15%)  │  Conditions (15%)   │
│       • FinBERT Governance Signals        • GST-Bank Reliability Score          • Live MCA Encumbrance │
│       • Director Interlock Contagion      • DSCR & Operating Cash Flow Coverage • Site Visit Sentiment │
└───────────────────────────────────────────────┬────────────────────────────────────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                           5. AUTONOMOUS CAM GENERATION & EXPORT                                        │
│  • LLM Credit Underwriter Agent (OpenRouter / OpenAI API with custom financial system prompts)         │
│  • Sectional Parser & SWOT Generator: Executive summary, risk mitigants, covenant structuring          │
│  • Document Exporter: Institutional-grade formatted DOCX & PDF generation                              │
└───────────────────────────────────────────────┬────────────────────────────────────────────────────────┘
                                                │
                                                ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                               6. REST API & INTERACTIVE DASHBOARD                                      │
│  • Modular FastAPI Backend (`/api/ingest/*`, `/api/ingest/status`, `/api/ingest/schema/edit`)          │
│  • Web Console: Drag-and-drop batch ingestion, visual schema editor, and live underwriting audit logs │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Modules & Capabilities

### 1. High-Precision OCR & Document Ingestion
* **Location:** `processing/ocr/` & `processing/classification/`
* **Multi-Engine PDF Parsing:** Coordinates `pdfplumber` (native text & layout), `Camelot` (lattice/stream financial table extraction), `PyMuPDF` (rasterization & metadata), and `pytesseract` (fallback for scanned photocopies).
* **Document Classification:** Categorizes uploaded collateral into canonical document classes (`bank_statement`, `gst_return`, `financial_statement`, `mca_filing`, `litigation_record`).
* **Schema Validation & Storage:** Persists raw and validated schema data to SQLite with field-level confidence scores, audit logs, and manual human-in-the-loop override endpoints.

### 2. GST vs. Bank Turnover Reconciliation
* **Location:** `processing/ocr/gst_bank_recon.py`
* **Fraud & Divergence Detection:** Compares reported outward taxable supplies from GSTR-1/3B against actual credit deposits in operational bank accounts.
* **Reliability Metric:** Computes divergence percentages and flags suspicious discrepancies (>15% divergence), circular routing, and fictitious revenue inflation.

### 3. Machine Learning & Risk Intelligence
* **Location:** `ml/ml/`
* **FinBERT Governance Extractor (`finbert_risk_extractor.ipynb`):** Uses `ProsusAI/finbert` tokenized in 256-token sliding windows to analyze management interview transcripts, annual reports, and legal notes to extract governance penalties and risk probabilities.
* **Isolation Forest Anomaly Engine (`isolation_forsest.ipynb`):** Runs unsupervised anomaly detection over daily transaction volumes, counterparty velocity, and sudden spikes in debit/credit ratios.
* **Graph Risk Engine (`graph_risk_engine.ipynb`):** Constructs a directed bipartite corporate graph (`NetworkX`) connecting borrowers, Director Identification Numbers (DIN), and associated entities to identify shell company networks, undisclosed related-party exposure, and circular guarantees.
* **Five Cs Scorecard Aggregator (`Aggregator.ipynb`):** Unifies quantitative balance sheet indicators with qualitative NLP and graph penalties into a normalized 0–100 underwriting grade:
  - **Character:** Director litigation, promoter pledge trends, FinBERT governance score.
  - **Capacity:** DSCR, interest coverage, GST-to-bank reliability score, cash conversion cycle.
  - **Capital:** Net worth, leverage ratio, debt-to-equity, promoter equity dilution.
  - **Collateral:** Live vs. satisfied MCA charges, asset coverage ratio, encumbrance percentage.
  - **Conditions:** Industry headwinds, site visit sentiment, customer concentration.

### 4. Autonomous CAM (Credit Appraisal Memo) Generation
* **Location:** `cam_generation/`
* **Structured Prompt Synthesis:** Assembles borrower context, financials, ratio spreads, and 5 Cs scores into institutional credit appraisal prompts.
* **LLM Client (`llm_client.py`):** Interfaces with LLMs via OpenRouter/OpenAI API with low-temperature deterministic reasoning (`temperature=0.2`).
* **Report Builder (`docx_builder.py`, `pdf_exporter.py`):** Automatically compiles the generated analysis into professional, formatted `.docx` and `.pdf` files containing borrower profiles, facility recommendations, key covenants, and SWOT analysis.

### 5. Modular FastAPI Backend & Web Client
* **Location:** `app/` & `frontend/`
* Complete REST API with CORS support, Pydantic request/response schemas, SQLite persistence, and endpoints for:
  - Single and batch document upload (`/api/ingest/upload`, `/api/ingest/batch`)
  - Real-time ingestion progress monitoring (`/api/ingest/status/{case_id}`)
  - Interactive schema review & manual correction (`/api/ingest/schema/edit`)
  - Key credit underwriting findings extraction (`/api/ingest/findings/{case_id}`)

---

## Repository Structure

```plaintext
IntelliSense/
├── app/                                # FastAPI Web Application
│   ├── app.py                          # Main API entry point and router setup
│   ├── config.py                       # Configuration settings and environment paths
│   ├── dependencies.py                 # Shared database, logging, and lifecycle dependencies
│   ├── ingestor_endpoints.py           # Ingestion, validation, and schema editing routes
│   └── README.md                       # Sub-module API documentation
├── cam_generation/                     # Credit Appraisal Memo Generation Subsystem
│   ├── run_cam_generation.py           # CLI runner for standalone CAM generation
│   ├── requirements.txt                # CAM-specific dependencies
│   └── src/cam_generation/
│       ├── generator.py                # Pipeline orchestrator (Prompt -> LLM -> DOCX/PDF)
│       ├── llm_client.py               # OpenRouter / OpenAI API connector
│       ├── prompt_builder.py           # Underwriting prompt engineering templates
│       ├── cam_parser.py               # Sectional text parser and schema validator
│       ├── docx_builder.py             # Formatted Word document generator
│       ├── pdf_exporter.py             # DOCX-to-PDF compiler
│       └── swot_manager.py             # Strengths, Weaknesses, Opportunities & Threats matrix
├── processing/                         # Core Data Ingestion & Transformation
│   ├── classification/
│   │   ├── document_classifier.py      # Classifier categorizing incoming financial docs
│   │   └── intelli_credit.db           # SQLite database for document metadata
│   └── ocr/
│       ├── ocr_service.py              # Multi-engine OCR (PyMuPDF, pdfplumber, Camelot, Tesseract)
│       ├── document_analyser.py        # Layout analysis and line-item table parsing
│       ├── financial_spreading.py      # P&L and Balance Sheet ratio computation engine
│       ├── gst_bank_recon.py           # GST vs. Bank credits cross-reconciliation
│       ├── schema_service.py           # Schema extraction and field normalization
│       └── schema_repository_sqlite.py # SQLite CRUD operations for financial schemas
├── ml/                                 # Machine Learning & Quantitative Risk Models
│   └── ml/
│       ├── Aggregator.ipynb            # Five Cs scorecard synthesis and weighted credit rating
│       ├── finbert_risk_extractor.ipynb# HuggingFace FinBERT governance & litigation risk extractor
│       ├── isolation_forsest.ipynb     # Isolation Forest banking transaction anomaly detector
│       ├── graph_risk_engine.ipynb     # NetworkX corporate linkage & related-party fraud engine
│       └── Pre_Cognitive_Analysis.ipynb# Early-warning signal generator from qualitative sources
├── frontend/                           # Web UI & Client Integrations
│   ├── home.html                       # Underwriter cockpit and document upload portal
│   ├── home2.html                      # Enhanced dashboard with live status cards
│   └── ingestor_api_client.js          # Asynchronous JavaScript client for FastAPI endpoints
├── demo_app/                           # Standalone prototype demo application
│   └── index.html                      # Interactive credit analyst review UI
├── INFO/                               # Data Dictionary, Audits & Specifications
│   ├── data_u_context_for_ai.md        # Dataset mapping across 2,238 Indian company scenarios
│   ├── intelli_credit_data_audit.md    # Master bridge, MCA, and GST linkage audit
│   └── requirments_doc.pdf             # Original product specifications
├── test_ocr_pipeline.py                # Comprehensive test suite for OCR and extraction
├── requirements.txt                    # Project-level dependencies
└── README.md                           # This documentation
```

---

## Tech Stack

| Domain | Technologies |
|---|---|
| **Backend & APIs** | Python 3.10+, FastAPI, Uvicorn, Pydantic, CORS Middleware |
| **Database & Storage** | SQLite (Schema and Document Registries), JSON schemas |
| **Document Processing & OCR** | PyMuPDF (`fitz`), pdfplumber, Camelot-py, OpenCV, Tesseract OCR (`pytesseract`), Pillow |
| **NLP & Deep Learning** | Hugging Face Transformers, `ProsusAI/finbert`, PyTorch, spaCy, Sentence-Transformers |
| **Machine Learning & Graph** | Scikit-Learn (Isolation Forest), NetworkX, Pandas, NumPy |
| **Generative AI & LLM** | OpenRouter API / OpenAI API, Jinja2/Structured Prompting |
| **Reporting & Export** | `python-docx`, Ghostscript / LibreOffice (PDF exporter) |
| **Frontend** | HTML5, CSS3, Modern JavaScript (Fetch API, DOM Manipulation) |

---

## Getting Started

### 1. Prerequisites
Ensure you have the following installed on your machine:
* **Python 3.10+**
* **Tesseract OCR:**
  * **macOS:** `brew install tesseract`
  * **Ubuntu/Debian:** `sudo apt-get install -y tesseract-ocr libtesseract-dev`
* **Ghostscript / Poppler** (required for `camelot-py` PDF table extraction):
  * **macOS:** `brew install poppler ghostscript`
  * **Ubuntu/Debian:** `sudo apt-get install -y poppler-utils ghostscript`

### 2. Clone the Repository & Set Up Environment
```bash
git clone https://github.com/your-username/IntelliSense.git
cd IntelliSense

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install core dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Install CAM generation dependencies (if working on memo export)
pip install -r cam_generation/requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory (or update `cam_generation/.env`):
```env
# OpenRouter / OpenAI API credentials for CAM generation
OPENROUTER_API_KEY=your_openrouter_api_key_here

# Application Configuration
PORT=8000
HOST=0.0.0.0
DEBUG=True
```

---

## Running the System

### 1. Start the FastAPI Server
```bash
# Run using Python directly
python app/app.py

# Or run using Uvicorn with hot-reload
uvicorn app.app:app --reload --host 0.0.0.0 --port 8000
```
* **Interactive Swagger UI:** [http://localhost:8000/docs](http://localhost:8000/docs)
* **ReDoc Documentation:** [http://localhost:8000/redoc](http://localhost:8000/redoc)
* **Health Check:** [http://localhost:8000/health](http://localhost:8000/health)

### 2. Run the Autonomous CAM Generator
To generate a complete Credit Appraisal Memo from a structured financial case JSON:
```bash
python cam_generation/run_cam_generation.py
```
Output artifacts are saved to `cam_generation/generated_cams/`:
* `{case_id}_CAM.docx`: Formatted Word document with executive summary, 5 Cs scorecard, and covenants.
* `{case_id}_CAM.pdf`: Optional exported PDF version.
* `{case_id}_raw.txt`: Raw LLM reasoning output.

### 3. Run the OCR & Processing Test Suite
To verify table extraction and OCR accuracy across sample financial documents:
```bash
python test_ocr_pipeline.py
```

### 4. Explore ML Risk Models
Launch Jupyter Notebook to inspect the quantitative models:
```bash
jupyter notebook ml/ml/
```
* Open `Aggregator.ipynb` to view the Five Cs weighting calculations.
* Open `finbert_risk_extractor.ipynb` to run sentiment scoring on annual reports.
* Open `isolation_forsest.ipynb` to inspect transaction anomaly detection.
* Open `graph_risk_engine.ipynb` to see network analysis on director interlocks.

---

## API Reference Overview

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Server health check and uptime status |
| `POST` | `/api/ingest/upload` | Upload a single document (PDF/image) with case ID and auto-classification |
| `POST` | `/api/ingest/batch` | Upload multiple files simultaneously for batch processing |
| `GET` | `/api/ingest/status/{case_id}` | Retrieve ingestion, OCR, and classification progress for a case |
| `PATCH` | `/api/ingest/validate` | Human-in-the-loop override for document classification |
| `PATCH` | `/api/ingest/schema/edit` | Edit, correct, or verify extracted financial schema fields |
| `GET` | `/api/ingest/findings/{case_id}`| Fetch summarized underwriting anomalies and credit flags |

---

## The "Five Cs of Credit" Evaluation Matrix

IntelliSense maps raw financial and non-financial data into institutional credit metrics:

```
┌───────────────┬──────────────────────────────────┬────────────────────────────────────────────────────────┐
│ Dimension     │ Weight │ Primary Data Sources    │ Key Underwriting Signals Evaluated                     │
├───────────────┼────────┼─────────────────────────┼────────────────────────────────────────────────────────┤
│ 1. Character  │  20%   │ MCA, Litigation, News   │ Promoter share pledge trends, FinBERT governance risk  │
│               │        │ Management Q&A          │ score, director disqualifications (MCA-21), defaults   │
├───────────────┼────────┼─────────────────────────┼────────────────────────────────────────────────────────┤
│ 2. Capacity   │  30%   │ Banking, GST, P&L       │ DSCR, GST-to-bank turnover reconciliation match %,     │
│               │        │ ITR filings             │ interest coverage ratio, operational cash flow margins │
├───────────────┼────────┼─────────────────────────┼────────────────────────────────────────────────────────┤
│ 3. Capital    │  20%   │ Balance Sheet, MCA      │ Tangible net worth (TNW), Total Debt / Equity,         │
│               │        │ Shareholding patterns   │ promoter capital infusion, retained earnings growth   │
├───────────────┼────────┼─────────────────────────┼────────────────────────────────────────────────────────┤
│ 4. Collateral │  15%   │ MCA Charges, Valuations │ Live vs. satisfied MCA encumbrance, asset cover ratio, │
│               │        │ Asset Registers         │ primary vs. collateral security charge quality         │
├───────────────┼────────┼─────────────────────────┼────────────────────────────────────────────────────────┤
│ 5. Conditions │  15%   │ Industry reports, ALM   │ Asset-Liability maturity mismatch (ALM buckets),       │
│               │        │ Site visit logs, Macro  │ customer/supplier concentration, macroeconomic risks   │
└───────────────┴────────┴─────────────────────────┴────────────────────────────────────────────────────────┘
```

---

## License & Acknowledgements

* **License:** Distributed under the MIT License. See `LICENSE` for more information.
* **Pretrained Models:** [ProsusAI/finbert](https://huggingface.co/ProsusAI/finbert) hosted via Hugging Face.
* **LLM Engine:** Powered by OpenRouter / OpenAI API.
