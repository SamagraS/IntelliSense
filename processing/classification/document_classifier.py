"""
Enhanced Intelli-Credit Document Classification with Semantic Analysis
Now includes: Better content analysis, Image OCR improvements, Semantic classifier
"""

import os
import re
import uuid
import shutil
import logging
import mimetypes
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path
from enum import Enum

# Third-party imports
import pandas as pd
import pdfplumber
from PIL import Image
import pytesseract
from pdf2image import convert_from_path
import docx
import numpy as np
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Boolean, Text, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Semantic classifier
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import cv2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('document_classification.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Database setup
Base = declarative_base()
DB_PATH = 'intelli_credit.db'
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)


# =====================================================================
# ENUMS
# =====================================================================

class DocumentType(str, Enum):
    ALM = "ALM"
    SHAREHOLDING = "Shareholding"
    BORROWING_PROFILE = "Borrowing_Profile"
    ANNUAL_REPORT = "Annual_Report"
    PORTFOLIO_CUTS = "Portfolio_Cuts"
    GST_FILING = "GST_Filing"
    BANK_STATEMENT = "Bank_Statement"
    ITR = "ITR"
    BOARD_MINUTES = "Board_Minutes"
    RATING_REPORT = "Rating_Report"
    SANCTION_LETTER = "Sanction_Letter"
    UNKNOWN = "Unknown"


class FileType(str, Enum):
    PDF = "pdf"
    XLSX = "xlsx"
    XLS = "xls"
    CSV = "csv"
    JPG = "jpg"
    PNG = "png"
    DOCX = "docx"
    DOC = "doc"


class ClassificationMethod(str, Enum):
    FILENAME = "filename"
    CONTENT = "content"
    STRUCTURE = "structure"
    SEMANTIC = "semantic"
    HYBRID = "hybrid"


class ValidationStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    EDITED = "edited"


# =====================================================================
# DATABASE MODELS
# =====================================================================

class DocumentClassification(Base):
    __tablename__ = 'document_classification'
    
    document_id = Column(String(36), primary_key=True)
    case_id = Column(String(50), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    file_size_bytes = Column(Integer, nullable=False)
    file_type = Column(SQLEnum(FileType), nullable=False)
    upload_timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    auto_classified_type = Column(SQLEnum(DocumentType), nullable=False)
    classification_method = Column(SQLEnum(ClassificationMethod), nullable=False)
    classification_confidence = Column(Float, nullable=False)
    
    human_validated_type = Column(SQLEnum(DocumentType), nullable=True)
    validation_status = Column(SQLEnum(ValidationStatus), default=ValidationStatus.PENDING, nullable=False)
    validated_by = Column(String(100), nullable=True)
    validation_timestamp = Column(DateTime, nullable=True)
    denial_reason = Column(Text, nullable=True)
    
    file_storage_path = Column(String(500), nullable=False)
    
    # Metadata for debugging
    classification_metadata = Column(Text, nullable=True)
    error_log = Column(Text, nullable=True)


# =====================================================================
# CLASSIFICATION PATTERNS (Enhanced)
# =====================================================================

class ClassificationPatterns:
    """Enhanced classification patterns with better content signatures"""
    
    # Filename patterns (kept minimal since user wants content-based classification)
    FILENAME_PATTERNS = {
        DocumentType.ALM: ['alm_'],
        DocumentType.SHAREHOLDING: ['shareholding_pattern', 'shp_'],
        DocumentType.BORROWING_PROFILE: ['borrowing_profile', 'bp_'],
        DocumentType.ANNUAL_REPORT: ['annual_report_'],
        DocumentType.PORTFOLIO_CUTS: ['portfolio_cuts', 'pc_'],
        DocumentType.GST_FILING: ['gstr', 'gst_filing'],
        DocumentType.BANK_STATEMENT: ['bank_statement_'],
        DocumentType.ITR: ['itr_', 'income_tax_return'],
        DocumentType.BOARD_MINUTES: ['board_minutes_', 'mom_'],
        DocumentType.RATING_REPORT: ['rating_report_', 'crisil', 'icra'],
        DocumentType.SANCTION_LETTER: ['sanction_letter_']
    }
    
    # Enhanced content signatures with scoring weights
    CONTENT_SIGNATURES = {
        DocumentType.ALM: {
            'strong': [  # High confidence indicators
                'maturity profile', 'asset liability management', 'gap analysis',
                'cumulative gap', 'liquidity gap', 'duration gap', 'alm statement'
            ],
            'medium': [
                '1-7 days', '8-14 days', 'maturity bucket', 'asset-liability'
            ],
            'weak': [
                'assets', 'liabilities', 'gap'
            ]
        },
        DocumentType.SHAREHOLDING: {
            'strong': [
                'promoter holding', 'pledged shares', 'shareholding pattern',
                'promoter pledge', 'equity share capital', 'promoter group'
            ],
            'medium': [
                'public shareholding', 'institutional holding', 'bse', 'nse'
            ],
            'weak': [
                'shares', 'holding', 'equity'
            ]
        },
        DocumentType.BORROWING_PROFILE: {
            'strong': [
                'existing facilities', 'facility type', 'sanctioned limit',
                'working capital limit', 'term loan facility', 'overdraft facility',
                'letter of credit', 'bank guarantee'
            ],
            'medium': [
                'lender', 'credit facility', 'loan account', 'outstanding amount'
            ],
            'weak': [
                'loan', 'facility', 'bank'
            ]
        },
        DocumentType.ANNUAL_REPORT: {
            'strong': [
                'director report', 'auditor report', 'annual general meeting',
                'consolidated financial statements', 'notes to accounts',
                'management discussion and analysis', 'corporate governance report',
                'independent auditor', 'statutory auditor'
            ],
            'medium': [
                'financial statements', 'cash flow statement', 'board of directors',
                'balance sheet', 'profit and loss'
            ],
            'weak': [
                'annual', 'report', 'financial year'
            ]
        },
        DocumentType.PORTFOLIO_CUTS: {
            'strong': [
                'gross npa', 'net npa', 'provisioning coverage ratio',
                'asset quality', 'portfolio concentration', 'restructured assets',
                'standard assets', 'sub-standard assets', 'doubtful assets'
            ],
            'medium': [
                'sector wise', 'npa ratio', 'loan portfolio'
            ],
            'weak': [
                'portfolio', 'npa', 'assets'
            ]
        },
        DocumentType.GST_FILING: {
            'strong': [
                'gstr-3b', 'gstr-2a', 'input tax credit', 'output tax',
                'gstin', 'integrated tax', 'central tax', 'state tax',
                'place of supply', 'reverse charge'
            ],
            'medium': [
                'gst return', 'tax period', 'filing status'
            ],
            'weak': [
                'gst', 'tax', 'goods and services'
            ]
        },
        DocumentType.BANK_STATEMENT: {
            'strong': [
                'opening balance', 'closing balance', 'running balance',
                'transaction date', 'value date', 'cheque no',
                'debit', 'credit', 'narration', 'account statement'
            ],
            'medium': [
                'account number', 'ifsc', 'branch'
            ],
            'weak': [
                'balance', 'transaction', 'bank'
            ]
        },
        DocumentType.ITR: {
            'strong': [
                'income tax return', 'assessment year', 'acknowledgement number',
                'total income', 'income from salary', 'income from business',
                'income from capital gains', 'deductions under chapter vi',
                'tax payable', 'verification', 'income tax act'
            ],
            'medium': [
                'pan', 'financial year', 'return filed', 'itr form'
            ],
            'weak': [
                'income', 'tax', 'return'
            ]
        },
        DocumentType.BOARD_MINUTES: {
            'strong': [
                'board of directors', 'resolved that', 'special resolution',
                'ordinary resolution', 'minutes of meeting', 'quorum',
                'chairperson', 'meeting held on', 'agenda', 'pursuant to'
            ],
            'medium': [
                'board meeting', 'directors present', 'resolution'
            ],
            'weak': [
                'meeting', 'board', 'resolution'
            ]
        },
        DocumentType.RATING_REPORT: {
            'strong': [
                'credit rating', 'rating rationale', 'rating outlook',
                'rating action', 'rating watch', 'rating affirmation',
                'rating upgrade', 'rating downgrade', 'crisil', 'icra',
                'care ratings', 'india ratings'
            ],
            'medium': [
                'rating agency', 'credit opinion', 'rating scale'
            ],
            'weak': [
                'rating', 'credit', 'outlook'
            ]
        },
        DocumentType.SANCTION_LETTER: {
            'strong': [
                'sanction letter', 'hereby sanctioned', 'facility sanctioned',
                'terms and conditions', 'drawdown', 'validity period',
                'rate of interest', 'repayment schedule', 'security offered',
                'covenants', 'loan agreement'
            ],
            'medium': [
                'loan amount', 'sanctioned amount', 'facility letter'
            ],
            'weak': [
                'sanction', 'approval', 'facility'
            ]
        }
    }
    
    # Structure signatures (unchanged)
    STRUCTURE_SIGNATURES = {
        DocumentType.ALM: [
            ['maturity', 'bucket'], ['assets', 'liabilities'],
            ['gap'], ['1-7', '8-14'], ['cumulative']
        ],
        DocumentType.SHAREHOLDING: [
            ['promoter', 'holding'], ['pledged'], ['shares'],
            ['category', 'shareholding'], ['public', 'promoter']
        ],
        DocumentType.BORROWING_PROFILE: [
            ['lender', 'bank'], ['sanctioned', 'amount'], ['outstanding'],
            ['interest', 'rate'], ['facility', 'type'], ['tenor']
        ],
        DocumentType.PORTFOLIO_CUTS: [
            ['npa'], ['gross', 'net'], ['provisioning'],
            ['sector'], ['concentration'], ['asset', 'quality']
        ],
        DocumentType.GST_FILING: [
            ['gst', 'gstin'], ['tax', 'amount'], ['igst', 'cgst', 'sgst'],
            ['output', 'input'], ['period'], ['filing']
        ],
        DocumentType.BANK_STATEMENT: [
            ['date', 'transaction'], ['debit', 'credit'], ['balance'],
            ['narration', 'description'], ['cheque', 'reference']
        ]
    }
    
    # Semantic embeddings for document types (used by semantic classifier)
    SEMANTIC_DESCRIPTIONS = {
        DocumentType.ALM: "Asset Liability Management statement showing maturity profile of assets and liabilities with gap analysis and liquidity buckets",
        DocumentType.SHAREHOLDING: "Shareholding pattern document showing promoter holdings, public holdings, institutional holdings, and pledged shares percentage",
        DocumentType.BORROWING_PROFILE: "List of existing debt facilities from various lenders showing sanctioned amounts, outstanding balances, interest rates, and facility types",
        DocumentType.ANNUAL_REPORT: "Annual report containing audited financial statements, director's report, auditor's report, corporate governance disclosures, and notes to accounts",
        DocumentType.PORTFOLIO_CUTS: "Portfolio performance analysis showing gross NPA, net NPA, provisioning coverage ratio, sector-wise concentration, and asset quality metrics",
        DocumentType.GST_FILING: "GST return filing document showing input tax credit, output tax, GSTIN details, tax periods, and GST compliance status",
        DocumentType.BANK_STATEMENT: "Bank account statement showing transaction history with debits, credits, running balance, and transaction narrations",
        DocumentType.ITR: "Income Tax Return filing document showing total income, deductions, tax payable, assessment year, and PAN details",
        DocumentType.BOARD_MINUTES: "Minutes of board meeting showing resolutions passed, quorum details, agenda items, and decisions made by directors",
        DocumentType.RATING_REPORT: "Credit rating report from rating agency showing current rating, rating outlook, rating rationale, and key rating drivers",
        DocumentType.SANCTION_LETTER: "Loan sanction letter stating sanctioned facility amount, interest rate, repayment terms, security requirements, and covenants"
    }


# =====================================================================
# UTILITY FUNCTIONS
# =====================================================================

class LocalFileHandler:
    """Handle local file operations and storage"""
    
    def __init__(self, storage_root: str = './data/uploads'):
        self.storage_root = Path(storage_root)
        self.storage_root.mkdir(parents=True, exist_ok=True)
        logger.info(f"LocalFileHandler initialized with storage root: {self.storage_root.absolute()}")
    
    def detect_file_type(self, filepath: str) -> FileType:
        """Detect file type from extension and MIME type"""
        try:
            ext = Path(filepath).suffix.lower().lstrip('.')
            
            # Map extensions to FileType enum
            ext_mapping = {
                'pdf': FileType.PDF,
                'xlsx': FileType.XLSX,
                'xls': FileType.XLS,
                'csv': FileType.CSV,
                'jpg': FileType.JPG,
                'jpeg': FileType.JPG,
                'png': FileType.PNG,
                'docx': FileType.DOCX,
                'doc': FileType.DOC
            }
            
            if ext in ext_mapping:
                logger.debug(f"File type detected from extension: {ext}")
                return ext_mapping[ext]
            
            # Fallback to MIME type detection
            mime_type, _ = mimetypes.guess_type(filepath)
            logger.debug(f"MIME type detected: {mime_type}")
            
            if mime_type:
                if 'pdf' in mime_type:
                    return FileType.PDF
                elif 'spreadsheet' in mime_type or 'excel' in mime_type:
                    return FileType.XLSX if 'openxml' in mime_type else FileType.XLS
                elif 'csv' in mime_type:
                    return FileType.CSV
                elif 'image' in mime_type:
                    if 'jpeg' in mime_type or 'jpg' in mime_type:
                        return FileType.JPG
                    elif 'png' in mime_type:
                        return FileType.PNG
                elif 'word' in mime_type:
                    return FileType.DOCX if 'openxml' in mime_type else FileType.DOC
            
            raise ValueError(f"Unsupported file type: {ext}")
            
        except Exception as e:
            logger.error(f"Error detecting file type for {filepath}: {str(e)}")
            raise
    
    def copy_to_storage(self, source_path: str, case_id: str, document_id: str) -> str:
        """Copy file to local storage and return storage path"""
        try:
            # Create case directory
            case_dir = self.storage_root / case_id
            case_dir.mkdir(parents=True, exist_ok=True)
            
            # Build destination path
            file_ext = Path(source_path).suffix
            dest_filename = f"{document_id}{file_ext}"
            dest_path = case_dir / dest_filename
            
            logger.info(f"Copying {source_path} to {dest_path}")
            
            # Copy file
            shutil.copy2(source_path, dest_path)
            
            # Return absolute path
            storage_path = str(dest_path.absolute())
            logger.info(f"Successfully copied to {storage_path}")
            
            return storage_path
            
        except Exception as e:
            logger.error(f"File copy failed: {str(e)}")
            raise
    
    def get_file_path(self, storage_path: str) -> Path:
        """Get Path object from storage path string"""
        return Path(storage_path)
    
    def file_exists(self, storage_path: str) -> bool:
        """Check if file exists in storage"""
        return Path(storage_path).exists()




# =====================================================================
# ENHANCED TEXT EXTRACTOR
# =====================================================================

class EnhancedTextExtractor:
    """Enhanced text extraction with better image preprocessing"""
    
    @staticmethod
    def extract_from_pdf(filepath: str, max_pages: int = 5) -> str:
        """Extract text from PDF with better fallback"""
        try:
            logger.info(f"Extracting text from PDF: {filepath}")
            text = ""
            
            # Try digital PDF first
            try:
                with pdfplumber.open(filepath) as pdf:
                    pages_to_read = min(max_pages, len(pdf.pages))
                    logger.debug(f"PDF has {len(pdf.pages)} pages, reading {pages_to_read}")
                    
                    for i in range(pages_to_read):
                        page = pdf.pages[i]
                        page_text = page.extract_text()
                        
                        if page_text:
                            text += page_text + "\n"
                    
                    if len(text.strip()) > 100:
                        logger.debug(f"✓ Digital extraction: {len(text)} chars")
                        return text
            except Exception as e:
                logger.warning(f"Digital extraction failed: {e}")
            
            # Fallback to OCR
            try:
                logger.info("Attempting OCR extraction with preprocessing")
                images = convert_from_path(filepath, first_page=1, last_page=max_pages, dpi=300)
                
                for i, image in enumerate(images):
                    # Preprocess image for better OCR
                    processed = EnhancedTextExtractor._preprocess_image(image)
                    page_text = pytesseract.image_to_string(processed)
                    text += page_text + "\n"
                    logger.debug(f"Page {i+1} OCR: {len(page_text)} chars")
                
                logger.debug(f"✓ OCR extraction: {len(text)} chars")
                return text
            except Exception as e:
                logger.error(f"OCR failed: {e}")
                return ""
            
        except Exception as e:
            logger.error(f"PDF extraction error: {e}", exc_info=True)
            return ""
    
    @staticmethod
    def extract_from_image(filepath: str) -> str:
        """Extract text from image with preprocessing"""
        try:
            logger.info(f"Extracting text from image: {filepath}")
            
            # Load image
            image = Image.open(filepath)
            
            # Preprocess
            processed = EnhancedTextExtractor._preprocess_image(image)
            
            # OCR with config
            custom_config = r'--oem 3 --psm 6'
            text = pytesseract.image_to_string(processed, config=custom_config)
            
            logger.debug(f"✓ Image OCR: {len(text)} chars")
            logger.debug(f"First 200 chars: {text[:200]}")
            
            return text
            
        except Exception as e:
            logger.error(f"Image extraction error: {e}", exc_info=True)
            return ""
    
    @staticmethod
    def _preprocess_image(pil_image: Image.Image) -> Image.Image:
        """Preprocess image for better OCR accuracy"""
        try:
            # Convert PIL to OpenCV format
            img = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
            
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Increase contrast using CLAHE
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(gray)
            
            # Denoise
            denoised = cv2.fastNlMeansDenoising(enhanced, None, h=10)
            
            # Adaptive thresholding
            thresh = cv2.adaptiveThreshold(
                denoised, 255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                blockSize=11,
                C=2
            )
            
            # Convert back to PIL
            return Image.fromarray(thresh)
            
        except Exception as e:
            logger.warning(f"Image preprocessing failed: {e}, using original")
            return pil_image
    
    @staticmethod
    def extract_from_excel(filepath: str) -> Tuple[str, pd.DataFrame]:
        """Extract from Excel"""
        try:
            logger.info(f"Reading Excel: {filepath}")
            excel_file = pd.ExcelFile(filepath)
            all_text = ""
            combined_df = pd.DataFrame()
            
            for sheet in excel_file.sheet_names[:3]:
                df = pd.read_excel(filepath, sheet_name=sheet)
                all_text += f"\n=== {sheet} ===\n{df.to_string()}\n"
                if combined_df.empty:
                    combined_df = df
            
            logger.debug(f"✓ Excel: {len(all_text)} chars")
            return all_text, combined_df
        except Exception as e:
            logger.error(f"Excel error: {e}")
            return "", pd.DataFrame()
    
    @staticmethod
    def extract_from_csv(filepath: str) -> Tuple[str, pd.DataFrame]:
        """Extract from CSV"""
        try:
            logger.info(f"Reading CSV: {filepath}")
            for enc in ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']:
                try:
                    df = pd.read_csv(filepath, encoding=enc)
                    text = df.to_string()
                    logger.debug(f"✓ CSV ({enc}): {len(text)} chars")
                    return text, df
                except:
                    continue
            return "", pd.DataFrame()
        except Exception as e:
            logger.error(f"CSV error: {e}")
            return "", pd.DataFrame()
    
    @staticmethod
    def extract_from_docx(filepath: str) -> str:
        """Extract from DOCX"""
        try:
            logger.info(f"Reading DOCX: {filepath}")
            doc = docx.Document(filepath)
            text = "\n".join([p.text for p in doc.paragraphs])
            logger.debug(f"✓ DOCX: {len(text)} chars")
            return text
        except Exception as e:
            logger.error(f"DOCX error: {e}")
            return ""



# =====================================================================
# SEMANTIC CLASSIFIER
# =====================================================================

class SemanticClassifier:
    """Semantic document classifier using sentence transformers"""
    
    def __init__(self):
        try:
            logger.info("Loading semantic model (sentence-transformers)...")
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
            
            # Pre-compute embeddings for document type descriptions
            self.type_descriptions = ClassificationPatterns.SEMANTIC_DESCRIPTIONS
            self.type_embeddings = {}
            
            for doc_type, description in self.type_descriptions.items():
                embedding = self.model.encode([description])[0]
                self.type_embeddings[doc_type] = embedding
            
            logger.info("✓ Semantic model loaded")
            
        except Exception as e:
            logger.error(f"Semantic model load failed: {e}")
            self.model = None
    
    def classify(self, text: str) -> Tuple[DocumentType, float]:
        """Classify text using semantic similarity"""
        try:
            if not self.model or not text or len(text) < 50:
                return DocumentType.UNKNOWN, 0.0
            
            # Take first 1000 chars for embedding (performance optimization)
            text_sample = text[:1000]
            
            # Encode document text
            text_embedding = self.model.encode([text_sample])[0]
            
            # Calculate similarity with each document type
            similarities = {}
            for doc_type, type_embedding in self.type_embeddings.items():
                similarity = cosine_similarity(
                    [text_embedding],
                    [type_embedding]
                )[0][0]
                similarities[doc_type] = similarity
            
            # Get best match
            best_type = max(similarities.items(), key=lambda x: x[1])
            
            # Convert similarity to confidence (0-1 scale)
            confidence = min(0.95, best_type[1])
            
            logger.debug(f"Semantic: {best_type[0].value} ({confidence:.2%})")
            return best_type[0], confidence
            
        except Exception as e:
            logger.error(f"Semantic classification error: {e}")
            return DocumentType.UNKNOWN, 0.0


# =====================================================================
# ENHANCED DOCUMENT CLASSIFIER
# =====================================================================

class EnhancedDocumentClassifier:
    """Multi-method classifier with semantic analysis"""
    
    def __init__(self):
        self.patterns = ClassificationPatterns()
        self.text_extractor = EnhancedTextExtractor()
        self.semantic_classifier = SemanticClassifier()
        logger.info("EnhancedDocumentClassifier initialized")
    
    def classify(
        self,
        filepath: str,
        file_type: FileType
    ) -> Tuple[DocumentType, float, ClassificationMethod, Dict[str, Any]]:
        """Enhanced multi-method classification"""
        try:
            logger.info(f"=== CLASSIFYING: {Path(filepath).name} ===")
            
            filename = Path(filepath).name
            metadata = {
                'filename': filename,
                'file_type': file_type.value,
                'methods_attempted': [],
                'all_scores': {}
            }
            
            all_results = []
            
            # Method 1: Filename (low priority now)
            doc_type_fn, conf_fn = self._classify_by_filename(filename)
            all_results.append(('filename', doc_type_fn, conf_fn * 0.5))  # Reduced weight
            metadata['methods_attempted'].append({
                'method': 'filename',
                'result': doc_type_fn.value,
                'confidence': conf_fn,
                'weighted': conf_fn * 0.5
            })
            
            # Method 2: Content-based (HIGH priority)
            if file_type in [FileType.PDF, FileType.DOCX]:
                doc_type_content, conf_content = self._classify_by_content(filepath, file_type)
                all_results.append(('content', doc_type_content, conf_content * 1.5))  # Increased weight
                metadata['methods_attempted'].append({
                    'method': 'content',
                    'result': doc_type_content.value,
                    'confidence': conf_content,
                    'weighted': conf_content * 1.5
                })
            
            # Method 3: Structure (for spreadsheets)
            elif file_type in [FileType.XLSX, FileType.XLS, FileType.CSV]:
                doc_type_struct, conf_struct = self._classify_by_structure(filepath, file_type)
                all_results.append(('structure', doc_type_struct, conf_struct * 1.2))
                metadata['methods_attempted'].append({
                    'method': 'structure',
                    'result': doc_type_struct.value,
                    'confidence': conf_struct,
                    'weighted': conf_struct * 1.2
                })
            
            # Method 4: Image OCR
            elif file_type in [FileType.JPG, FileType.PNG]:
                doc_type_img, conf_img = self._classify_image(filepath)
                all_results.append(('image_ocr', doc_type_img, conf_img * 1.3))
                metadata['methods_attempted'].append({
                    'method': 'image_ocr',
                    'result': doc_type_img.value,
                    'confidence': conf_img,
                    'weighted': conf_img * 1.3
                })
            
            # Method 5: Semantic classification (if available)
            if self.semantic_classifier.model:
                # Extract text first
                text = self._extract_text_for_semantic(filepath, file_type)
                if text:
                    doc_type_sem, conf_sem = self.semantic_classifier.classify(text)
                    all_results.append(('semantic', doc_type_sem, conf_sem * 1.4))  # Highest weight
                    metadata['methods_attempted'].append({
                        'method': 'semantic',
                        'result': doc_type_sem.value,
                        'confidence': conf_sem,
                        'weighted': conf_sem * 1.4
                    })
            
            # Aggregate results
            final_type, final_conf, final_method = self._aggregate_results(all_results, metadata)
            
            logger.info(f"✓ FINAL: {final_type.value} ({final_conf:.1%}) via {final_method}")
            return final_type, final_conf, final_method, metadata
            
        except Exception as e:
            logger.error(f"Classification failed: {e}", exc_info=True)
            return DocumentType.UNKNOWN, 0.0, ClassificationMethod.FILENAME, {'error': str(e)}
    
    def _aggregate_results(
        self,
        results: List[Tuple[str, DocumentType, float]],
        metadata: Dict
    ) -> Tuple[DocumentType, float, ClassificationMethod]:
        """Aggregate multiple classification results"""
        
        # Remove Unknown results
        valid_results = [(m, dt, c) for m, dt, c in results if dt != DocumentType.UNKNOWN]
        
        if not valid_results:
            return DocumentType.UNKNOWN, 0.0, ClassificationMethod.FILENAME
        
        # Score by document type
        type_scores = {}
        for method, doc_type, weighted_conf in valid_results:
            if doc_type not in type_scores:
                type_scores[doc_type] = []
            type_scores[doc_type].append((method, weighted_conf))
        
        # Calculate aggregate score for each type
        type_final_scores = {}
        for doc_type, scores in type_scores.items():
            # Average of all weighted scores for this type
            avg_score = sum(s for _, s in scores) / len(scores)
            # Bonus if multiple methods agree
            agreement_bonus = 0.1 if len(scores) > 1 else 0
            type_final_scores[doc_type] = avg_score + agreement_bonus
        
        # Get best
        best_type = max(type_final_scores.items(), key=lambda x: x[1])
        best_doc_type = best_type[0]
        confidence = min(0.95, best_type[1])
        
        # Determine primary method
        methods_for_best = [m for m, dt, _ in valid_results if dt == best_doc_type]
        if 'semantic' in methods_for_best:
            method = ClassificationMethod.SEMANTIC
        elif 'content' in methods_for_best:
            method = ClassificationMethod.CONTENT
        elif 'structure' in methods_for_best:
            method = ClassificationMethod.STRUCTURE
        elif len(methods_for_best) > 1:
            method = ClassificationMethod.HYBRID
        else:
            method = ClassificationMethod.FILENAME
        
        metadata['all_scores'] = {dt.value: sc for dt, sc in type_final_scores.items()}
        
        return best_doc_type, confidence, method
    
    def _classify_by_filename(self, filename: str) -> Tuple[DocumentType, float]:
        """Minimal filename classification"""
        filename_lower = filename.lower()
        
        for doc_type, keywords in self.patterns.FILENAME_PATTERNS.items():
            for keyword in keywords:
                if keyword in filename_lower:
                    return doc_type, 0.85
        
        return DocumentType.UNKNOWN, 0.0
    
    def _classify_by_content(self, filepath: str, file_type: FileType) -> Tuple[DocumentType, float]:
        """Enhanced content classification with weighted scoring"""
        try:
            # Extract text
            if file_type == FileType.PDF:
                text = self.text_extractor.extract_from_pdf(filepath, max_pages=5)
            elif file_type == FileType.DOCX:
                text = self.text_extractor.extract_from_docx(filepath)
            else:
                text = ""
            
            if not text or len(text) < 100:
                logger.warning(f"Insufficient text: {len(text)} chars")
                return DocumentType.UNKNOWN, 0.0
            
            text_lower = text.lower()
            logger.debug(f"Content text: {len(text)} chars")
            
            # Weighted scoring
            scores = {}
            for doc_type, signatures in self.patterns.CONTENT_SIGNATURES.items():
                strong_matches = sum(1 for sig in signatures['strong'] if sig in text_lower)
                medium_matches = sum(1 for sig in signatures['medium'] if sig in text_lower)
                weak_matches = sum(1 for sig in signatures['weak'] if sig in text_lower)
                
                # Weighted score
                score = (strong_matches * 3) + (medium_matches * 2) + (weak_matches * 0.5)
                
                if score > 0:
                    # Convert to confidence
                    confidence = min(0.95, 0.30 + (score * 0.08))
                    scores[doc_type] = (score, confidence)
                    logger.debug(f"{doc_type.value}: score={score}, conf={confidence:.2%}")
            
            if not scores:
                return DocumentType.UNKNOWN, 0.0
            
            best = max(scores.items(), key=lambda x: x[1][0])
            return best[0], best[1][1]
            
        except Exception as e:
            logger.error(f"Content classification error: {e}")
            return DocumentType.UNKNOWN, 0.0
    
    def _classify_by_structure(self, filepath: str, file_type: FileType) -> Tuple[DocumentType, float]:
        """Structure classification for spreadsheets"""
        try:
            if file_type in [FileType.XLSX, FileType.XLS]:
                _, df = self.text_extractor.extract_from_excel(filepath)
            elif file_type == FileType.CSV:
                _, df = self.text_extractor.extract_from_csv(filepath)
            else:
                return DocumentType.UNKNOWN, 0.0
            
            if df.empty:
                return DocumentType.UNKNOWN, 0.0
            
            columns_lower = [str(col).lower() for col in df.columns]
            logger.debug(f"Columns: {columns_lower}")
            
            scores = {}
            for doc_type, signature_sets in self.patterns.STRUCTURE_SIGNATURES.items():
                matches = 0
                for sig_set in signature_sets:
                    if all(any(kw in col for col in columns_lower) for kw in sig_set):
                        matches += 1
                
                if matches > 0:
                    confidence = min(0.95, 0.60 + (matches * 0.15))
                    scores[doc_type] = (matches, confidence)
            
            if not scores:
                return DocumentType.UNKNOWN, 0.0
            
            best = max(scores.items(), key=lambda x: x[1][0])
            return best[0], best[1][1]
            
        except Exception as e:
            logger.error(f"Structure error: {e}")
            return DocumentType.UNKNOWN, 0.0
    
    def _classify_image(self, filepath: str) -> Tuple[DocumentType, float]:
        """Image classification with enhanced OCR"""
        try:
            text = self.text_extractor.extract_from_image(filepath)
            
            if not text or len(text) < 50:
                logger.warning(f"Image OCR produced {len(text)} chars")
                return DocumentType.UNKNOWN, 0.0
            
            # Use content classification on extracted text
            text_lower = text.lower()
            
            scores = {}
            for doc_type, signatures in self.patterns.CONTENT_SIGNATURES.items():
                strong = sum(1 for s in signatures['strong'] if s in text_lower)
                medium = sum(1 for s in signatures['medium'] if s in text_lower)
                
                score = (strong * 3) + (medium * 2)
                if score > 0:
                    # Lower confidence for images
                    confidence = min(0.85, 0.25 + (score * 0.08))
                    scores[doc_type] = (score, confidence)
            
            if not scores:
                return DocumentType.UNKNOWN, 0.0
            
            best = max(scores.items(), key=lambda x: x[1][0])
            return best[0], best[1][1]
            
        except Exception as e:
            logger.error(f"Image classification error: {e}")
            return DocumentType.UNKNOWN, 0.0
    
    def _extract_text_for_semantic(self, filepath: str, file_type: FileType) -> str:
        """Extract text for semantic classification"""
        try:
            if file_type == FileType.PDF:
                return self.text_extractor.extract_from_pdf(filepath, max_pages=3)
            elif file_type == FileType.DOCX:
                return self.text_extractor.extract_from_docx(filepath)
            elif file_type in [FileType.XLSX, FileType.XLS]:
                text, _ = self.text_extractor.extract_from_excel(filepath)
                return text
            elif file_type == FileType.CSV:
                text, _ = self.text_extractor.extract_from_csv(filepath)
                return text
            elif file_type in [FileType.JPG, FileType.PNG]:
                return self.text_extractor.extract_from_image(filepath)
            return ""
        except:
            return ""


# =====================================================================
# MAIN UPLOAD & CLASSIFICATION PIPELINE
# =====================================================================

class DocumentUploadPipeline:
    """Complete pipeline for document upload and classification"""
    
    def __init__(self, db_session: Session, storage_root: str = './data/uploads'):
        self.session = db_session
        self.file_handler = LocalFileHandler(storage_root)
        self.classifier = EnhancedDocumentClassifier()
        logger.info("DocumentUploadPipeline initialized (LOCAL MODE)")
    
    def process_upload(
        self,
        filepath: str,
        case_id: str,
        user_email: Optional[str] = None
    ) -> DocumentClassification:
        """
        Complete upload and classification pipeline
        
        Args:
            filepath: Path to uploaded file
            case_id: Case ID to associate document with
            user_email: Email of user uploading (for audit)
        
        Returns:
            DocumentClassification database record
        """
        document_id = str(uuid.uuid4())
        error_log = []
        
        try:
            logger.info("=" * 80)
            logger.info(f"PROCESSING UPLOAD FOR CASE: {case_id}")
            logger.info(f"File: {filepath}")
            logger.info(f"Document ID: {document_id}")
            logger.info("=" * 80)
            
            # Step 1: Validate file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")
            
            # Step 2: Get file metadata
            filename = Path(filepath).name
            file_size = os.path.getsize(filepath)
            
            logger.info(f"[OK] File validated: {filename} ({file_size:,} bytes)")
            
            # Step 3: Detect file type
            try:
                file_type = self.file_handler.detect_file_type(filepath)
                logger.info(f"[OK] File type detected: {file_type.value}")
            except Exception as e:
                error_msg = f"File type detection failed: {str(e)}"
                logger.error(f"✗ {error_msg}")
                error_log.append(error_msg)
                raise
            
            # Step 4: Copy to local storage
            try:
                storage_path = self.file_handler.copy_to_storage(filepath, case_id, document_id)
                logger.info(f"[OK] File stored at: {storage_path}")
            except Exception as e:
                error_msg = f"File storage failed: {str(e)}"
                logger.error(f"✗ {error_msg}")
                error_log.append(error_msg)
                raise
            
            # Step 5: Classify document
            try:
                logger.info("Starting classification...")
                doc_type, confidence, method, metadata = self.classifier.classify(filepath, file_type)
                logger.info(f"[OK] Classification complete:")
                logger.info(f"  - Type: {doc_type.value}")
                logger.info(f"  - Confidence: {confidence:.1%}")
                logger.info(f"  - Method: {method.value}")
            except Exception as e:
                error_msg = f"Classification failed: {str(e)}"
                logger.error(f"✗ {error_msg}")
                error_log.append(error_msg)
                # Set to Unknown on classification failure
                doc_type = DocumentType.UNKNOWN
                confidence = 0.0
                method = ClassificationMethod.FILENAME
                metadata = {'error': str(e)}
            
            # Step 6: Create database record
            try:
                doc_record = DocumentClassification(
                    document_id=document_id,
                    case_id=case_id,
                    original_filename=filename,
                    file_size_bytes=file_size,
                    file_type=file_type,
                    upload_timestamp=datetime.utcnow(),
                    
                    auto_classified_type=doc_type,
                    classification_method=method,
                    classification_confidence=confidence,
                    
                    validation_status=ValidationStatus.PENDING,
                    file_storage_path=storage_path,
                    
                    classification_metadata=str(metadata),
                    error_log='\n'.join(error_log) if error_log else None
                )
                
                self.session.add(doc_record)
                self.session.commit()
                
                logger.info(f"[OK] Database record created")
                logger.info(f"[OK] Status: {ValidationStatus.PENDING.value} - AWAITING HUMAN VALIDATION")
                logger.info("=" * 80 + "\n")
                
                return doc_record
                
            except Exception as e:
                self.session.rollback()
                error_msg = f"Database error: {str(e)}"
                logger.error(f"✗ {error_msg}")
                raise
            
        except Exception as e:
            logger.error(f"✗ PIPELINE FAILED: {str(e)}", exc_info=True)
            logger.info("=" * 80 + "\n")
            
            # Attempt to create error record
            try:
                error_record = DocumentClassification(
                    document_id=document_id,
                    case_id=case_id,
                    original_filename=Path(filepath).name if os.path.exists(filepath) else "unknown",
                    file_size_bytes=os.path.getsize(filepath) if os.path.exists(filepath) else 0,
                    file_type=FileType.PDF,  # Default
                    
                    auto_classified_type=DocumentType.UNKNOWN,
                    classification_method=ClassificationMethod.FILENAME,
                    classification_confidence=0.0,
                    
                    validation_status=ValidationStatus.PENDING,
                    file_storage_path="",
                    
                    error_log=str(e)
                )
                
                self.session.add(error_record)
                self.session.commit()
                
                return error_record
            except:
                logger.error("Could not create error record in database")
                raise
    
    def batch_process(self, file_paths: List[str], case_id: str) -> List[DocumentClassification]:
        """Process multiple files in batch"""
        results = []
        
        logger.info("\n" + "=" * 80)
        logger.info(f"BATCH PROCESSING {len(file_paths)} FILES FOR CASE {case_id}")
        logger.info("=" * 80 + "\n")
        
        for i, filepath in enumerate(file_paths, 1):
            logger.info(f">>> Processing file {i}/{len(file_paths)}")
            
            try:
                result = self.process_upload(filepath, case_id)
                results.append(result)
            except Exception as e:
                logger.error(f"Batch processing failed for {filepath}: {str(e)}")
                # Continue with next file
                continue
        
        logger.info("\n" + "=" * 80)
        logger.info(f"BATCH COMPLETE: {len(results)}/{len(file_paths)} SUCCESSFUL")
        logger.info("=" * 80 + "\n")
        
        return results
    
    def get_case_status(self, case_id: str) -> Dict[str, Any]:
        """Get validation status for all documents in a case"""
        try:
            documents = self.session.query(DocumentClassification).filter_by(case_id=case_id).all()
            
            total = len(documents)
            pending = sum(1 for d in documents if d.validation_status == ValidationStatus.PENDING)
            approved = sum(1 for d in documents if d.validation_status == ValidationStatus.APPROVED)
            denied = sum(1 for d in documents if d.validation_status == ValidationStatus.DENIED)
            edited = sum(1 for d in documents if d.validation_status == ValidationStatus.EDITED)
            unknown = sum(1 for d in documents if d.auto_classified_type == DocumentType.UNKNOWN)
            
            can_proceed = pending == 0 and unknown == 0 and total > 0
            
            status = {
                'case_id': case_id,
                'total_documents': total,
                'pending_validation': pending,
                'approved': approved,
                'denied': denied,
                'edited': edited,
                'unknown_type': unknown,
                'can_proceed_to_extraction': can_proceed,
                'message': self._get_status_message(pending, unknown, total)
            }
            
            logger.info(f"Case {case_id} status: {status['message']}")
            return status
            
        except Exception as e:
            logger.error(f"Error getting case status: {str(e)}")
            raise
    
    def _get_status_message(self, pending: int, unknown: int, total: int) -> str:
        """Generate human-readable status message"""
        if total == 0:
            return "No documents uploaded"
        elif unknown > 0:
            return f"{unknown} document(s) could not be classified - manual classification required"
        elif pending > 0:
            return f"{pending} document(s) awaiting validation"
        else:
            return "✅ All documents validated - ready for extraction"


# =====================================================================
# VALIDATION API (for human-in-the-loop)
# =====================================================================

class ValidationAPI:
    """API for human validation of classifications"""
    
    def __init__(self, db_session: Session):
        self.session = db_session
        logger.info("ValidationAPI initialized")
    
    def approve_classification(self, document_id: str, user_email: str) -> bool:
        """Approve auto-classification"""
        try:
            doc = self.session.query(DocumentClassification).filter_by(document_id=document_id).first()
            
            if not doc:
                raise ValueError(f"Document not found: {document_id}")
            
            doc.validation_status = ValidationStatus.APPROVED
            doc.human_validated_type = doc.auto_classified_type
            doc.validated_by = user_email
            doc.validation_timestamp = datetime.utcnow()
            
            self.session.commit()
            
            logger.info(f"[OK] Document {document_id} APPROVED by {user_email}")
            logger.info(f"  Type: {doc.auto_classified_type.value}")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Approval failed: {str(e)}")
            raise
    
    def deny_classification(
        self,
        document_id: str,
        correct_type: DocumentType,
        reason: str,
        user_email: str
    ) -> bool:
        """Deny auto-classification and set correct type"""
        try:
            doc = self.session.query(DocumentClassification).filter_by(document_id=document_id).first()
            
            if not doc:
                raise ValueError(f"Document not found: {document_id}")
            
            doc.validation_status = ValidationStatus.DENIED
            doc.human_validated_type = correct_type
            doc.denial_reason = reason
            doc.validated_by = user_email
            doc.validation_timestamp = datetime.utcnow()
            
            self.session.commit()
            
            logger.info(f"[OK] Document {document_id} DENIED by {user_email}")
            logger.info(f"  Auto: {doc.auto_classified_type.value} → Correct: {correct_type.value}")
            logger.info(f"  Reason: {reason}")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Denial failed: {str(e)}")
            raise
    
    def edit_classification(
        self,
        document_id: str,
        new_type: DocumentType,
        user_email: str
    ) -> bool:
        """Edit classification (minor correction)"""
        try:
            doc = self.session.query(DocumentClassification).filter_by(document_id=document_id).first()
            
            if not doc:
                raise ValueError(f"Document not found: {document_id}")
            
            doc.validation_status = ValidationStatus.EDITED
            doc.human_validated_type = new_type
            doc.validated_by = user_email
            doc.validation_timestamp = datetime.utcnow()
            
            self.session.commit()
            
            logger.info(f"[OK] Document {document_id} EDITED by {user_email}")
            logger.info(f"  {doc.auto_classified_type.value} → {new_type.value}")
            return True
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"✗ Edit failed: {str(e)}")
            raise
    
    def get_pending_documents(self, case_id: str) -> List[Dict[str, Any]]:
        """Get all pending documents for a case"""
        try:
            docs = self.session.query(DocumentClassification).filter_by(
                case_id=case_id,
                validation_status=ValidationStatus.PENDING
            ).all()
            
            pending_list = []
            for doc in docs:
                pending_list.append({
                    'document_id': doc.document_id,
                    'filename': doc.original_filename,
                    'auto_classified_type': doc.auto_classified_type.value,
                    'confidence': doc.classification_confidence,
                    'method': doc.classification_method.value,
                    'file_type': doc.file_type.value,
                    'upload_timestamp': doc.upload_timestamp.isoformat()
                })
            
            return pending_list
            
        except Exception as e:
            logger.error(f"Error getting pending documents: {str(e)}")
            raise


# =====================================================================
# EXAMPLE USAGE & TESTING
# =====================================================================

def print_section(title: str):
    """Print formatted section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def reset_environment():
    import os
    import shutil

    print("🔄 Resetting environment (database + uploads)")

    if os.path.exists("intelli_credit.db"):
        os.remove("intelli_credit.db")

    if os.path.exists("./data/uploads"):
        shutil.rmtree("./data/uploads")

    os.makedirs("./data/uploads", exist_ok=True)


def main():
    reset_environment()
    """Example usage of the document upload and classification system"""
    
    # Initialize database
    Base.metadata.create_all(engine)
    session = Session()
    
    # Initialize pipeline
    pipeline = DocumentUploadPipeline(session, storage_root='./data/uploads')
    validation_api = ValidationAPI(session)
    
    print_section("INTELLI-CREDIT DOCUMENT CLASSIFICATION SYSTEM (LOCAL MODE)")
    print("All files stored locally in ./data/uploads/")
    print("Database: SQLite (intelli_credit.db)")
    
    # Example 1: Single file upload
    print_section("EXAMPLE 1: Single File Upload")
    
    test_dir = "./test_data"

    available_files = []

    if os.path.exists(test_dir):
        for f in os.listdir(test_dir):
            full_path = os.path.join(test_dir, f)
            if os.path.isfile(full_path):
                available_files.append(full_path)
    
    if not available_files:
        print("⚠ No test files found. Creating sample structure...")
        print("\nTo test the system, place documents in ./test_data/ with names like:")
        for name, path in test_files.items():
            print(f"  - {path}")
        
        # Create test directory
        os.makedirs('./test_data', exist_ok=True)
        
    else:
        print(f"Found {len(available_files)} test files:")
        for path in available_files:
            print(f"  - {path}")

        first_file = available_files[0]
        
        try:
            result = pipeline.process_upload(
                filepath=first_file,
                case_id="CAM_0001",
                user_email="analyst@lender.com"
            )
            
            print("\n✅ UPLOAD SUCCESSFUL!")
            print(f"  Document ID: {result.document_id}")
            print(f"  Classified as: {result.auto_classified_type.value}")
            print(f"  Confidence: {result.classification_confidence:.1%}")
            print(f"  Method: {result.classification_method.value}")
            print(f"  Status: {result.validation_status.value}")
            print(f"  Storage: {result.file_storage_path}")
            
        except Exception as e:
            print(f"\n❌ Upload failed: {str(e)}")
    
    # Example 2: Batch upload
    if len(available_files) > 1:
        print_section("EXAMPLE 2: Batch Upload")
        
        files_to_batch = available_files
        results = pipeline.batch_process(files_to_batch, case_id="CAM_0001")
        
        print(f"\n✅ BATCH PROCESSED {len(results)} FILES:")
        for r in results:
            print(f"  - {r.original_filename}")
            print(f"    Type: {r.auto_classified_type.value} ({r.classification_confidence:.0%})")
            print(f"    Status: {r.validation_status.value}")
    
    # Example 3: Check case status
    print_section("EXAMPLE 3: Case Status Check")
    
    status = pipeline.get_case_status("CAM_0001")
    
    print(f"Case ID: {status['case_id']}")
    print(f"Message: {status['message']}")
    print(f"\nDocument counts:")
    print(f"  Total: {status['total_documents']}")
    print(f"  Pending validation: {status['pending_validation']}")
    print(f"  Approved: {status['approved']}")
    print(f"  Denied: {status['denied']}")
    print(f"  Edited: {status['edited']}")
    print(f"  Unknown type: {status['unknown_type']}")
    print(f"\nCan proceed to extraction: {'✅ YES' if status['can_proceed_to_extraction'] else '❌ NO'}")
    
    # Example 4: Human validation
    print_section("EXAMPLE 4: Human Validation Workflow")
    
    pending_docs = validation_api.get_pending_documents("CAM_0001")
    
    if pending_docs:
        print(f"Found {len(pending_docs)} pending documents:\n")
        
        for i, doc in enumerate(pending_docs, 1):
            print(f"{i}. {doc['filename']}")
            print(f"   Auto-classified: {doc['auto_classified_type']} ({doc['confidence']:.0%})")
            print(f"   Document ID: {doc['document_id']}")
        
        # Approve first document as example
        first_doc = pending_docs[0]
        doc_id = first_doc['document_id']
        
        print(f"\n>>> Approving document: {first_doc['filename']}")
        validation_api.approve_classification(doc_id, user_email="analyst@lender.com")
        print("✅ Approved!")
        
    else:
        print("No pending documents to validate.")
    
    # Final status check
    print_section("FINAL STATUS")
    
    final_status = pipeline.get_case_status("CAM_0001")
    print(final_status['message'])
    
    session.close()
    
    print("\n" + "=" * 80)
    print("  SYSTEM TEST COMPLETE")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()