DATA_DIR = "."

RAW_REPORT_DIR = f"{DATA_DIR}/raw_reports"
PAGE_TEXT_DIR = f"{DATA_DIR}/processed_pages"
TABLE_DIR = f"{DATA_DIR}/tables"
FEATURE_DIR = f"{DATA_DIR}/features"
STRUCTURED_DIR = "../structured"
METADATA_DIR = f"{DATA_DIR}/metadata"

BSE_SCRIP_API = "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
BSE_ANN_API = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.bseindia.com/"
}

MIN_PAGE_COUNT = 50
MAX_PAGE_COUNT = 400

COVERAGE_THRESHOLD = 0.7