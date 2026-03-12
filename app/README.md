# IntelliCredit API Application

Modular FastAPI application with separate endpoint modules for each feature area.

## 📁 Project Structure

```
app/
├── __init__.py              # Package initialization
├── app.py                   # Main application (run this!)
├── config.py                # Configuration settings
├── dependencies.py          # Shared dependencies (database, logging, etc.)
├── ingestor_endpoints.py    # Document ingestion endpoints
└── README.md               # This file

# Future endpoint modules (add as needed):
├── research_endpoints.py    # Research/search endpoints
├── analytics_endpoints.py   # Analytics/reporting endpoints
├── scoring_endpoints.py     # Credit scoring endpoints
├── portfolio_endpoints.py   # Portfolio management endpoints
└── ...
```

## 🚀 Quick Start

### Run the Server

```bash
# From project root
python app/app.py

# Or with uvicorn (recommended for development)
uvicorn app.app:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

### View API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 📡 Current Endpoints

### Ingestor Module (`/api/ingest/*`)
- `POST /api/ingest/upload` - Upload single document
- `POST /api/ingest/batch` - Batch upload documents
- `GET /api/ingest/status/{case_id}` - Get case status
- `PATCH /api/ingest/validate` - Validate classification
- `PATCH /api/ingest/schema/edit` - Edit schema fields
- `GET /api/ingest/findings/{case_id}` - Get key findings

### System Endpoints
- `GET /` - API information
- `GET /health` - Health check

## ➕ Adding New Endpoint Modules

### 1. Create New Endpoint File

Create a new file in the `app/` directory (e.g., `research_endpoints.py`):

```python
"""
research_endpoints.py
=====================
Research and document search endpoints.
"""

from fastapi import APIRouter
from app.dependencies import logger

router = APIRouter(
    prefix="/api/research",
    tags=["research"],
)

@router.get("/search")
async def search_documents(query: str):
    """Search documents by query"""
    logger.info(f"Search query: {query}")
    return {"results": []}
```

### 2. Register in app.py

Add to `app/app.py`:

```python
# Import the new router
from app.research_endpoints import router as research_router

# Register it
app.include_router(research_router)
```

That's it! Your new endpoints are now available at `/api/research/*`

## 🔧 Configuration

All configuration is centralized in `config.py`. Key settings:

### Environment Variables

```bash
# Database
export INGESTOR_DB_PATH="intelli_credit.db"

# Uploads
export UPLOAD_DIR="./UPLOADS"

# Logging
export LOG_LEVEL="INFO"
export LOG_FILE="intellicredit_api.log"

# Processing
export MAX_WORKERS="4"
export OCR_DPI="300"

# CORS (comma-separated)
export CORS_ORIGINS="http://localhost:3000,https://yourdomain.com"

# Features
export ENABLE_SEMANTIC="true"
export ENABLE_BACKGROUND_TASKS="false"
```

### Modifying Configuration

Edit `app/config.py`:

```python
# Change defaults
API_TIMEOUT = 120.0  # Increase timeout
MAX_WORKERS = 8      # More parallel workers
```

## 📦 Dependencies

Managed in `dependencies.py`:

- **Database**: SQLAlchemy session management
- **Thread Pool**: Parallel processing executor
- **Logging**: Centralized logger configuration
- **Startup/Shutdown**: Lifecycle handlers

### Using Dependencies in Endpoints

```python
from app.dependencies import get_db, executor, logger
from sqlalchemy.orm import Session
from fastapi import Depends

@router.get("/example")
async def example(db: Session = Depends(get_db)):
    # Use db session
    logger.info("Querying database...")
    return {"status": "ok"}
```

## 🧪 Testing

```bash
# Test API health
curl http://localhost:8000/health

# Test ingestor upload
curl -X POST http://localhost:8000/api/ingest/upload \
  -F "file=@test.pdf" \
  -F "case_id=TEST001"

# Run full test suite
python processing/test_ingestor_api.py
```

## 📊 Monitoring & Logging

All logs are written to:
- **Console**: Real-time output
- **File**: `intellicredit_api.log`

Log format:
```
2024-03-12 19:30:45 [INFO] app.ingestor_endpoints: [CLASSIFY] Document.pdf → ANNUAL_REPORT (92%)
```

## 🔐 Security Best Practices

### Production Checklist

- [ ] Set specific CORS origins (not `*`)
- [ ] Add authentication middleware
- [ ] Enable HTTPS
- [ ] Rate limit endpoints
- [ ] Validate file uploads (size, type)
- [ ] Use environment variables for secrets
- [ ] Enable request logging
- [ ] Add API key validation

### Example: Add Authentication

```python
# In dependencies.py
from fastapi import Header, HTTPException

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != os.environ.get("API_KEY"):
        raise HTTPException(status_code=403, detail="Invalid API key")
    return x_api_key

# In endpoints
@router.post("/protected")
async def protected_endpoint(api_key: str = Depends(verify_api_key)):
    return {"status": "authorized"}
```

## 🚢 Deployment

### Development
```bash
python app/app.py
```

### Production (with Gunicorn)
```bash
pip install gunicorn
gunicorn app.app:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### Docker
```dockerfile
FROM python:3.11

# Install system dependencies
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . /app

RUN pip install -r processing/requirements_api.txt

EXPOSE 8000

CMD ["python", "app/app.py"]
```

## 📚 Documentation

- **API Setup Guide**: `processing/API_SETUP_GUIDE.md`
- **Quick Reference**: `processing/QUICK_REFERENCE.md`
- **Frontend Integration**: `frontend/ingestor_api_client.js`

## 🎓 Development Workflow

1. **Add Feature**: Create new endpoint file in `app/`
2. **Register**: Import and `app.include_router()` in `app.py`
3. **Test**: Use `/docs` for interactive testing
4. **Log**: Use `logger` from `dependencies.py`
5. **Configure**: Add settings to `config.py` if needed
6. **Document**: Update this README with new endpoints

## 💡 Tips

- **Hot Reload**: Use `--reload` flag during development
- **Debug Mode**: Set `LOG_LEVEL=DEBUG` for verbose logging
- **Parallel Processing**: Use `executor` for CPU-bound tasks
- **Database**: Use `get_db()` dependency for sessions
- **Error Handling**: All exceptions are logged automatically
- **CORS**: Already configured for local development

## 🆘 Troubleshooting

### "Module not found" errors
```bash
# Make sure you're running from project root
cd C:\Projects\IntelliSense
python app/app.py
```

### Port already in use
```bash
# Change port in app.py or use environment variable
export API_PORT=8001
# Then update app.py to use os.environ.get("API_PORT", "8000")
```

### Database locked
```bash
# SQLite doesn't handle high concurrency - consider PostgreSQL for production
```

---

**Happy Coding!** 🚀

For questions or issues, check the logs in `intellicredit_api.log`
