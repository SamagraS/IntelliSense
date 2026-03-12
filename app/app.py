"""
app.py
======
IntelliCredit Platform API - Main Application

This is the main entry point for running the API server.
All endpoint modules are imported and registered here.

Run with:
    python app/app.py
    
Or with uvicorn:
    uvicorn app.app:app --reload --host 0.0.0.0 --port 8000
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import (
    API_TITLE,
    API_DESCRIPTION,
    API_VERSION,
    CORS_ORIGINS,
)
from app.dependencies import startup_handler, shutdown_handler, logger

# Import all endpoint routers
from app.ingestor_endpoints import router as ingestor_router

# Add more routers here as you create new endpoint modules
# Example:
# from app.research_endpoints import router as research_router
# from app.analytics_endpoints import router as analytics_router
# from app.scoring_endpoints import router as scoring_router

# ═══════════════════════════════════════════════════════════════════
# FastAPI App Creation
# ═══════════════════════════════════════════════════════════════════

app = FastAPI(
    title=API_TITLE,
    description=API_DESCRIPTION,
    version=API_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

# ═══════════════════════════════════════════════════════════════════
# CORS Middleware
# ═══════════════════════════════════════════════════════════════════

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════════
# Startup & Shutdown Events
# ═══════════════════════════════════════════════════════════════════

app.add_event_handler("startup", startup_handler)
app.add_event_handler("shutdown", shutdown_handler)

# ═══════════════════════════════════════════════════════════════════
# Include Routers
# ═══════════════════════════════════════════════════════════════════

# Ingestor endpoints
app.include_router(ingestor_router)

# Add more routers as you create them:
# app.include_router(research_router)
# app.include_router(analytics_router)
# app.include_router(scoring_router)

logger.info("All endpoint routers registered")

# ═══════════════════════════════════════════════════════════════════
# Root & Health Endpoints
# ═══════════════════════════════════════════════════════════════════


@app.get("/")
async def root():
    """Root endpoint - API information"""
    return {
        "name": API_TITLE,
        "version": API_VERSION,
        "status": "running",
        "docs": "/docs",
        "endpoints": {
            "ingestor": "/api/ingest/*",
            # Add more as you create them:
            # "research": "/api/research/*",
            # "analytics": "/api/analytics/*",
            # "scoring": "/api/scoring/*",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": API_TITLE,
        "version": API_VERSION,
    }


# ═══════════════════════════════════════════════════════════════════
# Error Handlers
# ═══════════════════════════════════════════════════════════════════


@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler"""
    return JSONResponse(
        status_code=404,
        content={
            "error": "Endpoint not found",
            "path": str(request.url),
            "docs": "/docs",
        },
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Custom 500 handler"""
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred. Please check the logs.",
        },
    )


# ═══════════════════════════════════════════════════════════════════
# Run Server (if executed directly)
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    
    logger.info("Starting IntelliCredit API server...")
    logger.info("API Documentation: http://localhost:8000/docs")
    logger.info("ReDoc Documentation: http://localhost:8000/redoc")
    
    uvicorn.run(
        "app.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
