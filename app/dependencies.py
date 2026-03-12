"""
dependencies.py
===============
Shared dependencies, database connections, and utilities.
"""

import logging
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import (
    DB_URL,
    DB_CONNECT_ARGS,
    MAX_WORKERS,
    LOG_LEVEL,
    LOG_FILE,
    LOG_FORMAT,
)

# ═══════════════════════════════════════════════════════════════════
# Logging Setup
# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════
# Database Setup
# ═══════════════════════════════════════════════════════════════════

engine = create_engine(DB_URL, echo=False, connect_args=DB_CONNECT_ARGS)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


def get_db():
    """
    Dependency for getting database session.
    Usage in endpoints: db: Session = Depends(get_db)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════
# Thread Pool for Parallel Processing
# ═══════════════════════════════════════════════════════════════════

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

logger.info(f"Thread pool initialized with {MAX_WORKERS} workers")

# ═══════════════════════════════════════════════════════════════════
# Startup/Shutdown Handlers
# ═══════════════════════════════════════════════════════════════════


async def startup_handler():
    """
    Executed on application startup.
    Add initialization tasks here (database setup, cache connections, etc.)
    """
    logger.info("=" * 80)
    logger.info("IntelliCredit Platform API starting...")
    logger.info(f"Database: {DB_URL}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Workers: {MAX_WORKERS}")
    logger.info("=" * 80)


async def shutdown_handler():
    """
    Executed on application shutdown.
    Add cleanup tasks here (close connections, flush caches, etc.)
    """
    logger.info("=" * 80)
    logger.info("IntelliCredit Platform API shutting down...")
    
    # Shutdown thread pool
    executor.shutdown(wait=True)
    logger.info("Thread pool shutdown complete")
    
    logger.info("Shutdown complete")
    logger.info("=" * 80)
