"""
FastAPI application for the CSV Import Guardian Agent System.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.config import get_settings
from shared.database import get_database_manager
from shared.kafka_client import get_kafka_manager
from shared.storage import get_storage_manager

from api.routers import csv_import, templates, validation, exports, health

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting CSV Import Guardian API")
    
    try:
        # Initialize core services
        await get_database_manager()
        await get_kafka_manager()
        await get_storage_manager()
        
        logger.info("All services initialized successfully")
        yield
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise
    finally:
        logger.info("Shutting down CSV Import Guardian API")


# Create FastAPI application
app = FastAPI(
    title="CSV Import Guardian Agent System",
    description="Micro-agent architecture for CSV import processing with ETIM support",
    version="1.0.0",
    lifespan=lifespan,
)

# Get settings
settings = get_settings()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)}
    )


# Include routers
app.include_router(csv_import.router, prefix="/v1/csv-import", tags=["CSV Import"])
app.include_router(templates.router, prefix="/v1/csv-import/templates", tags=["Templates"])
app.include_router(validation.router, prefix="/v1/csv-import", tags=["Validation"])
app.include_router(exports.router, prefix="/v1/exports", tags=["Exports"])
app.include_router(health.router, prefix="", tags=["Health"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "CSV Import Guardian Agent System",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/v1/info")
async def get_system_info():
    """Get system information."""
    return {
        "service": "CSV Import Guardian Agent System",
        "version": "1.0.0",
        "environment": settings.environment,
        "features": [
            "File upload and profiling",
            "Automatic mapping suggestions",
            "Data validation with quality gates",
            "ETIM classification support",
            "BMEcat export generation",
            "Data lineage tracking",
            "Multi-tenant support",
        ],
        "supported_formats": [
            "CSV",
            "TSV",
            "Excel (XLSX, XLS)",
            "ZIP archives",
        ],
        "api_endpoints": {
            "upload": "/v1/csv-import/upload",
            "jobs": "/v1/csv-import/jobs",
            "templates": "/v1/csv-import/templates",
            "validation": "/v1/csv-import/suggest/rules",
            "exports": "/v1/exports",
            "health": "/health",
        },
    }


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    return app


if __name__ == "__main__":
    # Run the application
    uvicorn.run(
        "api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=settings.api_workers,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
    )