"""
Health check endpoints.
"""

import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, HTTPException

from shared.database import check_database_health
from shared.kafka_client import check_kafka_health
from shared.storage import check_storage_health

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health_check():
    """
    Comprehensive health check for all system components.
    """
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0.0",
            "components": {}
        }
        
        # Check database
        try:
            db_health = await check_database_health()
            health_status["components"]["database"] = db_health
        except Exception as e:
            health_status["components"]["database"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            health_status["status"] = "degraded"
        
        # Check Kafka
        try:
            kafka_health = await check_kafka_health()
            health_status["components"]["kafka"] = kafka_health
        except Exception as e:
            health_status["components"]["kafka"] = {
                "status": "unhealthy", 
                "error": str(e)
            }
            health_status["status"] = "degraded"
        
        # Check storage
        try:
            storage_health = await check_storage_health()
            health_status["components"]["storage"] = storage_health
        except Exception as e:
            health_status["components"]["storage"] = {
                "status": "unhealthy",
                "error": str(e)
            }
            health_status["status"] = "degraded"
        
        # Determine overall status
        unhealthy_components = [
            name for name, component in health_status["components"].items()
            if component.get("status") == "unhealthy"
        ]
        
        if unhealthy_components:
            if len(unhealthy_components) == len(health_status["components"]):
                health_status["status"] = "unhealthy"
            else:
                health_status["status"] = "degraded"
        
        # Return appropriate HTTP status
        if health_status["status"] == "unhealthy":
            return health_status, 503
        elif health_status["status"] == "degraded":
            return health_status, 200  # Still operational
        else:
            return health_status, 200
            
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }, 503


@router.get("/health/live")
async def liveness_check():
    """
    Simple liveness check - just confirms the API is responding.
    """
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/health/ready")
async def readiness_check():
    """
    Readiness check - confirms the API is ready to serve requests.
    """
    try:
        # Quick check of critical components
        db_health = await check_database_health()
        
        if db_health.get("status") == "healthy":
            return {
                "status": "ready",
                "timestamp": datetime.utcnow().isoformat()
            }
        else:
            return {
                "status": "not_ready",
                "timestamp": datetime.utcnow().isoformat(),
                "reason": "Database not healthy"
            }, 503
            
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        return {
            "status": "not_ready",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        }, 503


@router.get("/metrics")
async def get_metrics():
    """
    Get system metrics in Prometheus format.
    """
    try:
        # This would typically return Prometheus-formatted metrics
        # For now, return basic JSON metrics
        
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {
                "api_requests_total": 0,  # Would be tracked by middleware
                "api_request_duration_seconds": 0,
                "jobs_total": 0,
                "jobs_completed": 0,
                "jobs_failed": 0,
                "files_processed_total": 0,
                "data_quality_score_avg": 0,
            }
        }
        
        # TODO: Implement actual metrics collection
        # This could integrate with Prometheus client library
        
        return metrics
        
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))