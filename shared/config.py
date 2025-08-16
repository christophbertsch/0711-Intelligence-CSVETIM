"""
Configuration management for the CSV Import Guardian Agent System.
"""

import os
from functools import lru_cache
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # Database
    database_url: str = Field(
        default="postgresql://guardian:guardian_pass@localhost:5432/csv_guardian",
        description="Database connection URL"
    )
    
    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    kafka_security_protocol: str = Field(
        default="PLAINTEXT",
        description="Kafka security protocol"
    )
    kafka_sasl_mechanism: Optional[str] = Field(
        default=None,
        description="Kafka SASL mechanism"
    )
    kafka_sasl_username: Optional[str] = Field(
        default=None,
        description="Kafka SASL username"
    )
    kafka_sasl_password: Optional[str] = Field(
        default=None,
        description="Kafka SASL password"
    )
    kafka_consumer_group_prefix: str = Field(
        default="csv-guardian",
        description="Kafka consumer group prefix"
    )
    
    # Object Storage (S3-compatible)
    s3_endpoint_url: str = Field(
        default="http://localhost:9000",
        description="S3-compatible endpoint URL"
    )
    s3_access_key: str = Field(
        default="minioadmin",
        description="S3 access key"
    )
    s3_secret_key: str = Field(
        default="minioadmin",
        description="S3 secret key"
    )
    s3_bucket_name: str = Field(
        default="csv-guardian",
        description="Default S3 bucket name"
    )
    s3_region: str = Field(
        default="us-east-1",
        description="S3 region"
    )
    s3_use_ssl: bool = Field(
        default=False,
        description="Use SSL for S3 connections"
    )
    
    # Redis
    redis_url: str = Field(
        default="redis://localhost:6379",
        description="Redis connection URL"
    )
    
    # Monitoring & Tracing
    jaeger_endpoint: str = Field(
        default="http://localhost:14268/api/traces",
        description="Jaeger tracing endpoint"
    )
    enable_tracing: bool = Field(
        default=True,
        description="Enable distributed tracing"
    )
    
    # API Server
    api_host: str = Field(
        default="0.0.0.0",
        description="API server host"
    )
    api_port: int = Field(
        default=8000,
        description="API server port"
    )
    api_workers: int = Field(
        default=1,
        description="Number of API worker processes"
    )
    
    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    log_format: str = Field(
        default="json",
        description="Log format: json or text"
    )
    
    # Environment
    environment: str = Field(
        default="development",
        description="Environment: development, staging, production"
    )
    debug: bool = Field(
        default=False,
        description="Enable debug mode"
    )
    
    # Security
    secret_key: str = Field(
        default="dev-secret-key-change-in-production",
        description="Secret key for encryption"
    )
    allowed_origins: List[str] = Field(
        default=["*"],
        description="CORS allowed origins"
    )
    
    # Agent Configuration
    agent_name: Optional[str] = Field(
        default=None,
        description="Agent name (set by individual agents)"
    )
    agent_batch_size: int = Field(
        default=100,
        description="Default agent batch size"
    )
    agent_max_retries: int = Field(
        default=3,
        description="Maximum retry attempts for agents"
    )
    agent_timeout_seconds: int = Field(
        default=300,
        description="Agent operation timeout"
    )
    agent_concurrency: int = Field(
        default=1,
        description="Agent concurrency level"
    )
    
    # Processing Configuration
    max_file_size_mb: int = Field(
        default=500,
        description="Maximum file size in MB"
    )
    max_rows_per_file: int = Field(
        default=1000000,
        description="Maximum rows per file"
    )
    sample_size: int = Field(
        default=1000,
        description="Default sample size for profiling"
    )
    quality_gate_threshold: float = Field(
        default=0.8,
        description="Quality gate threshold (0.0-1.0)"
    )
    
    # Export Configuration
    export_timeout_minutes: int = Field(
        default=60,
        description="Export operation timeout in minutes"
    )
    export_batch_size: int = Field(
        default=10000,
        description="Export batch size"
    )
    
    # Cache Configuration
    cache_ttl_seconds: int = Field(
        default=3600,
        description="Default cache TTL in seconds"
    )
    
    # Rate Limiting
    rate_limit_requests_per_minute: int = Field(
        default=1000,
        description="Rate limit: requests per minute"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


class AgentSettings(Settings):
    """Agent-specific settings."""
    
    # Agent-specific overrides
    input_topics: List[str] = Field(
        default_factory=list,
        description="Input Kafka topics for this agent"
    )
    output_topics: List[str] = Field(
        default_factory=list,
        description="Output Kafka topics for this agent"
    )
    
    def get_consumer_group_id(self) -> str:
        """Get the consumer group ID for this agent."""
        return f"{self.kafka_consumer_group_prefix}-{self.agent_name}"
    
    def get_agent_config(self) -> dict:
        """Get agent-specific configuration."""
        return {
            "agent_name": self.agent_name,
            "input_topics": self.input_topics,
            "output_topics": self.output_topics,
            "batch_size": self.agent_batch_size,
            "max_retries": self.agent_max_retries,
            "timeout_seconds": self.agent_timeout_seconds,
            "concurrency": self.agent_concurrency,
            "consumer_group_id": self.get_consumer_group_id(),
        }


@lru_cache()
def get_settings() -> Settings:
    """Get cached application settings."""
    return Settings()


@lru_cache()
def get_agent_settings(agent_name: str) -> AgentSettings:
    """Get cached agent-specific settings."""
    settings = AgentSettings()
    settings.agent_name = agent_name
    
    # Load agent-specific topic configuration
    input_topics_env = f"{agent_name.upper()}_INPUT_TOPICS"
    output_topics_env = f"{agent_name.upper()}_OUTPUT_TOPICS"
    
    if input_topics := os.getenv(input_topics_env):
        settings.input_topics = [topic.strip() for topic in input_topics.split(",")]
    
    if output_topics := os.getenv(output_topics_env):
        settings.output_topics = [topic.strip() for topic in output_topics.split(",")]
    
    return settings


def get_kafka_config() -> dict:
    """Get Kafka configuration dictionary."""
    settings = get_settings()
    
    config = {
        "bootstrap_servers": settings.kafka_bootstrap_servers,
        "security_protocol": settings.kafka_security_protocol,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "group_id": settings.kafka_consumer_group_prefix,
    }
    
    # Add SASL configuration if provided
    if settings.kafka_sasl_mechanism:
        config.update({
            "sasl_mechanism": settings.kafka_sasl_mechanism,
            "sasl_plain_username": settings.kafka_sasl_username,
            "sasl_plain_password": settings.kafka_sasl_password,
        })
    
    return config


def get_s3_config() -> dict:
    """Get S3 configuration dictionary."""
    settings = get_settings()
    
    return {
        "endpoint_url": settings.s3_endpoint_url,
        "aws_access_key_id": settings.s3_access_key,
        "aws_secret_access_key": settings.s3_secret_key,
        "region_name": settings.s3_region,
        "use_ssl": settings.s3_use_ssl,
    }


def is_production() -> bool:
    """Check if running in production environment."""
    return get_settings().environment.lower() == "production"


def is_development() -> bool:
    """Check if running in development environment."""
    return get_settings().environment.lower() == "development"


def get_log_config() -> dict:
    """Get logging configuration."""
    settings = get_settings()
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
            },
            "text": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": settings.log_format,
                "level": settings.log_level,
            },
        },
        "root": {
            "level": settings.log_level,
            "handlers": ["console"],
        },
        "loggers": {
            "uvicorn": {
                "level": "INFO",
                "handlers": ["console"],
                "propagate": False,
            },
            "sqlalchemy.engine": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
            "kafka": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }


# Environment-specific configurations

class DevelopmentConfig(Settings):
    """Development environment configuration."""
    debug: bool = True
    log_level: str = "DEBUG"


class ProductionConfig(Settings):
    """Production environment configuration."""
    debug: bool = False
    log_level: str = "INFO"
    allowed_origins: List[str] = []  # Restrict CORS in production


class TestingConfig(Settings):
    """Testing environment configuration."""
    database_url: str = "sqlite:///./test.db"
    kafka_bootstrap_servers: str = "localhost:9092"
    s3_endpoint_url: str = "http://localhost:9000"
    debug: bool = True
    log_level: str = "DEBUG"


def get_config_for_environment(env: str) -> Settings:
    """Get configuration for specific environment."""
    configs = {
        "development": DevelopmentConfig,
        "production": ProductionConfig,
        "testing": TestingConfig,
    }
    
    config_class = configs.get(env.lower(), Settings)
    return config_class()