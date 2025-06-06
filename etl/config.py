"""
Configuration module for Stock Warehouse ETL.

Uses Pydantic Settings to manage environment variables and configuration.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Database Configuration
    database_url: str = Field(
        default="postgresql://stockuser:stockpass@localhost:5432/stockdb",
        description="PostgreSQL connection string"
    )
    database_url_async: str = Field(
        default="postgresql+asyncpg://stockuser:stockpass@localhost:5432/stockdb",
        description="Async PostgreSQL connection string"
    )
    
    # Data Paths
    data_landing_path: Path = Field(
        default=Path("./data/landing"),
        description="Directory for incoming data files"
    )
    data_archive_path: Path = Field(
        default=Path("./data/archive"),
        description="Directory for processed data files"
    )
    
    # Quality Control Thresholds
    qc_min_score: float = Field(
        default=75.0,
        ge=0.0,
        le=100.0,
        description="Minimum quality score to accept batch"
    )
    qc_max_null_pct: float = Field(
        default=5.0,
        ge=0.0,
        le=100.0,
        description="Maximum percentage of null values allowed"
    )
    qc_max_duplicate_pct: float = Field(
        default=1.0,
        ge=0.0,
        le=100.0,
        description="Maximum percentage of duplicate rows allowed"
    )
    
    # ETL Configuration
    batch_size: int = Field(
        default=10000,
        gt=0,
        description="Number of rows to process in each batch"
    )
    max_workers: int = Field(
        default=4,
        gt=0,
        description="Maximum number of worker threads"
    )
    
    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    log_file: Optional[Path] = Field(
        default=Path("./logs/etl.log"),
        description="Log file path"
    )
    
    # API Configuration (Future)
    api_host: str = Field(
        default="0.0.0.0",
        description="API server host"
    )
    api_port: int = Field(
        default=8000,
        gt=0,
        le=65535,
        description="API server port"
    )
    api_workers: int = Field(
        default=1,
        gt=0,
        description="Number of API worker processes"
    )
    
    # External Data Sources (Future)
    alpha_vantage_api_key: Optional[str] = Field(
        default=None,
        description="Alpha Vantage API key"
    )
    yahoo_finance_enabled: bool = Field(
        default=True,
        description="Enable Yahoo Finance data source"
    )
    polygon_api_key: Optional[str] = Field(
        default=None,
        description="Polygon.io API key"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get application settings singleton.
    
    Returns:
        Settings: Application configuration object
    """
    global _settings
    if _settings is None:
        _settings = Settings()
        
        # Ensure data directories exist
        _settings.data_landing_path.mkdir(parents=True, exist_ok=True)
        _settings.data_archive_path.mkdir(parents=True, exist_ok=True)
        
        # Ensure log directory exists
        if _settings.log_file:
            _settings.log_file.parent.mkdir(parents=True, exist_ok=True)
    
    return _settings


def reload_settings() -> Settings:
    """
    Force reload of settings (useful for testing).
    
    Returns:
        Settings: Fresh application configuration object
    """
    global _settings
    _settings = None
    return get_settings() 