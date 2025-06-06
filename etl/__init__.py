"""
Stock Warehouse ETL Package

This package contains modules for extracting, transforming, and loading
stock market data into the PostgreSQL warehouse.

Modules:
- config: Environment configuration and settings
- staging: Raw data staging operations  
- qc: Data quality validation and scoring
- loaders: Data loading into core tables
- gold_refresh: Materialized view refresh operations
- cli: Command-line interface for ETL operations
"""

__version__ = "0.1.0" 