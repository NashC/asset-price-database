"""
Staging operations for raw data loading.

Handles bulk COPY operations from CSV files into staging tables.
"""

import csv
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from .config import get_settings


def copy_to_stage(csv_path: Path, table: str = "stage_raw_prices") -> int:
    """
    Bulk-load raw CSV into staging table using PostgreSQL COPY.
    
    Args:
        csv_path: Path to CSV file to load
        table: Target staging table name
        
    Returns:
        int: Number of rows loaded
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV format is invalid
    """
    settings = get_settings()
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    logger.info(f"Loading CSV {csv_path} into staging table {table}")
    
    # First, validate CSV structure
    try:
        # Read just the header to validate structure
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            logger.debug(f"CSV header: {header}")
            
        # Count total rows for progress tracking
        total_rows = sum(1 for _ in open(csv_path)) - 1  # Subtract header
        logger.info(f"CSV contains {total_rows} data rows")
        
    except Exception as e:
        raise ValueError(f"Invalid CSV format: {e}")
    
    # Create database connection
    engine = create_engine(settings.database_url)
    
    try:
        # Purge existing staging data first (in separate transaction)
        _purge_stage_table(engine, table)
        
        with engine.connect() as conn:
            # Use pandas for robust CSV parsing and loading
            df = pd.read_csv(csv_path, dtype=str)  # Read all as strings initially
            
            # Add metadata columns
            df['source_file'] = str(csv_path)
            df['row_number'] = range(1, len(df) + 1)
            
            # Standardize column names for staging table
            df = _standardize_columns(df, csv_path)
            
            # Load into staging table using pandas
            rows_loaded = len(df)
            df.to_sql(table, conn, if_exists='append', index=False, method='multi')
            conn.commit()
            
            logger.success(f"Loaded {rows_loaded} rows into {table}")
            return rows_loaded
            
    except Exception as e:
        logger.error(f"Failed to load CSV into staging: {e}")
        raise


def _standardize_columns(df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """
    Standardize column names to match staging table schema.
    
    Args:
        df: Input DataFrame
        csv_path: Path to original CSV file (for symbol inference)
        
    Returns:
        pd.DataFrame: DataFrame with standardized columns
    """
    # Multiple possible column name mappings
    column_mappings = [
        # Standard Yahoo Finance format
        {
            'Date': 'date_str',
            'Open': 'open_str', 
            'High': 'high_str',
            'Low': 'low_str',
            'Close': 'close_str',
            'Volume': 'volume_str',
            'Adj Close': 'adj_close_str',
            'Symbol': 'symbol'
        },
        # Alternative formats
        {
            'date': 'date_str',
            'open': 'open_str',
            'high': 'high_str', 
            'low': 'low_str',
            'close': 'close_str',
            'volume': 'volume_str',
            'adj_close': 'adj_close_str',
            'adjclose': 'adj_close_str',
            'symbol': 'symbol',
            'ticker': 'symbol'
        },
        # Case-insensitive mapping
        {
            'DATE': 'date_str',
            'OPEN': 'open_str',
            'HIGH': 'high_str',
            'LOW': 'low_str', 
            'CLOSE': 'close_str',
            'VOLUME': 'volume_str',
            'ADJ_CLOSE': 'adj_close_str',
            'SYMBOL': 'symbol',
            'TICKER': 'symbol'
        }
    ]
    
    # Try each mapping
    df_mapped = df.copy()
    columns_found = set()
    
    for mapping in column_mappings:
        for old_col, new_col in mapping.items():
            if old_col in df_mapped.columns and new_col not in df_mapped.columns:
                df_mapped = df_mapped.rename(columns={old_col: new_col})
                columns_found.add(new_col)
                logger.debug(f"Mapped column '{old_col}' -> '{new_col}'")
    
    # Ensure required columns exist
    required_cols = ['symbol', 'date_str', 'open_str', 'high_str', 
                    'low_str', 'close_str']
    
    for col in required_cols:
        if col not in df_mapped.columns:
            if col == 'symbol':
                # Try to extract symbol from filename
                symbol = _extract_symbol_from_filename(csv_path)
                df_mapped['symbol'] = symbol
                logger.info(f"Inferred symbol '{symbol}' from filename")
            else:
                df_mapped[col] = None
                logger.warning(f"Missing column '{col}', filled with NULL")
    
    # Optional columns with defaults
    optional_cols = ['volume_str', 'adj_close_str']
    for col in optional_cols:
        if col not in df_mapped.columns:
            df_mapped[col] = None
            logger.debug(f"Optional column '{col}' not found, filled with NULL")
    
    return df_mapped


def _extract_symbol_from_filename(csv_path: Path) -> str:
    """
    Extract stock symbol from filename.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        str: Extracted symbol
    """
    filename = csv_path.stem
    
    # Common patterns to try
    patterns = [
        r'^([A-Z]{1,5})',  # Start with 1-5 uppercase letters
        r'([A-Z]{1,5})_',  # Letters followed by underscore
        r'([A-Z]{1,5})-',  # Letters followed by dash
        r'([A-Z-]{1,10})'  # Letters and dashes (for crypto like BTC-USD)
    ]
    
    import re
    for pattern in patterns:
        match = re.search(pattern, filename.upper())
        if match:
            return match.group(1)
    
    # Fallback: use entire filename (cleaned)
    symbol = re.sub(r'[^A-Z-]', '', filename.upper())
    return symbol if symbol else 'UNKNOWN'


def _purge_stage_table(engine, table: str) -> None:
    """
    Truncate staging table in a separate transaction.
    
    Args:
        engine: SQLAlchemy engine
        table: Staging table name to purge
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            conn.commit()
            logger.debug(f"Purged staging table {table}")
    except Exception as e:
        logger.error(f"Failed to purge staging table {table}: {e}")
        raise


def purge_stage(table: str = "stage_raw_prices") -> None:
    """
    Truncate staging table before each run.
    
    Args:
        table: Staging table name to purge
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    _purge_stage_table(engine, table)


def get_staging_summary(table: str = "stage_raw_prices") -> dict:
    """
    Get summary statistics of data in staging table.
    
    Args:
        table: Staging table name
        
    Returns:
        dict: Summary statistics
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
            row_count = result.scalar()
            
            # Get unique symbols
            result = conn.execute(text(f"SELECT COUNT(DISTINCT symbol) FROM {table} WHERE symbol IS NOT NULL"))
            symbol_count = result.scalar()
            
            # Get date range
            result = conn.execute(text(f"""
                SELECT 
                    MIN(date_str) as min_date,
                    MAX(date_str) as max_date
                FROM {table}
                WHERE date_str IS NOT NULL
            """))
            date_range = result.fetchone()
            
            # Get distinct source files
            result = conn.execute(text(f"SELECT COUNT(DISTINCT source_file) FROM {table}"))
            file_count = result.scalar()
            
            summary = {
                'row_count': row_count,
                'symbol_count': symbol_count,
                'file_count': file_count,
                'min_date': date_range[0] if date_range else None,
                'max_date': date_range[1] if date_range else None
            }
            
            logger.info(f"Staging summary: {summary}")
            return summary
            
    except Exception as e:
        logger.error(f"Failed to get staging summary: {e}")
        raise


def validate_staging_data(table: str = "stage_raw_prices") -> List[str]:
    """
    Validate data in staging table and return list of issues.
    
    Args:
        table: Staging table name
        
    Returns:
        List[str]: List of validation issues found
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    issues = []
    
    try:
        with engine.connect() as conn:
            # Check for missing required fields
            result = conn.execute(text(f"""
                SELECT 
                    SUM(CASE WHEN symbol IS NULL OR symbol = '' THEN 1 ELSE 0 END) as null_symbols,
                    SUM(CASE WHEN date_str IS NULL OR date_str = '' THEN 1 ELSE 0 END) as null_dates,
                    SUM(CASE WHEN close_str IS NULL OR close_str = '' THEN 1 ELSE 0 END) as null_closes
                FROM {table}
            """))
            nulls = result.fetchone()
            
            if nulls[0] > 0:
                issues.append(f"{nulls[0]} rows with missing symbol")
            if nulls[1] > 0:
                issues.append(f"{nulls[1]} rows with missing date")
            if nulls[2] > 0:
                issues.append(f"{nulls[2]} rows with missing close price")
            
            # Check for invalid date formats (more flexible patterns)
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table}
                WHERE date_str IS NOT NULL 
                AND date_str != ''
                AND date_str !~ '^[0-9]{{4}}-[0-9]{{1,2}}-[0-9]{{1,2}}$'
                AND date_str !~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$'
                AND date_str !~ '^[0-9]{{1,2}}-[0-9]{{1,2}}-[0-9]{{4}}$'
            """))
            invalid_dates = result.scalar()
            
            if invalid_dates > 0:
                issues.append(f"{invalid_dates} rows with invalid date format")
            
            # Check for non-numeric price fields
            price_fields = ['open_str', 'high_str', 'low_str', 'close_str']
            for field in price_fields:
                result = conn.execute(text(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE {field} IS NOT NULL 
                    AND {field} != ''
                    AND {field} !~ '^[0-9]*\.?[0-9]+$'
                """))
                invalid_prices = result.scalar()
                
                if invalid_prices > 0:
                    issues.append(f"{invalid_prices} rows with invalid {field}")
            
            # Check for duplicate symbol-date combinations
            result = conn.execute(text(f"""
                SELECT COUNT(*) as duplicate_count FROM (
                    SELECT symbol, date_str, COUNT(*) 
                    FROM {table} 
                    WHERE symbol IS NOT NULL AND date_str IS NOT NULL
                    GROUP BY symbol, date_str 
                    HAVING COUNT(*) > 1
                ) duplicates
            """))
            duplicate_count = result.scalar()
            
            if duplicate_count > 0:
                issues.append(f"{duplicate_count} duplicate symbol-date combinations")
            
            if issues:
                logger.warning(f"Staging validation issues: {issues}")
            else:
                logger.success("Staging data validation passed")
                
            return issues
            
    except Exception as e:
        logger.error(f"Failed to validate staging data: {e}")
        raise 