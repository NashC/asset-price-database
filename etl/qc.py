"""
Data Quality Control module.

Provides validation functions and quality scoring for incoming data.
"""

from datetime import datetime, date
from typing import Dict, List, Any

import pandas as pd
import numpy as np
from loguru import logger

from .config import get_settings


def validate_schema(df: pd.DataFrame) -> None:
    """
    Assert required columns and data types are present.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        ValueError: If schema validation fails
    """
    required_columns = [
        'symbol', 'date_str', 'open_str', 'high_str', 
        'low_str', 'close_str', 'volume_str'
    ]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    # Check for completely empty DataFrame
    if df.empty:
        raise ValueError("DataFrame is empty")
    
    logger.debug(f"Schema validation passed for {len(df)} rows")


def detect_duplicates(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """
    Return duplicate rows based on specified key columns.
    
    Args:
        df: DataFrame to check for duplicates
        keys: List of column names to use as duplicate detection keys
        
    Returns:
        pd.DataFrame: Rows that are duplicates
    """
    if not keys:
        keys = ['symbol', 'date_str']
    
    if df.empty:
        return pd.DataFrame()
    
    # Check if all key columns exist
    missing_keys = [key for key in keys if key not in df.columns]
    if missing_keys:
        logger.warning(f"Missing key columns for duplicate detection: {missing_keys}")
        return pd.DataFrame()
    
    # Find duplicates
    duplicates = df[df.duplicated(subset=keys, keep=False)]
    
    if not duplicates.empty:
        logger.warning(f"Found {len(duplicates)} duplicate rows")
        logger.debug(f"Duplicate keys: {duplicates[keys].drop_duplicates().to_dict('records')}")
    
    return duplicates


def score_quality(df: pd.DataFrame) -> float:
    """
    Calculate 0-100 quality score based on various data quality metrics.
    
    Args:
        df: DataFrame to score
        
    Returns:
        float: Quality score between 0-100
    """
    if df.empty:
        return 0.0
    
    try:
        scores = []
        
        # 1. Completeness Score (25 points)
        completeness_score = _calculate_completeness_score(df)
        scores.append(('completeness', completeness_score, 25))
        
        # 2. Validity Score (25 points) 
        validity_score = _calculate_validity_score(df)
        scores.append(('validity', validity_score, 25))
        
        # 3. Consistency Score (25 points)
        consistency_score = _calculate_consistency_score(df)
        scores.append(('consistency', consistency_score, 25))
        
        # 4. Uniqueness Score (25 points)
        uniqueness_score = _calculate_uniqueness_score(df)
        scores.append(('uniqueness', uniqueness_score, 25))
        
        # Calculate weighted total
        total_score = sum(score * weight for _, score, weight in scores) / 100
        
        # Log detailed breakdown
        score_details = {name: f"{score:.1f}" for name, score, _ in scores}
        logger.info(f"Quality scores: {score_details}, Total: {total_score:.1f}")
        
        return round(total_score, 2)
        
    except Exception as e:
        logger.error(f"Error calculating quality score: {e}")
        return 0.0


def _calculate_completeness_score(df: pd.DataFrame) -> float:
    """Calculate completeness score based on missing values."""
    required_fields = ['symbol', 'date_str', 'close_str']
    total_cells = len(df) * len(required_fields)
    
    if total_cells == 0:
        return 0.0
    
    missing_cells = 0
    for field in required_fields:
        if field in df.columns:
            missing_cells += df[field].isna().sum()
        else:
            missing_cells += len(df)  # Entire column missing
    
    completeness = (total_cells - missing_cells) / total_cells
    return completeness * 100


def _calculate_validity_score(df: pd.DataFrame) -> float:
    """Calculate validity score based on data format correctness."""
    total_rows = len(df)
    if total_rows == 0:
        return 0.0
    
    valid_rows = 0
    
    for _, row in df.iterrows():
        row_valid = True
        
        # Check date format
        if pd.notna(row.get('date_str')):
            try:
                datetime.strptime(str(row['date_str']), '%Y-%m-%d')
            except (ValueError, TypeError):
                row_valid = False
        
        # Check numeric fields
        numeric_fields = ['open_str', 'high_str', 'low_str', 'close_str', 'volume_str']
        for field in numeric_fields:
            if pd.notna(row.get(field)) and field in df.columns:
                try:
                    float(str(row[field]))
                except (ValueError, TypeError):
                    row_valid = False
                    break
        
        if row_valid:
            valid_rows += 1
    
    return (valid_rows / total_rows) * 100


def _calculate_consistency_score(df: pd.DataFrame) -> float:
    """Calculate consistency score based on OHLC relationships."""
    if df.empty:
        return 0.0
    
    # Convert price strings to numeric for validation
    price_cols = ['open_str', 'high_str', 'low_str', 'close_str']
    numeric_df = df.copy()
    
    for col in price_cols:
        if col in numeric_df.columns:
            numeric_df[col] = pd.to_numeric(numeric_df[col], errors='coerce')
    
    # Check OHLC consistency rules
    consistent_rows = 0
    total_rows = 0
    
    for _, row in numeric_df.iterrows():
        # Skip rows with missing price data
        if any(pd.isna(row.get(col)) for col in price_cols if col in numeric_df.columns):
            continue
            
        total_rows += 1
        
        open_price = row.get('open_str', 0)
        high_price = row.get('high_str', 0) 
        low_price = row.get('low_str', 0)
        close_price = row.get('close_str', 0)
        
        # Check OHLC rules
        if (high_price >= max(open_price, close_price) and
            low_price <= min(open_price, close_price) and
            high_price >= low_price and
            all(price > 0 for price in [open_price, high_price, low_price, close_price])):
            consistent_rows += 1
    
    if total_rows == 0:
        return 0.0
    
    return (consistent_rows / total_rows) * 100


def _calculate_uniqueness_score(df: pd.DataFrame) -> float:
    """Calculate uniqueness score based on duplicate detection."""
    if df.empty:
        return 100.0  # Empty is technically unique
    
    total_rows = len(df)
    duplicates = detect_duplicates(df, ['symbol', 'date_str'])
    duplicate_count = len(duplicates)
    
    uniqueness = (total_rows - duplicate_count) / total_rows
    return uniqueness * 100


def validate_price_ranges(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate price ranges and detect outliers.
    
    Args:
        df: DataFrame with price data
        
    Returns:
        dict: Validation results with outlier information
    """
    results = {
        'outliers': [],
        'extreme_values': [],
        'negative_prices': [],
        'zero_volumes': []
    }
    
    if df.empty:
        return results
    
    # Convert to numeric for analysis
    numeric_df = df.copy()
    price_cols = ['open_str', 'high_str', 'low_str', 'close_str']
    
    for col in price_cols:
        if col in numeric_df.columns:
            numeric_df[col] = pd.to_numeric(numeric_df[col], errors='coerce')
    
    if 'volume_str' in numeric_df.columns:
        numeric_df['volume_str'] = pd.to_numeric(numeric_df['volume_str'], errors='coerce')
    
    # Check for negative prices
    for col in price_cols:
        if col in numeric_df.columns:
            negative_mask = numeric_df[col] < 0
            if negative_mask.any():
                negative_rows = df[negative_mask][['symbol', 'date_str', col]]
                results['negative_prices'].extend(negative_rows.to_dict('records'))
    
    # Check for extreme price movements (>50% daily change)
    if 'close_str' in numeric_df.columns and 'symbol' in numeric_df.columns:
        try:
            numeric_df['prev_close'] = numeric_df.groupby('symbol')['close_str'].shift(1)
            numeric_df['pct_change'] = (
                (numeric_df['close_str'] - numeric_df['prev_close']) / 
                numeric_df['prev_close'] * 100
            )
            
            extreme_mask = abs(numeric_df['pct_change']) > 50
            if extreme_mask.any():
                extreme_rows = df[extreme_mask][['symbol', 'date_str', 'close_str']]
                results['extreme_values'].extend(extreme_rows.to_dict('records'))
        except Exception as e:
            logger.warning(f"Could not calculate price changes: {e}")
    
    # Check for zero volumes
    if 'volume_str' in numeric_df.columns:
        zero_volume_mask = numeric_df['volume_str'] == 0
        if zero_volume_mask.any():
            zero_vol_rows = df[zero_volume_mask][['symbol', 'date_str', 'volume_str']]
            results['zero_volumes'].extend(zero_vol_rows.to_dict('records'))
    
    # Log findings
    for check, items in results.items():
        if items:
            logger.warning(f"Found {len(items)} rows with {check}")
    
    return results


def generate_quality_report(df: pd.DataFrame, batch_name: str) -> Dict[str, Any]:
    """
    Generate comprehensive quality report for a data batch.
    
    Args:
        df: DataFrame to analyze
        batch_name: Name of the batch for reporting
        
    Returns:
        dict: Comprehensive quality report
    """
    logger.info(f"Generating quality report for batch: {batch_name}")
    
    report = {
        'batch_name': batch_name,
        'timestamp': datetime.now().isoformat(),
        'row_count': len(df),
        'column_count': len(df.columns),
        'quality_score': 0.0,
        'schema_valid': True,
        'duplicates': {},
        'outliers': {},
        'summary_stats': {}
    }
    
    try:
        # Schema validation
        validate_schema(df)
        report['quality_score'] = score_quality(df)
    except ValueError as e:
        report['schema_valid'] = False
        report['schema_errors'] = str(e)
        logger.error(f"Schema validation failed: {e}")
    
    # Duplicate analysis
    try:
        duplicates = detect_duplicates(df, ['symbol', 'date_str'])
        report['duplicates'] = {
            'count': len(duplicates),
            'percentage': (len(duplicates) / len(df) * 100) if len(df) > 0 else 0
        }
    except Exception as e:
        logger.error(f"Duplicate analysis failed: {e}")
        report['duplicates'] = {'count': 0, 'percentage': 0.0}
    
    # Outlier analysis
    try:
        report['outliers'] = validate_price_ranges(df)
    except Exception as e:
        logger.error(f"Outlier analysis failed: {e}")
        report['outliers'] = {}
    
    # Summary statistics
    try:
        if not df.empty:
            report['summary_stats'] = {
                'unique_symbols': df['symbol'].nunique() if 'symbol' in df.columns else 0,
                'date_range': {
                    'min': df['date_str'].min() if 'date_str' in df.columns else None,
                    'max': df['date_str'].max() if 'date_str' in df.columns else None
                },
                'missing_values': df.isnull().sum().to_dict()
            }
    except Exception as e:
        logger.error(f"Summary statistics failed: {e}")
        report['summary_stats'] = {}
    
    logger.success(f"Quality report generated: Score {report['quality_score']:.1f}/100")
    return report


 