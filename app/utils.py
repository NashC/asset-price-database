"""
Utility functions for Stock Warehouse application.

Common helpers and utility functions used across the application.
"""

import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Dict, Any, Union
from pathlib import Path

import pandas as pd
from loguru import logger


def validate_symbol(symbol: str) -> bool:
    """
    Validate if a symbol follows standard format.
    
    Args:
        symbol: Symbol to validate
        
    Returns:
        bool: True if valid symbol format
    """
    if not symbol or not isinstance(symbol, str):
        return False
    
    # Basic symbol validation: 1-10 alphanumeric characters, possibly with dash
    pattern = r'^[A-Z0-9-]{1,10}$'
    return bool(re.match(pattern, symbol.upper()))


def parse_date_string(date_str: str) -> Optional[date]:
    """
    Parse date string in various formats.
    
    Args:
        date_str: Date string to parse
        
    Returns:
        date: Parsed date or None if invalid
    """
    if not date_str:
        return None
    
    # Common date formats to try
    formats = [
        '%Y-%m-%d',
        '%m/%d/%Y',
        '%d/%m/%Y',
        '%Y%m%d',
        '%m-%d-%Y',
        '%d-%m-%Y'
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    logger.warning(f"Could not parse date string: {date_str}")
    return None


def get_trading_days(start: date, end: date) -> int:
    """
    Calculate number of trading days between two dates (approximate).
    
    Args:
        start: Start date
        end: End date
        
    Returns:
        int: Approximate number of trading days
    """
    if end < start:
        return 0
    
    total_days = (end - start).days + 1
    
    # Rough approximation: 5/7 of days are trading days
    # This doesn't account for holidays but gives a reasonable estimate
    trading_days = int(total_days * 5 / 7)
    
    return max(1, trading_days)


def format_currency(amount: float, currency: str = "USD") -> str:
    """
    Format currency amount for display.
    
    Args:
        amount: Amount to format
        currency: Currency code
        
    Returns:
        str: Formatted currency string
    """
    if currency.upper() == "USD":
        return f"${amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"


def calculate_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate daily returns from price series.
    
    Args:
        prices: Price series
        
    Returns:
        pd.Series: Daily returns
    """
    if len(prices) < 2:
        return pd.Series(dtype=float)
    
    returns = prices.pct_change().dropna()
    return returns


def calculate_volatility(returns: pd.Series, annualize: bool = True) -> float:
    """
    Calculate volatility from returns series.
    
    Args:
        returns: Returns series
        annualize: Whether to annualize the volatility
        
    Returns:
        float: Volatility
    """
    if len(returns) < 2:
        return 0.0
    
    vol = returns.std()
    
    if annualize:
        # Annualize assuming 252 trading days per year
        vol = vol * (252 ** 0.5)
    
    return vol


def detect_outliers(data: pd.Series, method: str = "iqr", threshold: float = 1.5) -> pd.Series:
    """
    Detect outliers in a data series.
    
    Args:
        data: Data series to analyze
        method: Method to use ('iqr' or 'zscore')
        threshold: Threshold for outlier detection
        
    Returns:
        pd.Series: Boolean series indicating outliers
    """
    if method == "iqr":
        Q1 = data.quantile(0.25)
        Q3 = data.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        
        outliers = (data < lower_bound) | (data > upper_bound)
        
    elif method == "zscore":
        z_scores = abs((data - data.mean()) / data.std())
        outliers = z_scores > threshold
        
    else:
        raise ValueError(f"Unknown outlier detection method: {method}")
    
    return outliers


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List[List]: List of chunks
    """
    chunks = []
    for i in range(0, len(lst), chunk_size):
        chunks.append(lst[i:i + chunk_size])
    return chunks


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero.
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
        
    Returns:
        float: Result of division or default
    """
    if denominator == 0:
        return default
    return numerator / denominator


def get_file_info(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Get information about a file.
    
    Args:
        file_path: Path to file
        
    Returns:
        dict: File information
    """
    path = Path(file_path)
    
    if not path.exists():
        return {'exists': False}
    
    stat = path.stat()
    
    return {
        'exists': True,
        'size_bytes': stat.st_size,
        'size_mb': round(stat.st_size / (1024 * 1024), 2),
        'modified_time': datetime.fromtimestamp(stat.st_mtime),
        'is_file': path.is_file(),
        'is_dir': path.is_dir(),
        'extension': path.suffix,
        'name': path.name,
        'stem': path.stem
    }


def create_date_range(start: date, end: date, freq: str = "D") -> List[date]:
    """
    Create a list of dates between start and end.
    
    Args:
        start: Start date
        end: End date
        freq: Frequency ('D' for daily, 'W' for weekly, 'M' for monthly)
        
    Returns:
        List[date]: List of dates
    """
    if freq == "D":
        delta = timedelta(days=1)
    elif freq == "W":
        delta = timedelta(weeks=1)
    elif freq == "M":
        delta = timedelta(days=30)  # Approximate
    else:
        raise ValueError(f"Unsupported frequency: {freq}")
    
    dates = []
    current = start
    
    while current <= end:
        dates.append(current)
        current += delta
    
    return dates


def memory_usage_mb() -> float:
    """
    Get current memory usage in MB.
    
    Returns:
        float: Memory usage in MB
    """
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
    except ImportError:
        logger.warning("psutil not available, cannot get memory usage")
        return 0.0


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        str: Formatted duration
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def sanitize_filename(filename: str) -> str:
    """
    Sanitize filename by removing invalid characters.
    
    Args:
        filename: Original filename
        
    Returns:
        str: Sanitized filename
    """
    # Remove invalid characters for most filesystems
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    
    # Remove leading/trailing spaces and dots
    filename = filename.strip(' .')
    
    # Ensure filename is not empty
    if not filename:
        filename = "unnamed_file"
    
    return filename 