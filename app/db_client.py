"""
Database client for Stock Warehouse.

Provides SQLAlchemy-based database access with helper methods for common queries.
"""

from datetime import date, datetime
from typing import List, Optional, Dict, Any
from contextlib import contextmanager

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from etl.config import get_settings


class StockDB:
    """
    Stock Warehouse database client.
    
    Provides connection management and helper methods for querying stock data.
    """
    
    def __init__(self, dsn: Optional[str] = None):
        """
        Initialize database client.
        
        Args:
            dsn: Database connection string. If None, uses settings from config.
        """
        if dsn is None:
            settings = get_settings()
            dsn = settings.database_url
        
        self.engine: Engine = create_engine(dsn, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        logger.debug(f"Initialized StockDB with engine: {self.engine.url}")
    
    @contextmanager
    def session(self):
        """Context manager for database sessions."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def prices(self, tickers: List[str], start: date, end: date, 
               table: str = "price_gold") -> pd.DataFrame:
        """
        Fetch daily price bars for given tickers and date range.
        
        Args:
            tickers: List of ticker symbols
            start: Start date (inclusive)
            end: End date (inclusive)
            table: Table/view to query (default: price_gold)
            
        Returns:
            pd.DataFrame: Price data with columns [symbol, price_date, open_price, 
                         high_price, low_price, close_price, volume, adj_close_price]
        """
        if not tickers:
            return pd.DataFrame()
        
        # Validate table name to prevent SQL injection
        allowed_tables = {'price_gold', 'price_raw', 'price_raw_intraday'}
        if table not in allowed_tables:
            raise ValueError(f"Invalid table name: {table}")
        
        # Convert tickers to uppercase for consistency
        tickers_upper = [ticker.upper() for ticker in tickers]
        
        # Use parameterized query with ANY operator for safety
        query = f"""
            SELECT 
                symbol,
                price_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                adj_close_price,
                asset_type,
                currency,
                exchange
            FROM {table}
            WHERE symbol = ANY(:symbols)
              AND price_date BETWEEN :start_date AND :end_date
            ORDER BY symbol, price_date
        """
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    text(query),
                    conn,
                    params={
                        'symbols': tickers_upper,
                        'start_date': start,
                        'end_date': end
                    }
                )
                
            logger.debug(f"Retrieved {len(df)} price records for {len(tickers)} tickers")
            return df
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch prices: {e}")
            raise
    
    def total_return(self, ticker: str, start: date, end: date) -> pd.Series:
        """
        Calculate total return series including dividends and splits (Phase M2).
        
        Args:
            ticker: Ticker symbol
            start: Start date
            end: End date
            
        Returns:
            pd.Series: Total return price series
        """
        # Placeholder for Phase M2 - currently returns adjusted close
        df = self.prices([ticker], start, end)
        
        if df.empty:
            return pd.Series(dtype=float)
        
        # For now, return adjusted close as proxy for total return
        return pd.Series(
            data=df['adj_close_price'].values,
            index=pd.to_datetime(df['price_date']),
            name=f"{ticker}_total_return"
        )
    
    def get_available_symbols(self, asset_type: Optional[str] = None) -> List[str]:
        """
        Get list of available symbols in the warehouse.
        
        Args:
            asset_type: Filter by asset type (STOCK, ETF, CRYPTO, etc.)
            
        Returns:
            List[str]: Available symbols
        """
        query = """
            SELECT DISTINCT symbol 
            FROM asset 
            WHERE is_active = true
        """
        
        params = {}
        if asset_type:
            query += " AND asset_type = :asset_type"
            params['asset_type'] = asset_type
        
        query += " ORDER BY symbol"
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                symbols = [row[0] for row in result]
                
            logger.debug(f"Found {len(symbols)} available symbols")
            return symbols
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get available symbols: {e}")
            raise
    
    def get_date_range(self, symbol: Optional[str] = None) -> Dict[str, date]:
        """
        Get available date range for data.
        
        Args:
            symbol: Optional symbol to check specific date range
            
        Returns:
            dict: Dictionary with 'min_date' and 'max_date' keys
        """
        query = """
            SELECT 
                MIN(price_date) as min_date,
                MAX(price_date) as max_date
            FROM price_gold
        """
        
        params = {}
        if symbol:
            query += " WHERE symbol = :symbol"
            params['symbol'] = symbol.upper()
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params)
                row = result.fetchone()
                
                if row and row[0] and row[1]:
                    return {
                        'min_date': row[0],
                        'max_date': row[1]
                    }
                else:
                    return {'min_date': None, 'max_date': None}
                    
        except SQLAlchemyError as e:
            logger.error(f"Failed to get date range: {e}")
            raise
    
    def get_asset_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed asset information.
        
        Args:
            symbol: Asset symbol
            
        Returns:
            dict: Asset information or None if not found
        """
        query = """
            SELECT 
                asset_id,
                symbol,
                asset_type,
                currency,
                exchange,
                company_name,
                sector,
                industry,
                market_cap,
                is_active,
                created_at,
                updated_at
            FROM asset 
            WHERE symbol = :symbol
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {'symbol': symbol.upper()})
                row = result.fetchone()
                
                if row:
                    return {
                        'asset_id': row[0],
                        'symbol': row[1],
                        'asset_type': row[2],
                        'currency': row[3],
                        'exchange': row[4],
                        'company_name': row[5],
                        'sector': row[6],
                        'industry': row[7],
                        'market_cap': row[8],
                        'is_active': row[9],
                        'created_at': row[10],
                        'updated_at': row[11]
                    }
                
                return None
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get asset info for {symbol}: {e}")
            raise
    
    def get_latest_prices(self, symbols: Optional[List[str]] = None, 
                         limit: int = 100) -> pd.DataFrame:
        """
        Get latest available prices for symbols.
        
        Args:
            symbols: List of symbols (if None, gets all symbols)
            limit: Maximum number of results
            
        Returns:
            pd.DataFrame: Latest prices
        """
        query = """
            WITH latest_dates AS (
                SELECT 
                    symbol,
                    MAX(price_date) as latest_date
                FROM price_gold
        """
        
        params = {'limit': limit}
        
        if symbols:
            symbols_upper = [s.upper() for s in symbols]
            query += " WHERE symbol = ANY(:symbols)"
            params['symbols'] = symbols_upper
        
        query += """
                GROUP BY symbol
            )
            SELECT 
                pg.symbol,
                pg.price_date,
                pg.close_price,
                pg.volume,
                pg.asset_type,
                pg.exchange
            FROM price_gold pg
            JOIN latest_dates ld ON pg.symbol = ld.symbol 
                AND pg.price_date = ld.latest_date
            ORDER BY pg.symbol
            LIMIT :limit
        """
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
                
            logger.debug(f"Retrieved latest prices for {len(df)} symbols")
            return df
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to get latest prices: {e}")
            raise
    
    def get_price_summary(self, symbol: str, days: int = 30) -> Dict[str, Any]:
        """
        Get price summary statistics for a symbol.
        
        Args:
            symbol: Asset symbol
            days: Number of recent days to analyze
            
        Returns:
            dict: Summary statistics
        """
        query = """
            SELECT 
                COUNT(*) as trading_days,
                MIN(low_price) as min_price,
                MAX(high_price) as max_price,
                AVG(close_price) as avg_price,
                STDDEV(close_price) as price_volatility,
                AVG(volume) as avg_volume,
                MIN(price_date) as start_date,
                MAX(price_date) as end_date,
                (SELECT close_price FROM price_gold 
                 WHERE symbol = :symbol 
                 ORDER BY price_date DESC LIMIT 1) as latest_price
            FROM price_gold 
            WHERE symbol = :symbol 
              AND price_date >= CURRENT_DATE - INTERVAL ':days days'
        """
        
        try:
            with self.engine.connect() as conn:
                # Note: Can't parameterize interval directly, so validate days parameter
                if not isinstance(days, int) or days < 1 or days > 365:
                    raise ValueError("Days must be an integer between 1 and 365")
                
                # Build safe query with validated days parameter
                safe_query = query.replace(':days', str(days))
                
                result = conn.execute(text(safe_query), {'symbol': symbol.upper()})
                row = result.fetchone()
                
                if row and row[0] > 0:
                    return {
                        'symbol': symbol.upper(),
                        'trading_days': row[0],
                        'min_price': float(row[1]) if row[1] else None,
                        'max_price': float(row[2]) if row[2] else None,
                        'avg_price': float(row[3]) if row[3] else None,
                        'price_volatility': float(row[4]) if row[4] else None,
                        'avg_volume': int(row[5]) if row[5] else None,
                        'start_date': row[6],
                        'end_date': row[7],
                        'latest_price': float(row[8]) if row[8] else None,
                        'analysis_days': days
                    }
                else:
                    return {'symbol': symbol.upper(), 'error': 'No data found'}
                    
        except (SQLAlchemyError, ValueError) as e:
            logger.error(f"Failed to get price summary for {symbol}: {e}")
            raise
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check.
        
        Returns:
            dict: Health check results
        """
        try:
            with self.engine.connect() as conn:
                # Test basic connectivity
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
                
                # Get basic stats
                result = conn.execute(text("""
                    SELECT 
                        (SELECT COUNT(*) FROM asset) as asset_count,
                        (SELECT COUNT(*) FROM price_raw) as price_count,
                        (SELECT COUNT(*) FROM batch_meta) as batch_count,
                        (SELECT MAX(price_date) FROM price_gold) as latest_date
                """))
                
                stats = result.fetchone()
                
                return {
                    'status': 'healthy',
                    'database_connected': True,
                    'asset_count': stats[0],
                    'price_count': stats[1],
                    'batch_count': stats[2],
                    'latest_date': stats[3],
                    'timestamp': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                'status': 'unhealthy',
                'database_connected': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            } 