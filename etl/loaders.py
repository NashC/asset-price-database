"""
Data loaders for inserting validated data into core warehouse tables.

Handles asset management, batch tracking, and price data insertion.
"""

from datetime import datetime, date
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from .config import get_settings


@dataclass
class BatchMeta:
    """Metadata for an ETL batch."""
    source_id: int
    batch_name: str
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    quality_score: Optional[float] = None


def upsert_asset(symbol: str, asset_type: str, currency: str = "USD", 
                exchange: Optional[str] = None, company_name: Optional[str] = None,
                sector: Optional[str] = None) -> int:
    """
    Insert or update asset and return asset_id.
    
    Args:
        symbol: Asset symbol (e.g., 'AAPL')
        asset_type: Type of asset ('STOCK', 'ETF', 'CRYPTO', etc.)
        currency: Currency code (default: 'USD')
        exchange: Exchange code (e.g., 'NASDAQ')
        company_name: Full company name
        sector: Business sector
        
    Returns:
        int: asset_id of the upserted asset
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            # First, try to find existing asset
            result = conn.execute(text("""
                SELECT asset_id FROM asset 
                WHERE symbol = :symbol AND asset_type = :asset_type
            """), {
                'symbol': symbol,
                'asset_type': asset_type
            })
            
            existing = result.fetchone()
            if existing:
                asset_id = existing[0]
                logger.debug(f"Found existing asset {symbol} with ID {asset_id}")
                
                # Update metadata if provided
                if company_name or sector or exchange:
                    conn.execute(text("""
                        UPDATE asset 
                        SET company_name = COALESCE(:company_name, company_name),
                            sector = COALESCE(:sector, sector),
                            exchange = COALESCE(:exchange, exchange),
                            updated_at = NOW()
                        WHERE asset_id = :asset_id
                    """), {
                        'asset_id': asset_id,
                        'company_name': company_name,
                        'sector': sector,
                        'exchange': exchange
                    })
                    conn.commit()
                    logger.debug(f"Updated metadata for asset {symbol}")
                
                return asset_id
            
            # Insert new asset
            result = conn.execute(text("""
                INSERT INTO asset (symbol, asset_type, currency, exchange, 
                                 company_name, sector)
                VALUES (:symbol, :asset_type, :currency, :exchange, 
                        :company_name, :sector)
                RETURNING asset_id
            """), {
                'symbol': symbol,
                'asset_type': asset_type,
                'currency': currency,
                'exchange': exchange,
                'company_name': company_name,
                'sector': sector
            })
            
            asset_id = result.scalar()
            conn.commit()
            
            logger.info(f"Created new asset {symbol} ({asset_type}) with ID {asset_id}")
            return asset_id
            
    except Exception as e:
        logger.error(f"Failed to upsert asset {symbol}: {e}")
        raise


def insert_batch(meta: BatchMeta) -> int:
    """
    Create batch metadata record and return batch_id.
    
    Args:
        meta: Batch metadata object
        
    Returns:
        int: batch_id of the created batch
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                INSERT INTO batch_meta (source_id, batch_name, file_path, 
                                      file_size_bytes, row_count, quality_score)
                VALUES (:source_id, :batch_name, :file_path, 
                        :file_size_bytes, :row_count, :quality_score)
                RETURNING batch_id
            """), {
                'source_id': meta.source_id,
                'batch_name': meta.batch_name,
                'file_path': meta.file_path,
                'file_size_bytes': meta.file_size_bytes,
                'row_count': meta.row_count,
                'quality_score': meta.quality_score
            })
            
            batch_id = result.scalar()
            conn.commit()
            
            logger.info(f"Created batch {meta.batch_name} with ID {batch_id}")
            return batch_id
            
    except Exception as e:
        logger.error(f"Failed to create batch {meta.batch_name}: {e}")
        raise


def insert_price_rows(df: pd.DataFrame, asset_id: int, batch_id: int, 
                     source_id: int) -> int:
    """
    Insert price data into price_raw table with conflict handling.
    
    Args:
        df: DataFrame with price data (from staging)
        asset_id: Asset ID for the prices
        batch_id: Batch ID for lineage
        source_id: Data source ID
        
    Returns:
        int: Number of rows successfully inserted
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    if df.empty:
        logger.warning("No price data to insert")
        return 0
    
    # Prepare data for insertion
    price_records = []
    skipped_rows = 0
    
    for _, row in df.iterrows():
        try:
            # Parse and validate data
            price_date = datetime.strptime(str(row['date_str']), '%Y-%m-%d').date()
            open_price = float(row['open_str'])
            high_price = float(row['high_str'])
            low_price = float(row['low_str'])
            close_price = float(row['close_str'])
            
            # Optional fields
            volume = None
            if pd.notna(row.get('volume_str')):
                try:
                    volume = int(float(row['volume_str']))
                except (ValueError, TypeError):
                    volume = None
            
            adj_close_price = None
            if pd.notna(row.get('adj_close_str')):
                try:
                    adj_close_price = float(row['adj_close_str'])
                except (ValueError, TypeError):
                    adj_close_price = None
            
            # Basic validation
            if not (high_price >= max(open_price, close_price) and
                   low_price <= min(open_price, close_price) and
                   high_price >= low_price and
                   all(p > 0 for p in [open_price, high_price, low_price, close_price])):
                logger.warning(f"Invalid OHLC data for {price_date}, skipping")
                skipped_rows += 1
                continue
            
            price_records.append({
                'asset_id': asset_id,
                'batch_id': batch_id,
                'source_id': source_id,
                'price_date': price_date,
                'open_price': open_price,
                'high_price': high_price,
                'low_price': low_price,
                'close_price': close_price,
                'volume': volume,
                'adj_close_price': adj_close_price
            })
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse row {row.get('row_number', '?')}: {e}")
            skipped_rows += 1
            continue
    
    if not price_records:
        logger.error("No valid price data to insert after validation")
        return 0
    
    # Batch insert with proper ON CONFLICT handling
    inserted_count = 0
    batch_size = settings.batch_size
    
    try:
        with engine.connect() as conn:
            # Process records in batches
            for i in range(0, len(price_records), batch_size):
                batch_records = price_records[i:i + batch_size]
                batch_inserted = _insert_price_batch(conn, batch_records)
                inserted_count += batch_inserted
            
            conn.commit()
            
            logger.success(f"Inserted {inserted_count} price rows, skipped {skipped_rows} invalid rows")
            return inserted_count
            
    except Exception as e:
        logger.error(f"Failed to insert price data: {e}")
        raise


def _insert_price_batch(conn, price_records: List[Dict]) -> int:
    """
    Insert a batch of price records using proper SQL with ON CONFLICT.
    
    Args:
        conn: Database connection
        price_records: List of price record dictionaries
        
    Returns:
        int: Number of records inserted
    """
    if not price_records:
        return 0
    
    # Build the SQL for batch insert with ON CONFLICT
    insert_sql = """
        INSERT INTO price_raw (
            asset_id, batch_id, source_id, price_date, granularity,
            open_price, high_price, low_price, close_price, volume, adj_close_price
        ) VALUES (
            :asset_id, :batch_id, :source_id, :price_date, 'DAILY',
            :open_price, :high_price, :low_price, :close_price, :volume, :adj_close_price
        )
        ON CONFLICT (asset_id, price_date, source_id, granularity) 
        DO UPDATE SET
            batch_id = EXCLUDED.batch_id,
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            adj_close_price = EXCLUDED.adj_close_price,
            created_at = NOW()
    """
    
    inserted_count = 0
    
    for record in price_records:
        try:
            result = conn.execute(text(insert_sql), record)
            inserted_count += 1
        except Exception as e:
            logger.debug(f"Failed to insert/update price record: {e}")
            continue
    
    return inserted_count


def update_batch_status(batch_id: int, status: str, error_message: Optional[str] = None,
                       row_count: Optional[int] = None) -> None:
    """
    Update batch status and completion metadata.
    
    Args:
        batch_id: Batch ID to update
        status: New status ('SUCCESS', 'FAILED', 'PARTIAL')
        error_message: Error message if failed
        row_count: Final row count if different from initial
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE batch_meta 
                SET status = :status,
                    end_time = NOW(),
                    error_message = :error_message,
                    row_count = COALESCE(:row_count, row_count)
                WHERE batch_id = :batch_id
            """), {
                'batch_id': batch_id,
                'status': status,
                'error_message': error_message,
                'row_count': row_count
            })
            conn.commit()
            
            logger.info(f"Updated batch {batch_id} status to {status}")
            
    except Exception as e:
        logger.error(f"Failed to update batch status: {e}")
        raise


def get_source_id(source_name: str) -> int:
    """
    Get source_id for a given source name.
    
    Args:
        source_name: Name of the data source
        
    Returns:
        int: source_id
        
    Raises:
        ValueError: If source not found
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT source_id FROM data_source 
                WHERE source_name = :source_name AND is_active = true
            """), {'source_name': source_name})
            
            row = result.fetchone()
            if not row:
                raise ValueError(f"Data source '{source_name}' not found or inactive")
            
            return row[0]
            
    except Exception as e:
        logger.error(f"Failed to get source ID for {source_name}: {e}")
        raise


def load_corporate_actions(df: pd.DataFrame, asset_id: int, batch_id: int) -> int:
    """
    Load corporate actions data (Phase M2 - placeholder for now).
    
    Args:
        df: DataFrame with corporate actions data
        asset_id: Asset ID
        batch_id: Batch ID for lineage
        
    Returns:
        int: Number of actions loaded
    """
    # Placeholder implementation for Phase M2
    logger.info("Corporate actions loading not yet implemented (Phase M2)")
    return 0


def get_asset_info(symbol: str, asset_type: str) -> Optional[Dict[str, Any]]:
    """
    Get asset information by symbol and type.
    
    Args:
        symbol: Asset symbol
        asset_type: Asset type
        
    Returns:
        dict: Asset information or None if not found
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT asset_id, symbol, asset_type, currency, exchange, 
                       company_name, sector, is_active
                FROM asset 
                WHERE symbol = :symbol AND asset_type = :asset_type
            """), {
                'symbol': symbol,
                'asset_type': asset_type
            })
            
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
                    'is_active': row[7]
                }
            
            return None
            
    except Exception as e:
        logger.error(f"Failed to get asset info for {symbol}: {e}")
        raise 