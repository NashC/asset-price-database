"""
Gold dataset refresh operations.

Handles materialized view refresh for clean, analysis-ready data.
"""

from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from loguru import logger
from sqlalchemy import create_engine, text

from .config import get_settings


def refresh_daily_gold(concurrent: bool = True) -> None:
    """
    Execute REFRESH MATERIALIZED VIEW for daily price gold dataset.
    
    Args:
        concurrent: Whether to use CONCURRENTLY option (slower but non-blocking)
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    # Check if concurrent refresh is possible (requires unique index)
    if concurrent and not _has_unique_index(engine, 'price_gold'):
        logger.warning("No unique index found on price_gold, falling back to blocking refresh")
        concurrent = False
    
    refresh_type = "CONCURRENTLY" if concurrent else ""
    
    try:
        logger.info(f"Starting {'concurrent' if concurrent else 'blocking'} refresh of price_gold")
        
        with engine.connect() as conn:
            # Refresh the materialized view
            refresh_sql = f"REFRESH MATERIALIZED VIEW {refresh_type} price_gold"
            conn.execute(text(refresh_sql))
            conn.commit()
            
        logger.success("Successfully refreshed price_gold materialized view")
        
    except Exception as e:
        logger.error(f"Failed to refresh price_gold: {e}")
        # If concurrent refresh fails, try non-concurrent as fallback
        if concurrent:
            logger.info("Retrying with non-concurrent refresh...")
            refresh_daily_gold(concurrent=False)
        else:
            raise


def _has_unique_index(engine, view_name: str) -> bool:
    """
    Check if a materialized view has a unique index (required for CONCURRENTLY).
    
    Args:
        engine: SQLAlchemy engine
        view_name: Name of the materialized view
        
    Returns:
        bool: True if unique index exists
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM pg_indexes 
                WHERE tablename = :view_name 
                AND indexdef LIKE '%UNIQUE%'
            """), {'view_name': view_name})
            
            unique_count = result.scalar()
            return unique_count > 0
            
    except Exception as e:
        logger.warning(f"Could not check for unique indexes on {view_name}: {e}")
        return False


def refresh_intraday_gold() -> None:
    """
    Refresh intraday gold views (Phase M3 - placeholder for now).
    """
    logger.info("Intraday gold refresh not yet implemented (Phase M3)")
    # Placeholder for future intraday materialized views


def refresh_all_views(concurrent: bool = True) -> None:
    """
    Refresh all materialized views in the warehouse.
    
    Args:
        concurrent: Whether to use concurrent refresh
    """
    logger.info("Starting refresh of all materialized views")
    
    views_to_refresh = [
        'price_gold'
        # Add more materialized views here as they're created
    ]
    
    if concurrent and len(views_to_refresh) > 1:
        # Refresh views in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_view = {
                executor.submit(_refresh_single_view, view, True): view 
                for view in views_to_refresh
            }
            
            for future in as_completed(future_to_view):
                view = future_to_view[future]
                try:
                    future.result()
                    logger.success(f"Refreshed {view}")
                except Exception as e:
                    logger.error(f"Failed to refresh {view}: {e}")
    else:
        # Refresh views sequentially
        for view in views_to_refresh:
            try:
                _refresh_single_view(view, concurrent)
                logger.success(f"Refreshed {view}")
            except Exception as e:
                logger.error(f"Failed to refresh {view}: {e}")
    
    logger.success("Completed refresh of all materialized views")


def _refresh_single_view(view_name: str, concurrent: bool) -> None:
    """
    Refresh a single materialized view.
    
    Args:
        view_name: Name of the materialized view
        concurrent: Whether to use CONCURRENTLY option
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    # Check if view exists
    if not _view_exists(engine, view_name):
        logger.warning(f"Materialized view {view_name} does not exist, skipping")
        return
    
    # Check for unique index if concurrent refresh requested
    if concurrent and not _has_unique_index(engine, view_name):
        logger.warning(f"No unique index on {view_name}, using blocking refresh")
        concurrent = False
    
    refresh_type = "CONCURRENTLY" if concurrent else ""
    
    try:
        with engine.connect() as conn:
            refresh_sql = f"REFRESH MATERIALIZED VIEW {refresh_type} {view_name}"
            conn.execute(text(refresh_sql))
            conn.commit()
    except Exception as e:
        # Fallback to non-concurrent if concurrent fails
        if concurrent:
            logger.warning(f"Concurrent refresh failed for {view_name}, trying blocking refresh")
            _refresh_single_view(view_name, False)
        else:
            raise


def _view_exists(engine, view_name: str) -> bool:
    """
    Check if a materialized view exists.
    
    Args:
        engine: SQLAlchemy engine
        view_name: Name of the materialized view
        
    Returns:
        bool: True if view exists
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM pg_matviews 
                WHERE matviewname = :view_name
            """), {'view_name': view_name})
            
            return result.scalar() > 0
            
    except Exception as e:
        logger.warning(f"Could not check if view {view_name} exists: {e}")
        return False


def get_view_stats(view_name: str = "price_gold") -> dict:
    """
    Get statistics about a materialized view.
    
    Args:
        view_name: Name of the materialized view
        
    Returns:
        dict: View statistics
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            # Check if view exists first
            if not _view_exists(engine, view_name):
                return {
                    'view_name': view_name,
                    'error': 'View does not exist'
                }
            
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {view_name}"))
            row_count = result.scalar()
            
            # Get date range and symbol count (if price view)
            if 'price' in view_name.lower():
                result = conn.execute(text(f"""
                    SELECT 
                        MIN(price_date) as min_date,
                        MAX(price_date) as max_date,
                        COUNT(DISTINCT symbol) as symbol_count
                    FROM {view_name}
                """))
                stats = result.fetchone()
            else:
                stats = (None, None, None)
            
            # Get materialized view metadata
            result = conn.execute(text("""
                SELECT 
                    schemaname,
                    matviewname,
                    hasindexes,
                    ispopulated
                FROM pg_matviews 
                WHERE matviewname = :view_name
            """), {'view_name': view_name})
            
            view_info = result.fetchone()
            
            return {
                'view_name': view_name,
                'row_count': row_count,
                'min_date': stats[0] if stats else None,
                'max_date': stats[1] if stats else None,
                'symbol_count': stats[2] if stats else None,
                'has_indexes': view_info[2] if view_info else None,
                'is_populated': view_info[3] if view_info else None
            }
            
    except Exception as e:
        logger.error(f"Failed to get stats for {view_name}: {e}")
        raise


def schedule_refresh_job() -> None:
    """
    Schedule automatic refresh of materialized views (future enhancement).
    
    This would integrate with a job scheduler like Celery or APScheduler.
    """
    logger.info("Scheduled refresh jobs not yet implemented")
    # Future: Integrate with job scheduler
    # - Daily refresh at off-peak hours
    # - Incremental refresh for large datasets
    # - Monitoring and alerting


def validate_view_freshness(view_name: str = "price_gold", 
                          max_age_hours: int = 24) -> bool:
    """
    Check if a materialized view is fresh enough.
    
    Args:
        view_name: Name of the materialized view
        max_age_hours: Maximum age in hours before view is considered stale
        
    Returns:
        bool: True if view is fresh, False if stale
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            # Check if view exists
            if not _view_exists(engine, view_name):
                logger.warning(f"View {view_name} does not exist")
                return False
            
            # Check the latest data in the view vs raw table
            result = conn.execute(text("""
                WITH view_latest AS (
                    SELECT MAX(created_at) as latest_view
                    FROM price_gold
                ),
                raw_latest AS (
                    SELECT MAX(created_at) as latest_raw
                    FROM price_raw
                )
                SELECT 
                    EXTRACT(EPOCH FROM (raw_latest.latest_raw - view_latest.latest_view)) / 3600 as hours_behind
                FROM view_latest, raw_latest
                WHERE view_latest.latest_view IS NOT NULL 
                AND raw_latest.latest_raw IS NOT NULL
            """))
            
            row = result.fetchone()
            if not row or row[0] is None:
                logger.warning(f"Could not determine freshness of {view_name}")
                return False
            
            hours_behind = row[0]
            is_fresh = hours_behind <= max_age_hours
            
            if not is_fresh:
                logger.warning(f"View {view_name} is {hours_behind:.1f} hours behind raw data")
            else:
                logger.debug(f"View {view_name} is {hours_behind:.1f} hours behind (acceptable)")
            
            return is_fresh
            
    except Exception as e:
        logger.error(f"Failed to validate freshness of {view_name}: {e}")
        return False


def optimize_view_indexes(view_name: str = "price_gold") -> None:
    """
    Analyze and optimize indexes on materialized views.
    
    Args:
        view_name: Name of the materialized view to optimize
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            # Check if view exists
            if not _view_exists(engine, view_name):
                logger.warning(f"View {view_name} does not exist")
                return
            
            # Analyze the table for better query planning
            conn.execute(text(f"ANALYZE {view_name}"))
            
            # Get index usage statistics
            result = conn.execute(text("""
                SELECT 
                    indexname,
                    idx_scan,
                    idx_tup_read,
                    idx_tup_fetch
                FROM pg_stat_user_indexes 
                WHERE relname = :view_name
            """), {'view_name': view_name})
            
            indexes = result.fetchall()
            
            logger.info(f"Analyzed {view_name} with {len(indexes)} indexes")
            
            for idx in indexes:
                logger.debug(f"Index {idx[0]}: {idx[1]} scans, {idx[2]} tuples read")
            
    except Exception as e:
        logger.error(f"Failed to optimize indexes for {view_name}: {e}")
        raise 