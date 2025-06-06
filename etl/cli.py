"""
Command-line interface for Stock Warehouse ETL operations.

Provides commands for loading data, refreshing views, and managing the warehouse.
"""

import sys
from pathlib import Path
from typing import Optional

import click
import pandas as pd
from loguru import logger

from .config import get_settings
from .staging import copy_to_stage, get_staging_summary, validate_staging_data
from .qc import generate_quality_report, score_quality
from .loaders import (
    BatchMeta, upsert_asset, insert_batch, insert_price_rows, 
    update_batch_status, get_source_id
)
from .gold_refresh import refresh_daily_gold, get_view_stats


def setup_logging() -> None:
    """Configure logging for CLI operations."""
    settings = get_settings()
    
    # Remove default handler
    logger.remove()
    
    # Add console handler
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    # Add file handler if configured
    if settings.log_file:
        logger.add(
            settings.log_file,
            level=settings.log_level,
            rotation="10 MB",
            retention="30 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"
        )


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(verbose: bool) -> None:
    """Stock Warehouse ETL Command Line Interface."""
    if verbose:
        # Override log level for verbose mode
        settings = get_settings()
        settings.log_level = "DEBUG"
    
    setup_logging()
    logger.info("Stock Warehouse ETL CLI started")


@main.command()
@click.argument('csv_path', type=click.Path(exists=True, path_type=Path))
@click.option('--source', '-s', default='MANUAL_CSV', 
              help='Data source name (default: MANUAL_CSV)')
@click.option('--symbol', help='Override symbol (if not in CSV)')
@click.option('--asset-type', default='STOCK', 
              type=click.Choice(['STOCK', 'ETF', 'CRYPTO', 'INDEX']),
              help='Asset type (default: STOCK)')
@click.option('--exchange', help='Exchange code (e.g., NASDAQ)')
@click.option('--company-name', help='Company name')
@click.option('--sector', help='Business sector')
@click.option('--dry-run', is_flag=True, help='Validate only, do not load')
def load(csv_path: Path, source: str, symbol: Optional[str], asset_type: str,
         exchange: Optional[str], company_name: Optional[str], 
         sector: Optional[str], dry_run: bool) -> None:
    """
    Load price data from CSV file into the warehouse.
    
    CSV_PATH: Path to the CSV file containing price data
    """
    try:
        logger.info(f"Loading data from {csv_path}")
        
        # Step 1: Load into staging
        rows_staged = copy_to_stage(csv_path)
        logger.info(f"Staged {rows_staged} rows")
        
        # Step 2: Get staging summary
        summary = get_staging_summary()
        logger.info(f"Staging summary: {summary}")
        
        # Step 3: Validate staging data
        issues = validate_staging_data()
        if issues:
            logger.warning(f"Data quality issues found: {issues}")
        
        # Step 4: Load staging data for quality check
        from sqlalchemy import create_engine
        settings = get_settings()
        engine = create_engine(settings.database_url)
        
        staging_df = pd.read_sql("SELECT * FROM stage_raw_prices", engine)
        
        # Step 5: Generate quality report
        batch_name = f"{csv_path.stem}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
        quality_report = generate_quality_report(staging_df, batch_name)
        
        logger.info(f"Quality score: {quality_report['quality_score']:.1f}/100")
        
        if dry_run:
            logger.info("Dry run completed - no data loaded into warehouse")
            return
        
        # Check quality threshold
        settings = get_settings()
        if quality_report['quality_score'] < settings.qc_min_score:
            logger.error(f"Quality score {quality_report['quality_score']:.1f} below threshold {settings.qc_min_score}")
            sys.exit(1)
        
        # Step 6: Get or create asset
        if symbol:
            asset_symbol = symbol
        else:
            # Try to get symbol from staging data
            unique_symbols = staging_df['symbol'].unique()
            if len(unique_symbols) == 1:
                asset_symbol = unique_symbols[0]
            else:
                logger.error(f"Multiple symbols found: {unique_symbols}. Please specify --symbol")
                sys.exit(1)
        
        asset_id = upsert_asset(
            symbol=asset_symbol,
            asset_type=asset_type,
            exchange=exchange,
            company_name=company_name,
            sector=sector
        )
        
        # Step 7: Create batch record
        source_id = get_source_id(source)
        
        batch_meta = BatchMeta(
            source_id=source_id,
            batch_name=batch_name,
            file_path=str(csv_path),
            file_size_bytes=csv_path.stat().st_size,
            row_count=len(staging_df),
            quality_score=quality_report['quality_score']
        )
        
        batch_id = insert_batch(batch_meta)
        
        # Step 8: Insert price data
        try:
            inserted_count = insert_price_rows(staging_df, asset_id, batch_id, source_id)
            
            # Update batch status
            update_batch_status(batch_id, 'SUCCESS', row_count=inserted_count)
            
            logger.success(f"Successfully loaded {inserted_count} price records for {asset_symbol}")
            
        except Exception as e:
            update_batch_status(batch_id, 'FAILED', str(e))
            raise
        
        # Step 9: Refresh gold view
        logger.info("Refreshing materialized views...")
        refresh_daily_gold(concurrent=True)
        
        logger.success("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        sys.exit(1)


@main.command()
@click.option('--concurrent', is_flag=True, default=True,
              help='Use concurrent refresh (default: True)')
def refresh(concurrent: bool) -> None:
    """Refresh materialized views."""
    try:
        logger.info("Starting materialized view refresh")
        refresh_daily_gold(concurrent=concurrent)
        logger.success("Materialized view refresh completed")
        
    except Exception as e:
        logger.error(f"Refresh failed: {e}")
        sys.exit(1)


@main.command()
@click.option('--view', default='price_gold', help='View name to check')
def status(view: str) -> None:
    """Show warehouse status and statistics."""
    try:
        logger.info(f"Getting status for {view}")
        
        stats = get_view_stats(view)
        
        click.echo(f"\n📊 Warehouse Status - {view}")
        click.echo("=" * 50)
        
        if 'error' in stats:
            click.echo(f"❌ Error: {stats['error']}")
            return
        
        click.echo(f"Row Count:     {stats['row_count']:,}")
        if stats['symbol_count']:
            click.echo(f"Symbols:       {stats['symbol_count']:,}")
        if stats['min_date'] and stats['max_date']:
            click.echo(f"Date Range:    {stats['min_date']} to {stats['max_date']}")
        click.echo(f"Has Indexes:   {stats['has_indexes']}")
        click.echo(f"Is Populated:  {stats['is_populated']}")
        
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        sys.exit(1)


@main.command()
@click.argument('csv_path', type=click.Path(exists=True, path_type=Path))
def validate(csv_path: Path) -> None:
    """Validate CSV file without loading."""
    try:
        logger.info(f"Validating {csv_path}")
        
        # Load into staging for validation
        copy_to_stage(csv_path)
        
        # Get staging data
        from sqlalchemy import create_engine
        settings = get_settings()
        engine = create_engine(settings.database_url)
        
        staging_df = pd.read_sql("SELECT * FROM stage_raw_prices", engine)
        
        # Generate quality report
        batch_name = f"validation_{csv_path.stem}"
        quality_report = generate_quality_report(staging_df, batch_name)
        
        # Display results
        click.echo(f"\n📋 Validation Report - {csv_path.name}")
        click.echo("=" * 50)
        click.echo(f"Quality Score:    {quality_report['quality_score']:.1f}/100")
        click.echo(f"Row Count:        {quality_report['row_count']:,}")
        click.echo(f"Schema Valid:     {quality_report['schema_valid']}")
        click.echo(f"Duplicates:       {quality_report['duplicates']['count']} ({quality_report['duplicates']['percentage']:.1f}%)")
        
        if quality_report.get('summary_stats'):
            stats = quality_report['summary_stats']
            click.echo(f"Unique Symbols:   {stats.get('unique_symbols', 'N/A')}")
            if stats.get('date_range'):
                date_range = stats['date_range']
                click.echo(f"Date Range:       {date_range.get('min', 'N/A')} to {date_range.get('max', 'N/A')}")
        
        # Show any issues
        outliers = quality_report.get('outliers', {})
        if any(outliers.values()):
            click.echo("\n⚠️  Issues Found:")
            for issue_type, issues in outliers.items():
                if issues:
                    click.echo(f"  {issue_type}: {len(issues)} rows")
        
        if quality_report['quality_score'] >= 75:
            click.echo("\n✅ Validation PASSED")
        else:
            click.echo("\n❌ Validation FAILED")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


@main.command()
def sources() -> None:
    """List available data sources."""
    try:
        from sqlalchemy import create_engine, text
        settings = get_settings()
        engine = create_engine(settings.database_url)
        
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT source_name, source_type, is_active, 
                       rate_limit_per_minute, api_key_required
                FROM data_source 
                ORDER BY source_type, source_name
            """))
            
            sources = result.fetchall()
        
        click.echo("\n📡 Available Data Sources")
        click.echo("=" * 50)
        
        for source in sources:
            status = "✅" if source[2] else "❌"
            api_key = "🔑" if source[4] else "🔓"
            rate_limit = f"({source[3]}/min)" if source[3] else ""
            
            click.echo(f"{status} {source[0]} ({source[1]}) {api_key} {rate_limit}")
        
    except Exception as e:
        logger.error(f"Failed to list sources: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 