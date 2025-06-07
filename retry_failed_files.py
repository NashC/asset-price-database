#!/usr/bin/env python3
"""
Retry failed files from bulk loading process.

This script processes the failed files sequentially to avoid race conditions
that occur during parallel processing of the staging table.
"""

import time
from pathlib import Path
from datetime import datetime

from etl.config import get_settings
from etl.staging import copy_to_stage
from etl.qc import generate_quality_report
from etl.loaders import BatchMeta, upsert_asset, insert_batch, insert_price_rows, update_batch_status, get_source_id
from etl.gold_refresh import refresh_daily_gold

import pandas as pd
from sqlalchemy import create_engine
from loguru import logger


class FailedFileRetry:
    def __init__(self, data_dir: str = "data/stock_data_20250606", source: str = "YAHOO_FINANCE_API"):
        self.data_dir = Path(data_dir)
        self.source = source
        self.source_id = get_source_id(source)
        
        # Statistics
        self.successful_loads = 0
        self.failed_loads = 0
        self.errors = []
        self.start_time = None
        
        # Known failed symbols from the bulk loading summary
        self.failed_symbols = [
            'ZEUS', 'NMAI', 'VALU', 'SPMA', 'PBMR', 'AIFD', 'WNC', 'GIL', 
            'MADE', 'RDCM'  # Add more as needed
        ]
    
    def extract_symbol_from_filename(self, filepath: Path) -> str:
        """Extract symbol from filename (e.g., AAPL.csv -> AAPL)"""
        return filepath.stem.upper()
    
    def get_failed_files(self) -> list:
        """Get list of CSV files for failed symbols"""
        failed_files = []
        
        # Try to load from missing_symbols.txt if it exists
        missing_symbols_file = Path("missing_symbols.txt")
        if missing_symbols_file.exists():
            with open(missing_symbols_file, 'r') as f:
                missing_symbols = {line.strip() for line in f if line.strip()}
            logger.info(f"Loaded {len(missing_symbols)} missing symbols from {missing_symbols_file}")
        else:
            # Fallback to hardcoded list
            missing_symbols = set(self.failed_symbols)
            logger.info(f"Using hardcoded list of {len(missing_symbols)} failed symbols")
        
        for csv_file in self.data_dir.glob("*.csv"):
            symbol = self.extract_symbol_from_filename(csv_file)
            if symbol in missing_symbols:
                failed_files.append(csv_file)
        
        return failed_files
    
    def get_all_failed_files_from_errors(self) -> list:
        """
        Alternative: Get all files that might have failed based on the error pattern.
        This is more comprehensive than the hardcoded list.
        """
        # You could implement logic here to identify failed files
        # For now, let's use a broader approach - retry any file that's not already loaded
        return list(self.data_dir.glob("*.csv"))
    
    def is_symbol_loaded(self, symbol: str) -> bool:
        """Check if symbol is already loaded in the database"""
        settings = get_settings()
        engine = create_engine(settings.database_url)
        
        try:
            with engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT COUNT(*) FROM asset 
                    WHERE symbol = :symbol AND is_active = true
                """), {'symbol': symbol})
                
                count = result.scalar()
                return count > 0
        except Exception as e:
            logger.error(f"Error checking if symbol {symbol} is loaded: {e}")
            return False
    
    def load_single_file(self, filepath: Path) -> dict:
        """Load a single CSV file (sequential, no race conditions)"""
        symbol = self.extract_symbol_from_filename(filepath)
        
        result = {
            'file': str(filepath),
            'symbol': symbol,
            'success': False,
            'message': '',
            'records_loaded': 0,
            'quality_score': 0.0
        }
        
        try:
            logger.info(f"Processing {symbol} from {filepath.name}")
            
            # Step 1: Load to staging (this will purge and reload)
            copy_to_stage(filepath)
            
            # Step 2: Get staging data immediately after loading
            settings = get_settings()
            engine = create_engine(settings.database_url)
            staging_df = pd.read_sql("SELECT * FROM stage_raw_prices", engine)
            
            if staging_df.empty:
                result['message'] = "No data in staging table after loading"
                logger.warning(f"No staging data for {symbol}")
                return result
            
            logger.info(f"Loaded {len(staging_df)} rows to staging for {symbol}")
            
            # Step 3: Quality check
            batch_name = f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            quality_report = generate_quality_report(staging_df, batch_name)
            result['quality_score'] = quality_report['quality_score']
            
            # Check quality threshold
            if quality_report['quality_score'] < settings.qc_min_score:
                result['message'] = f"Quality score {quality_report['quality_score']:.1f} below threshold"
                logger.warning(f"Quality score too low for {symbol}: {quality_report['quality_score']:.1f}")
                return result
            
            # Step 4: Create/get asset
            asset_id = upsert_asset(
                symbol=symbol,
                asset_type='STOCK',
                exchange=None,
                company_name=None,
                sector=None
            )
            
            # Step 5: Create batch record
            batch_meta = BatchMeta(
                source_id=self.source_id,
                batch_name=batch_name,
                file_path=str(filepath),
                file_size_bytes=filepath.stat().st_size,
                row_count=len(staging_df),
                quality_score=quality_report['quality_score']
            )
            
            batch_id = insert_batch(batch_meta)
            
            # Step 6: Insert price data
            inserted_count = insert_price_rows(staging_df, asset_id, batch_id, self.source_id)
            
            # Update batch status
            update_batch_status(batch_id, 'SUCCESS', row_count=inserted_count)
            
            result['success'] = True
            result['message'] = 'Loaded successfully'
            result['records_loaded'] = inserted_count
            
            logger.success(f"Successfully loaded {inserted_count} records for {symbol}")
            
        except Exception as e:
            result['message'] = f"Error: {str(e)}"
            logger.error(f"Failed to load {symbol}: {e}")
            
        return result
    
    def retry_failed_files(self, skip_existing: bool = True):
        """Retry loading failed files sequentially"""
        # Get failed files - you can choose which method to use
        failed_files = self.get_failed_files()  # Use hardcoded list
        # failed_files = self.get_all_failed_files_from_errors()  # Use all files
        
        if skip_existing:
            # Filter out already loaded symbols
            files_to_process = []
            for filepath in failed_files:
                symbol = self.extract_symbol_from_filename(filepath)
                if not self.is_symbol_loaded(symbol):
                    files_to_process.append(filepath)
                else:
                    logger.info(f"Skipping {symbol} - already loaded")
            failed_files = files_to_process
        
        print(f"\nRetrying {len(failed_files)} failed files sequentially...")
        
        self.start_time = time.time()
        
        for i, filepath in enumerate(failed_files):
            print(f"\nProgress: {i+1}/{len(failed_files)} - Processing {filepath.name}")
            
            result = self.load_single_file(filepath)
            
            if result['success']:
                self.successful_loads += 1
                print(f"✅ Success: {result['symbol']} - {result['records_loaded']} records")
            else:
                self.failed_loads += 1
                self.errors.append(result)
                print(f"❌ Failed: {result['symbol']} - {result['message']}")
            
            # Small delay to avoid overwhelming the database
            time.sleep(0.1)
        
        # Final materialized view refresh
        if self.successful_loads > 0:
            print(f"\n🔄 Refreshing materialized view after {self.successful_loads} successful loads...")
            try:
                refresh_daily_gold(concurrent=True)
                print("✅ View refresh completed")
            except Exception as e:
                print(f"⚠️ View refresh failed: {e}")
    
    def print_summary(self):
        """Print final summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        print(f"\n\n{'='*60}")
        print("FAILED FILES RETRY SUMMARY")
        print(f"{'='*60}")
        print(f"Successful loads:      {self.successful_loads:,}")
        print(f"Failed loads:          {self.failed_loads:,}")
        print(f"Total time:            {elapsed/60:.1f} minutes")
        
        if self.errors:
            print(f"\nERRORS (First 10):")
            print(f"{'='*60}")
            for i, error in enumerate(self.errors[:10]):
                print(f"Symbol: {error['symbol']}")
                print(f"Error: {error['message']}")
                print("-" * 40)
                if i >= 9:
                    break
            
            if len(self.errors) > 10:
                print(f"... and {len(self.errors) - 10} more errors")


def main():
    """Main function to retry failed files"""
    retry_processor = FailedFileRetry()
    
    try:
        retry_processor.retry_failed_files(skip_existing=True)
    finally:
        retry_processor.print_summary()


if __name__ == "__main__":
    main() 