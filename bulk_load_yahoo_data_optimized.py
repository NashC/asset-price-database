#!/usr/bin/env python3
"""
Optimized bulk loader for Yahoo Finance CSV files.

Key optimizations:
1. Batch materialized view refreshes (every 100 files instead of every file)
2. Direct database operations instead of subprocess calls
3. Connection pooling and reuse
4. Parallel processing with optimized worker count
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import argparse
from typing import List, Dict, Any

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.db_client import StockDB
from etl.staging import copy_to_stage, get_staging_summary
from etl.qc import generate_quality_report
from etl.loaders import upsert_asset, insert_batch, insert_price_rows, update_batch_status, get_source_id, BatchMeta
from etl.gold_refresh import refresh_daily_gold
from etl.config import get_settings


class OptimizedBulkLoader:
    def __init__(self, data_dir: str, source: str = "YAHOO_FINANCE_API", max_workers: int = 4, 
                 refresh_batch_size: int = 100):
        self.data_dir = Path(data_dir)
        self.source = source
        self.max_workers = max_workers
        self.refresh_batch_size = refresh_batch_size  # Refresh view every N files
        self.db = StockDB()
        
        # Statistics
        self.total_files = 0
        self.processed_files = 0
        self.successful_loads = 0
        self.failed_loads = 0
        self.skipped_files = 0
        self.start_time = None
        self.last_refresh_count = 0
        
        # Error tracking
        self.errors = []
        
        # Get source_id once
        self.source_id = get_source_id(source)
        
    def get_csv_files(self):
        """Get all CSV files in the data directory"""
        csv_files = list(self.data_dir.glob("*.csv"))
        self.total_files = len(csv_files)
        print(f"Found {self.total_files} CSV files to process")
        return csv_files
    
    def get_loaded_symbols(self):
        """Get symbols that are already loaded from source"""
        try:
            from sqlalchemy import text
            query = """
            SELECT DISTINCT a.symbol 
            FROM asset a 
            JOIN price_raw pr ON a.asset_id = pr.asset_id 
            JOIN data_source ds ON pr.source_id = ds.source_id 
            WHERE ds.source_name = :source_name
            """
            with self.db.engine.connect() as conn:
                result = conn.execute(text(query), {'source_name': self.source})
                loaded_symbols = {row[0] for row in result}
            print(f"Found {len(loaded_symbols)} symbols already loaded from {self.source}")
            return loaded_symbols
        except Exception as e:
            print(f"Warning: Could not check existing symbols: {e}")
            return set()
    
    def extract_symbol_from_filename(self, filepath: Path) -> str:
        """Extract symbol from filename (e.g., AAPL.csv -> AAPL)"""
        return filepath.stem.upper()
    
    def load_single_file_direct(self, filepath: Path) -> dict:
        """Load a single CSV file using direct database operations (no subprocess)"""
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
            # Step 1: Load to staging
            copy_to_stage(filepath)
            
            # Step 2: Get staging data
            from sqlalchemy import create_engine
            settings = get_settings()
            engine = create_engine(settings.database_url)
            staging_df = pd.read_sql("SELECT * FROM stage_raw_prices", engine)
            
            if staging_df.empty:
                result['message'] = "No data in staging table"
                return result
            
            # Step 3: Quality check
            batch_name = f"{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            quality_report = generate_quality_report(staging_df, batch_name)
            result['quality_score'] = quality_report['quality_score']
            
            # Check quality threshold
            settings = get_settings()
            if quality_report['quality_score'] < settings.qc_min_score:
                result['message'] = f"Quality score {quality_report['quality_score']:.1f} below threshold"
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
            
        except Exception as e:
            result['message'] = f"Error: {str(e)}"
            
        return result
    
    def should_refresh_view(self) -> bool:
        """Check if we should refresh the materialized view"""
        return (self.successful_loads - self.last_refresh_count) >= self.refresh_batch_size
    
    def refresh_view_if_needed(self):
        """Refresh materialized view if batch size reached"""
        if self.should_refresh_view():
            try:
                print(f"\n🔄 Refreshing materialized view (after {self.successful_loads} successful loads)...")
                refresh_daily_gold(concurrent=True)
                self.last_refresh_count = self.successful_loads
                print("✅ View refresh completed")
            except Exception as e:
                print(f"⚠️ View refresh failed: {e}")
    
    def print_progress(self, current: int, total: int, start_time: float):
        """Print progress with ETA"""
        if current == 0:
            return
            
        elapsed = time.time() - start_time
        rate = current / elapsed
        remaining = total - current
        eta_seconds = remaining / rate if rate > 0 else 0
        
        eta_str = f"{int(eta_seconds // 3600):02d}:{int((eta_seconds % 3600) // 60):02d}:{int(eta_seconds % 60):02d}"
        
        print(f"\rProgress: {current}/{total} ({current/total*100:.1f}%) | "
              f"Rate: {rate:.1f} files/sec | "
              f"ETA: {eta_str} | "
              f"Success: {self.successful_loads} | "
              f"Failed: {self.failed_loads} | "
              f"Skipped: {self.skipped_files}", end='', flush=True)
    
    def load_sequential_optimized(self, csv_files: list, skip_existing: bool = True):
        """Load files sequentially with optimizations"""
        print(f"\nStarting optimized sequential loading of {len(csv_files)} files...")
        print(f"Materialized view refresh every {self.refresh_batch_size} files")
        
        loaded_symbols = self.get_loaded_symbols() if skip_existing else set()
        
        self.start_time = time.time()
        
        for i, filepath in enumerate(csv_files):
            symbol = self.extract_symbol_from_filename(filepath)
            
            # Skip if already loaded
            if skip_existing and symbol in loaded_symbols:
                self.skipped_files += 1
                self.processed_files += 1
                self.print_progress(self.processed_files, len(csv_files), self.start_time)
                continue
            
            # Load the file
            result = self.load_single_file_direct(filepath)
            
            if result['success']:
                self.successful_loads += 1
                # Check if we should refresh the view
                self.refresh_view_if_needed()
            else:
                self.failed_loads += 1
                self.errors.append(result)
            
            self.processed_files += 1
            self.print_progress(self.processed_files, len(csv_files), self.start_time)
    
    def load_parallel_optimized(self, csv_files: list, skip_existing: bool = True):
        """Load files in parallel with optimizations"""
        print(f"\nStarting optimized parallel loading of {len(csv_files)} files with {self.max_workers} workers...")
        print(f"Materialized view refresh every {self.refresh_batch_size} files")
        
        loaded_symbols = self.get_loaded_symbols() if skip_existing else set()
        
        # Filter out already loaded symbols
        files_to_process = []
        for filepath in csv_files:
            symbol = self.extract_symbol_from_filename(filepath)
            if not skip_existing or symbol not in loaded_symbols:
                files_to_process.append(filepath)
            else:
                self.skipped_files += 1
        
        print(f"Processing {len(files_to_process)} files (skipping {self.skipped_files} already loaded)")
        
        self.start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all jobs
            future_to_file = {
                executor.submit(self.load_single_file_direct, filepath): filepath 
                for filepath in files_to_process
            }
            
            # Process completed jobs
            for future in as_completed(future_to_file):
                result = future.result()
                
                if result['success']:
                    self.successful_loads += 1
                    # Check if we should refresh the view
                    self.refresh_view_if_needed()
                else:
                    self.failed_loads += 1
                    self.errors.append(result)
                
                self.processed_files += 1
                self.print_progress(
                    self.processed_files + self.skipped_files, 
                    len(csv_files), 
                    self.start_time
                )
    
    def final_refresh(self):
        """Do final materialized view refresh"""
        if self.successful_loads > self.last_refresh_count:
            print(f"\n🔄 Final materialized view refresh...")
            try:
                refresh_daily_gold(concurrent=True)
                print("✅ Final refresh completed")
            except Exception as e:
                print(f"⚠️ Final refresh failed: {e}")
    
    def print_summary(self):
        """Print final summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        print(f"\n\n{'='*60}")
        print("OPTIMIZED BULK LOADING SUMMARY")
        print(f"{'='*60}")
        print(f"Total files found:     {self.total_files:,}")
        print(f"Files processed:       {self.processed_files:,}")
        print(f"Successful loads:      {self.successful_loads:,}")
        print(f"Failed loads:          {self.failed_loads:,}")
        print(f"Skipped (existing):    {self.skipped_files:,}")
        print(f"Total time:            {elapsed/60:.1f} minutes")
        print(f"Average rate:          {self.processed_files/elapsed:.1f} files/sec" if elapsed > 0 else "")
        print(f"View refreshes:        {(self.successful_loads // self.refresh_batch_size) + 1}")
        
        if self.errors:
            print(f"\n{'='*60}")
            print("ERRORS (First 10):")
            print(f"{'='*60}")
            for error in self.errors[:10]:
                print(f"File: {error['file']}")
                print(f"Symbol: {error['symbol']}")
                print(f"Error: {error['message']}")
                print("-" * 40)
            
            if len(self.errors) > 10:
                print(f"... and {len(self.errors) - 10} more errors")


def main():
    parser = argparse.ArgumentParser(description="Optimized bulk load Yahoo Finance CSV files")
    parser.add_argument("--data-dir", default="data/stock_data_20250606", 
                       help="Directory containing CSV files")
    parser.add_argument("--source", default="YAHOO_FINANCE_API",
                       help="Data source name")
    parser.add_argument("--parallel", action="store_true",
                       help="Use parallel processing")
    parser.add_argument("--workers", type=int, default=4,
                       help="Number of parallel workers")
    parser.add_argument("--refresh-batch", type=int, default=100,
                       help="Refresh materialized view every N files")
    parser.add_argument("--no-skip", action="store_true",
                       help="Don't skip already loaded symbols")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be processed without loading")
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = OptimizedBulkLoader(
        data_dir=args.data_dir,
        source=args.source,
        max_workers=args.workers,
        refresh_batch_size=args.refresh_batch
    )
    
    # Get files to process
    csv_files = loader.get_csv_files()
    
    if args.dry_run:
        loaded_symbols = loader.get_loaded_symbols() if not args.no_skip else set()
        to_process = 0
        to_skip = 0
        
        for filepath in csv_files:
            symbol = loader.extract_symbol_from_filename(filepath)
            if not args.no_skip and symbol in loaded_symbols:
                to_skip += 1
            else:
                to_process += 1
        
        print(f"\nOPTIMIZED DRY RUN SUMMARY:")
        print(f"Total files: {len(csv_files):,}")
        print(f"Would process: {to_process:,}")
        print(f"Would skip: {to_skip:,}")
        print(f"View refreshes: {(to_process // args.refresh_batch) + 1}")
        return
    
    # Confirm before proceeding
    print(f"\nAbout to load {len(csv_files):,} CSV files")
    print(f"Source: {args.source}")
    print(f"Mode: {'Parallel' if args.parallel else 'Sequential'} (OPTIMIZED)")
    print(f"Workers: {args.workers if args.parallel else 1}")
    print(f"Skip existing: {not args.no_skip}")
    print(f"Refresh batch size: {args.refresh_batch}")
    
    response = input("\nProceed? (y/N): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    # Start loading
    try:
        if args.parallel:
            loader.load_parallel_optimized(csv_files, skip_existing=not args.no_skip)
        else:
            loader.load_sequential_optimized(csv_files, skip_existing=not args.no_skip)
            
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    
    # Final refresh
    loader.final_refresh()
    
    # Print summary
    loader.print_summary()


if __name__ == "__main__":
    main() 