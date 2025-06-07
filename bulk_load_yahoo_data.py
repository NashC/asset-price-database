#!/usr/bin/env python3
"""
Bulk loader for Yahoo Finance CSV files in data/stock_data_20250606/

Features:
- Progress tracking with estimated completion time
- Error handling and logging
- Resume capability (skips already loaded symbols)
- Parallel processing option
- Summary statistics
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import argparse

# Add the project root to Python path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.db_client import StockDB


class BulkLoader:
    def __init__(self, data_dir: str, source: str = "YAHOO_FINANCE_API", max_workers: int = 4):
        self.data_dir = Path(data_dir)
        self.source = source
        self.max_workers = max_workers
        self.db = StockDB()
        
        # Statistics
        self.total_files = 0
        self.processed_files = 0
        self.successful_loads = 0
        self.failed_loads = 0
        self.skipped_files = 0
        self.start_time = None
        
        # Error tracking
        self.errors = []
        
    def get_csv_files(self):
        """Get all CSV files in the data directory"""
        csv_files = list(self.data_dir.glob("*.csv"))
        self.total_files = len(csv_files)
        print(f"Found {self.total_files} CSV files to process")
        return csv_files
    
    def get_loaded_symbols(self):
        """Get symbols that are already loaded from YAHOO_FINANCE_API source"""
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
    
    def load_single_file(self, filepath: Path, skip_existing: bool = True) -> dict:
        """Load a single CSV file"""
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
            # Build command
            cmd = [
                'apdb', 'load', str(filepath),
                '--source', self.source,
                '--symbol', symbol,
                '--asset-type', 'STOCK'
            ]
            
            # Run the command
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per file
            )
            
            if process.returncode == 0:
                # Parse output for statistics
                output = process.stdout
                
                # Extract records loaded
                for line in output.split('\n'):
                    if 'Successfully loaded' in line and 'price records' in line:
                        try:
                            records = int(line.split('Successfully loaded')[1].split('price records')[0].strip())
                            result['records_loaded'] = records
                        except:
                            pass
                    elif 'Quality score:' in line:
                        try:
                            score = float(line.split('Quality score:')[1].split('/')[0].strip())
                            result['quality_score'] = score
                        except:
                            pass
                
                result['success'] = True
                result['message'] = 'Loaded successfully'
                
            else:
                result['message'] = f"Command failed: {process.stderr.strip()}"
                
        except subprocess.TimeoutExpired:
            result['message'] = "Timeout (>5 minutes)"
        except Exception as e:
            result['message'] = f"Error: {str(e)}"
            
        return result
    
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
    
    def load_sequential(self, csv_files: list, skip_existing: bool = True):
        """Load files sequentially"""
        print(f"\nStarting sequential loading of {len(csv_files)} files...")
        
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
            result = self.load_single_file(filepath, skip_existing)
            
            if result['success']:
                self.successful_loads += 1
            else:
                self.failed_loads += 1
                self.errors.append(result)
            
            self.processed_files += 1
            self.print_progress(self.processed_files, len(csv_files), self.start_time)
            
            # Brief pause to avoid overwhelming the system
            time.sleep(0.1)
    
    def load_parallel(self, csv_files: list, skip_existing: bool = True):
        """Load files in parallel"""
        print(f"\nStarting parallel loading of {len(csv_files)} files with {self.max_workers} workers...")
        
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
                executor.submit(self.load_single_file, filepath, skip_existing): filepath 
                for filepath in files_to_process
            }
            
            # Process completed jobs
            for future in as_completed(future_to_file):
                result = future.result()
                
                if result['success']:
                    self.successful_loads += 1
                else:
                    self.failed_loads += 1
                    self.errors.append(result)
                
                self.processed_files += 1
                self.print_progress(
                    self.processed_files + self.skipped_files, 
                    len(csv_files), 
                    self.start_time
                )
    
    def print_summary(self):
        """Print final summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0
        
        print(f"\n\n{'='*60}")
        print("BULK LOADING SUMMARY")
        print(f"{'='*60}")
        print(f"Total files found:     {self.total_files:,}")
        print(f"Files processed:       {self.processed_files:,}")
        print(f"Successful loads:      {self.successful_loads:,}")
        print(f"Failed loads:          {self.failed_loads:,}")
        print(f"Skipped (existing):    {self.skipped_files:,}")
        print(f"Total time:            {elapsed/60:.1f} minutes")
        print(f"Average rate:          {self.processed_files/elapsed:.1f} files/sec" if elapsed > 0 else "")
        
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
    
    def save_error_log(self, filename: str = None):
        """Save errors to a log file"""
        if not self.errors:
            return
            
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bulk_load_errors_{timestamp}.log"
        
        with open(filename, 'w') as f:
            f.write(f"Bulk Load Error Log - {datetime.now()}\n")
            f.write(f"{'='*60}\n\n")
            
            for error in self.errors:
                f.write(f"File: {error['file']}\n")
                f.write(f"Symbol: {error['symbol']}\n")
                f.write(f"Error: {error['message']}\n")
                f.write("-" * 40 + "\n")
        
        print(f"\nError log saved to: {filename}")


def main():
    parser = argparse.ArgumentParser(description="Bulk load Yahoo Finance CSV files")
    parser.add_argument("--data-dir", default="data/stock_data_20250606", 
                       help="Directory containing CSV files")
    parser.add_argument("--source", default="YAHOO_FINANCE_API",
                       help="Data source name")
    parser.add_argument("--parallel", action="store_true",
                       help="Use parallel processing")
    parser.add_argument("--workers", type=int, default=4,
                       help="Number of parallel workers")
    parser.add_argument("--no-skip", action="store_true",
                       help="Don't skip already loaded symbols")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be processed without loading")
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = BulkLoader(
        data_dir=args.data_dir,
        source=args.source,
        max_workers=args.workers
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
        
        print(f"\nDRY RUN SUMMARY:")
        print(f"Total files: {len(csv_files):,}")
        print(f"Would process: {to_process:,}")
        print(f"Would skip: {to_skip:,}")
        return
    
    # Confirm before proceeding
    print(f"\nAbout to load {len(csv_files):,} CSV files")
    print(f"Source: {args.source}")
    print(f"Mode: {'Parallel' if args.parallel else 'Sequential'}")
    print(f"Workers: {args.workers if args.parallel else 1}")
    print(f"Skip existing: {not args.no_skip}")
    
    response = input("\nProceed? (y/N): ").strip().lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    # Start loading
    try:
        if args.parallel:
            loader.load_parallel(csv_files, skip_existing=not args.no_skip)
        else:
            loader.load_sequential(csv_files, skip_existing=not args.no_skip)
            
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    
    # Print summary
    loader.print_summary()
    
    # Save error log if there were errors
    if loader.errors:
        loader.save_error_log()


if __name__ == "__main__":
    main() 