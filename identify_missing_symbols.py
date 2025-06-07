#!/usr/bin/env python3
"""
Identify missing symbols from the bulk loading process.

This script compares the CSV files in the data directory with the symbols
loaded in the database to identify which ones failed to load.
"""

from pathlib import Path
from sqlalchemy import create_engine, text
from etl.config import get_settings


def get_csv_symbols(data_dir: str = "data/stock_data_20250606") -> set:
    """Get all symbols from CSV files in the directory"""
    data_path = Path(data_dir)
    csv_files = list(data_path.glob("*.csv"))
    
    symbols = set()
    for csv_file in csv_files:
        symbol = csv_file.stem.upper()
        symbols.add(symbol)
    
    print(f"Found {len(csv_files)} CSV files with {len(symbols)} unique symbols")
    return symbols


def get_loaded_symbols() -> set:
    """Get all symbols currently loaded in the database"""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT DISTINCT symbol 
                FROM asset 
                WHERE is_active = true
            """))
            
            symbols = {row[0] for row in result}
            print(f"Found {len(symbols)} symbols loaded in database")
            return symbols
    except Exception as e:
        print(f"Error getting loaded symbols: {e}")
        return set()


def identify_missing_symbols(data_dir: str = "data/stock_data_20250606"):
    """Identify symbols that are in CSV files but not in database"""
    csv_symbols = get_csv_symbols(data_dir)
    loaded_symbols = get_loaded_symbols()
    
    missing_symbols = csv_symbols - loaded_symbols
    
    print(f"\n{'='*60}")
    print("MISSING SYMBOLS ANALYSIS")
    print(f"{'='*60}")
    print(f"Total CSV files:       {len(csv_symbols):,}")
    print(f"Loaded symbols:        {len(loaded_symbols):,}")
    print(f"Missing symbols:       {len(missing_symbols):,}")
    print(f"Success rate:          {(len(loaded_symbols)/len(csv_symbols)*100):.1f}%")
    
    if missing_symbols:
        print(f"\nMISSING SYMBOLS (First 50):")
        print(f"{'='*60}")
        sorted_missing = sorted(missing_symbols)
        for i, symbol in enumerate(sorted_missing[:50]):
            print(f"{symbol}")
            if i >= 49:
                break
        
        if len(missing_symbols) > 50:
            print(f"... and {len(missing_symbols) - 50} more")
        
        # Save to file for retry script
        missing_file = Path("missing_symbols.txt")
        with open(missing_file, 'w') as f:
            for symbol in sorted_missing:
                f.write(f"{symbol}\n")
        
        print(f"\nMissing symbols saved to: {missing_file}")
    
    return missing_symbols


def main():
    """Main function"""
    missing_symbols = identify_missing_symbols()
    
    if missing_symbols:
        print(f"\nTo retry these failed files, run:")
        print(f"python retry_failed_files.py")
    else:
        print(f"\n✅ All symbols successfully loaded!")


if __name__ == "__main__":
    main() 