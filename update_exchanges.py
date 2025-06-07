#!/usr/bin/env python3
"""
Exchange Update Script for Asset Price Database

Updates stock symbols with their correct exchange information based on
exchange-specific symbol files with priority handling.

Priority Order: NYSE > NASDAQ > AMEX > TSX
"""

import logging
import sys
from pathlib import Path
from typing import Dict, Set, List, Tuple, Optional
from collections import defaultdict, Counter
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from etl.config import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('exchange_update.log')
    ]
)
logger = logging.getLogger(__name__)


class ExchangeUpdater:
    """Handles updating stock symbols with exchange information."""
    
    def __init__(self):
        """Initialize the exchange updater."""
        self.settings = get_settings()
        self.engine = create_engine(self.settings.database_url)
        self.exchange_files = {
            'NYSE': 'data/symbol_exchanges/NYSE.txt',
            'NASDAQ': 'data/symbol_exchanges/NASDAQ.txt', 
            'AMEX': 'data/symbol_exchanges/AMEX.txt',
            'TSX': 'data/symbol_exchanges/TSX.txt'
        }
        self.priority_order = ['NYSE', 'NASDAQ', 'AMEX', 'TSX']
        self.symbol_mapping: Dict[str, str] = {}
        self.exchange_stats: Dict[str, int] = {}
        
    def load_exchange_files(self) -> Dict[str, Set[str]]:
        """
        Load and parse all exchange symbol files.
        
        Returns:
            Dict mapping exchange name to set of symbols
        """
        exchange_symbols = {}
        
        for exchange, file_path in self.exchange_files.items():
            try:
                logger.info(f"Loading {exchange} symbols from {file_path}")
                
                # Read the file, skip header row
                df = pd.read_csv(file_path, sep='\t', header=0)
                
                # Extract symbols from first column, clean and normalize
                symbols = set()
                for symbol in df.iloc[:, 0]:  # First column
                    if pd.notna(symbol) and isinstance(symbol, str):
                        # Clean symbol: uppercase, strip whitespace
                        clean_symbol = str(symbol).strip().upper()
                        if clean_symbol and len(clean_symbol) <= 20:  # DB constraint
                            symbols.add(clean_symbol)
                
                exchange_symbols[exchange] = symbols
                self.exchange_stats[exchange] = len(symbols)
                logger.info(f"Loaded {len(symbols)} symbols for {exchange}")
                
            except Exception as e:
                logger.error(f"Failed to load {exchange} file {file_path}: {e}")
                exchange_symbols[exchange] = set()
                self.exchange_stats[exchange] = 0
        
        return exchange_symbols
    
    def analyze_symbol_overlaps(self, exchange_symbols: Dict[str, Set[str]]) -> Dict[str, List[str]]:
        """
        Analyze which symbols appear in multiple exchanges.
        
        Args:
            exchange_symbols: Dict mapping exchange to symbol sets
            
        Returns:
            Dict mapping symbol to list of exchanges it appears in
        """
        symbol_exchanges = defaultdict(list)
        
        for exchange, symbols in exchange_symbols.items():
            for symbol in symbols:
                symbol_exchanges[symbol].append(exchange)
        
        # Count overlaps
        overlap_counts = Counter(len(exchanges) for exchanges in symbol_exchanges.values())
        
        logger.info("Symbol overlap analysis:")
        for count, num_symbols in overlap_counts.items():
            if count == 1:
                logger.info(f"  {num_symbols} symbols appear in only 1 exchange")
            else:
                logger.info(f"  {num_symbols} symbols appear in {count} exchanges")
        
        # Log some examples of multi-exchange symbols
        multi_exchange = {sym: exch for sym, exch in symbol_exchanges.items() if len(exch) > 1}
        if multi_exchange:
            logger.info(f"Examples of multi-exchange symbols:")
            for symbol, exchanges in list(multi_exchange.items())[:10]:
                logger.info(f"  {symbol}: {', '.join(exchanges)}")
        
        return dict(symbol_exchanges)
    
    def create_priority_mapping(self, symbol_exchanges: Dict[str, List[str]]) -> Dict[str, str]:
        """
        Create symbol-to-exchange mapping using priority order.
        
        Args:
            symbol_exchanges: Dict mapping symbol to list of exchanges
            
        Returns:
            Dict mapping symbol to single exchange (highest priority)
        """
        mapping = {}
        priority_stats = Counter()
        
        for symbol, exchanges in symbol_exchanges.items():
            # Find highest priority exchange for this symbol
            selected_exchange = None
            for priority_exchange in self.priority_order:
                if priority_exchange in exchanges:
                    selected_exchange = priority_exchange
                    break
            
            if selected_exchange:
                mapping[symbol] = selected_exchange
                priority_stats[selected_exchange] += 1
        
        logger.info("Priority mapping results:")
        for exchange in self.priority_order:
            count = priority_stats[exchange]
            logger.info(f"  {exchange}: {count} symbols assigned")
        
        return mapping
    
    def get_database_symbols(self) -> Set[str]:
        """
        Get all stock symbols currently in the database.
        
        Returns:
            Set of symbols in database
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT symbol 
                    FROM asset 
                    WHERE asset_type = 'STOCK' AND is_active = true
                """))
                
                symbols = {row[0] for row in result}
                logger.info(f"Found {len(symbols)} stock symbols in database")
                return symbols
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to get database symbols: {e}")
            raise
    
    def validate_mapping(self, symbol_mapping: Dict[str, str], db_symbols: Set[str]) -> Tuple[Dict[str, str], Set[str]]:
        """
        Validate symbol mapping against database symbols.
        
        Args:
            symbol_mapping: Symbol to exchange mapping
            db_symbols: Set of symbols in database
            
        Returns:
            Tuple of (valid_mapping, unmatched_symbols)
        """
        valid_mapping = {}
        unmatched_symbols = set()
        
        # Find symbols in mapping that exist in database
        for symbol, exchange in symbol_mapping.items():
            if symbol in db_symbols:
                valid_mapping[symbol] = exchange
            else:
                unmatched_symbols.add(symbol)
        
        # Find database symbols not in any exchange file
        db_unmatched = db_symbols - set(symbol_mapping.keys())
        
        logger.info("Mapping validation results:")
        logger.info(f"  Symbols to update: {len(valid_mapping)}")
        logger.info(f"  Exchange symbols not in DB: {len(unmatched_symbols)}")
        logger.info(f"  DB symbols not in exchange files: {len(db_unmatched)}")
        
        # Log some examples of unmatched DB symbols
        if db_unmatched:
            logger.info("Examples of DB symbols not found in exchange files:")
            for symbol in list(db_unmatched)[:20]:
                logger.info(f"  {symbol}")
        
        return valid_mapping, db_unmatched
    
    def backup_current_exchanges(self) -> None:
        """Create backup of current exchange data."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT symbol, exchange 
                    FROM asset 
                    WHERE asset_type = 'STOCK' AND exchange IS NOT NULL
                """))
                
                backup_data = [(row[0], row[1]) for row in result]
                
                # Save to file
                backup_df = pd.DataFrame(backup_data, columns=['symbol', 'exchange'])
                backup_file = f"exchange_backup_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
                backup_df.to_csv(backup_file, index=False)
                
                logger.info(f"Backed up {len(backup_data)} existing exchange assignments to {backup_file}")
                
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise
    
    def update_exchanges_batch(self, symbol_mapping: Dict[str, str], batch_size: int = 1000) -> Dict[str, int]:
        """
        Update exchange information in batches.
        
        Args:
            symbol_mapping: Symbol to exchange mapping
            batch_size: Number of symbols to update per batch
            
        Returns:
            Dict with update statistics
        """
        stats = {
            'total_symbols': len(symbol_mapping),
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        symbols = list(symbol_mapping.items())
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        logger.info(f"Starting batch updates: {len(symbols)} symbols in {total_batches} batches")
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(symbols))
            batch_symbols = symbols[start_idx:end_idx]
            
            try:
                with self.engine.begin() as conn:  # Use transaction
                    batch_updated = 0
                    batch_skipped = 0
                    
                    for symbol, exchange in batch_symbols:
                        result = conn.execute(text("""
                            UPDATE asset 
                            SET exchange = :exchange, updated_at = NOW()
                            WHERE symbol = :symbol 
                              AND asset_type = 'STOCK' 
                              AND (exchange IS NULL OR exchange != :exchange)
                        """), {
                            'symbol': symbol,
                            'exchange': exchange
                        })
                        
                        if result.rowcount > 0:
                            batch_updated += 1
                        else:
                            batch_skipped += 1
                    
                    stats['updated'] += batch_updated
                    stats['skipped'] += batch_skipped
                    
                    logger.info(f"Batch {batch_num + 1}/{total_batches}: "
                              f"Updated {batch_updated}, Skipped {batch_skipped}")
                    
            except SQLAlchemyError as e:
                logger.error(f"Error in batch {batch_num + 1}: {e}")
                stats['errors'] += len(batch_symbols)
                continue
        
        return stats
    
    def verify_updates(self) -> Dict[str, int]:
        """
        Verify the exchange updates were successful.
        
        Returns:
            Dict with verification statistics
        """
        try:
            with self.engine.connect() as conn:
                # Count by exchange
                result = conn.execute(text("""
                    SELECT exchange, COUNT(*) as count
                    FROM asset 
                    WHERE asset_type = 'STOCK' AND is_active = true
                    GROUP BY exchange
                    ORDER BY count DESC
                """))
                
                exchange_counts = dict(result.fetchall())
                
                # Count nulls
                result = conn.execute(text("""
                    SELECT COUNT(*) as null_count
                    FROM asset 
                    WHERE asset_type = 'STOCK' AND is_active = true AND exchange IS NULL
                """))
                
                null_count = result.scalar()
                exchange_counts['NULL'] = null_count
                
                logger.info("Post-update exchange distribution:")
                for exchange, count in exchange_counts.items():
                    logger.info(f"  {exchange}: {count}")
                
                return exchange_counts
                
        except SQLAlchemyError as e:
            logger.error(f"Failed to verify updates: {e}")
            raise
    
    def run_update(self) -> None:
        """Execute the complete exchange update process."""
        try:
            logger.info("=== Starting Exchange Update Process ===")
            
            # Phase 1: Data Preparation
            logger.info("Phase 1: Loading exchange files...")
            exchange_symbols = self.load_exchange_files()
            
            if not any(exchange_symbols.values()):
                logger.error("No exchange symbols loaded. Aborting.")
                return
            
            # Analyze overlaps
            logger.info("Analyzing symbol overlaps...")
            symbol_exchanges = self.analyze_symbol_overlaps(exchange_symbols)
            
            # Create priority mapping
            logger.info("Creating priority mapping...")
            symbol_mapping = self.create_priority_mapping(symbol_exchanges)
            
            # Phase 2: Database Validation
            logger.info("Phase 2: Validating against database...")
            db_symbols = self.get_database_symbols()
            valid_mapping, unmatched = self.validate_mapping(symbol_mapping, db_symbols)
            
            if not valid_mapping:
                logger.error("No valid symbol mappings found. Aborting.")
                return
            
            # Phase 3: Backup and Update
            logger.info("Phase 3: Creating backup...")
            self.backup_current_exchanges()
            
            logger.info("Starting database updates...")
            update_stats = self.update_exchanges_batch(valid_mapping)
            
            # Phase 4: Verification
            logger.info("Phase 4: Verifying updates...")
            final_counts = self.verify_updates()
            
            # Final summary
            logger.info("=== Update Complete ===")
            logger.info(f"Total symbols processed: {update_stats['total_symbols']}")
            logger.info(f"Successfully updated: {update_stats['updated']}")
            logger.info(f"Skipped (no change): {update_stats['skipped']}")
            logger.info(f"Errors: {update_stats['errors']}")
            
            coverage_pct = (update_stats['updated'] / len(db_symbols)) * 100
            logger.info(f"Database coverage: {coverage_pct:.1f}%")
            
        except Exception as e:
            logger.error(f"Exchange update failed: {e}")
            raise


def main():
    """Main entry point."""
    updater = ExchangeUpdater()
    updater.run_update()


if __name__ == "__main__":
    main() 