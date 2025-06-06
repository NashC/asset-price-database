"""
Pytest configuration and shared fixtures for Stock Warehouse tests.

Provides database fixtures, test data, and common test utilities.
"""

import os
import tempfile
from pathlib import Path
from datetime import date, datetime
from typing import Generator, Dict, Any

import pytest
import pandas as pd
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from etl.config import get_settings, reload_settings
from app.db_client import StockDB


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """
    Provide a PostgreSQL test container for the test session.
    """
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def test_database_url(postgres_container: PostgresContainer) -> str:
    """
    Get database URL for test container.
    """
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session")
def test_engine(test_database_url: str):
    """
    Create SQLAlchemy engine for test database.
    """
    engine = create_engine(test_database_url)
    return engine


@pytest.fixture(scope="session")
def setup_test_schema(test_engine):
    """
    Set up test database schema by running DDL scripts.
    """
    # Get the path to DDL files
    ddl_path = Path(__file__).parent.parent / "db" / "ddl"
    
    # Execute DDL files in order
    ddl_files = [
        "001_core.sql",
        "002_corp_actions.sql", 
        "003_intraday.sql",
        "999_views.sql"
    ]
    
    with test_engine.connect() as conn:
        for ddl_file in ddl_files:
            ddl_file_path = ddl_path / ddl_file
            if ddl_file_path.exists():
                with open(ddl_file_path, 'r') as f:
                    ddl_content = f.read()
                
                # Execute DDL (split by semicolon for multiple statements)
                for statement in ddl_content.split(';'):
                    statement = statement.strip()
                    if statement:
                        try:
                            conn.execute(text(statement))
                        except Exception as e:
                            # Some statements might fail in test environment, log and continue
                            print(f"DDL statement failed (continuing): {e}")
        
        conn.commit()
    
    return test_engine


@pytest.fixture
def test_settings(test_database_url: str, tmp_path: Path):
    """
    Override settings for testing.
    """
    # Set environment variables for testing
    os.environ["DATABASE_URL"] = test_database_url
    os.environ["DATA_LANDING_PATH"] = str(tmp_path / "landing")
    os.environ["DATA_ARCHIVE_PATH"] = str(tmp_path / "archive")
    os.environ["LOG_LEVEL"] = "DEBUG"
    
    # Reload settings to pick up test environment
    settings = reload_settings()
    
    yield settings
    
    # Cleanup environment variables
    for key in ["DATABASE_URL", "DATA_LANDING_PATH", "DATA_ARCHIVE_PATH", "LOG_LEVEL"]:
        if key in os.environ:
            del os.environ[key]


@pytest.fixture
def db_client(test_settings, setup_test_schema) -> StockDB:
    """
    Provide database client for testing.
    """
    return StockDB(test_settings.database_url)


@pytest.fixture
def sample_csv_data() -> pd.DataFrame:
    """
    Generate sample CSV data for testing.
    """
    dates = pd.date_range('2024-01-01', '2024-01-10', freq='D')
    
    data = []
    for i, date_val in enumerate(dates):
        base_price = 100 + i
        data.append({
            'Date': date_val.strftime('%Y-%m-%d'),
            'Open': base_price,
            'High': base_price + 2,
            'Low': base_price - 1,
            'Close': base_price + 1,
            'Volume': 1000000 + i * 10000,
            'Adj Close': base_price + 1
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def sample_csv_file(sample_csv_data: pd.DataFrame, tmp_path: Path) -> Path:
    """
    Create a sample CSV file for testing.
    """
    csv_file = tmp_path / "AAPL_test.csv"
    sample_csv_data.to_csv(csv_file, index=False)
    return csv_file


@pytest.fixture
def seed_test_data(db_client: StockDB, setup_test_schema):
    """
    Seed test database with sample data.
    """
    with db_client.engine.connect() as conn:
        # Insert test data sources
        conn.execute(text("""
            INSERT INTO data_source (source_name, source_type, is_active) VALUES
            ('TEST_SOURCE', 'FILE', true),
            ('TEST_API', 'API', true)
            ON CONFLICT (source_name) DO NOTHING
        """))
        
        # Insert test assets
        conn.execute(text("""
            INSERT INTO asset (symbol, asset_type, currency, exchange, company_name) VALUES
            ('AAPL', 'STOCK', 'USD', 'NASDAQ', 'Apple Inc.'),
            ('MSFT', 'STOCK', 'USD', 'NASDAQ', 'Microsoft Corporation'),
            ('BTC-USD', 'CRYPTO', 'USD', 'CRYPTO', 'Bitcoin')
            ON CONFLICT (symbol, asset_type) DO NOTHING
        """))
        
        # Insert test batch
        conn.execute(text("""
            INSERT INTO batch_meta (source_id, batch_name, status, quality_score) 
            SELECT 
                (SELECT source_id FROM data_source WHERE source_name = 'TEST_SOURCE'),
                'test_batch_001',
                'SUCCESS',
                95.5
        """))
        
        # Insert test price data
        conn.execute(text("""
            INSERT INTO price_raw (asset_id, batch_id, source_id, price_date, 
                                 open_price, high_price, low_price, close_price, volume)
            SELECT 
                a.asset_id,
                b.batch_id,
                b.source_id,
                '2024-01-01'::date,
                100.0,
                102.0,
                99.0,
                101.0,
                1000000
            FROM asset a, batch_meta b
            WHERE a.symbol = 'AAPL' 
              AND b.batch_name = 'test_batch_001'
        """))
        
        conn.commit()
    
    # Refresh materialized view
    with db_client.engine.connect() as conn:
        try:
            conn.execute(text("REFRESH MATERIALIZED VIEW price_gold"))
            conn.commit()
        except Exception:
            # View might not exist in test environment
            pass


@pytest.fixture
def mock_quality_report() -> Dict[str, Any]:
    """
    Mock quality report for testing.
    """
    return {
        'batch_name': 'test_batch',
        'timestamp': datetime.now().isoformat(),
        'row_count': 100,
        'column_count': 8,
        'quality_score': 85.5,
        'schema_valid': True,
        'duplicates': {'count': 2, 'percentage': 2.0},
        'outliers': {
            'extreme_values': [],
            'negative_prices': [],
            'zero_volumes': []
        },
        'summary_stats': {
            'unique_symbols': 1,
            'date_range': {'min': '2024-01-01', 'max': '2024-01-10'},
            'missing_values': {'symbol': 0, 'date_str': 0, 'close_str': 1}
        }
    }


@pytest.fixture
def temp_env_file(tmp_path: Path) -> Path:
    """
    Create temporary .env file for testing.
    """
    env_file = tmp_path / ".env"
    env_content = """
DATABASE_URL=postgresql://test:test@localhost:5432/testdb
QC_MIN_SCORE=75.0
BATCH_SIZE=1000
LOG_LEVEL=DEBUG
"""
    env_file.write_text(env_content)
    return env_file


# Test utilities
def assert_dataframe_equal(df1: pd.DataFrame, df2: pd.DataFrame, check_dtype: bool = False):
    """
    Assert that two DataFrames are equal with better error messages.
    """
    try:
        pd.testing.assert_frame_equal(df1, df2, check_dtype=check_dtype)
    except AssertionError as e:
        print(f"DataFrames are not equal:\n{e}")
        print(f"DF1 shape: {df1.shape}, DF2 shape: {df2.shape}")
        print(f"DF1 columns: {list(df1.columns)}")
        print(f"DF2 columns: {list(df2.columns)}")
        raise


def create_test_price_data(symbol: str, start_date: str, days: int = 10) -> pd.DataFrame:
    """
    Create test price data for a given symbol.
    """
    dates = pd.date_range(start_date, periods=days, freq='D')
    
    data = []
    base_price = 100.0
    
    for i, date_val in enumerate(dates):
        price = base_price + i * 0.5  # Slight upward trend
        data.append({
            'symbol': symbol,
            'date_str': date_val.strftime('%Y-%m-%d'),
            'open_str': str(price),
            'high_str': str(price + 1),
            'low_str': str(price - 0.5),
            'close_str': str(price + 0.5),
            'volume_str': str(1000000 + i * 10000),
            'adj_close_str': str(price + 0.5)
        })
    
    return pd.DataFrame(data) 