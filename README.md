# Asset Price Database

A production-ready data warehouse for centralizing daily OHLCV (Open, High, Low, Close, Volume) data from multiple sources including equities, ETFs, and cryptocurrencies. The system provides complete data lineage tracking, quality control, and REST API access.

## 🎯 Current Status: Production Ready

✅ **10,920+ Stock Symbols** loaded from Yahoo Finance  
✅ **99.9% Exchange Coverage** (NYSE, NASDAQ, AMEX, TSX)  
✅ **8,408 CSV Files** successfully processed  
✅ **Production-Grade ETL** with quality scoring and validation  
✅ **Complete Data Lineage** tracking and audit trails  

## 📊 Database Statistics

| Exchange | Symbols | Percentage |
|----------|---------|------------|
| NASDAQ   | 4,540   | 41.6%      |
| AMEX     | 3,586   | 32.8%      |
| NYSE     | 2,422   | 22.2%      |
| TSX      | 371     | 3.4%       |
| **Total** | **10,919** | **99.9%** |

## 🏗️ Architecture

```
Raw Data Sources → Staging → QC/Validation → Raw Tables → Gold Views → API
     ↓              ↓           ↓              ↓          ↓        ↓
  CSV/API       stage_*    Quality Score   price_raw  price_gold  REST
```

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- PostgreSQL 15+ (or use Docker setup)

### Setup
```bash
# Clone repository
git clone https://github.com/NashC/asset-price-database.git
cd asset-price-database

# Create virtual environment
uv venv --python 3.11
source .venv/bin/activate

# Install dependencies
uv pip install -e .

# Start database
docker-compose up -d

# Apply schema
alembic upgrade head

# Seed initial data
psql -h localhost -U assetuser -d assetpricedb -f db/seeds/data_source_seed.sql
```

## 💾 Data Loading

### Load Individual CSV Files
```bash
# Load stock data
asset-price-db load data/samples/AAPL.csv --symbol AAPL --asset-type STOCK --exchange NASDAQ

# Load crypto data
asset-price-db load data/samples/BTC.csv --symbol BTC --asset-type CRYPTO

# Validate before loading
asset-price-db validate data/samples/AAPL.csv
```

### Bulk Loading (Production)
```bash
# Load entire directory of Yahoo Finance CSV files
python bulk_load_yahoo_data_optimized.py

# Monitor progress and quality scores
asset-price-db status --view price_gold
```

## 🔍 Querying Data

### Python API
```python
from app.db_client import StockDB
from datetime import date

# Initialize client
db = StockDB()

# Get price data
prices = db.prices(['AAPL', 'MSFT'], 
                  start=date(2024, 1, 1), 
                  end=date(2024, 1, 31))

# Filter by exchange
nasdaq_stocks = db.get_available_symbols(asset_type='STOCK', exchange='NASDAQ')

# Get asset information
asset_info = db.get_asset_info('AAPL')
```

### REST API
```bash
# Start API server
uvicorn app.fastapi_server:app --reload

# Query endpoints
curl "http://localhost:8000/symbols?asset_type=STOCK&exchange=NASDAQ"
curl "http://localhost:8000/prices?symbols=AAPL&start_date=2024-01-01&end_date=2024-01-31"
curl "http://localhost:8000/assets/AAPL"
```

### Direct SQL
```sql
-- Get latest prices by exchange
SELECT symbol, price_date, close_price, exchange
FROM price_gold 
WHERE exchange = 'NASDAQ' 
ORDER BY price_date DESC 
LIMIT 10;

-- Exchange distribution
SELECT exchange, COUNT(*) as symbol_count
FROM asset 
WHERE asset_type = 'STOCK' 
GROUP BY exchange 
ORDER BY symbol_count DESC;
```

## 📁 Project Structure

```
asset_price_database/
├── app/                    # Application layer
│   ├── db_client.py       # Database client with query methods
│   ├── fastapi_server.py  # REST API server
│   └── utils.py           # Utility functions
├── etl/                   # ETL pipeline
│   ├── cli.py            # Command-line interface
│   ├── staging.py        # CSV loading and staging
│   ├── qc.py             # Quality control and validation
│   ├── loaders.py        # Data loading and asset management
│   └── gold_refresh.py   # Materialized view management
├── db/                   # Database schema and seeds
│   ├── ddl/              # DDL scripts (tables, views, indexes)
│   └── seeds/            # Initial data and configuration
├── data/                 # Data files
│   ├── samples/          # Sample CSV files for testing
│   └── symbol_exchanges/ # Exchange symbol mappings
└── tests/                # Comprehensive test suite
```

## 🛠️ Key Features

- **Multi-Asset Support**: Stocks, ETFs, crypto, indices, bonds, commodities
- **Complete Data Lineage**: Full audit trail for all data sources
- **Quality Control**: 0-100 scoring with comprehensive validation
- **Exchange Metadata**: 99.9% coverage for stock symbols
- **Materialized Views**: Fast analytics with deduplicated datasets
- **Bulk Loading**: Optimized for processing thousands of CSV files
- **Production ETL**: Batch processing, error handling, and recovery
- **REST API**: FastAPI with Pydantic models and OpenAPI docs
- **Docker Environment**: PostgreSQL 15 + PGAdmin for development

## 📈 Development Phases

- **✅ M1**: Core daily price loader with ETL pipeline
- **🔄 M2**: Corporate actions (dividends, splits)
- **🔄 M3**: Intraday data with partitioning
- **🔄 M4**: Complete REST API with authentication

## 🧪 Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=etl --cov=app --cov-report=html

# Run specific test categories
pytest -m unit        # Unit tests only
pytest -m integration # Integration tests only
```

## 📊 Monitoring & Health

```bash
# Check warehouse status
asset-price-db status

# Refresh materialized views
asset-price-db refresh --concurrent

# Health check
curl http://localhost:8000/health
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run quality checks: `pre-commit run --all-files`
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🔗 Links

- **Repository**: https://github.com/NashC/asset-price-database
- **Documentation**: See `.cursor/rules/` for comprehensive guides
- **API Docs**: http://localhost:8000/docs (when server is running) 