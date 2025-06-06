# Asset Price Database

A production-ready data warehouse for centralizing daily OHLCV data from multiple sources (equities, ETFs, cryptocurrencies) with comprehensive ETL pipeline, quality control, and REST API.

## 🎯 Features

- **Multi-Asset Support**: Stocks, ETFs, cryptocurrencies, indices, bonds, commodities
- **Source Lineage**: Complete data provenance tracking across providers
- **Quality Control**: Automated validation with 0-100 scoring system
- **ETL Pipeline**: Staging → Validation → Loading → Gold refresh
- **REST API**: FastAPI-based endpoints for data access (Phase M4)
- **Materialized Views**: Fast query performance with `price_gold` aggregations
- **CLI Interface**: Production-ready command-line tools
- **Docker Support**: Containerized PostgreSQL + PGAdmin stack

## 🏗️ Architecture

```
Raw Data Sources → Staging → QC/Validation → Raw Tables → Gold Views → API
     ↓              ↓           ↓              ↓          ↓        ↓
  CSV/API       stage_*    Quality Score   price_raw  price_gold  REST
```

### Database Schema

- **Core Tables**: `asset`, `price_raw`, `batch_meta`, `data_source`
- **Quality Control**: `data_quality_log` with automated scoring
- **Corporate Actions**: `corporate_action`, `dividend_cash`, `stock_split` (Phase M2)
- **Intraday Support**: Partitioned `price_raw_intraday` (Phase M3)
- **Gold Views**: Materialized `price_gold` for analytics

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+ (or use included Docker setup)
- Poetry (recommended) or pip

### Installation

1. **Clone & Setup**
   ```bash
   git clone https://github.com/NashC/asset-price-database.git
   cd asset_price_database
   poetry install  # or pip install -r requirements.txt
   ```

2. **Database Setup**
   ```bash
   # Start PostgreSQL + PGAdmin
   docker-compose up -d
   
   # Apply schema
   poetry run alembic upgrade head
   
   # Seed data sources
   psql -h localhost -U assetuser -d assetpricedb -f db/seeds/data_source_seed.sql
   ```

3. **Environment Configuration**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

### Basic Usage

```bash
# Load CSV data
poetry run asset-price-db load data/sample.csv --symbol AAPL --asset-type STOCK

# Validate data quality
poetry run asset-price-db validate data/sample.csv

# Check warehouse status
poetry run asset-price-db status

# Refresh materialized views
poetry run asset-price-db refresh

# List data sources
poetry run asset-price-db sources
```

## 📊 Data Pipeline

### 1. Staging (`etl/staging.py`)
- Bulk CSV loading with flexible column mapping
- Automatic symbol extraction from filenames
- Support for multiple date formats
- Metadata tracking (file path, row numbers)

### 2. Quality Control (`etl/qc.py`)
- **Completeness**: Missing value analysis
- **Validity**: Data format validation
- **Consistency**: OHLC relationship checks
- **Uniqueness**: Duplicate detection
- **Overall Score**: 0-100 weighted scoring

### 3. Loading (`etl/loaders.py`)
- Asset upsert with metadata enrichment
- Batch lineage tracking
- Price data insertion with conflict resolution
- Transaction management and rollback

### 4. Gold Refresh (`etl/gold_refresh.py`)
- Materialized view refresh (concurrent/blocking)
- Freshness validation
- Index optimization
- Automated scheduling (future)

## 🔧 CLI Commands

```bash
# Load data with full pipeline
asset-price-db load path/to/data.csv \
  --symbol AAPL \
  --asset-type STOCK \
  --exchange NASDAQ \
  --company-name "Apple Inc." \
  --sector Technology

# Dry run validation only
asset-price-db load data.csv --dry-run

# Refresh views
asset-price-db refresh --concurrent

# Get warehouse statistics
asset-price-db status --view price_gold

# Validate CSV without loading
asset-price-db validate data.csv

# Short alias also available
apdb load data/samples/BTC.csv --symbol BTC --asset-type CRYPTO
```

## 🌐 REST API (Phase M4)

Start the API server:
```bash
poetry run uvicorn app.fastapi_server:app --reload
```

### Example Endpoints

```bash
# Health check
curl http://localhost:8000/health

# Get available symbols
curl http://localhost:8000/symbols?asset_type=STOCK

# Fetch price data
curl "http://localhost:8000/prices?symbols=AAPL,MSFT&start_date=2024-01-01&end_date=2024-01-31"

# Asset information
curl http://localhost:8000/assets/AAPL

# Price summary statistics
curl http://localhost:8000/prices/AAPL/summary?days=30
```

## 🐳 Docker Setup

The included `docker-compose.yml` provides:
- **PostgreSQL 15**: Main database with health checks
- **PGAdmin**: Web interface at `http://localhost:8080`

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f

# Access PGAdmin
# URL: http://localhost:8080
# Email: admin@assetpricedb.com
# Password: admin123
```

## 📁 Project Structure

```
asset_price_database/
├── etl/                     # ETL pipeline modules
│   ├── config.py           # Settings & environment
│   ├── staging.py          # CSV loading & staging
│   ├── qc.py               # Quality control & scoring
│   ├── loaders.py          # Data insertion & batch management
│   ├── gold_refresh.py     # Materialized view refresh
│   └── cli.py              # Command-line interface
├── app/                     # Application layer
│   ├── db_client.py        # Database client with helper queries
│   ├── fastapi_server.py   # REST API server (Phase M4)
│   └── utils.py            # Utility functions
├── db/                      # Database schema & seeds
│   ├── ddl/                # Data definition language
│   │   ├── 001_core.sql    # Core tables & constraints
│   │   ├── 002_corp_actions.sql  # Corporate actions (M2)
│   │   ├── 003_intraday.sql      # Intraday tables (M3)
│   │   └── 999_views.sql   # Materialized views
│   └── seeds/              # Initial data
├── tests/                   # Test suite
│   ├── conftest.py         # Pytest configuration
│   ├── test_qc.py          # Quality control tests
│   └── test_loaders.py     # Data loading tests
├── docs/                    # Documentation
└── docker-compose.yml      # Container orchestration
```

## 🧪 Testing

```bash
# Run all tests
poetry run pytest

# Run with coverage
poetry run pytest --cov=etl --cov=app --cov-report=html

# Run specific test categories
poetry run pytest -m unit
poetry run pytest -m integration

# Run tests in verbose mode
poetry run pytest -v
```

### Test Environment
- Uses `testcontainers` for isolated PostgreSQL instances
- Automatic schema setup and teardown
- Comprehensive fixtures for test data

## ⚙️ Configuration

Environment variables (`.env` file):

```bash
# Database
DATABASE_URL=postgresql://assetuser:assetpass@localhost:5432/assetpricedb

# Quality Control
QC_MIN_SCORE=75.0
QC_MAX_NULL_PCT=5.0
QC_MAX_DUPLICATE_PCT=1.0

# ETL Settings
BATCH_SIZE=10000
MAX_WORKERS=4

# Logging
LOG_LEVEL=INFO
LOG_FILE=./logs/etl.log

# API Settings (Phase M4)
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=1

# External APIs (Future)
ALPHA_VANTAGE_API_KEY=your_key_here
POLYGON_API_KEY=your_key_here
```

## 📈 Roadmap

### ✅ Phase M1: Core Loader (COMPLETED)
- [x] Daily bars in `price_gold`
- [x] Complete ETL pipeline
- [x] Quality control system
- [x] CLI interface
- [x] Docker setup

### 🔄 Phase M2: Corporate Actions (IN PROGRESS)
- [ ] Dividend tracking
- [ ] Stock splits handling
- [ ] Total return calculations
- [ ] Adjusted price history

### 📊 Phase M3: Intraday Data (PLANNED)
- [ ] 1-minute bars support
- [ ] Partitioned storage strategy
- [ ] Real-time ingestion
- [ ] Market hours validation

### 🌐 Phase M4: API v1 (SCAFFOLDED)
- [ ] Complete REST endpoints
- [ ] Authentication & rate limiting
- [ ] WebSocket streaming
- [ ] API documentation

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Setup

```bash
# Install with development dependencies
poetry install --with dev

# Install pre-commit hooks
poetry run pre-commit install

# Run code formatting
poetry run black .
poetry run isort .

# Run type checking
poetry run mypy etl/ app/

# Run linting
poetry run flake8 etl/ app/
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [docs/PLANNING.md](docs/PLANNING.md)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions

## 🏆 Acknowledgments

- Built with modern Python 3.11+ features
- PostgreSQL 15 for robust data storage
- SQLAlchemy 2.x for modern ORM capabilities
- FastAPI for high-performance API endpoints
- Pydantic v2 for data validation 