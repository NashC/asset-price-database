# Planning Doc – Stock-Data SQL Warehouse

## 1. Objectives
- Centralize daily OHLCV for **equities, ETFs, crypto**.
- Preserve **multi-source** lineage; expose "gold" dataset.
- Self-hosted, single-node Postgres; future upgrade path → TimescaleDB.

## 2. Milestones
| ID | Milestone | Deliverable |
|----|-----------|-------------|
| M1 | Core Loader | Daily bars in `price_gold`; etl/ finished. |
| M2 | Corporate Actions | `corporate_action`, `dividend_cash`; total-return view. |
| M3 | Intraday Bars | `price_raw_intraday`; 1-min BTC + AAPL demo. |
| M4 | API v1 | FastAPI endpoint `/prices`. |

## 3. Data Sources
### Current
- Kaggle stock market datasets (NYSE, NASDAQ, S&P 500)
- Manual CSV uploads

### Future Integration
- Yahoo Finance API
- Alpha Vantage
- Polygon.io
- Cryptocurrency exchanges (Binance, Coinbase)

## 4. Schema Design Principles
- **Immutable raw data**: Never modify `price_raw` after insert
- **Source lineage**: Every row tracks origin (file, API, batch)
- **Quality scoring**: Automated validation with 0-100 scores
- **Materialized views**: Fast queries via `price_gold` aggregations

## 5. ETL Pipeline Flow
```
Raw CSV/API → Staging → Validation → Raw Tables → Gold Views
     ↓           ↓          ↓           ↓          ↓
  Landing    stage_*    QC Checks   price_raw  price_gold
```

## 6. Open Questions
1. Confirm crypto exchange list for seed data.  
2. Desired granularity enum names (MIN1 vs 1MIN).  
3. Backup target paths on home-server NAS.
4. Corporate actions data sources and format standardization.
5. Intraday storage optimization (partitioning strategy).

## 7. Technical Decisions
- **Python 3.11+**: Modern async support, better type hints
- **SQLAlchemy 2.x**: Async ORM with better performance
- **Pydantic v2**: Fast validation and settings management
- **PostgreSQL 15**: Mature, reliable with JSON support
- **Alembic**: Database migration management
- **Poetry**: Dependency management and packaging

## 8. Performance Considerations
- Batch inserts for large datasets (10K+ rows)
- Materialized view refresh scheduling
- Index strategy for time-series queries
- Connection pooling for concurrent loads

## 9. Future Enhancements
- TimescaleDB migration for time-series optimization
- Real-time streaming data ingestion
- Machine learning feature engineering
- Multi-tenant support for different portfolios

---

_Last updated: 2024-01-15_ 