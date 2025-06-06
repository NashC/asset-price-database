-- Core schema for Stock Data Warehouse
-- Contains: asset, data_source, batch_meta, price_raw tables

-- Asset types enumeration
CREATE TYPE asset_type_enum AS ENUM (
    'STOCK',
    'ETF', 
    'CRYPTO',
    'INDEX',
    'BOND',
    'COMMODITY'
);

-- Data granularity enumeration  
CREATE TYPE granularity_enum AS ENUM (
    'DAILY',
    'MIN1',
    'MIN5', 
    'MIN15',
    'MIN30',
    'HOUR1',
    'HOUR4'
);

-- Assets master table
CREATE TABLE asset (
    asset_id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    asset_type asset_type_enum NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    exchange VARCHAR(10),
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_asset_symbol_type UNIQUE (symbol, asset_type)
);

-- Data sources tracking
CREATE TABLE data_source (
    source_id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE,
    source_type VARCHAR(20) NOT NULL, -- 'API', 'FILE', 'MANUAL'
    base_url VARCHAR(255),
    api_key_required BOOLEAN NOT NULL DEFAULT false,
    rate_limit_per_minute INTEGER,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_source_type CHECK (source_type IN ('API', 'FILE', 'MANUAL'))
);

-- ETL batch metadata for lineage tracking
CREATE TABLE batch_meta (
    batch_id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES data_source(source_id),
    batch_name VARCHAR(100) NOT NULL,
    file_path VARCHAR(500),
    file_size_bytes BIGINT,
    row_count INTEGER,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    error_message TEXT,
    quality_score DECIMAL(5,2),
    
    CONSTRAINT chk_batch_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL'))
);

-- Raw price data with full lineage
CREATE TABLE price_raw (
    price_id BIGSERIAL PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES asset(asset_id),
    batch_id INTEGER NOT NULL REFERENCES batch_meta(batch_id),
    source_id INTEGER NOT NULL REFERENCES data_source(source_id),
    price_date DATE NOT NULL,
    granularity granularity_enum NOT NULL DEFAULT 'DAILY',
    open_price DECIMAL(15,6) NOT NULL,
    high_price DECIMAL(15,6) NOT NULL,
    low_price DECIMAL(15,6) NOT NULL,
    close_price DECIMAL(15,6) NOT NULL,
    volume BIGINT,
    adj_close_price DECIMAL(15,6),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- UNIQUE constraint needed for ON CONFLICT in loaders
    CONSTRAINT uq_price_raw_asset_date_source UNIQUE (asset_id, price_date, source_id, granularity),
    
    CONSTRAINT chk_price_ohlc CHECK (
        high_price >= open_price AND 
        high_price >= close_price AND
        low_price <= open_price AND 
        low_price <= close_price AND
        high_price >= low_price
    ),
    CONSTRAINT chk_positive_prices CHECK (
        open_price > 0 AND high_price > 0 AND 
        low_price > 0 AND close_price > 0
    )
);

-- Data quality log
CREATE TABLE data_quality_log (
    log_id SERIAL PRIMARY KEY,
    batch_id INTEGER NOT NULL REFERENCES batch_meta(batch_id),
    check_name VARCHAR(50) NOT NULL,
    check_result VARCHAR(20) NOT NULL,
    score DECIMAL(5,2),
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_quality_result CHECK (check_result IN ('PASS', 'WARN', 'FAIL'))
);

-- Staging table for raw CSV imports
CREATE TABLE stage_raw_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20),
    date_str VARCHAR(20),
    open_str VARCHAR(20),
    high_str VARCHAR(20), 
    low_str VARCHAR(20),
    close_str VARCHAR(20),
    volume_str VARCHAR(20),
    adj_close_str VARCHAR(20),
    source_file VARCHAR(500),
    row_number INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX idx_price_raw_asset_date ON price_raw(asset_id, price_date);
CREATE INDEX idx_price_raw_date ON price_raw(price_date);
CREATE INDEX idx_price_raw_batch ON price_raw(batch_id);
CREATE INDEX idx_price_raw_source ON price_raw(source_id);
CREATE INDEX idx_asset_symbol ON asset(symbol);
CREATE INDEX idx_asset_type ON asset(asset_type);
CREATE INDEX idx_batch_meta_source ON batch_meta(source_id);
CREATE INDEX idx_batch_meta_status ON batch_meta(status);
CREATE INDEX idx_stage_raw_prices_symbol ON stage_raw_prices(symbol);

-- Update trigger for asset.updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_asset_updated_at 
    BEFORE UPDATE ON asset 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column(); 