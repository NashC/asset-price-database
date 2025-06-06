-- Intraday Price Schema (Phase M3)
-- Contains: price_raw_intraday table for minute/hourly bars

-- Intraday price data (partitioned by date for performance)
CREATE TABLE price_raw_intraday (
    price_id BIGSERIAL,
    asset_id INTEGER NOT NULL REFERENCES asset(asset_id),
    batch_id INTEGER NOT NULL REFERENCES batch_meta(batch_id),
    source_id INTEGER NOT NULL REFERENCES data_source(source_id),
    price_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    granularity granularity_enum NOT NULL,
    open_price DECIMAL(15,6) NOT NULL,
    high_price DECIMAL(15,6) NOT NULL,
    low_price DECIMAL(15,6) NOT NULL,
    close_price DECIMAL(15,6) NOT NULL,
    volume BIGINT,
    trade_count INTEGER,
    vwap DECIMAL(15,6), -- Volume Weighted Average Price
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_intraday_ohlc CHECK (
        high_price >= open_price AND 
        high_price >= close_price AND
        low_price <= open_price AND 
        low_price <= close_price AND
        high_price >= low_price
    ),
    CONSTRAINT chk_intraday_positive_prices CHECK (
        open_price > 0 AND high_price > 0 AND 
        low_price > 0 AND close_price > 0
    ),
    CONSTRAINT chk_intraday_granularity CHECK (
        granularity IN ('MIN1', 'MIN5', 'MIN15', 'MIN30', 'HOUR1', 'HOUR4')
    )
) PARTITION BY RANGE (price_datetime);

-- Create monthly partitions (example for 2024)
CREATE TABLE price_raw_intraday_2024_01 PARTITION OF price_raw_intraday
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE price_raw_intraday_2024_02 PARTITION OF price_raw_intraday
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

CREATE TABLE price_raw_intraday_2024_03 PARTITION OF price_raw_intraday
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

-- Add more partitions as needed...

-- Indexes for intraday data
CREATE INDEX idx_intraday_asset_datetime ON price_raw_intraday(asset_id, price_datetime);
CREATE INDEX idx_intraday_datetime ON price_raw_intraday(price_datetime);
CREATE INDEX idx_intraday_granularity ON price_raw_intraday(granularity);
CREATE INDEX idx_intraday_batch ON price_raw_intraday(batch_id);

-- Function to create new monthly partitions automatically
CREATE OR REPLACE FUNCTION create_monthly_partition(table_name TEXT, start_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    end_date DATE;
BEGIN
    partition_name := table_name || '_' || to_char(start_date, 'YYYY_MM');
    end_date := start_date + INTERVAL '1 month';
    
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
                    FOR VALUES FROM (%L) TO (%L)',
                   partition_name, table_name, start_date, end_date);
                   
    -- Create indexes on the new partition
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(asset_id, price_datetime)',
                   'idx_' || partition_name || '_asset_datetime', partition_name);
END;
$$ LANGUAGE plpgsql;

-- Market hours validation function
CREATE OR REPLACE FUNCTION is_market_hours(dt TIMESTAMP WITH TIME ZONE, exchange VARCHAR(10))
RETURNS BOOLEAN AS $$
BEGIN
    -- Simplified market hours check (extend as needed)
    CASE exchange
        WHEN 'NYSE', 'NASDAQ' THEN
            RETURN EXTRACT(DOW FROM dt) BETWEEN 1 AND 5 AND
                   EXTRACT(HOUR FROM dt AT TIME ZONE 'America/New_York') BETWEEN 9 AND 16;
        WHEN 'CRYPTO' THEN
            RETURN TRUE; -- 24/7 trading
        ELSE
            RETURN TRUE; -- Default to allow all hours
    END CASE;
END;
$$ LANGUAGE plpgsql; 