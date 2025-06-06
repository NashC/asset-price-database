-- Gold Dataset Views - Clean, analysis-ready data
-- Contains: price_gold materialized view and helper views

-- Main gold dataset - deduplicated daily prices with latest data preference
CREATE MATERIALIZED VIEW price_gold AS
WITH ranked_prices AS (
    SELECT 
        pr.*,
        a.symbol,
        a.asset_type,
        a.currency,
        a.exchange,
        a.company_name,
        ds.source_name,
        ROW_NUMBER() OVER (
            PARTITION BY pr.asset_id, pr.price_date 
            ORDER BY bm.start_time DESC, pr.created_at DESC
        ) as rn
    FROM price_raw pr
    JOIN asset a ON pr.asset_id = a.asset_id
    JOIN data_source ds ON pr.source_id = ds.source_id  
    JOIN batch_meta bm ON pr.batch_id = bm.batch_id
    WHERE a.is_active = true
      AND bm.status = 'SUCCESS'
      AND pr.granularity = 'DAILY'
)
SELECT 
    price_id,
    asset_id,
    symbol,
    asset_type,
    currency,
    exchange,
    company_name,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    adj_close_price,
    source_name,
    created_at
FROM ranked_prices 
WHERE rn = 1;

-- Unique index for fast lookups
CREATE UNIQUE INDEX idx_price_gold_asset_date ON price_gold(asset_id, price_date);
CREATE INDEX idx_price_gold_symbol_date ON price_gold(symbol, price_date);
CREATE INDEX idx_price_gold_date ON price_gold(price_date);
CREATE INDEX idx_price_gold_symbol ON price_gold(symbol);

-- Total return view (includes dividends and splits - Phase M2)
CREATE VIEW price_total_return AS
SELECT 
    pg.asset_id,
    pg.symbol,
    pg.price_date,
    pg.close_price,
    pg.adj_close_price,
    COALESCE(dc.dividend_amount, 0) as dividend_amount,
    CASE 
        WHEN ss.split_ratio_to IS NOT NULL 
        THEN CAST(ss.split_ratio_to AS DECIMAL) / ss.split_ratio_from
        ELSE 1.0 
    END as split_factor,
    -- Total return calculation (placeholder for Phase M2)
    pg.adj_close_price as total_return_price
FROM price_gold pg
LEFT JOIN corporate_action ca ON pg.asset_id = ca.asset_id 
    AND pg.price_date = ca.ex_date
LEFT JOIN dividend_cash dc ON ca.action_id = dc.action_id
    AND ca.action_type = 'DIVIDEND_CASH'
LEFT JOIN stock_split ss ON ca.action_id = ss.action_id
    AND ca.action_type = 'STOCK_SPLIT';

-- Asset summary statistics view
CREATE VIEW asset_summary AS
SELECT 
    a.asset_id,
    a.symbol,
    a.asset_type,
    a.currency,
    a.exchange,
    a.company_name,
    COUNT(pg.price_date) as trading_days,
    MIN(pg.price_date) as first_date,
    MAX(pg.price_date) as last_date,
    AVG(pg.close_price) as avg_price,
    MIN(pg.low_price) as min_price,
    MAX(pg.high_price) as max_price,
    AVG(pg.volume) as avg_volume,
    STDDEV(pg.close_price) as price_volatility
FROM asset a
LEFT JOIN price_gold pg ON a.asset_id = pg.asset_id
WHERE a.is_active = true
GROUP BY a.asset_id, a.symbol, a.asset_type, a.currency, 
         a.exchange, a.company_name;

-- Data quality summary view
CREATE VIEW data_quality_summary AS
SELECT 
    ds.source_name,
    DATE_TRUNC('month', bm.start_time) as month,
    COUNT(*) as total_batches,
    COUNT(CASE WHEN bm.status = 'SUCCESS' THEN 1 END) as successful_batches,
    COUNT(CASE WHEN bm.status = 'FAILED' THEN 1 END) as failed_batches,
    AVG(bm.quality_score) as avg_quality_score,
    SUM(bm.row_count) as total_rows_processed
FROM batch_meta bm
JOIN data_source ds ON bm.source_id = ds.source_id
GROUP BY ds.source_name, DATE_TRUNC('month', bm.start_time)
ORDER BY month DESC, ds.source_name;

-- Recent price changes view (last 30 days)
CREATE VIEW recent_price_changes AS
WITH price_changes AS (
    SELECT 
        symbol,
        asset_type,
        price_date,
        close_price,
        LAG(close_price) OVER (PARTITION BY asset_id ORDER BY price_date) as prev_close,
        LAG(price_date) OVER (PARTITION BY asset_id ORDER BY price_date) as prev_date
    FROM price_gold
    WHERE price_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    symbol,
    asset_type,
    price_date,
    close_price,
    prev_close,
    CASE 
        WHEN prev_close IS NOT NULL 
        THEN ROUND(((close_price - prev_close) / prev_close * 100)::numeric, 2)
        ELSE NULL 
    END as pct_change,
    close_price - prev_close as price_change
FROM price_changes
WHERE prev_close IS NOT NULL
ORDER BY price_date DESC, symbol;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_gold_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY price_gold;
    -- Add other materialized views here as they're created
END;
$$ LANGUAGE plpgsql; 