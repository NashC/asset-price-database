-- Seed data for common data sources
-- Insert default data sources for the warehouse

INSERT INTO data_source (source_name, source_type, base_url, api_key_required, rate_limit_per_minute, is_active) VALUES
-- File-based sources
('KAGGLE_UPLOAD', 'FILE', NULL, false, NULL, true),
('MANUAL_CSV', 'FILE', NULL, false, NULL, true),
('YAHOO_CSV_DOWNLOAD', 'FILE', 'https://finance.yahoo.com', false, NULL, true),

-- API sources (for future integration)
('YAHOO_FINANCE_API', 'API', 'https://query1.finance.yahoo.com', false, 2000, true),
('ALPHA_VANTAGE', 'API', 'https://www.alphavantage.co/query', true, 5, true),
('POLYGON_IO', 'API', 'https://api.polygon.io', true, 1000, true),
('FINNHUB', 'API', 'https://finnhub.io/api/v1', true, 60, true),
('IEX_CLOUD', 'API', 'https://cloud.iexapis.com/stable', true, 100, true),

-- Crypto sources
('BINANCE_API', 'API', 'https://api.binance.com', false, 1200, true),
('COINBASE_API', 'API', 'https://api.exchange.coinbase.com', false, 10, true),
('CRYPTOCOMPARE', 'API', 'https://min-api.cryptocompare.com', true, 100, true),

-- Manual entry
('MANUAL_ENTRY', 'MANUAL', NULL, false, NULL, true)

ON CONFLICT (source_name) DO NOTHING;

-- Insert some sample assets for testing
INSERT INTO asset (symbol, asset_type, currency, exchange, company_name, sector) VALUES
-- Major US stocks
('AAPL', 'STOCK', 'USD', 'NASDAQ', 'Apple Inc.', 'Technology'),
('MSFT', 'STOCK', 'USD', 'NASDAQ', 'Microsoft Corporation', 'Technology'),
('GOOGL', 'STOCK', 'USD', 'NASDAQ', 'Alphabet Inc.', 'Technology'),
('AMZN', 'STOCK', 'USD', 'NASDAQ', 'Amazon.com Inc.', 'Consumer Discretionary'),
('TSLA', 'STOCK', 'USD', 'NASDAQ', 'Tesla Inc.', 'Consumer Discretionary'),
('META', 'STOCK', 'USD', 'NASDAQ', 'Meta Platforms Inc.', 'Technology'),
('NVDA', 'STOCK', 'USD', 'NASDAQ', 'NVIDIA Corporation', 'Technology'),

-- Major ETFs
('SPY', 'ETF', 'USD', 'NYSE', 'SPDR S&P 500 ETF Trust', 'Equity'),
('QQQ', 'ETF', 'USD', 'NASDAQ', 'Invesco QQQ Trust', 'Equity'),
('VTI', 'ETF', 'USD', 'NYSE', 'Vanguard Total Stock Market ETF', 'Equity'),
('IWM', 'ETF', 'USD', 'NYSE', 'iShares Russell 2000 ETF', 'Equity'),

-- Major cryptocurrencies
('BTC-USD', 'CRYPTO', 'USD', 'CRYPTO', 'Bitcoin', 'Cryptocurrency'),
('ETH-USD', 'CRYPTO', 'USD', 'CRYPTO', 'Ethereum', 'Cryptocurrency'),
('ADA-USD', 'CRYPTO', 'USD', 'CRYPTO', 'Cardano', 'Cryptocurrency'),
('SOL-USD', 'CRYPTO', 'USD', 'CRYPTO', 'Solana', 'Cryptocurrency')

ON CONFLICT (symbol, asset_type) DO NOTHING; 