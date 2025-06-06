-- Corporate Actions Schema (Phase M2)
-- Contains: corporate_action, dividend_cash, stock_split tables

-- Corporate action types
CREATE TYPE corp_action_type_enum AS ENUM (
    'DIVIDEND_CASH',
    'DIVIDEND_STOCK', 
    'STOCK_SPLIT',
    'STOCK_MERGER',
    'SPIN_OFF',
    'RIGHTS_ISSUE',
    'SPECIAL_DIVIDEND'
);

-- Corporate actions master table
CREATE TABLE corporate_action (
    action_id SERIAL PRIMARY KEY,
    asset_id INTEGER NOT NULL REFERENCES asset(asset_id),
    action_type corp_action_type_enum NOT NULL,
    ex_date DATE NOT NULL,
    record_date DATE,
    payment_date DATE,
    announcement_date DATE,
    description TEXT,
    source_id INTEGER NOT NULL REFERENCES data_source(source_id),
    batch_id INTEGER REFERENCES batch_meta(batch_id),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_corp_action_dates CHECK (
        ex_date IS NOT NULL AND
        (record_date IS NULL OR record_date >= ex_date) AND
        (payment_date IS NULL OR payment_date >= ex_date)
    )
);

-- Cash dividend details
CREATE TABLE dividend_cash (
    dividend_id SERIAL PRIMARY KEY,
    action_id INTEGER NOT NULL REFERENCES corporate_action(action_id),
    dividend_amount DECIMAL(10,4) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    tax_rate DECIMAL(5,4),
    
    CONSTRAINT chk_dividend_positive CHECK (dividend_amount > 0)
);

-- Stock split details  
CREATE TABLE stock_split (
    split_id SERIAL PRIMARY KEY,
    action_id INTEGER NOT NULL REFERENCES corporate_action(action_id),
    split_ratio_from INTEGER NOT NULL,
    split_ratio_to INTEGER NOT NULL,
    
    CONSTRAINT chk_split_ratio CHECK (
        split_ratio_from > 0 AND split_ratio_to > 0
    )
);

-- Stock dividend details
CREATE TABLE dividend_stock (
    stock_div_id SERIAL PRIMARY KEY,
    action_id INTEGER NOT NULL REFERENCES corporate_action(action_id),
    dividend_ratio DECIMAL(8,6) NOT NULL,
    
    CONSTRAINT chk_stock_div_positive CHECK (dividend_ratio > 0)
);

-- Indexes for corporate actions
CREATE INDEX idx_corp_action_asset_date ON corporate_action(asset_id, ex_date);
CREATE INDEX idx_corp_action_ex_date ON corporate_action(ex_date);
CREATE INDEX idx_corp_action_type ON corporate_action(action_type);
CREATE INDEX idx_dividend_cash_action ON dividend_cash(action_id);
CREATE INDEX idx_stock_split_action ON stock_split(action_id); 