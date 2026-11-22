CREATE TABLE IF NOT EXISTS scores (
    id SERIAL PRIMARY KEY,
    transaction_id TEXT NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag INTEGER NOT NULL,
    model_version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    us_state TEXT,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS scores_created_at_idx ON scores (created_at);

CREATE ROLE grafana_reader LOGIN PASSWORD 'grafana_demo_read';
GRANT SELECT ON scores TO grafana_reader;
