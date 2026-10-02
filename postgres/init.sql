CREATE TABLE IF NOT EXISTS transaction_scores (
    id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    score DOUBLE PRECISION NOT NULL,
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_created_at
    ON transaction_scores (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_transaction_scores_fraud
    ON transaction_scores (fraud_flag, created_at DESC);