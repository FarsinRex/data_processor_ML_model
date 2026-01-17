-- schema.sql - Updated with IF NOT EXISTS checks
DROP INDEX IF EXISTS idx_events_processed;
DROP INDEX IF EXISTS idx_events_timestamp;
DROP INDEX IF EXISTS idx_cost_metrics_timestamp;
-- Table 1: Store individual events (raw data)
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    data_size_kb FLOAT NOT NULL,
    priority VARCHAR(20) NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    batch_id INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Create indexes (DROP IF EXISTS first to avoid duplicates)
CREATE INDEX IF NOT EXISTS idx_events_processed ON events(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);

-- Table 2: Store batch processing metadata
CREATE TABLE IF NOT EXISTS batches (
    id SERIAL PRIMARY KEY,
    batch_size INTEGER NOT NULL,
    total_data_size_kb FLOAT NOT NULL,
    processing_time_seconds FLOAT,
    processing_cost FLOAT,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table 3: Store cost metrics for ML training
CREATE TABLE IF NOT EXISTS cost_metrics (
    id SERIAL PRIMARY KEY,
    batch_id INTEGER REFERENCES batches(id),
    batch_size INTEGER NOT NULL,
    total_data_kb FLOAT NOT NULL,
    processing_time_seconds FLOAT NOT NULL,
    cost_per_event FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cost_metrics_timestamp ON cost_metrics(timestamp);