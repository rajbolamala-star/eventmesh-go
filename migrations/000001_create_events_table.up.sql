CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE NOT NULL,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    processing_started_at TIMESTAMPTZ NULL,
    processed_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT events_status_check CHECK (
        status IN ('RECEIVED', 'PROCESSING', 'PROCESSED', 'FAILED', 'RETRYING', 'DLQ', 'REPLAY_PENDING', 'REPLAYED')
    ),
    CONSTRAINT events_attempt_count_check CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events (status);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
