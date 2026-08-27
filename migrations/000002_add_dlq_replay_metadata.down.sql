DROP INDEX IF EXISTS idx_events_dlq_at;

ALTER TABLE events
    DROP CONSTRAINT IF EXISTS events_status_check;

ALTER TABLE events
    ADD CONSTRAINT events_status_check CHECK (
        status IN ('RECEIVED', 'PROCESSING', 'PROCESSED', 'FAILED', 'RETRYING', 'DLQ')
    );

ALTER TABLE events
    DROP COLUMN IF EXISTS original_offset,
    DROP COLUMN IF EXISTS original_partition,
    DROP COLUMN IF EXISTS original_topic,
    DROP COLUMN IF EXISTS replayed_at,
    DROP COLUMN IF EXISTS dlq_at,
    DROP COLUMN IF EXISTS last_failed_at,
    DROP COLUMN IF EXISTS first_failed_at;

