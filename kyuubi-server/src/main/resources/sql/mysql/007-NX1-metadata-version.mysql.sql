SELECT '< NX1: add optimistic-lock version column to metadata' AS ' ';

-- Optimistic concurrency control for the metadata table. Existing rows default to 0; every
-- update issues `SET version = version + 1 WHERE ... AND version = <observed>` so concurrent
-- writers across Kyuubi instances cannot silently clobber each other.
ALTER TABLE metadata ADD COLUMN version int NOT NULL DEFAULT 0
    COMMENT 'optimistic-lock revision, incremented on every update';
