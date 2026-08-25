-- NX1: routing table for the Spark Connect frontend
-- Maps a hashed per-session bearer token to the engine that owns it, so that any Kyuubi instance
-- -- after a restart, or a second HA replica -- can route Spark Connect traffic for a session it
-- did not itself create. Engine locations are deliberately not stored: every instance rediscovers
-- those from the Kubernetes API server through its own driver pod informer.
CREATE TABLE IF NOT EXISTS spark_connect_session(
    key_id bigserial PRIMARY KEY,
    token_id varchar(64) NOT NULL,
    session_id varchar(36) NOT NULL,
    user_name varchar(128) NOT NULL,
    engine_tag varchar(36) NOT NULL,
    create_time bigint NOT NULL
);

COMMENT ON COLUMN spark_connect_session.key_id IS 'the auto increment key id';
COMMENT ON COLUMN spark_connect_session.token_id IS 'SHA-256 hex digest of the session bearer token';
COMMENT ON COLUMN spark_connect_session.session_id IS 'the Kyuubi session handle, which is an UUID';
COMMENT ON COLUMN spark_connect_session.user_name IS 'the user who owns the session';
COMMENT ON COLUMN spark_connect_session.engine_tag IS 'the kyuubi-unique-tag label value of the engine';
COMMENT ON COLUMN spark_connect_session.create_time IS 'the session create time';

CREATE UNIQUE INDEX IF NOT EXISTS spark_connect_session_unique_token_id_index ON spark_connect_session(token_id);
CREATE INDEX IF NOT EXISTS spark_connect_session_session_id_index ON spark_connect_session(session_id);
