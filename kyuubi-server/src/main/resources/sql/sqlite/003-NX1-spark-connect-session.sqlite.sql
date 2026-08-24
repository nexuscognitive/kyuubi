-- NX1: routing table for the Spark Connect frontend
-- Maps a hashed per-session bearer token to the engine that owns it, so that any Kyuubi instance
-- -- after a restart, or a second HA replica -- can route Spark Connect traffic for a session it
-- did not itself create. Engine locations are deliberately not stored: every instance rediscovers
-- those from the Kubernetes API server through its own driver pod informer.
CREATE TABLE IF NOT EXISTS spark_connect_session(
    key_id INTEGER PRIMARY KEY AUTOINCREMENT, -- the auto increment key id
    token_id varchar(64) NOT NULL, -- SHA-256 hex digest of the session bearer token
    session_id varchar(36) NOT NULL, -- the Kyuubi session handle, which is an UUID
    user_name varchar(128) NOT NULL, -- the user who owns the session
    engine_tag varchar(36) NOT NULL, -- the kyuubi-unique-tag label value of the engine
    create_time bigint NOT NULL -- the session create time
);

CREATE UNIQUE INDEX IF NOT EXISTS spark_connect_session_unique_token_id_index ON spark_connect_session(token_id);

CREATE INDEX IF NOT EXISTS spark_connect_session_session_id_index ON spark_connect_session(session_id);
