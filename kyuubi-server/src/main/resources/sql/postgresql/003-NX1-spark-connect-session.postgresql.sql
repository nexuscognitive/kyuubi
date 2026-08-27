-- NX1: routing table for the Spark Connect frontend
-- Binds a user to the Spark Connect engine that serves them, so that any Kyuubi instance -- after
-- a restart, or a second HA replica -- can route Spark Connect traffic for a session it did not
-- itself create. Engine locations are deliberately not stored: every instance rediscovers those
-- from the Kubernetes API server through its own driver pod informer.
-- Nothing derived from the caller's own credential is stored. Callers present the platform
-- credential they already hold and it is resolved through Kyuubi's authentication chain on every
-- call, so it is neither written here nor digested here. engine_token is Kyuubi's own credential
-- for one engine, and has to be readable because the instance that relays a call is not
-- necessarily the one that launched the driver.
CREATE TABLE IF NOT EXISTS spark_connect_session(
    key_id bigserial PRIMARY KEY,
    user_name varchar(128) NOT NULL,
    session_id varchar(36) NOT NULL,
    engine_tag varchar(36) NOT NULL,
    engine_token varchar(64) NOT NULL,
    create_time bigint NOT NULL,
    generation int NOT NULL DEFAULT 0,
    restart_count int NOT NULL DEFAULT 0,
    last_restart_time bigint NOT NULL DEFAULT 0,
    recovery_state varchar(16) NOT NULL DEFAULT '',
    recovery_message text,
    driver_post_mortems text
);

COMMENT ON COLUMN spark_connect_session.key_id IS 'the auto increment key id';
COMMENT ON COLUMN spark_connect_session.user_name IS 'the user the engine belongs to';
COMMENT ON COLUMN spark_connect_session.session_id IS 'the Kyuubi session handle, empty once it has closed';
COMMENT ON COLUMN spark_connect_session.engine_tag IS 'the kyuubi-unique-tag label value of the engine';
COMMENT ON COLUMN spark_connect_session.engine_token IS 'the credential Kyuubi presents to the engine';
COMMENT ON COLUMN spark_connect_session.create_time IS 'the binding create time';
COMMENT ON COLUMN spark_connect_session.generation IS 'how many engines this binding has had; a new one is a new Spark session';
COMMENT ON COLUMN spark_connect_session.restart_count IS 'how many times recovery has relaunched a driver';
COMMENT ON COLUMN spark_connect_session.last_restart_time IS 'when the most recent relaunch started';
COMMENT ON COLUMN spark_connect_session.recovery_state IS 'empty, RECOVERING or ABANDONED';
COMMENT ON COLUMN spark_connect_session.recovery_message IS 'why recovery is where it is, above all why it was abandoned';
COMMENT ON COLUMN spark_connect_session.driver_post_mortems IS 'JSON: what killed this binding drivers, newest first, captured while each pod still existed';

CREATE INDEX IF NOT EXISTS spark_connect_session_user_name_index ON spark_connect_session(user_name);
CREATE INDEX IF NOT EXISTS spark_connect_session_session_id_index ON spark_connect_session(session_id);
