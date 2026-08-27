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
    key_id INTEGER PRIMARY KEY AUTOINCREMENT, -- the auto increment key id
    user_name varchar(128) NOT NULL, -- the user the engine belongs to
    session_id varchar(36) NOT NULL, -- the Kyuubi session handle, empty once it has closed
    engine_tag varchar(36) NOT NULL, -- the kyuubi-unique-tag label value of the engine
    engine_token varchar(64) NOT NULL, -- the credential Kyuubi presents to the engine
    create_time bigint NOT NULL, -- the binding create time
    generation int NOT NULL DEFAULT 0, -- how many engines this binding has had, and a new one is a new Spark session
    restart_count int NOT NULL DEFAULT 0, -- how many times recovery has relaunched a driver
    last_restart_time bigint NOT NULL DEFAULT 0, -- when the most recent relaunch started
    recovery_state varchar(16) NOT NULL DEFAULT '', -- empty, RECOVERING or ABANDONED
    recovery_message text, -- why recovery is where it is, above all why it was abandoned
    engine_conf text, -- JSON: the conf the client asked its engine to be launched with
    driver_post_mortems text -- JSON: what killed this binding's drivers, newest first, captured while each pod still existed
);

CREATE INDEX IF NOT EXISTS spark_connect_session_user_name_index ON spark_connect_session(user_name);

CREATE INDEX IF NOT EXISTS spark_connect_session_session_id_index ON spark_connect_session(session_id);
