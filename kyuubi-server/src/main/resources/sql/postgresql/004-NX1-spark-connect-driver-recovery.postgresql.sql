-- NX1: driver recovery bookkeeping and driver post-mortems for the Spark Connect routing table
-- A Spark Connect driver that dies takes its Kubernetes events with it: events are namespaced
-- objects with a short TTL, garbage-collected once the object they involve is gone, so an
-- operator arriving hours later has nothing left to read. The post-mortem is therefore captured
-- while the pod still exists and stored here, where it outlives both the pod and the Kyuubi
-- process that watched it die.
-- generation is what tells a client its Spark session was replaced. A relaunched driver is a new
-- JVM with none of the temporary views, cached frames, artifacts or session conf the old one
-- held, so a bumped generation is a state loss, not a hiccup.
-- Applied automatically on startup by JDBCMetadataStore.migrateSchema; this file records the
-- change for operators who manage their schema by hand.
ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS generation int NOT NULL DEFAULT 0;

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS restart_count int NOT NULL DEFAULT 0;

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS last_restart_time bigint NOT NULL DEFAULT 0;

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS recovery_state varchar(16) NOT NULL DEFAULT '';

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS recovery_message text;

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS engine_conf text;

ALTER TABLE spark_connect_session ADD COLUMN IF NOT EXISTS driver_post_mortems text;

COMMENT ON COLUMN spark_connect_session.generation IS 'how many engines this binding has had; a new one is a new Spark session';
COMMENT ON COLUMN spark_connect_session.restart_count IS 'how many times recovery has relaunched a driver';
COMMENT ON COLUMN spark_connect_session.last_restart_time IS 'when the most recent relaunch started';
COMMENT ON COLUMN spark_connect_session.recovery_state IS 'empty, RECOVERING or ABANDONED';
COMMENT ON COLUMN spark_connect_session.recovery_message IS 'why recovery is where it is, above all why it was abandoned';
COMMENT ON COLUMN spark_connect_session.engine_conf IS 'JSON: the conf the client asked its engine to be launched with';
COMMENT ON COLUMN spark_connect_session.driver_post_mortems IS 'JSON: what killed this binding drivers, newest first, captured while each pod still existed';
