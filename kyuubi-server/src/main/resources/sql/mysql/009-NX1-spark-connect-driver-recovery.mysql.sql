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
ALTER TABLE spark_connect_session ADD COLUMN generation int NOT NULL DEFAULT 0
    COMMENT 'how many engines this binding has had; a new one is a new Spark session';

ALTER TABLE spark_connect_session ADD COLUMN restart_count int NOT NULL DEFAULT 0
    COMMENT 'how many times recovery has relaunched a driver';

ALTER TABLE spark_connect_session ADD COLUMN last_restart_time bigint NOT NULL DEFAULT 0
    COMMENT 'when the most recent relaunch started';

ALTER TABLE spark_connect_session ADD COLUMN recovery_state varchar(16) NOT NULL DEFAULT ''
    COMMENT 'empty, RECOVERING or ABANDONED';

ALTER TABLE spark_connect_session ADD COLUMN recovery_message mediumtext
    COMMENT 'why recovery is where it is, above all why it was abandoned';

ALTER TABLE spark_connect_session ADD COLUMN driver_post_mortems mediumtext
    COMMENT 'JSON: what killed this binding drivers, newest first, captured while each pod still existed';
