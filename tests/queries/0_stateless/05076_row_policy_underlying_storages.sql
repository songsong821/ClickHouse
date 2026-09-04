DROP ROW POLICY IF EXISTS rp_target_policy ON rp_target;
DROP ROW POLICY IF EXISTS rp_mv_policy ON rp_mv;
DROP ROW POLICY IF EXISTS rp_buffer_policy ON rp_buffer;
DROP TABLE IF EXISTS rp_merge;
DROP TABLE IF EXISTS rp_mv_over_buffer;
DROP TABLE IF EXISTS rp_buffer;
DROP TABLE IF EXISTS rp_mv;
DROP TABLE IF EXISTS rp_source;
DROP TABLE IF EXISTS rp_target;

SET optimize_trivial_count_query = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;

CREATE TABLE rp_target (id UInt32, tenant_id UInt32, active UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO rp_target VALUES (1, 1, 1), (2, 1, 0), (3, 2, 1), (4, 2, 0);

CREATE TABLE rp_source (id UInt32, tenant_id UInt32, active UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW rp_mv TO rp_target AS SELECT id, tenant_id, active FROM rp_source;
CREATE TABLE rp_buffer (id UInt32, tenant_id UInt32, active UInt8)
    ENGINE = Buffer(currentDatabase(), rp_target, 1, 100, 1000, 10000, 1000000, 10000000, 100000000);
-- These rows stay in the buffer for the duration of the test.
INSERT INTO rp_buffer VALUES (5, 1, 1), (6, 2, 1);
CREATE TABLE rp_merge (id UInt32, tenant_id UInt32, active UInt8) ENGINE = Merge(currentDatabase(), '^rp_buffer$');
-- A two-hop chain: the view reads the buffer, which reads the target.
CREATE MATERIALIZED VIEW rp_mv_over_buffer TO rp_buffer AS SELECT id, tenant_id, active FROM rp_source;

CREATE ROW POLICY rp_target_policy ON rp_target FOR SELECT USING tenant_id = 1 TO CURRENT_USER;

-- A policy on the target table is applied when reading through a materialized view or a buffer.
SELECT 'Target policy through MaterializedView, old analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 0;
SELECT 'Target policy through MaterializedView, analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 1;
SELECT 'Target policy through Buffer, old analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 0;
SELECT 'Target policy through Buffer, analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 1;
SELECT 'Target policy through Merge over Buffer, old analyzer', arraySort(groupArray(id)) FROM rp_merge SETTINGS enable_analyzer = 0;
SELECT 'Target policy through Merge over Buffer, analyzer', arraySort(groupArray(id)) FROM rp_merge SETTINGS enable_analyzer = 1;
SELECT 'Target policy through MaterializedView over Buffer, old analyzer', arraySort(groupArray(id)) FROM rp_mv_over_buffer SETTINGS enable_analyzer = 0;
SELECT 'Target policy through MaterializedView over Buffer, analyzer', arraySort(groupArray(id)) FROM rp_mv_over_buffer SETTINGS enable_analyzer = 1;

-- The trivial count optimization must not bypass the target policy.
SELECT 'Count through MaterializedView, trivial count disabled', count() FROM rp_mv SETTINGS optimize_trivial_count_query = 0;
SELECT 'Count through MaterializedView, trivial count enabled', count() FROM rp_mv SETTINGS optimize_trivial_count_query = 1;
SELECT 'Count through Buffer, trivial count disabled', count() FROM rp_buffer SETTINGS optimize_trivial_count_query = 0;
SELECT 'Count through Buffer, trivial count enabled', count() FROM rp_buffer SETTINGS optimize_trivial_count_query = 1;

-- Policies of the wrapper and of its target are combined with a logical AND.
CREATE ROW POLICY rp_mv_policy ON rp_mv FOR SELECT USING active = 1 TO CURRENT_USER;
CREATE ROW POLICY rp_buffer_policy ON rp_buffer FOR SELECT USING active = 1 TO CURRENT_USER;
SELECT 'Combined policies through MaterializedView, old analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 0;
SELECT 'Combined policies through MaterializedView, analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 1;
SELECT 'Combined policies through Buffer, old analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 0;
SELECT 'Combined policies through Buffer, analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 1;
SELECT 'Combined policies through MaterializedView over Buffer, analyzer', arraySort(groupArray(id)) FROM rp_mv_over_buffer SETTINGS enable_analyzer = 1;

-- Without the target policy only the wrapper policies remain.
DROP ROW POLICY rp_target_policy ON rp_target;
SELECT 'MaterializedView policy only, old analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 0;
SELECT 'MaterializedView policy only, analyzer', arraySort(groupArray(id)) FROM rp_mv SETTINGS enable_analyzer = 1;
SELECT 'Buffer policy only, old analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 0;
SELECT 'Buffer policy only, analyzer', arraySort(groupArray(id)) FROM rp_buffer SETTINGS enable_analyzer = 1;

DROP ROW POLICY rp_mv_policy ON rp_mv;
DROP ROW POLICY rp_buffer_policy ON rp_buffer;
DROP TABLE rp_merge;
DROP TABLE rp_mv_over_buffer;
DROP TABLE rp_buffer;
DROP TABLE rp_mv;
DROP TABLE rp_source;
DROP TABLE rp_target;
