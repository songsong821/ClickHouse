-- Tags: no-replicated-database
-- A database with `lazy_load_tables` stands every table in for a `StorageTableProxy` until it is
-- first accessed. Once the table is loaded, the database hands out the storage itself, so everything
-- that recognizes a table by its concrete type works again.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- `USE` only so that the queries below can say `currentDatabase()`.
USE {CLICKHOUSE_DATABASE_1:Identifier};

-- Nothing has touched the table yet, so it is still only a proxy.
SELECT 'cold engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';

-- Reading the table loads it, and the database then replaces the proxy with the loaded storage.
SELECT 'select', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

SELECT 'engine', engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT 'parts_columns', count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't' AND active;

-- A mutation and a lightweight delete both need the real storage.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
SELECT 'after alter delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 2;
SELECT 'after lightweight delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

-- The same statements must also work as the very first thing addressed to a table that has not been
-- accessed at all since the database was attached.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DELETE FROM {CLICKHOUSE_DATABASE_1:Identifier}.t WHERE id = 3;
SELECT 'cold lightweight delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t DELETE WHERE id = 4 SETTINGS mutations_sync = 2;
SELECT 'cold alter delete', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
