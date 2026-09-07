-- The `plain_rewritable` metadata storage (like `plain` and `web`) removes blobs synchronously inside the
-- transaction and never replicates them, so a disk using it must not create the background
-- `BlobKillerThread` and `BlobCopierThread`. A disk with the default `local` metadata still gets both.

CREATE TABLE t_local_metadata (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '05136_local_metadata',
    type = 'object_storage',
    object_storage_type = 'local',
    path = 'disks/05136_local_metadata/');

CREATE TABLE t_plain_rewritable (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    name = '05136_plain_rewritable',
    type = 'object_storage',
    object_storage_type = 'local',
    metadata_type = 'plain_rewritable',
    path = 'disks/05136_plain_rewritable/');

SYSTEM FLUSH LOGS text_log;

SELECT 'local metadata', countDistinct(logger_name) FROM system.text_log
WHERE logger_name IN ('05136_local_metadata::BlobKillerThread', '05136_local_metadata::BlobCopierThread');

SELECT 'plain_rewritable', count() FROM system.text_log
WHERE logger_name IN ('05136_plain_rewritable::BlobKillerThread', '05136_plain_rewritable::BlobCopierThread');

DROP TABLE t_local_metadata;
DROP TABLE t_plain_rewritable;
