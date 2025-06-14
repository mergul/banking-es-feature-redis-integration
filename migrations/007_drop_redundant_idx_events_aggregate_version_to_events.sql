-- Drop the redundant non-unique index on (aggregate_id, version)
-- A unique index implicitly created by the unique_aggregate_id_version constraint
-- (added in 006_add_unique_constraint_aggregate_id_version_to_events.sql)
-- serves the purpose for both indexing and uniqueness for these columns.
DROP INDEX IF EXISTS idx_events_aggregate_version;
