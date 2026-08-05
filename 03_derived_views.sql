-- =============================================================================
-- 03_derived_views.sql — first derived layer over the raw telemetry.
-- These are the views the rules engine (and the verifier baselines) read.
-- Run after 01/02. All pricing comes from optimizer_ops.collector_config so
-- price changes never rewrite raw history.
-- =============================================================================

-- Convenience: config as one row of columns.
CREATE OR REPLACE VIEW optimizer_ops.v_config AS
SELECT
  MAX(IF(key = 'on_demand_usd_per_tib',              value, NULL)) AS on_demand_usd_per_tib,
  MAX(IF(key = 'storage_logical_active_usd_per_gib', value, NULL)) AS p_log_active,
  MAX(IF(key = 'storage_logical_lt_usd_per_gib',     value, NULL)) AS p_log_lt,
  MAX(IF(key = 'storage_physical_active_usd_per_gib',value, NULL)) AS p_phy_active,
  MAX(IF(key = 'storage_physical_lt_usd_per_gib',    value, NULL)) AS p_phy_lt,
  MAX(IF(key = 'partition_candidate_min_bytes',      value, NULL)) AS partition_min_bytes,
  MAX(IF(key = 'cluster_candidate_min_bytes',        value, NULL)) AS cluster_min_bytes
FROM optimizer_ops.collector_config;

-- -----------------------------------------------------------------------------
-- Costed jobs. NOTE the billing-mode honesty rule: est_on_demand_usd is only a
-- real dollar figure for jobs that ran on-demand (reservation_id IS NULL).
-- For reservation jobs, slot_ms is the currency — price it against the
-- edition rate in the rules engine, never against bytes.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW optimizer_ops.v_jobs_costed AS
SELECT
  j.*,
  SAFE_DIVIDE(j.total_bytes_billed, POW(1024, 4)) * c.on_demand_usd_per_tib AS est_on_demand_usd,
  (j.reservation_id IS NULL)                                                AS ran_on_demand
FROM optimizer_ops.jobs_events j
CROSS JOIN optimizer_ops.v_config c
WHERE j.job_type = 'QUERY'
  AND j.statement_type != 'SCRIPT';   -- parents double-count their children

-- -----------------------------------------------------------------------------
-- Per-table read/write stats, 90 days.
-- !! SCOPE WARNING: this sees only jobs collected by THIS deployment.
-- A table unread here may be read daily from another project. No archive or
-- delete recommendation may cite this view without org-wide confirmation
-- (JOBS_BY_ORGANIZATION collection and/or Data Access audit logs).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW optimizer_ops.v_table_read_write_90d AS
WITH reads AS (
  SELECT
    rt.project_id, rt.dataset_id, rt.table_id,
    COUNT(*)                          AS scan_jobs,
    COUNT(DISTINCT j.user_email)      AS distinct_readers,
    COUNT(DISTINCT j.project_id)      AS reader_projects,
    SUM(j.total_bytes_billed)         AS bytes_billed_reads,
    SUM(j.total_slot_ms)              AS slot_ms_reads,
    SUM(j.est_on_demand_usd)          AS est_on_demand_usd_reads,   -- on-demand jobs only in spirit; filter below if needed
    MAX(j.creation_time)              AS last_read_at
  FROM optimizer_ops.v_jobs_costed j, UNNEST(j.referenced_tables) rt
  WHERE j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  GROUP BY 1, 2, 3
),
writes AS (
  SELECT
    j.destination_table.project_id, j.destination_table.dataset_id, j.destination_table.table_id,
    COUNT(*)              AS write_jobs,
    MAX(j.creation_time)  AS last_write_at,
    SUM(j.total_modified_partitions) AS modified_partitions
  FROM optimizer_ops.jobs_events j
  WHERE j.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
    AND j.destination_table.table_id IS NOT NULL
    AND j.statement_type != 'SCRIPT'
  GROUP BY 1, 2, 3
)
SELECT
  COALESCE(r.project_id, w.project_id)  AS project_id,
  COALESCE(r.dataset_id, w.dataset_id)  AS dataset_id,
  COALESCE(r.table_id,  w.table_id)     AS table_id,
  r.scan_jobs, r.distinct_readers, r.reader_projects,
  r.bytes_billed_reads, r.slot_ms_reads, r.est_on_demand_usd_reads, r.last_read_at,
  w.write_jobs, w.last_write_at, w.modified_partitions
FROM reads r
FULL OUTER JOIN writes w
  ON  r.project_id = w.project_id
  AND r.dataset_id = w.dataset_id
  AND r.table_id   = w.table_id;

-- -----------------------------------------------------------------------------
-- Query families, 28 days — keyed on the normalized-literals hash.
-- This is the verifier's baseline unit AND the MV-candidate signal.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW optimizer_ops.v_query_families_28d AS
SELECT
  query_hash,
  COUNT(*)                                   AS executions,
  COUNT(DISTINCT user_email)                 AS distinct_users,
  SUM(total_bytes_billed)                    AS total_bytes_billed,
  AVG(total_bytes_billed)                    AS avg_bytes_billed,
  SUM(total_slot_ms)                         AS total_slot_ms,
  SUM(est_on_demand_usd)                     AS est_on_demand_usd,
  APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)] AS p50_duration_ms,
  APPROX_QUANTILES(duration_ms, 100)[OFFSET(95)] AS p95_duration_ms,
  COUNTIF(cache_hit)                         AS cache_hits,
  ANY_VALUE(query_preview)                   AS sample_preview,
  ANY_VALUE(referenced_tables)               AS sample_referenced_tables
FROM optimizer_ops.v_jobs_costed
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
  AND query_hash IS NOT NULL
GROUP BY query_hash;

-- -----------------------------------------------------------------------------
-- Dataset storage billing-model gap (rule C1-05 input).
-- Physical billing charges time-travel + fail-safe bytes at the active rate;
-- logical billing does not charge them at all — the math below reflects that.
-- Positive monthly_saving_usd = the flip is worth reviewing.
-- Remember the one-way-door: ~24 h to take effect, 14-day lock to switch back.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW optimizer_ops.v_dataset_storage_billing_gap AS
WITH latest AS (
  SELECT * FROM optimizer_ops.table_state_daily
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM optimizer_ops.table_state_daily)
),
per_dataset AS (
  SELECT
    region, project_id, dataset_id,
    SUM(active_logical_bytes)      AS active_logical,
    SUM(long_term_logical_bytes)   AS lt_logical,
    SUM(active_physical_bytes)     AS active_physical,
    SUM(long_term_physical_bytes)  AS lt_physical,
    SUM(time_travel_physical_bytes) AS tt_physical,
    SUM(fail_safe_physical_bytes)   AS fs_physical
  FROM latest
  GROUP BY 1, 2, 3
)
SELECT
  d.*,
  SAFE_DIVIDE(d.active_logical + d.lt_logical,
              NULLIF(d.active_physical + d.lt_physical, 0))            AS compression_ratio,
  (d.active_logical / POW(1024, 3)) * c.p_log_active
    + (d.lt_logical / POW(1024, 3)) * c.p_log_lt                        AS monthly_cost_logical_usd,
  ((d.active_physical + d.tt_physical + d.fs_physical) / POW(1024, 3)) * c.p_phy_active
    + (d.lt_physical / POW(1024, 3)) * c.p_phy_lt                       AS monthly_cost_physical_usd,
  ((d.active_logical / POW(1024, 3)) * c.p_log_active
    + (d.lt_logical / POW(1024, 3)) * c.p_log_lt)
  - (((d.active_physical + d.tt_physical + d.fs_physical) / POW(1024, 3)) * c.p_phy_active
    + (d.lt_physical / POW(1024, 3)) * c.p_phy_lt)                      AS monthly_saving_if_physical_usd
FROM per_dataset d
CROSS JOIN optimizer_ops.v_config c;

-- -----------------------------------------------------------------------------
-- Unpartitioned, heavily-scanned tables with a viable time column
-- (rule C3-01 candidate feed — the rules engine still confirms date-predicate
-- usage from query text / native insights before scoring).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW optimizer_ops.v_unpartitioned_scan_targets AS
WITH latest_state AS (
  SELECT * FROM optimizer_ops.table_state_daily
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM optimizer_ops.table_state_daily)
    AND table_type = 'BASE TABLE'
),
latest_cols AS (
  SELECT * FROM optimizer_ops.columns_daily
  WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM optimizer_ops.columns_daily)
),
part_flags AS (
  SELECT project_id, dataset_id, table_id,
         LOGICAL_OR(is_partitioning_column = 'YES')                    AS is_partitioned,
         LOGICAL_OR(clustering_ordinal_position IS NOT NULL)           AS is_clustered,
         ARRAY_AGG(IF(data_type IN ('DATE', 'TIMESTAMP', 'DATETIME'),
                      column_name, NULL) IGNORE NULLS)                 AS time_columns
  FROM latest_cols
  GROUP BY 1, 2, 3
)
SELECT
  s.region, s.project_id, s.dataset_id, s.table_id,
  s.total_logical_bytes,
  f.is_clustered,
  f.time_columns,
  rw.scan_jobs, rw.bytes_billed_reads, rw.est_on_demand_usd_reads, rw.last_read_at
FROM latest_state s
JOIN part_flags f
  ON  f.project_id = s.project_id AND f.dataset_id = s.dataset_id AND f.table_id = s.table_id
LEFT JOIN optimizer_ops.v_table_read_write_90d rw
  ON  rw.project_id = s.project_id AND rw.dataset_id = s.dataset_id AND rw.table_id = s.table_id
CROSS JOIN optimizer_ops.v_config c
WHERE f.is_partitioned = FALSE
  AND s.total_logical_bytes >= c.partition_min_bytes
  AND ARRAY_LENGTH(f.time_columns) > 0
  AND COALESCE(rw.scan_jobs, 0) > 0
ORDER BY rw.bytes_billed_reads DESC;
