-- =============================================================================
-- 01_ops_schema.sql — optimizer_ops dataset: collector target tables
-- =============================================================================
-- Run once per ops project (idempotent). Adjust LOCATION to your primary region.
-- The collector (02_collector_run.sql) MERGEs/INSERTs into these tables daily.
--
-- Conventions:
--   * Raw layer stores bytes/slots, never dollars — pricing happens in views
--     (03_derived_views.sql) so billing-mode changes don't rewrite history.
--   * Snapshot tables are partitioned on snapshot_date with expirations, so
--     the ops dataset doesn't itself become a storage-hygiene finding.
--   * Anything with schema drift risk (recommenders, reservations, MVs) is
--     also captured as raw JSON so a Google schema change never loses data.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS optimizer_ops
OPTIONS (location = 'US', description = 'BigQuery optimization control plane — telemetry and findings');

-- -----------------------------------------------------------------------------
-- Event telemetry
-- -----------------------------------------------------------------------------

-- One row per job. MERGE-keyed on (region, project_id, job_id).
-- NOTE: rows with statement_type = 'SCRIPT' are parent jobs whose children are
-- also present; filter them out in aggregations to avoid double counting.
CREATE TABLE IF NOT EXISTS optimizer_ops.jobs_events (
  region                    STRING NOT NULL,
  project_id                STRING,
  job_id                    STRING,
  parent_job_id             STRING,
  user_email                STRING,
  job_type                  STRING,
  statement_type            STRING,
  priority                  STRING,
  state                     STRING,
  cache_hit                 BOOL,
  creation_time             TIMESTAMP,
  start_time                TIMESTAMP,
  end_time                  TIMESTAMP,
  duration_ms               INT64,
  total_bytes_processed     INT64,
  total_bytes_billed        INT64,
  total_slot_ms             INT64,
  reservation_id            STRING,
  transaction_id            STRING,
  referenced_tables         ARRAY<STRUCT<project_id STRING, dataset_id STRING, table_id STRING>>,
  destination_table         STRUCT<project_id STRING, dataset_id STRING, table_id STRING>,
  labels                    ARRAY<STRUCT<key STRING, value STRING>>,
  query_hash                STRING,   -- query_info.query_hashes.normalized_literals
  query_preview             STRING,   -- first 1 KB only; full text is deliberately NOT collected (privacy)
  resource_warning          STRING,
  performance_insights_json STRING,
  mv_statistics_json        STRING,
  bi_engine_json            STRING,
  error_json                STRING,
  total_modified_partitions INT64,
  dml_inserted              INT64,
  dml_updated               INT64,
  dml_deleted               INT64,
  collected_at              TIMESTAMP,
  run_id                    STRING
)
PARTITION BY DATE(creation_time)
CLUSTER BY project_id, query_hash
OPTIONS (description = 'INFORMATION_SCHEMA.JOBS archive (outlives the 180-day native window)');

-- Hourly slot-concurrency shape (from JOBS_TIMELINE). Feeds reservation sizing
-- and the on-demand vs Editions model (W-01).
CREATE TABLE IF NOT EXISTS optimizer_ops.jobs_hourly_slots (
  region         STRING NOT NULL,
  hour_ts        TIMESTAMP,
  project_id     STRING,
  reservation_id STRING,     -- '(on-demand)' when NULL at source
  total_slot_ms  INT64,
  jobs           INT64,
  collected_at   TIMESTAMP,
  run_id         STRING
)
PARTITION BY DATE(hour_ts)
CLUSTER BY region, project_id;

-- -----------------------------------------------------------------------------
-- State telemetry (daily snapshots)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS optimizer_ops.table_state_daily (
  snapshot_date              DATE NOT NULL,
  region                     STRING,
  project_id                 STRING,
  dataset_id                 STRING,
  table_id                   STRING,
  table_type                 STRING,
  table_created              TIMESTAMP,
  ddl                        STRING,   -- current partition/cluster spec captured as "before" state
  require_partition_filter   STRING,   -- from TABLE_OPTIONS (string form)
  partition_expiration_days  STRING,
  expiration_timestamp       STRING,
  total_rows                 INT64,
  total_partitions           INT64,
  total_logical_bytes        INT64,
  active_logical_bytes       INT64,
  long_term_logical_bytes    INT64,
  total_physical_bytes       INT64,
  active_physical_bytes      INT64,
  long_term_physical_bytes   INT64,
  time_travel_physical_bytes INT64,
  fail_safe_physical_bytes   INT64,
  storage_last_modified      TIMESTAMP,
  run_id                     STRING
)
PARTITION BY snapshot_date
CLUSTER BY dataset_id, table_id
OPTIONS (partition_expiration_days = 400);

CREATE TABLE IF NOT EXISTS optimizer_ops.columns_daily (
  snapshot_date               DATE NOT NULL,
  region                      STRING,
  project_id                  STRING,
  dataset_id                  STRING,
  table_id                    STRING,
  column_name                 STRING,
  data_type                   STRING,
  is_partitioning_column      STRING,   -- 'YES' / 'NO'
  clustering_ordinal_position INT64,    -- NULL when not a clustering column
  run_id                      STRING
)
PARTITION BY snapshot_date
CLUSTER BY dataset_id, table_id
OPTIONS (partition_expiration_days = 90);

CREATE TABLE IF NOT EXISTS optimizer_ops.table_partitions_daily (
  snapshot_date       DATE NOT NULL,
  region              STRING,
  project_id          STRING,
  dataset_id          STRING,
  table_id            STRING,
  partition_id        STRING,
  total_rows          INT64,
  total_logical_bytes INT64,
  last_modified       TIMESTAMP,
  storage_tier        STRING,          -- ACTIVE / LONG_TERM
  run_id              STRING
)
PARTITION BY snapshot_date
CLUSTER BY dataset_id, table_id
OPTIONS (partition_expiration_days = 120);

CREATE TABLE IF NOT EXISTS optimizer_ops.dataset_state_daily (
  snapshot_date DATE NOT NULL,
  region        STRING,
  project_id    STRING,
  dataset_id    STRING,
  location      STRING,
  created       TIMESTAMP,
  last_modified TIMESTAMP,
  options       ARRAY<STRUCT<name STRING, value STRING>>,  -- SCHEMATA_OPTIONS, unpivoted-safe
  -- storage_billing_model is not reliably exposed in INFORMATION_SCHEMA;
  -- the driver should backfill this column via the Datasets API.
  storage_billing_model STRING,
  run_id        STRING
)
PARTITION BY snapshot_date
OPTIONS (partition_expiration_days = 400);

-- Schema-drift-proof capture of MVs, views, constraints, reservations, etc.
CREATE TABLE IF NOT EXISTS optimizer_ops.object_state_raw_daily (
  snapshot_date DATE NOT NULL,
  region        STRING,
  kind          STRING,   -- MATERIALIZED_VIEW | VIEW | TABLE_CONSTRAINT | RESERVATION | ASSIGNMENT | CAPACITY_COMMITMENT
  project_id    STRING,
  dataset_id    STRING,
  object_id     STRING,
  raw_json      STRING,   -- TO_JSON_STRING of the full source row
  run_id        STRING
)
PARTITION BY snapshot_date
CLUSTER BY kind
OPTIONS (partition_expiration_days = 400);

-- -----------------------------------------------------------------------------
-- Native recommender ingest
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS optimizer_ops.recommender_recommendations (
  recommendation_id       STRING NOT NULL,   -- MERGE key
  region                  STRING,
  recommender             STRING,            -- e.g. google.bigquery.table.PartitionClusterRecommender
  subtype                 STRING,
  project_id              STRING,
  description             STRING,
  state                   STRING,            -- ACTIVE | CLAIMED | SUCCEEDED | FAILED | DISMISSED
  last_updated_time       TIMESTAMP,
  target_resources_json   STRING,
  additional_details_json STRING,            -- contains est GB / slot-hours saved; extract in views
  raw_json                STRING,
  first_seen_at           TIMESTAMP,
  last_seen_at            TIMESTAMP,
  run_id                  STRING
)
CLUSTER BY recommender, state;

CREATE TABLE IF NOT EXISTS optimizer_ops.recommender_insights (
  insight_id        STRING NOT NULL,
  region            STRING,
  recommender       STRING,
  project_id        STRING,
  state             STRING,
  last_updated_time TIMESTAMP,
  raw_json          STRING,
  first_seen_at     TIMESTAMP,
  last_seen_at      TIMESTAMP,
  run_id            STRING
)
CLUSTER BY recommender, state;

-- -----------------------------------------------------------------------------
-- Ops
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS optimizer_ops.collector_audit (
  run_id          STRING,
  audit_timestamp TIMESTAMP,
  step            STRING,
  region          STRING,
  status          STRING,   -- OK | SKIPPED | ERROR
  row_count       INT64,
  message         STRING
)
PARTITION BY DATE(audit_timestamp)
OPTIONS (partition_expiration_days = 180);

-- Pricing/threshold constants for the derived layer. Values here are
-- US multi-region list prices as of Aug 2026 — VERIFY PER REGION before use.
CREATE TABLE IF NOT EXISTS optimizer_ops.collector_config (
  key   STRING NOT NULL,
  value NUMERIC
);

MERGE optimizer_ops.collector_config T
USING (
  SELECT 'on_demand_usd_per_tib' AS key, NUMERIC '6.25' AS value UNION ALL
  SELECT 'storage_logical_active_usd_per_gib',  NUMERIC '0.02' UNION ALL
  SELECT 'storage_logical_lt_usd_per_gib',      NUMERIC '0.01' UNION ALL
  SELECT 'storage_physical_active_usd_per_gib', NUMERIC '0.04' UNION ALL
  SELECT 'storage_physical_lt_usd_per_gib',     NUMERIC '0.02' UNION ALL
  SELECT 'slot_hour_usd_standard',              NUMERIC '0.04' UNION ALL   -- verify: edition + region
  SELECT 'slot_hour_usd_enterprise',            NUMERIC '0.06' UNION ALL   -- verify
  SELECT 'slot_hour_usd_enterprise_plus',       NUMERIC '0.10' UNION ALL   -- verify
  SELECT 'partition_candidate_min_bytes',       NUMERIC '107374182400' UNION ALL  -- 100 GiB
  SELECT 'cluster_candidate_min_bytes',         NUMERIC '10737418240'             -- 10 GiB
) S
ON T.key = S.key
WHEN NOT MATCHED THEN INSERT (key, value) VALUES (S.key, S.value);

-- -----------------------------------------------------------------------------
-- Change Sets & Executive ROI Ledger
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS optimizer_ops.change_sets (
  change_set_id              STRING NOT NULL,
  created_at                 TIMESTAMP NOT NULL,
  rule_ids                   ARRAY<STRING>,
  rule_versions              ARRAY<STRING>,
  apply_class                INT64,
  source                     STRING,
  native_rec_names           ARRAY<STRING>,
  target_project             STRING,
  target_dataset             STRING,
  target_table               STRING,
  target_region              STRING,
  finding_summary            STRING,
  evidence                   STRING,
  observation_days           INT64,
  current_config_ddl         STRING,
  proposed_change            STRING,
  execution_route            STRING,
  owner_principal            STRING,
  owner_source               STRING,
  gross_monthly_savings_usd  NUMERIC,
  recurring_monthly_cost_usd NUMERIC,
  one_time_apply_cost_usd    NUMERIC,
  net_monthly_value_usd      NUMERIC,
  savings_basis              STRING,
  confidence                 NUMERIC,
  confidence_factors         STRING,
  score                      NUMERIC,
  blast_radius               STRING,
  risk_notes                 ARRAY<STRING>,
  state                      STRING NOT NULL,
  state_history              STRING,
  approvals                  STRING,
  rejection_reason           STRING,
  rejection_note             STRING,
  snooze_until               TIMESTAMP,
  expires_at                 TIMESTAMP,
  rollback_plan              STRING,
  verification_plan          STRING,
  applied_at                 TIMESTAMP,
  verification_result        STRING,
  realized_over_predicted    NUMERIC
)
PARTITION BY DATE(created_at)
CLUSTER BY state, target_dataset;

CREATE TABLE IF NOT EXISTS optimizer_ops.bq_optimization_savings_ledger (
  event_timestamp               TIMESTAMP NOT NULL,
  project_id                    STRING NOT NULL,
  dataset_id                    STRING,
  table_id                      STRING,
  entity_type                   STRING NOT NULL,
  target_entity                 STRING NOT NULL,
  optimization_type             STRING,
  status                        STRING NOT NULL,
  estimated_monthly_savings_usd NUMERIC,
  realized_monthly_savings_usd  NUMERIC,
  backup_clone_table            STRING
)
PARTITION BY DATE(event_timestamp);
