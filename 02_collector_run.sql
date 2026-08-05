-- =============================================================================
-- 02_collector_run.sql — daily collector (BigQuery script, runnable as a
-- scheduled query in the project whose workload you are collecting)
-- =============================================================================
-- WHAT IT DOES (per region in `regions`):
--   1. MERGE job events from INFORMATION_SCHEMA.JOBS (3-day lookback, idempotent)
--   2. MERGE hourly slot rollup from INFORMATION_SCHEMA.JOBS_TIMELINE
--   3. Snapshot table state  (TABLES + TABLE_OPTIONS + TABLE_STORAGE)
--   4. Snapshot columns      (COLUMNS: partition/cluster flags, types)
--   5. Snapshot partitions   (per-dataset loop — PARTITIONS is dataset-scoped)
--   6. Snapshot dataset state(SCHEMATA + SCHEMATA_OPTIONS)
--   7. Raw-JSON snapshots    (MATERIALIZED_VIEWS, VIEWS, TABLE_CONSTRAINTS)
--   8. MERGE native recommender output (RECOMMENDATIONS + INSIGHTS)
--   9. Optional: reservations snapshot from the reservation admin project
--
-- SCOPE / PERMISSIONS:
--   * Default source is INFORMATION_SCHEMA.JOBS → jobs run IN THIS PROJECT.
--     Deploy per analytics project, or set jobs_view='JOBS_BY_ORGANIZATION'
--     (needs org-level bigquery.jobs.listAll; some columns may be restricted).
--   * Recommenders: grant the recommender viewer roles
--     (roles/recommender.bigqueryPartitionClusterViewer etc.) to the collector SA.
--   * REMEMBER: unused-table decisions still require org-wide reads
--     (JOBS_BY_ORGANIZATION and/or Data Access audit logs) — project-scope
--     JOBS alone must never drive an archive/delete rule.
--
-- IDEMPOTENCY: job/timeline steps MERGE with a lookback window; snapshot steps
-- DELETE today's partition for the region, then INSERT. Safe to re-run.
-- =============================================================================

DECLARE regions        ARRAY<STRING> DEFAULT ['us'];      -- EDIT: all regions holding your data, e.g. ['us','eu','us-east4']
DECLARE jobs_view      STRING DEFAULT 'JOBS';             -- or 'JOBS_BY_ORGANIZATION'
DECLARE recs_view      STRING DEFAULT 'RECOMMENDATIONS';  -- or 'RECOMMENDATIONS_BY_ORGANIZATION'
DECLARE lookback_days  INT64  DEFAULT 3;                  -- late-arriving jobs safety margin
DECLARE admin_project  STRING DEFAULT NULL;               -- reservation admin project, or NULL to skip step 9
DECLARE run_id         STRING DEFAULT GENERATE_UUID();

FOR r IN (SELECT region FROM UNNEST(regions) AS region) DO
BEGIN
  DECLARE src STRING DEFAULT CONCAT('`region-', r.region, '`.INFORMATION_SCHEMA');
  DECLARE ds_list ARRAY<STRING> DEFAULT [];
  DECLARE i INT64 DEFAULT 0;

  -- ==========================================================================
  -- 1. Job events
  -- ==========================================================================
  EXECUTE IMMEDIATE FORMAT("""
    MERGE optimizer_ops.jobs_events T
    USING (
      SELECT
        '%s' AS region,
        project_id, job_id, parent_job_id, user_email,
        job_type, statement_type, priority, state, cache_hit,
        creation_time, start_time, end_time,
        TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
        total_bytes_processed, total_bytes_billed, total_slot_ms,
        reservation_id, transaction_id,
        referenced_tables, destination_table, labels,
        query_info.query_hashes.normalized_literals AS query_hash,
        SUBSTR(query, 1, 1024)                      AS query_preview,
        query_info.resource_warning                 AS resource_warning,
        TO_JSON_STRING(query_info.performance_insights) AS performance_insights_json,
        TO_JSON_STRING(materialized_view_statistics)    AS mv_statistics_json,
        TO_JSON_STRING(bi_engine_statistics)            AS bi_engine_json,
        TO_JSON_STRING(error_result)                    AS error_json,
        total_modified_partitions,
        dml_statistics.inserted_row_count AS dml_inserted,
        dml_statistics.updated_row_count  AS dml_updated,
        dml_statistics.deleted_row_count  AS dml_deleted
      FROM %s.%s
      WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)
        AND creation_time <  CURRENT_TIMESTAMP()
    ) S
    ON  T.region = S.region
    AND T.project_id = S.project_id
    AND T.job_id = S.job_id
    AND T.creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)  -- target pruning
    WHEN MATCHED THEN UPDATE SET
      state = S.state, end_time = S.end_time, duration_ms = S.duration_ms,
      total_bytes_processed = S.total_bytes_processed,
      total_bytes_billed    = S.total_bytes_billed,
      total_slot_ms         = S.total_slot_ms,
      error_json            = S.error_json,
      collected_at          = CURRENT_TIMESTAMP(),
      run_id                = '%s'
    WHEN NOT MATCHED THEN INSERT (
      region, project_id, job_id, parent_job_id, user_email,
      job_type, statement_type, priority, state, cache_hit,
      creation_time, start_time, end_time, duration_ms,
      total_bytes_processed, total_bytes_billed, total_slot_ms,
      reservation_id, transaction_id,
      referenced_tables, destination_table, labels,
      query_hash, query_preview, resource_warning,
      performance_insights_json, mv_statistics_json, bi_engine_json, error_json,
      total_modified_partitions, dml_inserted, dml_updated, dml_deleted,
      collected_at, run_id
    ) VALUES (
      S.region, S.project_id, S.job_id, S.parent_job_id, S.user_email,
      S.job_type, S.statement_type, S.priority, S.state, S.cache_hit,
      S.creation_time, S.start_time, S.end_time, S.duration_ms,
      S.total_bytes_processed, S.total_bytes_billed, S.total_slot_ms,
      S.reservation_id, S.transaction_id,
      S.referenced_tables, S.destination_table, S.labels,
      S.query_hash, S.query_preview, S.resource_warning,
      S.performance_insights_json, S.mv_statistics_json, S.bi_engine_json, S.error_json,
      S.total_modified_partitions, S.dml_inserted, S.dml_updated, S.dml_deleted,
      CURRENT_TIMESTAMP(), '%s'
    )
  """, r.region, src, jobs_view, lookback_days, lookback_days + 1, run_id, run_id);

  INSERT INTO optimizer_ops.collector_audit
  VALUES (run_id, CURRENT_TIMESTAMP(), '1_jobs_events', r.region, 'OK', @@row_count, NULL);

  -- ==========================================================================
  -- 2. Hourly slot rollup
  -- ==========================================================================
  EXECUTE IMMEDIATE FORMAT("""
    MERGE optimizer_ops.jobs_hourly_slots T
    USING (
      SELECT
        '%s' AS region,
        TIMESTAMP_TRUNC(period_start, HOUR) AS hour_ts,
        project_id,
        COALESCE(reservation_id, '(on-demand)') AS reservation_id,
        SUM(period_slot_ms)       AS total_slot_ms,
        COUNT(DISTINCT job_id)    AS jobs
      FROM %s.JOBS_TIMELINE
      WHERE job_creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)
        AND period_start      >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)
      GROUP BY 1, 2, 3, 4
    ) S
    ON  T.region = S.region AND T.hour_ts = S.hour_ts
    AND T.project_id = S.project_id AND T.reservation_id = S.reservation_id
    AND T.hour_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL %d DAY)
    WHEN MATCHED THEN UPDATE SET
      total_slot_ms = S.total_slot_ms, jobs = S.jobs,
      collected_at = CURRENT_TIMESTAMP(), run_id = '%s'
    WHEN NOT MATCHED THEN INSERT (region, hour_ts, project_id, reservation_id, total_slot_ms, jobs, collected_at, run_id)
    VALUES (S.region, S.hour_ts, S.project_id, S.reservation_id, S.total_slot_ms, S.jobs, CURRENT_TIMESTAMP(), '%s')
  """, r.region, src, lookback_days, lookback_days, lookback_days + 1, run_id, run_id);

  INSERT INTO optimizer_ops.collector_audit
  VALUES (run_id, CURRENT_TIMESTAMP(), '2_hourly_slots', r.region, 'OK', @@row_count, NULL);

  -- ==========================================================================
  -- 3. Table state snapshot (TABLES + TABLE_OPTIONS + TABLE_STORAGE)
  -- ==========================================================================
  DELETE FROM optimizer_ops.table_state_daily
  WHERE snapshot_date = CURRENT_DATE() AND region = r.region;

  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO optimizer_ops.table_state_daily
      SELECT
        CURRENT_DATE(), '%s',
        t.table_catalog, t.table_schema, t.table_name, t.table_type, t.creation_time,
        t.ddl,
        opt.require_partition_filter, opt.partition_expiration_days, opt.expiration_timestamp,
        s.total_rows, s.total_partitions,
        s.total_logical_bytes,  s.active_logical_bytes,  s.long_term_logical_bytes,
        s.total_physical_bytes, s.active_physical_bytes, s.long_term_physical_bytes,
        s.time_travel_physical_bytes, s.fail_safe_physical_bytes,
        s.storage_last_modified_time,
        '%s'
      FROM %s.TABLES t
      LEFT JOIN (
        SELECT table_catalog, table_schema, table_name,
               MAX(IF(option_name = 'require_partition_filter',  option_value, NULL)) AS require_partition_filter,
               MAX(IF(option_name = 'partition_expiration_days', option_value, NULL)) AS partition_expiration_days,
               MAX(IF(option_name = 'expiration_timestamp',      option_value, NULL)) AS expiration_timestamp
        FROM %s.TABLE_OPTIONS
        GROUP BY 1, 2, 3
      ) opt
        ON  opt.table_catalog = t.table_catalog
        AND opt.table_schema  = t.table_schema
        AND opt.table_name    = t.table_name
      LEFT JOIN %s.TABLE_STORAGE s
        ON  s.project_id   = t.table_catalog
        AND s.table_schema = t.table_schema
        AND s.table_name   = t.table_name
        AND s.deleted      = FALSE
      WHERE t.table_schema != 'optimizer_ops'
    """, r.region, run_id, src, src, src);

    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '3_table_state', r.region, 'OK', @@row_count, NULL);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '3_table_state', r.region, 'SKIPPED', 0,
            CONCAT('TABLE_STORAGE not ready or enabled yet: ', @@error.message));
  END;

  -- ==========================================================================
  -- 4. Columns snapshot (candidate partition/cluster columns, current spec)
  -- ==========================================================================
  DELETE FROM optimizer_ops.columns_daily
  WHERE snapshot_date = CURRENT_DATE() AND region = r.region;

  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO optimizer_ops.columns_daily
    SELECT
      CURRENT_DATE(), '%s',
      table_catalog, table_schema, table_name,
      column_name, data_type, is_partitioning_column, clustering_ordinal_position,
      '%s'
    FROM %s.COLUMNS
    WHERE table_schema != 'optimizer_ops'
  """, r.region, run_id, src);

  INSERT INTO optimizer_ops.collector_audit
  VALUES (run_id, CURRENT_TIMESTAMP(), '4_columns', r.region, 'OK', @@row_count, NULL);

  -- ==========================================================================
  -- 5. Partitions snapshot — PARTITIONS is dataset-scoped, so loop datasets
  -- ==========================================================================
  DELETE FROM optimizer_ops.table_partitions_daily
  WHERE snapshot_date = CURRENT_DATE() AND region = r.region;

  EXECUTE IMMEDIATE FORMAT("""
    SELECT COALESCE(ARRAY_AGG(schema_name), [])
    FROM %s.SCHEMATA
    WHERE schema_name NOT IN ('INFORMATION_SCHEMA', 'optimizer_ops')
  """, src) INTO ds_list;

  SET i = 0;
  WHILE i < ARRAY_LENGTH(ds_list) DO
    BEGIN
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO optimizer_ops.table_partitions_daily
        SELECT
          CURRENT_DATE(), '%s',
          '%s', table_schema, table_name,
          partition_id, total_rows, total_logical_bytes, last_modified_time, storage_tier,
          '%s'
        FROM `%s.%s.INFORMATION_SCHEMA.PARTITIONS`
      """, r.region, @@project_id, run_id, @@project_id, ds_list[OFFSET(i)]);
    EXCEPTION WHEN ERROR THEN
      INSERT INTO optimizer_ops.collector_audit
      VALUES (run_id, CURRENT_TIMESTAMP(), '5_partitions', r.region, 'SKIPPED',
              0, CONCAT('dataset=', ds_list[OFFSET(i)], ' :: ', @@error.message));
    END;
    SET i = i + 1;
  END WHILE;

  INSERT INTO optimizer_ops.collector_audit
  VALUES (run_id, CURRENT_TIMESTAMP(), '5_partitions', r.region, 'OK', NULL,
          CONCAT(CAST(ARRAY_LENGTH(ds_list) AS STRING), ' datasets scanned'));

  -- ==========================================================================
  -- 6. Dataset state snapshot
  -- ==========================================================================
  DELETE FROM optimizer_ops.dataset_state_daily
  WHERE snapshot_date = CURRENT_DATE() AND region = r.region;

  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO optimizer_ops.dataset_state_daily
    SELECT
      CURRENT_DATE(), '%s',
      s.catalog_name, s.schema_name, s.location,
      s.creation_time, s.last_modified_time,
      COALESCE(o.options, []),
      CAST(NULL AS STRING),   -- storage_billing_model: backfilled by driver via Datasets API
      '%s'
    FROM %s.SCHEMATA s
    LEFT JOIN (
      SELECT catalog_name, schema_name,
             ARRAY_AGG(STRUCT(option_name AS name, option_value AS value)) AS options
      FROM %s.SCHEMATA_OPTIONS
      GROUP BY 1, 2
    ) o USING (catalog_name, schema_name)
    WHERE s.schema_name != 'optimizer_ops'
  """, r.region, run_id, src, src);

  INSERT INTO optimizer_ops.collector_audit
  VALUES (run_id, CURRENT_TIMESTAMP(), '6_dataset_state', r.region, 'OK', @@row_count, NULL);

  -- ==========================================================================
  -- 7. Raw-JSON object snapshots (schema-drift-proof; each wrapped so one
  --    unsupported view in a region never fails the whole run)
  -- ==========================================================================
  DELETE FROM optimizer_ops.object_state_raw_daily
  WHERE snapshot_date = CURRENT_DATE() AND region = r.region
    AND kind IN ('MATERIALIZED_VIEW', 'VIEW', 'TABLE_CONSTRAINT');

  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO optimizer_ops.object_state_raw_daily
      SELECT CURRENT_DATE(), '%s', 'MATERIALIZED_VIEW',
             t.table_catalog, t.table_schema, t.table_name, TO_JSON_STRING(t), '%s'
      FROM %s.MATERIALIZED_VIEWS t
    """, r.region, run_id, src);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '7_mvs', r.region, 'SKIPPED', 0, @@error.message);
  END;

  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO optimizer_ops.object_state_raw_daily
      SELECT CURRENT_DATE(), '%s', 'VIEW',
             t.table_catalog, t.table_schema, t.table_name, TO_JSON_STRING(t), '%s'
      FROM %s.VIEWS t
      WHERE t.table_schema != 'optimizer_ops'
    """, r.region, run_id, src);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '7_views', r.region, 'SKIPPED', 0, @@error.message);
  END;

  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO optimizer_ops.object_state_raw_daily
      SELECT CURRENT_DATE(), '%s', 'TABLE_CONSTRAINT',
             t.constraint_catalog, t.constraint_schema, t.table_name, TO_JSON_STRING(t), '%s'
      FROM %s.TABLE_CONSTRAINTS t
    """, r.region, run_id, src);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '7_constraints', r.region, 'SKIPPED', 0, @@error.message);
  END;

  -- ==========================================================================
  -- 8. Native recommenders (+ write-back happens in the executor, not here)
  -- ==========================================================================
  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      MERGE optimizer_ops.recommender_recommendations T
      USING (
        SELECT
          recommendation_id,
          '%s' AS region,
          recommender, subtype, project_id, description, state, last_updated_time,
          TO_JSON_STRING(target_resources)   AS target_resources_json,
          TO_JSON_STRING(additional_details) AS additional_details_json,
          TO_JSON_STRING(t)                  AS raw_json
        FROM %s.%s t
      ) S
      ON T.recommendation_id = S.recommendation_id
      WHEN MATCHED THEN UPDATE SET
        state = S.state, last_updated_time = S.last_updated_time,
        additional_details_json = S.additional_details_json, raw_json = S.raw_json,
        last_seen_at = CURRENT_TIMESTAMP(), run_id = '%s'
      WHEN NOT MATCHED THEN INSERT (
        recommendation_id, region, recommender, subtype, project_id, description,
        state, last_updated_time, target_resources_json, additional_details_json,
        raw_json, first_seen_at, last_seen_at, run_id
      ) VALUES (
        S.recommendation_id, S.region, S.recommender, S.subtype, S.project_id, S.description,
        S.state, S.last_updated_time, S.target_resources_json, S.additional_details_json,
        S.raw_json, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '%s'
      )
    """, r.region, src, recs_view, run_id, run_id);

    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '8_recommendations', r.region, 'OK', @@row_count, NULL);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '8_recommendations', r.region, 'SKIPPED', 0, @@error.message);
  END;

  BEGIN
    EXECUTE IMMEDIATE FORMAT("""
      MERGE optimizer_ops.recommender_insights T
      USING (
        SELECT insight_id, '%s' AS region, recommender, project_id, state,
               last_updated_time, TO_JSON_STRING(t) AS raw_json
        FROM %s.INSIGHTS t
      ) S
      ON T.insight_id = S.insight_id
      WHEN MATCHED THEN UPDATE SET
        state = S.state, last_updated_time = S.last_updated_time,
        raw_json = S.raw_json, last_seen_at = CURRENT_TIMESTAMP(), run_id = '%s'
      WHEN NOT MATCHED THEN INSERT
        (insight_id, region, recommender, project_id, state, last_updated_time,
         raw_json, first_seen_at, last_seen_at, run_id)
      VALUES
        (S.insight_id, S.region, S.recommender, S.project_id, S.state, S.last_updated_time,
         S.raw_json, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '%s')
    """, r.region, src, run_id, run_id);

    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '8_insights', r.region, 'OK', @@row_count, NULL);
  EXCEPTION WHEN ERROR THEN
    INSERT INTO optimizer_ops.collector_audit
    VALUES (run_id, CURRENT_TIMESTAMP(), '8_insights', r.region, 'SKIPPED', 0, @@error.message);
  END;

  -- ==========================================================================
  -- 9. Reservations (only when a reservation admin project is configured)
  -- ==========================================================================
  IF admin_project IS NOT NULL THEN
    DELETE FROM optimizer_ops.object_state_raw_daily
    WHERE snapshot_date = CURRENT_DATE() AND region = r.region
      AND kind IN ('RESERVATION', 'ASSIGNMENT', 'CAPACITY_COMMITMENT');

    BEGIN
      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO optimizer_ops.object_state_raw_daily
        SELECT CURRENT_DATE(), '%s', 'RESERVATION', '%s', NULL, NULL, TO_JSON_STRING(t), '%s'
        FROM `%s.region-%s`.INFORMATION_SCHEMA.RESERVATIONS t
      """, r.region, admin_project, run_id, admin_project, r.region);

      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO optimizer_ops.object_state_raw_daily
        SELECT CURRENT_DATE(), '%s', 'ASSIGNMENT', '%s', NULL, NULL, TO_JSON_STRING(t), '%s'
        FROM `%s.region-%s`.INFORMATION_SCHEMA.ASSIGNMENTS t
      """, r.region, admin_project, run_id, admin_project, r.region);

      EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO optimizer_ops.object_state_raw_daily
        SELECT CURRENT_DATE(), '%s', 'CAPACITY_COMMITMENT', '%s', NULL, NULL, TO_JSON_STRING(t), '%s'
        FROM `%s.region-%s`.INFORMATION_SCHEMA.CAPACITY_COMMITMENTS t
      """, r.region, admin_project, run_id, admin_project, r.region);

      INSERT INTO optimizer_ops.collector_audit
      VALUES (run_id, CURRENT_TIMESTAMP(), '9_reservations', r.region, 'OK', NULL, NULL);
    EXCEPTION WHEN ERROR THEN
      INSERT INTO optimizer_ops.collector_audit
      VALUES (run_id, CURRENT_TIMESTAMP(), '9_reservations', r.region, 'SKIPPED', 0, @@error.message);
    END;
  END IF;

END;
END FOR;

INSERT INTO optimizer_ops.collector_audit
VALUES (run_id, CURRENT_TIMESTAMP(), 'run_complete', NULL, 'OK', NULL, NULL);
