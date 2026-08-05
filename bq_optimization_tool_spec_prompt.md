# BigQuery Optimization Recommendation & Automation Tool — Architecture & AI Prompt Specification (v2)

Use the prompt below as a **Product Requirements Document (PRD)** or as a **Master Instruction Prompt** when briefing an engineering team or AI assistant (e.g., Gemini, Cursor, Claude) to build or refactor your BigQuery optimization tool.

---

```markdown
# PROJECT SPECIFICATION: Enterprise BigQuery Optimization Recommendation & Automation Tool (BQ-OptiMate)

## 1. OBJECTIVE & VISION
We are building a production-grade, configuration-driven **BigQuery Optimization Recommendation and Automation Tool**. 
The tool must inspect any Google Cloud / BigQuery project at the **project level**, analyze schema metadata, table storage, query execution patterns, and spending, and then:
1. **Recommend** actionable optimizations based on industry best practices and FinOps pricing models.
2. **Automate / Apply** remediation safely using **Zero-Copy Backup Clones**, **automated data parity regression tests**, and **auto-rollback** (with a dry-run toggle).
3. **Alert & Prove ROI** by notifying engineering teams on spending anomalies and logging realized dollar savings into an **executive ledger**.

### Core Requirement: ZERO HARDCODED VALUES
- The tool MUST NOT contain any hardcoded project IDs, dataset names, table names, email addresses, thresholds, or IAM credentials in the source code.
- It MUST be 100% portable across teams and GCP projects via a simple configuration file (`config.yaml`) and CLI/environment flags (`--project-id`).

---

## 2. CONFIGURATION-DRIVEN ARCHITECTURE
Implement a modular architecture driven by a YAML configuration schema (`config.yaml`):

```yaml
project:
  id: "my-gcp-project-id"           # Can be overridden via CLI flag --project-id
  exclude_datasets:                 # Datasets to ignore (e.g. sandbox, audit logs)
    - "sandbox_.*"
    - "test_.*"
  labels_filter:                    # Optional: only scan tables matching specific labels
    env: "production"

thresholds:
  table_size_min_gb: 10             # Minimum table size to recommend partitioning/clustering
  expensive_query_cost_usd: 5.00    # Single query spend threshold for alerts
  full_scan_alert_gb: 100           # Alert when a query scans > N GB without partition filter
  unused_table_days: 60             # Days without read/write to recommend table expiration/cold archive
  min_monthly_savings_usd: 10.00    # Minimum monthly savings threshold to generate a recommendation

remediation:
  mode: "dry-run"                   # Options: "dry-run" | "auto-apply" | "recommend-only"
  auto_apply_safe_changes: false    # E.g., adding clustering or requiring partition filters
  backup_retention_days: 14         # Days to retain zero-copy backup clones before auto-expiration
  auto_rollback_on_failure: true    # Revert changes immediately if post-remediation data parity check fails

alerting:
  channels:
    - type: "slack"
      webhook_url_env: "SLACK_WEBHOOK_URL"
    - type: "email"
      recipients: ["data-eng-alerts@company.com"]
    - type: "pubsub"
      topic_name: "projects/my-gcp-project-id/topics/bq-alerts"
```

---

## 3. CORE OPTIMIZATION ENGINES & DETECTION RULES

The app must be organized into pluggable engines that query **`INFORMATION_SCHEMA`**, **Cloud Monitoring / Recommender API**, and **Cloud Billing**:

### A. Table & Storage Optimization Engine
1. **Partitioning Recommendations (`INFORMATION_SCHEMA.TABLES`, `TABLE_STORAGE`)**:
   - Detect unpartitioned tables larger than `thresholds.table_size_min_gb` where estimated monthly savings exceed `thresholds.min_monthly_savings_usd`.
   - Inspect column data types (`TIMESTAMP`, `DATE`, `DATETIME`, or integer IDs) to suggest optimal partition keys and granularities (`DAY`, `MONTH`, `YEAR`).
   - Recommend enabling `require_partition_filter = TRUE` on large fact tables.
2. **Clustering Recommendations (`INFORMATION_SCHEMA.COLUMNS`, `JOBS_BY_PROJECT`)**:
   - Identify tables where queries frequently filter or group by high-cardinality columns (`WHERE user_id = ...`, `GROUP BY region`) but lack clustering.
   - Recommend up to 4 clustering columns ordered by filter frequency.
3. **Materialized View & BI Engine Recommendations**:
   - Analyze repeated aggregate queries in `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (same `GROUP BY` and aggregations executed > 10 times/day).
   - Generate SQL DDL to create Materialized Views for high-frequency aggregations.
4. **Storage Cost Optimization & FinOps Models**:
   - Compare **Physical vs. Logical** billing models using `INFORMATION_SCHEMA.TABLE_STORAGE` and recommend switching dataset billing models where compressed physical storage saves > 20%.
   - Identify **unused/cold tables** (> 60 days unaccessed) and recommend setting default table expiration or moving to archival storage.

### B. Query Anti-Pattern & Performance Engine
Query `region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT` (or configured region) over the last 14/30 days to flag:
1. **`SELECT *` on Large Tables**: Queries selecting all columns on tables > 10 GB.
2. **Unfiltered Full Table Scans**: Queries against partitioned tables that do not include the partition column in the `WHERE` clause.
3. **Exploding / Cartesian Joins**: Queries where output rows exceed input rows by > 10x or spilling to disk (`total_bytes_spilled > 0`).
4. **Non-Sargable Predicates**: Using functions on indexed/partitioned columns (e.g., `WHERE DATE(timestamp_col) = ...` or `LOWER(col)`) which defeat pruning.
5. **Slot Bottlenecks**: Queries consuming excessive slot-milliseconds (`total_slot_ms`).

### C. Alerting & FinOps ROI Engine
1. **Real-Time / Daily Cost Alerts**:
   - Track total daily spend and alert if spend exceeds a configurable daily budget threshold.
   - Send instant alerts for single queries exceeding `expensive_query_cost_usd`.
2. **Full Table Scan Alerts**:
   - Trigger notifications when any query scans more than `full_scan_alert_gb` in a single execution.
3. **Executive Ledger & Realized Dollar Savings**:
   - Persist all identified recommendations and automated actions into a dedicated ledger dataset/table (`bq_optimization_savings_ledger`).
   - Calculate projected vs. realized dollar savings to prove ROI to engineering leadership via Looker Studio dashboards.

---

## 4. SAFETY-FIRST AUTOMATION & REMEDIATION WORKFLOW

When running in `--mode=auto-apply` for structural schema changes (e.g., partitioning or clustering an existing table), execute the following **Zero-Copy Safety Pipeline**:

1. **Step 1: Zero-Copy Backup Clone**:
   - Create an instant, zero-cost BigQuery Table Clone before any modification:
     ```sql
     CREATE TABLE `project.dataset.table_backup_20260804`
     CLONE `project.dataset.table`
     OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 14 DAY));
     ```
2. **Step 2: Execute DDL / Table Restructuring**:
   - Execute the re-partitioning or clustering DDL (`CREATE OR REPLACE TABLE ... PARTITION BY ... AS SELECT * FROM clone`).
3. **Step 3: Automated Data Parity Regression Test**:
   - Run an automated regression check comparing the backup clone and the newly created table:
     - Exact row count equality (`COUNT(*)`).
     - Column checksum / hash parity on key metrics.
4. **Step 4: Commit or Auto-Rollback**:
   - If the parity test passes: mark the remediation as `SUCCESS` in the audit ledger.
   - If the parity test fails (`auto_rollback_on_failure: true`): immediately restore the original table from the clone (`CREATE OR REPLACE TABLE ... AS SELECT * FROM clone`) and alert the engineering team.

---

## 5. EXECUTIVE LEDGER TABLE SCHEMA

Create an audit and ROI ledger table (`bq_optimization_savings_ledger`) with the following schema:

| Column Name | Type | Description |
| :--- | :--- | :--- |
| `event_timestamp` | `TIMESTAMP` | When the optimization was identified or applied |
| `project_id` | `STRING` | GCP Project ID |
| `entity_type` | `STRING` | `TABLE`, `QUERY`, `DATASET`, or `STORAGE_MODEL` |
| `target_entity` | `STRING` | Fully qualified name (`project.dataset.table` or `job_id`) |
| `optimization_type` | `STRING` | E.g., `ADD_PARTITIONING`, `ADD_CLUSTERING`, `PHYSICAL_STORAGE`, `QUERY_REFACTOR` |
| `status` | `STRING` | `RECOMMENDED`, `APPLIED`, `DRY_RUN`, `ROLLBACK_TRIGGERED` |
| `estimated_monthly_savings_usd` | `NUMERIC` | Projected monthly dollar savings |
| `realized_monthly_savings_usd` | `NUMERIC` | Post-application measured dollar savings |
| `backup_clone_table` | `STRING` | Name of the Zero-Copy Backup Clone created (if applicable) |

---

## 6. USER INTERFACE & DEPLOYMENT REQUIREMENT

- **CLI Interface**:
  - `bq-optimize --project <project_id> --config ./config.yaml --mode dry-run --output report.html`
- **Docker / Cloud Run Job Deployment**:
  - Provide a Dockerfile and Terraform / Cloud Build configuration so teams can deploy the tool as a scheduled daily Cloud Run Job or Airflow/Cloud Composer DAG with standard Workload Identity IAM roles (`roles/bigquery.metadataViewer`, `roles/bigquery.resourceViewer`, `roles/bigquery.jobUser`).
- **Extensibility**:
  - Write modular Python code using `google-cloud-bigquery` and `pydantic` for configuration validation, with clear abstract base classes (`BaseOptimizationRule`) so new rules can be added in a single file without touching core logic.
```
