# Enterprise BigQuery Optimization Control Plane (BQ-OptiMate)

An end-to-end, configuration-driven **BigQuery Optimization Control Plane** that implements zero-hardcoding portability, 4-class rule evaluation, 9-stage Copy-Swap-Rebind safe execution (with Zero-Copy Backup Clones and automated data parity regression checks), and closed-loop ROI verification.

---

## 1. File Inventory & Architecture Map

| File | Type | Description |
| :--- | :--- | :--- |
| [`config.yaml`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/config.yaml) | YAML Config | Centralized operational parameters, thresholds (`min_monthly_savings_usd`), safety toggles, and alerting channels. |
| [`01_ops_schema.sql`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/01_ops_schema.sql) | SQL DDL | Creates `optimizer_ops` dataset, raw event telemetry tables, daily snapshots, and idempotent pricing config constants. |
| [`02_collector_run.sql`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/02_collector_run.sql) | BigQuery Script | Daily scheduled query that merges `INFORMATION_SCHEMA.JOBS`, table/column/partition metadata, and native AI recommenders. |
| [`03_derived_views.sql`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/03_derived_views.sql) | SQL Views | Derived views decoupling bytes from dollar pricing (`v_jobs_costed`), 90d read/write stats, query families, and storage billing gap. |
| [`04_rules_engine.py`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/04_rules_engine.py) | Python Module | Evaluates Class 1–4 rules, calculates confidence discounts (`d_summation`, `d_window`), and detects SQL anti-patterns (`SELECT *`, cache busters, function-wrapped predicates). |
| [`05_plan_compiler.py`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/05_plan_compiler.py) | Python Module | Groups findings by target object, deduplicates, resolves conflicts, attaches rollback/verification plans, and writes to `optimizer_ops.change_sets`. |
| [`06_executor_state_machine.py`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/06_executor_state_machine.py) | Python Module | Executes remediations safely. For Class 3: runs `S0–S9` Copy-Swap-Rebind state machine with **Zero-Copy Backup Clones** (`CREATE TABLE ... CLONE ...`) and **Automated Data Parity Regression Tests** with auto-rollback. |
| [`07_verifier.py`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/07_verifier.py) | Python Module | Evaluates `VERIFYING` change sets over 14–28 days, calculates realized dollar savings, writes receipts to `bq_optimization_savings_ledger`, and writes back to Google Recommender API. |
| [`bq_optimate.py`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/bq_optimate.py) | CLI Entrypoint | Turnkey command-line orchestrator tying evaluation, compilation, execution, and verification together. |
| [`bq_optimization_control_plane_design_doc.md`](file:///usr/local/google/home/temiomisore/.gemini/jetski/brain/de5ba862-d82e-4a13-bd9f-9fd903a30103/bq_optimization_control_plane_design_doc.md) | Specification | The canonical v1 Design Document. |

---

## 2. Quickstart Deployment Guide

### Step 1: Provision SQL Foundation (Phase 0)
Run the SQL scripts in BigQuery under your designated Ops Project:
```bash
# 1. Create schema and config tables
bq query --use_legacy_sql=false < 01_ops_schema.sql

# 2. Deploy daily collector (run manually or as a Cloud Scheduler / Scheduled Query)
bq query --use_legacy_sql=false < 02_collector_run.sql

# 3. Create derived views
bq query --use_legacy_sql=false < 03_derived_views.sql
```

### Step 2: Configure Parameters
Edit `config.yaml` to specify your target project, minimum dollar thresholds, and exclude regex patterns:
```yaml
project:
  ops_project_id: "my-gcp-project"
thresholds:
  min_monthly_savings_usd: 10.00
  d_summation_discount: 0.5
remediation:
  default_mode: "dry-run"
  backup_retention_days: 14
  auto_rollback_on_failure: true
```

### Step 3: Run the Python Control Plane CLI
Use `bq_optimate.py` to run evaluation, compilation, execution, or verification:

```bash
# 1. Evaluate rules and compile findings into change sets
python3 bq_optimate.py --action=evaluate --config=config.yaml

# 2. Run a dry-run apply of an approved change set
python3 bq_optimate.py --action=apply --config=config.yaml --mode=dry-run --change-set-id=<UUID>

# 3. Run a safe auto-apply (with zero-copy backup clone & automated parity check)
python3 bq_optimate.py --action=apply --config=config.yaml --mode=auto-apply --change-set-id=<UUID>

# 4. Verify post-apply change sets and generate realized ROI receipts
python3 bq_optimate.py --action=verify --config=config.yaml
```
