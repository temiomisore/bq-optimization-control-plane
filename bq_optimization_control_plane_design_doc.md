# BigQuery Optimization Control Plane — Design Document

**Status:** Draft v1 (merged design)  
**Date:** August 2026  
**Scope:** Design for a recommendation → human approval → automated apply → verification system for BigQuery cost and performance optimization.

---

## 1. Executive summary

Detection of BigQuery optimization opportunities is nearly a commodity. Google already ships a partitioning/clustering recommender, a materialized view recommender, a slot estimator, Gemini-assisted query optimization in BigQuery Studio, and an open-source anti-pattern recognition tool. What nobody ships end-to-end is the **control plane**: cross-signal ranking normalized to dollars, ownership attribution, a *safe* apply layer that preserves everything bound to a table, and closed-loop verification that proves realized savings.

This system is that control plane. The loop:

```
Collect telemetry → Score & compile change plans → Human review (approve/reject)
→ Apply safely (PR or guarded DDL) → Verify savings for 14–28 days → Feed results
back into scoring confidence
```

The product's trust is built on three commitments:

1. **Nothing applies without an approval**, and approvals expire.
2. **Every apply is reversible** for a hold window, with bindings (IAM, row/column security, indexes, views) preserved.
3. **Every estimate gets a receipt** — realized vs. predicted savings, measured per query family, published back to the approver.

---

## 2. Goals and non-goals

**Goals**

- Verified, dollar-denominated savings across compute (on-demand and Editions), storage, and configuration.
- Zero-incident applies: no dropped security policies, no lost writes, no broken pipelines.
- Organization-wide coverage with per-table ownership routing.
- A learning system: rejection reasons and realized/predicted ratios continuously recalibrate confidence.

**Non-goals**

- Re-implementing Google's detectors (we ingest and discount them instead).
- Auto-editing user SQL in production (query-level fixes ship as pull requests only).
- Real-time query interception or admission control.
- BigLake / Iceberg external tables (out of scope for v1; flagged, not acted on).

---

## 3. System architecture

| Component | Responsibility | Implementation sketch |
|---|---|---|
| **Collector** | Daily ingest of event, state, and recommender telemetry into `optimizer_ops` | Cloud Run job + Cloud Scheduler; MERGE into partitioned ops tables |
| **Rules engine** | Evaluate rule catalog over telemetry; emit findings with evidence and savings math | SQL rules + Python orchestration; anti-pattern tool for SQL parsing |
| **Plan compiler** | Deduplicate, merge, and conflict-resolve findings into per-object change sets with rollback + verification plans | Python service |
| **Recommendation store** | Change-set records + full lifecycle state machine | BigQuery table (schema in §7) |
| **Review service** | Cards, approval routing, expiry, rejection capture | Small Cloud Run web app and/or Slack approvals |
| **Executor** | Dual-path apply: CI pull request (IaC-managed targets) or guarded direct DDL (unmanaged) | Cloud Run worker, dedicated service account; PR bot for Terraform/dbt/Dataform |
| **Verifier** | Baseline/post measurement keyed on query fingerprints; regression alerts; rollback; realized-savings receipts | Scheduled queries + Cloud Run |

All components write structured audit events to `optimizer_ops.audit_log` and to Cloud Audit Logs.

---

## 4. Telemetry specification (Collector)

### 4.1 Event telemetry — `INFORMATION_SCHEMA.JOBS`

Region-qualified (`region-us.INFORMATION_SCHEMA.JOBS`, iterate all active regions), retained ~180 days by Google — the ops dataset becomes the long-term archive from day one. Pull incrementally, always filtered on `creation_time` (the view's partition column).

Key columns and the rules they feed:

| Column | Feeds |
|---|---|
| `total_bytes_billed`, `total_bytes_processed` | All on-demand savings math |
| `total_slot_ms`, `reservation_id` | Editions savings math, reservation sizing |
| `query` (text) | Anti-pattern detection (via parser, not regex) |
| `referenced_tables`, `destination_table` | Per-table aggregation, blast radius, written-but-never-read detection |
| `query_info.query_hashes.normalized_literals` | Query-family fingerprinting (dedup literals) — the key for MV detection and verification baselines |
| `query_info.performance_insights` | Slot contention, shuffle pressure, stage regressions |
| `cache_hit` | Cache-buster rule |
| `materialized_view_statistics` | MV usage/rejection monitoring |
| `total_modified_partitions`, `dml_statistics` | Write-cost side of partition/cluster math |
| `labels`, `user_email`, `project_id` | Attribution and ownership routing |
| `statement_type`, `error_result`, `state` | Read/write classification, failure patterns |

Additionally: hourly rollups of `JOBS_TIMELINE` / `RESERVATIONS_TIMELINE` (slot concurrency shape for capacity rules); full execution plans (`job_stages` / `jobs.get`) hydrated only for the top-N most expensive query families.

### 4.2 State telemetry

- `TABLES` — including the `ddl` column (current partition/cluster spec captured as "before" state).
- `TABLE_OPTIONS` — `require_partition_filter`, expirations.
- `COLUMNS` — candidate partition columns (DATE/TIMESTAMP/DATETIME/INT64), column counts.
- `PARTITIONS` (dataset-scoped; iterate datasets) — per-partition rows/bytes/last-modified: skew, staleness, expiration candidates, partition-count vs. the 10,000-partition limit.
- `TABLE_STORAGE` — active vs. long-term, logical vs. physical bytes including time-travel and fail-safe physical bytes. Compression ratio = logical ÷ physical → billing-model rule.
- `SCHEMATA_OPTIONS` + Datasets API — default expirations, current storage billing model.
- `MATERIALIZED_VIEWS` — refresh health (a stale MV is a silent outage).
- `VIEWS` — dependency graph input.
- `RESERVATIONS`, `ASSIGNMENTS`, `CAPACITY_COMMITMENTS` — capacity posture.
- `TABLE_CONSTRAINTS` — PK/FK presence (join-optimization opportunities).

### 4.3 Native recommender ingest

Query `INFORMATION_SCHEMA.RECOMMENDATIONS` and `INFORMATION_SCHEMA.INSIGHTS` daily. Two recommender IDs:

- `google.bigquery.table.PartitionClusterRecommender`
- `google.bigquery.materializedview.Recommender`

Store: `recommendation_id`, `recommender`, `subtype`, `state`, `target_resources`, `last_updated_time`, and the additional-details payload (estimated GB saved/month, slot-hours saved/month, suggested MV SQL where present) plus the insight's observation period (evidence-window size for the review card).

**State write-back (required):** on approve → `markClaimed`; on verified → `markSucceeded`; on reject → `markDismissed`. Without this, Active Assist keeps re-surfacing handled items and the console contradicts the tool.

### 4.4 Sources outside INFORMATION_SCHEMA

- **Data Access audit logs** (Cloud Logging sink → BigQuery): cross-project reads. `JOBS` records jobs where they *ran*, not where the data lives — a table can look dead in its home project while another project reads it daily. **No archive/delete rule may fire without org-level jobs data or audit-log confirmation.**
- **Data Transfer Service API**: scheduled-query configs and owners (zombie-schedule rule; quiesce inventory).
- **Cloud Billing export**: ground truth for calibrating estimates vs. invoice.
- **Ownership signals**: Terraform state, dbt `manifest.json`, Dataform repos, table labels → drives executor routing (§9.1).
- **Dataplex / Knowledge Catalog lineage**: downstream-dependent inventory for blast radius on review cards.

### 4.5 Collection governance

Query text contains literals; literals can contain customer data. Store the normalized hash plus a truncated preview; keep full text in a restricted, optionally CMEK-protected table. Collector runs under a read-only service account.

---

## 5. Rule catalog — organized by apply class

Classes are defined by **what machinery the executor needs**, because that drives the approval workflow:

- **Class 1** — in-place configuration change; single API/DDL call; reversible in one step (reversible ≠ non-breaking — see caveats).
- **Class 2** — additive object with ongoing cost; non-destructive; needs a cost watchdog.
- **Class 3** — rebuild-and-rebind or destructive; copy/swap machinery; two-person approval.
- **Class 4** — code change; ships as a pull request against the customer's repo; never bot-applied to production.

### Class 1 — in-place configuration

| ID | Rule | Detection signal | Action | Savings basis | Key caveats |
|---|---|---|---|---|---|
| C1-01 | Add/modify clustering | Native recommender + own filter/join analysis on ≥1 GB tables | Update clustering spec; optional full re-sort via `UPDATE t SET k=k WHERE true` | Slot-hours (Editions) / bytes (on-demand) | Spec change is cheap; **the re-sort is a full-table rewrite** (one-time DML cost) and **resets long-term storage to active for 90 days**. Apply-cost both. |
| C1-02 | Enable `require_partition_filter` | Partitioned table with recurring unfiltered scans | Set option | Bytes | **Instantly breaks any query without a filter.** Mandatory pre-scan of 90-day history for would-break queries; block until list is resolved or owners ack. |
| C1-03 | Partition/table expirations on staging & scratch | Datasets with write-once, never-read tables accumulating | Set default + per-table expirations | Storage $ | Grace-period notice to writers. |
| C1-04 | Time-travel window tuning | High-churn tables under physical billing with 7-day window | Reduce to 2–4 days | Storage $ (time-travel bytes) | Shrinks recovery window — surface explicitly on card. |
| C1-05 | Dataset storage billing model flip | Compression ratio × physical rate < logical cost, net of time-travel + fail-safe bytes | Switch logical ↔ physical | Storage $ | Takes effect within ~24 h; **14-day lock before switching back** — treat as a one-way door for two weeks. High-churn tables can flip the math. |
| C1-06 | Enable history-based (adaptive) optimizations | Project/region where off | Set `default_query_optimizer_options='adaptive=on'` | Slot-hours | Verify current default first — Google has been enabling by default. |
| C1-07 | Reservation resize / idle-slot sharing | `RESERVATIONS_TIMELINE` utilization vs. baseline + autoscale ceiling | Adjust baseline/ceiling/sharing | Slot $ | Model p95 concurrency, not averages. |
| C1-08 | Pause zombie scheduled queries | DTS config writing tables with no downstream reads in 60–90 d (org-wide check) | Pause (not delete) | Bytes/slot + storage | Owner sign-off required; auto-unpause runbook. |
| C1-09 | Cost guardrails | Projects with on-demand spikes | Default `maximum_bytes_billed`, custom quotas | Avoided spend | Advisory tier; can break legitimate big jobs — owner ack. |
| C1-10 | PK/FK unenforced constraints | Frequent joins on candidate keys, constraints absent | Add constraints | Slot-hours (join elimination) | Metadata-only; verify optimizer benefit in verification window. |
| W-01 | Billing-mode fit (on-demand ↔ Editions) | 90-day workload modeled under both schemes | Migration proposal + reservation plan | Whole-workload $ | Mechanically Class 1, **governed as Class 3** (two-person; often the largest single line item). Break-even commonly ~300–500 TiB/month scanned, workload-dependent. |

### Class 2 — additive objects (recurring cost; watchdog required)

| ID | Rule | Detection signal | Action | Savings basis | Key caveats |
|---|---|---|---|---|---|
| C2-01 | Materialized view | Native MV recommender (repetitive families, savings net of maintenance) + own fingerprint clustering | Create MV from suggested SQL | Bytes/slot at query time − refresh cost | Watchdog on refresh spend with auto-disable threshold; monitor `materialized_view_statistics` for rejection reasons. |
| C2-02 | Search index | Point-lookup / needle-in-haystack families on large tables | Create index; optional BACKGROUND reservation for freshness SLO | Slot-hours + latency | Index storage cost; verify index actually used (`search_statistics`). |
| C2-03 | BI Engine reservation | Dashboard-pattern workloads (high-frequency small scans) | Size reservation | Slot/bytes + latency | Validate acceleration coverage before counting savings. |

### Class 3 — rebuild-and-rebind / destructive

| ID | Rule | Detection signal | Action | Savings basis | Key caveats |
|---|---|---|---|---|---|
| C3-01 | Partition an unpartitioned table / change scheme | Native recommender + replay math on ≥100 GB tables with date-filtered scans | Copy-swap state machine (§9.4) | Bytes (on-demand) / slot-hours | Partitioning cannot change in place. Legacy SQL workflows break — pre-scan. New table starts with **no time-travel history**. |
| C3-02 | Consolidate date-sharded tables | `name_YYYYMMDD` families | Union into one partitioned table + compatibility view over old names during migration | Bytes + metadata overhead | Wildcard-query consumers need the compatibility view. |
| C3-03 | Partition granularity change | Tiny daily partitions or nearing the 10,000-partition cap | Day → month (or hour → day) rebuild | Metadata + bytes | Same swap machinery. |
| C3-04 | Denormalize into nested/repeated | Chronic large-join families | Human-led design; tool supplies evidence only | Slot-hours | Never auto-generated in v1. |
| C3-05 | Archive/delete unused tables | No reads in 90–180 d **confirmed org-wide (audit logs + org jobs view)** | Export to GCS (Parquet/Avro) + manifest → snapshot → drop | Storage $ | The cross-project blind spot is the most dangerous failure in the system. Restore runbook mandatory. |

### Class 4 — code changes (PR-only)

Detected with the open-source anti-pattern recognition tool (ZetaSQL parse of jobs history; scope to top-N slot/bytes families) plus custom checks. Each finding opens a PR against the owning dbt/Dataform/repo with the rewritten SQL, the evidence, and the estimated delta. The bot never edits production SQL directly.

| ID | Pattern | Fix |
|---|---|---|
| C4-01 | `SELECT *` on wide tables | Explicit column list |
| C4-02 | Missing partition filter | Add pruning predicate |
| C4-03 | Function-wrapped partition column (defeats pruning) | Rewrite predicate sargable |
| C4-04 | Self-join that should be a window function | Window rewrite |
| C4-05 | Exploding/cross join without pre-aggregation | Pre-aggregate / join-key fix |
| C4-06 | `NOT IN` with nullable subquery | `NOT EXISTS` |
| C4-07 | Exact `COUNT(DISTINCT)` where tolerance allows | `APPROX_COUNT_DISTINCT` |
| C4-08 | Cache-busters (`CURRENT_TIMESTAMP()` etc.) in cacheable queries | Deterministic date bounds |
| C4-09 | `ORDER BY` without `LIMIT` in outer query | Remove/limit |
| C4-10 | STRING join keys where INT64 exists | Advisory only (schema change → escalates to Class 3 evidence) |

---

## 6. Scoring and the plan compiler

### 6.1 Normalize to one currency — honestly

Bytes-saved and slot-hours-saved are not interchangeable:

- **On-demand targets:** only bytes-processed reductions are dollars. Slot/latency improvements score $0 (tracked separately as performance points on the card).
- **Editions targets:** slot-hour reductions are dollars at the applicable edition rate × utilization factor; bytes-only wins that don't reduce slot time score $0.

```
net_monthly_value = gross_monthly_savings
                    − recurring_monthly_cost              # MV refresh, index storage, …
                    − one_time_apply_cost / 12            # rebuild DML, copy compute, eng time

score = net_monthly_value × confidence / risk_weight
        risk_weight: Class 1 = 1.0 · Class 2 = 1.3 · Class 4 = 1.5 · Class 3 = 2.0

confidence = base(rule) × d_summation × d_window × d_volatility × d_history
```

### 6.2 Confidence discounts

| Discount | Trigger | Treatment |
|---|---|---|
| `d_summation` | Native clustering-recommender estimates on tables whose workload includes many-stage queries reading the same table (self-joins, CTE fan-out). Google documents this "subquery summation" overestimation — savings are summed per execution stage without job-level dedup and can exceed the table's total monthly billed bytes. | Detect via plan stages per job; either recompute savings at job level or apply 0.3–0.5. Cap any displayed estimate at the table's actual monthly spend. |
| `d_window` | Evidence window < 30 days | `observed_days / 30` |
| `d_volatility` | Unstable workload | `1 / (1 + CV(weekly bytes))` |
| `d_history` | Rolling realized ÷ predicted per rule type (from §10) | Clamp 0.25–1.25; starts at 1.0 |

### 6.3 Covering the native recommenders' blind spots

Google suppresses recommendations for: tables < 100 GB (partition) / < 10 GB (cluster), tables already partitioned/clustered, high DML write cost, no reads in 30 days, and estimated savings under ~1 slot-hour. Custom rules C1-01/C3-01 extend below those thresholds (with proportionally lower confidence), and C3-05/C1-03 handle exactly the "not read in 30 days" population the recommender ignores.

### 6.4 Plan compiler

Findings are compiled, not listed:

1. **Group** all findings per target object.
2. **Merge** compatible ones into a single change set (partition + cluster on one table = one card, one rebuild).
3. **Conflict-resolve**: a pending repartition supersedes/sequences an MV or index rec on the same table; a billing-model flip and a large re-sort on the same dataset get ordered so the re-sort doesn't land mid-flip.
4. **Attach** a rollback plan and a verification plan to every change set.
5. **Emit** one review card per change set with dependency ordering.

---

## 7. Finding / change-set schema

```sql
CREATE TABLE optimizer_ops.change_sets (
  change_set_id        STRING NOT NULL,          -- UUID
  created_at           TIMESTAMP NOT NULL,
  rule_ids             ARRAY<STRING>,            -- merged findings
  rule_versions        ARRAY<STRING>,
  apply_class          INT64,                    -- 1..4
  source               STRING,                   -- NATIVE_RECOMMENDER | CUSTOM_RULE | ANTIPATTERN_TOOL | MIXED
  native_rec_names     ARRAY<STRING>,            -- for Recommender API write-back

  target_project       STRING,
  target_dataset       STRING,
  target_table         STRING,                   -- NULL for dataset/project-scope
  target_region        STRING,

  finding_summary      STRING,                   -- plain-English "what and why"
  evidence             JSON,                     -- metrics, sample job ids, window, insight refs
  observation_days     INT64,
  current_config_ddl   STRING,                   -- captured "before"

  proposed_change      JSON,                     -- structured params + generated DDL / PR payload
  execution_route      STRING,                   -- CI_PULL_REQUEST | DIRECT_GUARDED
  owner_principal      STRING,
  owner_source         STRING,                   -- LABEL | IAC_CODEOWNERS | TOP_WRITER | DATASET_ADMIN

  gross_monthly_savings_usd    NUMERIC,
  recurring_monthly_cost_usd   NUMERIC,
  one_time_apply_cost_usd      NUMERIC,
  net_monthly_value_usd        NUMERIC,
  savings_basis        STRING,                   -- BYTES_ON_DEMAND | SLOT_EDITIONS | STORAGE | MIXED
  confidence           NUMERIC,                  -- post-discount
  confidence_factors   JSON,                     -- each discount and its value
  score                NUMERIC,

  blast_radius         JSON,                     -- dependent views/MVs/schedules/consumers (lineage)
  risk_notes           ARRAY<STRING>,            -- footguns triggered (e.g. RPF_BREAKS_N_QUERIES)

  state                STRING NOT NULL,          -- see lifecycle below
  state_history        ARRAY<STRUCT<state STRING, at TIMESTAMP, actor STRING, note STRING>>,
  approvals            ARRAY<STRUCT<principal STRING, at TIMESTAMP, role STRING>>,
  rejection_reason     STRING,                   -- BREAKS_PIPELINE | TABLE_DEPRECATED |
                                                 -- SAVINGS_NOT_CREDIBLE | WRONG_OWNER | TIMING | OTHER
  rejection_note       STRING,
  snooze_until         TIMESTAMP,
  expires_at           TIMESTAMP,                -- auto-void when evidence goes stale

  rollback_plan        JSON,
  verification_plan    JSON,                     -- baseline window, families, thresholds
  applied_at           TIMESTAMP,
  verification_result  JSON,                     -- realized $, per-family deltas
  realized_over_predicted NUMERIC
)
PARTITION BY DATE(created_at)
CLUSTER BY state, target_dataset;
```

**Lifecycle states**

```
DETECTED → SCORED → PENDING_REVIEW → { APPROVED | REJECTED | EXPIRED | SUPERSEDED }
APPROVED → SCHEDULED → APPLYING → APPLIED → VERIFYING → { VERIFIED | REGRESSED }
REGRESSED → ROLLING_BACK → ROLLED_BACK
APPLYING → FAILED (clean abort; pre-swap failures never leave partial state)
```

---

## 8. Human-in-the-loop workflow

**The card is a contract.** Every card shows: plain-English finding; evidence (metrics + window length); estimated savings **with confidence band** and the basis (bytes vs. slots vs. storage); one-time and recurring costs; the exact change (DDL or PR diff link); blast radius from lineage; risk notes (every triggered footgun, e.g. "enabling this breaks 3 queries — listed"); the rollback plan; the verification plan; and an expiry date.

**Routing.** Owner resolution order: table `owner` label → IaC CODEOWNERS → top writer principal (90-day jobs) → dataset admin. Approval policy:

| Class | Approval |
|---|---|
| 1 | Resolved owner |
| 2 | Owner + acknowledgment of the recurring-cost watchdog |
| 3 (and W-01) | Owner **and** platform/second approver (two-person) |
| 4 | Normal code review in the owning repo (the PR *is* the approval) |

**Expiry.** Approvals are valid 14 days; unactioned approvals lapse. Cards auto-void (`EXPIRED`) when their evidence window is > 45 days stale — recommendations built on old workload data must not sit in a queue.

**Rejection capture is mandatory** — reason enum + free text. Rejections snooze the finding (default 90 days; `TABLE_DEPRECATED` snoozes until the table changes; `SAVINGS_NOT_CREDIBLE` feeds `d_history` down for that rule). A thumbs-down that is just a dismiss button caps the tool at v1 forever; the rejection corpus is the training set.

**Global guardrails:** change windows + customer freeze calendar; rate limits (max 1 structural change per table per 7 days; max N Class-1 changes per dataset per day); kill switch that halts all applies.

---

## 9. Executor

### 9.1 Routing by ownership (first decision, made at detection time)

- **IaC/dbt/Dataform-managed target** → the executor emits a **merge request** into the customer's repo/CI (Terraform resource change, dbt `partition_by`/`cluster_by` config, Dataform config), attaching the evidence and verification plan. Out-of-band DDL on managed tables creates state drift and gets reverted on the next apply — so the PR path is not just governance, it's correctness.
- **Unmanaged target** → direct **guarded DDL** path below.
- The card always displays which route will run.

### 9.2 Class 1 path (direct)

capture current config → pre-checks (rule-specific, e.g. C1-02 would-break query scan must be empty or acked) → apply single change → record → enter VERIFYING.

### 9.3 Class 2 path (direct)

create object → register **cost watchdog** (MV refresh spend, index storage) with an auto-disable threshold and owner alert → VERIFYING includes watchdog outcomes.

### 9.4 Class 3 state machine — copy, swap, rebind (with streaming branch)

```
S0 PRECHECK ─ S1 CAPTURE ─ S2 QUIESCE ─┬─ S2a DRAIN (pausable writers)
                                       └─ S2b DUAL-WRITE (streaming)
        → S3 BUILD → S4 DELTA-SYNC → S5 VALIDATE → S6 SWAP → S7 REBIND
        → S8 POSTCHECK → S9 HOLD → VERIFYING

Any failure S0–S5  → CLEAN ABORT: resume writers, drop copy, card → FAILED (no partial state)
Failure/regression after S6 → ROLLBACK: reverse rename within hold window, rebind, resume
```

**S0 PRECHECK** — change window open; freeze calendar clear; legacy SQL scan (partition changes break legacy SQL workflows — block until migrated or acked); copy capacity/budget check; conflict check against other in-flight change sets on the object.

**S1 CAPTURE** — snapshot everything bound to the table ID, because renames don't carry it: full DDL; IAM policy (`getIamPolicy`); row-access policies; column-level security / policy tags; labels, descriptions, table & column options; constraints; inventory of dependent views, MVs, clones, snapshots, search indexes, scheduled queries, DTS configs, orchestrator DAGs. Take a table snapshot (cheap; deltas-only billing) as the restore anchor.

**S2 QUIESCE** — pause scheduled queries and DTS configs; hold Composer/Dataform/dbt jobs touching the table. Losing writes that land between the `AS SELECT` and the rename is the single most common way this operation goes wrong. Branch:
- **S2a DRAIN** (batch-only writers): stop producers, wait for watermark, proceed.
- **S2b DUAL-WRITE** (streaming via Storage Write API / Dataflow — cannot be politely paused): either (a) point the writer at both old and new tables for the migration window, or (b) buffer upstream (Pub/Sub retain + replay) across a short cutover. Choose per pipeline; record which.

**S3 BUILD** — `CREATE TABLE new PARTITION BY … CLUSTER BY … AS SELECT …` (per-partition loads for very large tables). Apply captured options/labels. Do **not** enable `require_partition_filter` during migration.

**S4 DELTA-SYNC** — if not dual-writing, MERGE all writes captured since S3 started.

**S5 VALIDATE** — schema equality; per-partition row counts; aggregate checksum (e.g. `SUM(FARM_FINGERPRINT(TO_JSON_STRING(t)))` full or stratified sample); dry-run the top-20 historical query families against the new table and compare byte estimates to prediction.

**S6 SWAP** — brief write freeze → final micro-delta → `ALTER TABLE original RENAME TO original_bak_<ts>` → `ALTER TABLE new RENAME TO original`. Seconds of unavailability.

**S7 REBIND** — re-apply IAM policy; recreate row-access policies; reattach policy tags/CLS; recreate search indexes; `CREATE OR REPLACE` dependent MVs; resume schedules, DTS, DAGs, and streaming writers (collapse dual-write).

**S8 POSTCHECK** — canary queries per top family; watch error rates and consumer failures for T+2h.

**S9 HOLD** — keep `original_bak_<ts>` with a 7-day expiration (the undo button). Record explicitly: **the new table starts with no time-travel history**; the S1 snapshot is the recovery anchor for the pre-swap era.

**C3-05 (archive/delete) variant** — replaces S3–S7 with: org-wide read confirmation (audit logs + org jobs view, 180 d) → export to GCS (Parquet/Avro) + manifest → snapshot → drop → restore runbook attached to the card.

### 9.5 Class 4 path

Anti-pattern tool output (scoped to top-N cost families) → rewrite proposal → PR into the owning repo with evidence and predicted delta → merged/closed status synced back to the change set. No verification hold needed beyond the standard §10 measurement.

---

## 10. Verification and the feedback loop

**Baseline.** At approval time, freeze 14–28 days of per-family metrics for the target: keyed on `query_hashes.normalized_literals` joined to `referenced_tables` — comparing the *same query shapes*, not raw table totals, so workload drift doesn't pollute the receipt. Metrics: bytes billed / execution, slot-ms / execution, p50/p95 duration, execution count.

**Post-window.** Same length, same families.

**Regression rule.** Cost-per-execution or p95 duration up > 15% on ≥ 3 families with ≥ 20 executions → `REGRESSED`: page the owner, present one-click rollback. (Clustering on the wrong columns genuinely makes things worse; this is not theoretical.)

**Receipt.** Realized savings = Σ Δ(cost/exec) × post-window executions, priced by the target's billing mode. Published on the card and in a monthly roll-up: *"estimated $X/mo at 0.7 confidence → realized $Y/mo."*

**Learning.** `realized ÷ predicted` per rule type rolls into `d_history`. This single loop is what converts the tool from a dashboard into something people let touch production — and it is the gate for Phase 5 auto-approval.

---

## 11. Security and governance

- **Two identities.** Collector SA: read-only (jobs viewer at org scope where possible, recommender viewer roles, ops-dataset writer). Executor SA: per-target, time-boxed grants issued just-in-time at approval; never a standing project-wide admin.
- **Audit.** Every state transition and every statement executed → `optimizer_ops.audit_log` + Cloud Audit Logs.
- **Query-text privacy.** Hash + truncated preview by default; full text restricted (CMEK optional). Relevant for regulated customers (healthcare, telco).
- **Recommender sync.** markClaimed / markSucceeded / markDismissed as per §4.3.
- **Blast-radius sources.** `referenced_tables` (180 d) + audit logs + Dataplex lineage; a card without a populated blast radius cannot be approved for Class 3.

---

## 12. Rollout phases

| Phase | Weeks | Milestone | Exit criterion |
|---|---|---|---|
| 0 | 1 | Ops project, datasets, SAs, Terraform | Collector dry run |
| 1 | 2–5 | **Observe-only**: telemetry + scored findings + savings report | Stakeholders agree the numbers are credible — prove the number before asking for write access |
| 2 | 5–8 | Class 1 with full HITL + verification | ≥ 5 verified Class-1 applies, zero incidents |
| 3 | 8–11 | Class 2 + cost watchdogs + auto-disable | MV/index recurring costs tracked to ±10% |
| 4 | 11–16 | Class 3 dual-path (PR + guarded DDL), Class 4 PR bot | ≥ 2 clean swaps incl. one streaming dual-write |
| 5 | ongoing | Policy auto-approval for rules with stable realized/predicted (≥ K applies within ± 20%), still logged and reversible | Per-rule graduation |

---

## 13. Risks, open questions, build-vs-buy

- **Market.** DoiT (BigQuery Lens), Masthead Data, and adjacent FinOps vendors sell into this space, and Google keeps absorbing detection (recommenders, Gemini "Optimize"). The durable differentiator is not detection — it is **lineage-aware safe apply + verified savings attribution**. (Verify any competitor names/claims independently before customer-facing use.)
- **Open questions for v1:** multi-region ops-dataset strategy; handling tables owned by external SaaS writers; BigLake/Iceberg (observe-only); how aggressively to backfill re-sorts on very large clustered tables (cost vs. benefit per table).
- **Honest limitation:** savings estimates assume the last 30–90 days predict the future; `d_volatility` mitigates but does not eliminate this.

---

## Appendix A — Operational footguns (quick reference)

1. Partitioning cannot change in place; copy-swap only; new table has no time-travel history; legacy SQL workflows break.
2. Renames don't carry ID-bound artifacts: IAM, row/column security, policy tags, clones, snapshots, search indexes, MVs — capture and rebind (S1/S7).
3. Writers must be quiesced or dual-written across the swap; streaming cannot be paused politely.
4. `require_partition_filter` is reversible but instantly breaking — pre-scan history for would-break queries.
5. Clustering spec changes affect only new data; re-sorting is a paid full rewrite and resets long-term storage to active for 90 days.
6. Storage billing model: ~24 h to take effect, 14-day lock before switching back; physical billing charges time-travel + fail-safe bytes.
7. Native clustering estimates can exceed a table's actual monthly spend via subquery summation — discount and cap.
8. Native recommenders are silent below 100 GB / 10 GB, on already-configured tables, on high-DML tables, on unread tables, and under ~1 slot-hour of savings — custom rules cover the gaps.
9. `JOBS` is scoped to where the job ran: unused-table decisions require org-level jobs data or Data Access audit logs.
10. 10,000-partition ceiling per table; tiny daily partitions add metadata overhead — consider monthly granularity.
11. On-demand vs. Editions changes which savings are real dollars; never mix bases in one estimate.

## Appendix B — Pricing constants (as of Aug 2026; verify per region before quoting)

- On-demand compute: $6.25 / TiB processed; first 1 TiB per month free.
- Editions compute: ≈ $0.04–$0.10 per slot-hour (Standard → Enterprise Plus); pay-as-you-go or 1/3-yr commitments.
- Storage (logical): $0.02 / GiB-mo active, $0.01 long-term. Storage (physical, compressed): $0.04 / $0.02 — typically wins at ≥ ~2:1 compression net of time-travel/fail-safe.
- Long-term rate applies automatically after 90 consecutive days without modification (per table/partition).
- Break-even for capacity pricing: commonly ~300–500 TiB scanned/month, workload-dependent.

## Appendix C — References

- Partition & cluster recommendations (incl. apply procedure, caveats, subquery summation): https://cloud.google.com/bigquery/docs/manage-partition-cluster-recommendations
- Materialized view recommendations: https://cloud.google.com/bigquery/docs/manage-materialized-recommendations
- Recommendations overview: https://cloud.google.com/bigquery/docs/recommendations-intro
- `INFORMATION_SCHEMA.RECOMMENDATIONS`: https://cloud.google.com/bigquery/docs/information-schema-recommendations
- `INFORMATION_SCHEMA.INSIGHTS`: https://cloud.google.com/bigquery/docs/information-schema-insights
- `INFORMATION_SCHEMA.JOBS`: https://cloud.google.com/bigquery/docs/information-schema-jobs
- Cost best practices: https://cloud.google.com/bigquery/docs/best-practices-costs
- Pricing: https://cloud.google.com/bigquery/pricing
- Anti-pattern recognition tool (open source): https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition
- Launch blog (recommender internals — candidate generation, read/write pattern analyzers): https://cloud.google.com/blog/products/data-analytics/new-bigquery-partitioning-and-clustering-recommendations/
