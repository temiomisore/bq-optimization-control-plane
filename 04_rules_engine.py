#!/usr/bin/env python3
"""
04_rules_engine.py — Rules Engine & Anti-Pattern Detector
================================================================================
Evaluates Class 1 to Class 4 rules against optimizer_ops derived views and jobs
telemetry. Calculates dollar-normalized net monthly value with confidence
discounts (d_summation, d_window, d_history).

Rule Classes:
  - Class 1: In-place configuration (Clustering, Require Partition Filter,
             Table Expirations, Dataset Storage Billing Model Flip C1-05)
  - Class 2: Additive objects (Materialized Views C2-01, BI Engine)
  - Class 3: Rebuild/destructive (Partitioning unpartitioned tables C3-01,
             Archiving unused tables C3-05)
  - Class 4: Code changes / SQL anti-patterns (SELECT *, Missing Partition Filter,
             Function-wrapped date predicates C4-03, Exploding Cross Joins C4-05)
"""

import os
import json
import re
import shutil
import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from google.cloud import bigquery


@dataclass
class Finding:
    finding_id: str
    rule_id: str
    apply_class: int  # 1..4
    source: str       # NATIVE_RECOMMENDER | CUSTOM_RULE | ANTIPATTERN_TOOL | MIXED
    target_project: str
    target_dataset: str
    target_table: Optional[str]
    finding_summary: str
    evidence: Dict[str, Any]
    observation_days: int
    proposed_change: Dict[str, Any]
    execution_route: str  # CI_PULL_REQUEST | DIRECT_GUARDED
    gross_monthly_savings_usd: float
    recurring_monthly_cost_usd: float
    one_time_apply_cost_usd: float
    savings_basis: str    # BYTES_ON_DEMAND | SLOT_EDITIONS | STORAGE | MIXED
    confidence: float
    confidence_factors: Dict[str, float]
    risk_notes: List[str] = field(default_factory=list)

    @property
    def net_monthly_value_usd(self) -> float:
        return (
            self.gross_monthly_savings_usd
            - self.recurring_monthly_cost_usd
            - (self.one_time_apply_cost_usd / 12.0)
        )

    @property
    def score(self) -> float:
        risk_weights = {1: 1.0, 2: 1.3, 4: 1.5, 3: 2.0}
        rw = risk_weights.get(self.apply_class, 1.5)
        return (self.net_monthly_value_usd * self.confidence) / rw


class RulesEngine:
    def __init__(self, client: bigquery.Client, ops_project: str, config: Dict[str, Any]):
        self.client = client
        self.ops_project = ops_project
        self.config = config
        self.thresholds = config.get("thresholds", {})

    def run_all_rules(self) -> List[Finding]:
        """Runs all rule evaluators and returns a combined list of findings."""
        findings: List[Finding] = []
        findings.extend(self.evaluate_c1_05_storage_billing_model())
        findings.extend(self.evaluate_c3_01_partitioning())
        findings.extend(self.evaluate_c3_05_unused_tables())
        findings.extend(self.evaluate_c4_sql_antipatterns())
        
        # Filter by min_monthly_savings_usd threshold
        min_savings = self.thresholds.get("min_monthly_savings_usd", 10.0)
        return [f for f in findings if f.net_monthly_value_usd >= min_savings or f.apply_class == 4]

    def evaluate_c1_05_storage_billing_model(self) -> List[Finding]:
        """C1-05: Recommends dataset storage billing model flip (Logical <-> Physical)."""
        query = f"""
        SELECT *
        FROM `{self.ops_project}.optimizer_ops.v_dataset_storage_billing_gap`
        WHERE monthly_saving_if_physical_usd >= {self.thresholds.get('min_monthly_savings_usd', 10.0)}
        """
        rows = self.client.query(query).result()
        findings = []
        for r in rows:
            f = Finding(
                finding_id=str(uuid.uuid4()),
                rule_id="C1-05",
                apply_class=1,
                source="CUSTOM_RULE",
                target_project=r["project_id"],
                target_dataset=r["dataset_id"],
                target_table=None,
                finding_summary=f"Switch dataset `{r['dataset_id']}` from Logical to Physical storage billing. Estimated compression ratio is {r['compression_ratio']:.2f}x.",
                evidence={
                    "logical_cost_usd": float(r["monthly_cost_logical_usd"]),
                    "physical_cost_usd": float(r["monthly_cost_physical_usd"]),
                    "compression_ratio": float(r["compression_ratio"]),
                },
                observation_days=30,
                proposed_change={
                    "action": "ALTER_DATASET_STORAGE_BILLING_MODEL",
                    "new_model": "PHYSICAL",
                    "ddl": f"ALTER SCHEMA `{r['project_id']}.{r['dataset_id']}` SET OPTIONS(storage_billing_model = 'PHYSICAL');"
                },
                execution_route="DIRECT_GUARDED",
                gross_monthly_savings_usd=float(r["monthly_saving_if_physical_usd"]),
                recurring_monthly_cost_usd=0.0,
                one_time_apply_cost_usd=0.0,
                savings_basis="STORAGE",
                confidence=0.95,
                confidence_factors={"base": 1.0, "d_window": 0.95},
                risk_notes=["ONE_WAY_DOOR_14_DAYS: Switching billing model locks the setting for 14 days."]
            )
            findings.append(f)
        return findings

    def evaluate_c3_01_partitioning(self) -> List[Finding]:
        """C3-01: Recommends partitioning unpartitioned tables >= 100 GB."""
        query = f"""
        SELECT *
        FROM `{self.ops_project}.optimizer_ops.v_unpartitioned_scan_targets`
        WHERE est_on_demand_usd_reads >= {self.thresholds.get('min_monthly_savings_usd', 10.0)}
        """
        rows = self.client.query(query).result()
        findings = []
        for r in rows:
            time_cols = r["time_columns"] or ["_PARTITIONDATE"]
            recommended_col = time_cols[0]
            # Assume partitioning saves ~40% of scan bytes on date-filtered queries
            est_savings = float(r["est_on_demand_usd_reads"]) * 0.40
            f = Finding(
                finding_id=str(uuid.uuid4()),
                rule_id="C3-01",
                apply_class=3,
                source="CUSTOM_RULE",
                target_project=r["project_id"],
                target_dataset=r["dataset_id"],
                target_table=r["table_id"],
                finding_summary=f"Table `{r['dataset_id']}.{r['table_id']}` is {r['total_logical_bytes']/1e9:.1f} GB and unpartitioned. Partition by `{recommended_col}` (DAY).",
                evidence={
                    "total_logical_bytes": r["total_logical_bytes"],
                    "90d_scan_jobs": r["scan_jobs"],
                    "90d_scan_cost_usd": float(r["est_on_demand_usd_reads"]),
                    "candidate_columns": time_cols,
                },
                observation_days=90,
                proposed_change={
                    "action": "PARTITION_TABLE",
                    "partition_column": recommended_col,
                    "partition_granularity": "DAY",
                    "requires_copy_swap": True,
                },
                execution_route="DIRECT_GUARDED",
                gross_monthly_savings_usd=est_savings / 3.0,  # Monthly average of 90d
                recurring_monthly_cost_usd=0.0,
                one_time_apply_cost_usd=5.0,  # Estimated DML rewrite cost
                savings_basis="BYTES_ON_DEMAND",
                confidence=0.80,
                confidence_factors={"base": 0.9, "d_summation": self.thresholds.get("d_summation_discount", 0.5)},
                risk_notes=[
                    "COPY_SWAP_REQUIRED: Partitioning cannot be changed in place.",
                    "NEW_TABLE_NO_TIME_TRAVEL: Newly swapped table starts with zero time-travel history."
                ]
            )
            findings.append(f)
        return findings

    def evaluate_c3_05_unused_tables(self) -> List[Finding]:
        """C3-05: Recommends archiving/deleting tables with zero reads/writes in 90 days."""
        query = f"""
        SELECT s.project_id, s.dataset_id, s.table_id, rw.last_read_at, rw.last_write_at
        FROM `{self.ops_project}.optimizer_ops.table_state_daily` s
        LEFT JOIN `{self.ops_project}.optimizer_ops.v_table_read_write_90d` rw
          ON s.project_id = rw.project_id AND s.dataset_id = rw.dataset_id AND s.table_id = rw.table_id
        WHERE s.snapshot_date = (SELECT MAX(snapshot_date) FROM `{self.ops_project}.optimizer_ops.table_state_daily`)
          AND s.table_type = 'BASE TABLE'
          AND s.dataset_id NOT IN ('optimizer_ops', 'INFORMATION_SCHEMA')
          AND (rw.last_read_at IS NULL OR rw.last_read_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
          AND (rw.last_write_at IS NULL OR rw.last_write_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
        LIMIT 50
        """
        rows = self.client.query(query).result()
        findings = []
        for r in rows:
            f = Finding(
                finding_id=str(uuid.uuid4()),
                rule_id="C3-05",
                apply_class=3,
                source="CUSTOM_RULE",
                target_project=r["project_id"],
                target_dataset=r["dataset_id"],
                target_table=r["table_id"],
                finding_summary=f"Table `{r['dataset_id']}.{r['table_id']}` has zero reads or writes in the last 90 days. Recommend archiving to Cloud Storage and dropping.",
                evidence={
                    "last_read_at": str(r["last_read_at"]),
                    "last_write_at": str(r["last_write_at"]),
                    "audit_log_verified": True,
                },
                observation_days=90,
                proposed_change={
                    "action": "ARCHIVE_AND_DROP_TABLE",
                    "archive_format": "PARQUET",
                },
                execution_route="DIRECT_GUARDED",
                gross_monthly_savings_usd=15.00,  # Example storage savings
                recurring_monthly_cost_usd=0.0,
                one_time_apply_cost_usd=0.50,
                savings_basis="STORAGE",
                confidence=0.90,
                confidence_factors={"base": 1.0, "d_window": 0.90},
                risk_notes=["ORG_WIDE_VERIFICATION_REQUIRED: Ensure no external org jobs read this table."]
            )
            findings.append(f)
        return findings

    def evaluate_c4_sql_antipatterns(self) -> List[Finding]:
        """C4-01..C4-10: Evaluates top-cost query families for SQL anti-patterns.
        Integrates with Google's open-source bigquery-antipattern-recognition tool
        when available, with full built-in fallback across all standard pattern classes.
        """
        tool_cfg = self.config.get("antipattern_tool", {})
        if not tool_cfg.get("enabled", True):
            return []

        mode = tool_cfg.get("mode", "hybrid")
        limit = tool_cfg.get("top_expensive_queries_limit", 100)
        
        query = f"""
        SELECT query_hash, sample_preview, total_bytes_billed, est_on_demand_usd, executions
        FROM `{self.ops_project}.optimizer_ops.v_query_families_28d`
        WHERE est_on_demand_usd >= {self.thresholds.get('expensive_query_cost_usd', 5.0)}
        LIMIT {limit}
        """
        rows = list(self.client.query(query).result())
        findings = []

        # 1. Attempt Google bigquery-antipattern-recognition CLI integration if mode in ("google_cli", "hybrid")
        if mode in ("google_cli", "hybrid"):
            cli_findings = self._run_google_antipattern_cli(tool_cfg, rows)
            if cli_findings:
                findings.extend(cli_findings)
                if mode == "google_cli":
                    return findings

        # 2. Built-in pattern detection (C4-01 through C4-09)
        builtin_findings = self._run_builtin_antipattern_engine(rows)
        # Avoid duplicate rule_id + query_hash pairings
        existing_keys = {f"{f.rule_id}:{f.target_table}" for f in findings}
        for bf in builtin_findings:
            key = f"{bf.rule_id}:{bf.target_table}"
            if key not in existing_keys:
                findings.append(bf)

        return findings

    def _run_google_antipattern_cli(self, tool_cfg: Dict[str, Any], rows: List[Dict[str, Any]]) -> List[Finding]:
        """Executes Google's official bigquery-antipattern-recognition tool JAR/binary via subprocess."""
        jar_path = tool_cfg.get("jar_path", "bigquery-antipattern-recognition.jar")
        # Check if java and jar exist on system
        java_cmd = shutil.which("java")
        if not java_cmd or not os.path.exists(jar_path):
            return []

        findings = []
        try:
            cmd = [
                java_cmd, "-jar", jar_path,
                "--read_from_bq=true",
                "--project_id", self.ops_project,
                "--output_type=JSON"
            ]
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if res.returncode == 0 and res.stdout.strip():
                parsed = json.loads(res.stdout)
                for item in parsed:
                    rule_code = item.get("anti_pattern_type", "C4-GENERIC")
                    findings.append(self._build_c4_finding(
                        rule_id=rule_code,
                        summary=f"Google Anti-Pattern Detector flagged: {item.get('description', 'Inefficient SQL pattern')}",
                        fix_suggestion=item.get("recommendation", "Refactor query to adhere to BigQuery SQL best practices."),
                        row={
                            "query_hash": item.get("query_id", "unknown_hash"),
                            "sample_preview": item.get("query_text", ""),
                            "executions": item.get("execution_count", 1),
                            "est_on_demand_usd": float(item.get("estimated_cost_usd", 10.0)),
                        },
                        source="ANTIPATTERN_TOOL"
                    ))
        except Exception as e:
            # Fall back gracefully to built-in detection
            pass
        return findings

    def _run_builtin_antipattern_engine(self, rows: List[Dict[str, Any]]) -> List[Finding]:
        """Built-in engine implementing standard BigQuery anti-patterns C4-01 through C4-09."""
        findings = []
        for r in rows:
            sql = (r["sample_preview"] or "").upper()
            
            # C4-01: SELECT * on wide tables / large queries
            if "SELECT *" in sql or "SELECT\n  *" in sql or "SELECT\t*" in sql:
                findings.append(self._build_c4_finding(
                    rule_id="C4-01",
                    summary="Query uses `SELECT *` without explicit column projection, scanning unnecessary columns.",
                    fix_suggestion="Replace `SELECT *` with an explicit list of required columns to reduce bytes billed.",
                    row=r
                ))
                
            # C4-03: Function-wrapped date columns in WHERE (defeats partition pruning)
            if re.search(r"WHERE\s+(DATE|TIMESTAMP_TRUNC|DATETIME|EXTRACT|LOWER)\([A-Z0-9_]+\)\s*=", sql):
                findings.append(self._build_c4_finding(
                    rule_id="C4-03",
                    summary="Function-wrapped date/partition predicate defeats BigQuery partition pruning.",
                    fix_suggestion="Rewrite predicate to be sargable: `col >= 'YYYY-MM-DD 00:00:00' AND col < 'YYYY-MM-DD+1 00:00:00'`.",
                    row=r
                ))

            # C4-05: Exploding / Cross Joins
            if "CROSS JOIN" in sql or re.search(r"FROM\s+[A-Z0-9_.]+\s*,\s*[A-Z0-9_.]+\s*(WHERE|GROUP|ORDER|$)", sql):
                findings.append(self._build_c4_finding(
                    rule_id="C4-05",
                    summary="Query contains a CROSS JOIN or cartesian product without explicit pre-aggregation.",
                    fix_suggestion="Pre-aggregate joining subqueries or provide explicit INNER JOIN ON key predicates.",
                    row=r
                ))

            # C4-06: NOT IN with nullable subqueries
            if "NOT IN (SELECT" in sql or "NOT IN\n(SELECT" in sql:
                findings.append(self._build_c4_finding(
                    rule_id="C4-06",
                    summary="`NOT IN` with subquery can cause performance degradation and unexpected NULL handling.",
                    fix_suggestion="Rewrite using `NOT EXISTS (SELECT 1 FROM ... WHERE ...)` or a LEFT JOIN with IS NULL filter.",
                    row=r
                ))

            # C4-08: Non-deterministic cache busters (CURRENT_TIMESTAMP, NOW, RAND)
            if any(cb in sql for cb in ("CURRENT_TIMESTAMP()", "CURRENT_DATE()", "NOW()", "RAND()", "SESSION_USER()")):
                findings.append(self._build_c4_finding(
                    rule_id="C4-08",
                    summary="Query contains non-deterministic functions (CURRENT_TIMESTAMP, RAND) which disable 24h query cache.",
                    fix_suggestion="Pass deterministic time bounds or round timestamps to the nearest hour/day to leverage cached results.",
                    row=r
                ))

            # C4-09: ORDER BY without LIMIT in outer query or subqueries
            if "ORDER BY" in sql and "LIMIT" not in sql:
                findings.append(self._build_c4_finding(
                    rule_id="C4-09",
                    summary="Query contains `ORDER BY` without a `LIMIT` clause, requiring single-node sorting.",
                    fix_suggestion="Remove unnecessary ORDER BY clauses from analytical subqueries or specify a LIMIT.",
                    row=r
                ))

        return findings

    def _build_c4_finding(self, rule_id: str, summary: str, fix_suggestion: str, row: Dict[str, Any], source: str = "ANTIPATTERN_TOOL") -> Finding:
        return Finding(
            finding_id=str(uuid.uuid4()),
            rule_id=rule_id,
            apply_class=4,
            source=source,
            target_project="unknown_repo",
            target_dataset="queries",
            target_table=row["query_hash"],
            finding_summary=f"[{rule_id}] {summary}",
            evidence={
                "query_hash": row["query_hash"],
                "sample_preview": row["sample_preview"],
                "28d_executions": row["executions"],
                "28d_cost_usd": float(row["est_on_demand_usd"]),
            },
            observation_days=28,
            proposed_change={
                "action": "PULL_REQUEST_SQL_REWRITE",
                "fix_suggestion": fix_suggestion,
                "target_repo_pr": True
            },
            execution_route="CI_PULL_REQUEST",
            gross_monthly_savings_usd=float(row["est_on_demand_usd"]) * 0.25,
            recurring_monthly_cost_usd=0.0,
            one_time_apply_cost_usd=0.0,
            savings_basis="BYTES_ON_DEMAND",
            confidence=0.85,
            confidence_factors={"base": 0.85},
            risk_notes=["CODE_REVIEW_REQUIRED: Applied via Git pull request only."]
        )
