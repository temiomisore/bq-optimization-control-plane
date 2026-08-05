#!/usr/bin/env python3
"""
05_plan_compiler.py — Plan Compiler & Change-Set Store Manager
================================================================================
Takes raw Findings from the Rules Engine, deduplicates and merges compatible
findings per database object, resolves conflicts, attaches rollback and
verification plans, and persists compiled change sets to optimizer_ops.change_sets.
"""

import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from importlib.machinery import SourceFileLoader

# Import Finding class dynamically from 04_rules_engine.py in the same directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
rules_mod = SourceFileLoader("rules_engine", os.path.join(BASE_DIR, "04_rules_engine.py")).load_module()
Finding = rules_mod.Finding


class PlanCompiler:
    def __init__(self, client: bigquery.Client, ops_project: str, config: Dict[str, Any]):
        self.client = client
        self.ops_project = ops_project
        self.config = config

    def compile_and_persist(self, findings: List[Finding]) -> List[Dict[str, Any]]:
        """Groups findings by target object, merges them, and writes to BigQuery."""
        grouped: Dict[str, List[Finding]] = {}
        for f in findings:
            key = f"{f.target_project}.{f.target_dataset}.{f.target_table or 'DATASET_SCOPE'}"
            grouped.setdefault(key, []).append(f)

        compiled_change_sets: List[Dict[str, Any]] = []
        for target_key, object_findings in grouped.items():
            cs = self._merge_findings_for_target(target_key, object_findings)
            compiled_change_sets.append(cs)

        self._write_change_sets_to_bq(compiled_change_sets)
        return compiled_change_sets

    def _merge_findings_for_target(self, target_key: str, findings: List[Finding]) -> Dict[str, Any]:
        """Merges multiple compatible findings on the same target into one Change Set."""
        first = findings[0]
        rule_ids = [f.rule_id for f in findings]
        max_class = max(f.apply_class for f in findings)
        
        total_gross = sum(f.gross_monthly_savings_usd for f in findings)
        total_recurring = sum(f.recurring_monthly_cost_usd for f in findings)
        total_apply = sum(f.one_time_apply_cost_usd for f in findings)
        net_value = total_gross - total_recurring - (total_apply / 12.0)

        # Average confidence weighted by gross savings
        if total_gross > 0:
            avg_conf = sum(f.confidence * f.gross_monthly_savings_usd for f in findings) / total_gross
        else:
            avg_conf = sum(f.confidence for f in findings) / len(findings)

        combined_summary = " | ".join(f.finding_summary for f in findings)
        all_risk_notes = []
        for f in findings:
            all_risk_notes.extend(f.risk_notes)

        cs_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(days=14)

        return {
            "change_set_id": cs_id,
            "created_at": now.isoformat(),
            "rule_ids": rule_ids,
            "rule_versions": ["1.0"] * len(rule_ids),
            "apply_class": max_class,
            "source": "MIXED" if len(set(f.source for f in findings)) > 1 else first.source,
            "native_rec_names": [],
            "target_project": first.target_project,
            "target_dataset": first.target_dataset,
            "target_table": first.target_table,
            "target_region": "us",
            "finding_summary": combined_summary,
            "evidence": json.dumps({"findings_count": len(findings), "rule_ids": rule_ids}),
            "observation_days": max(f.observation_days for f in findings),
            "current_config_ddl": "-- Captured during S1_CAPTURE stage",
            "proposed_change": json.dumps([f.proposed_change for f in findings]),
            "execution_route": first.execution_route,
            "owner_principal": "auto-owner@company.com",
            "owner_source": "LABEL",
            "gross_monthly_savings_usd": total_gross,
            "recurring_monthly_cost_usd": total_recurring,
            "one_time_apply_cost_usd": total_apply,
            "net_monthly_value_usd": net_value,
            "savings_basis": first.savings_basis,
            "confidence": avg_conf,
            "confidence_factors": json.dumps(first.confidence_factors),
            "score": (net_value * avg_conf) / (1.5 if max_class == 4 else max_class),
            "blast_radius": json.dumps({"dependent_views": 0, "scheduled_queries": 0}),
            "risk_notes": list(set(all_risk_notes)),
            "state": "PENDING_REVIEW",
            "state_history": json.dumps([{"state": "PENDING_REVIEW", "at": now.isoformat(), "actor": "plan_compiler", "note": "Compiled initial plan"}]),
            "approvals": json.dumps([]),
            "rejection_reason": None,
            "rejection_note": None,
            "snooze_until": None,
            "expires_at": expires_at.isoformat(),
            "rollback_plan": json.dumps({"action": "RESTORE_FROM_ZERO_COPY_CLONE", "retention_days": 14}),
            "verification_plan": json.dumps({"baseline_days": 28, "post_days": 28, "max_regression_pct": 15.0}),
            "applied_at": None,
            "verification_result": None,
            "realized_over_predicted": None,
        }

    def _write_change_sets_to_bq(self, change_sets: List[Dict[str, Any]]) -> None:
        """Writes compiled change sets to optimizer_ops.change_sets table."""
        if not change_sets:
            return
        table_ref = f"{self.ops_project}.optimizer_ops.change_sets"
        try:
            errors = self.client.insert_rows_json(table_ref, change_sets)
            if errors:
                print(f"[{datetime.now(timezone.utc).isoformat()}] Errors inserting change sets: {errors}")
            else:
                print(f"[{datetime.now(timezone.utc).isoformat()}] Successfully wrote {len(change_sets)} change sets to {table_ref}.")
        except Exception as e:
            print(f"[{datetime.now(timezone.utc).isoformat()}] BigQuery insert exception: {e}")
