"""
=============================================================================
Project Ghost Burn v2 — Remediation Playbook
=============================================================================
Converts every audit finding into an implementable fix with:
  - Databricks Compute Policy JSON configs (copy-paste ready, syntax v2025+)
  - Serverless SQL Warehouse Budget Policy (NEW v2)
  - Automated monitoring SQL queries (schedulable as Databricks Workflows)
  - Before/after cost projections per remediation
  - 4-week phased implementation roadmap

Author:   Wyatt Barandino — Data Analyst
Version:  2.0.0 — March 2026
License:  MIT

POLICY SYNTAX NOTE:
    All Databricks Compute Policy JSON in this file uses the v2025+
    cluster policy specification. Custom tags use block-level syntax
    under a "custom_tags" key — NOT dot-notation keys. This matches
    the current Databricks Clusters API:
    POST /api/2.0/policies/clusters/create

    For SQL Warehouse policies, the budgetPolicy object is used via:
    POST /api/2.1/sql/warehouses/{id} (budget_policy field)

    Cross-reference: https://docs.databricks.com/en/admin/clusters/policies.html

DISCLAIMER:
    All data used is 100% synthetic. No real Databricks billing data,
    customer information, workspace configurations, or proprietary schemas
    were used. Not affiliated with or endorsed by Databricks, Inc.

USAGE:
    Run in Google Colab after 01_data_simulation.py has been executed.
    Reads ghost_burn_billing.csv, outputs remediation impact analysis.
=============================================================================
"""

# ── CELL 1: IMPORTS & DATA LOAD ──────────────────────────────────────────────

import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings("ignore")

# ── Load audit data ──────────────────────────────────────────────────────────
df = pd.read_csv(
    "ghost_burn_billing.csv",
    parse_dates=["usage_start_time", "usage_end_time"]
)
df["parsed_meta"] = df["usage_metadata"].apply(
    lambda x: json.loads(x) if pd.notna(x) else {}
)

TOTAL_SPEND  = df["estimated_cost_usd"].sum()
AUDIT_DAYS   = 90
ANNUAL_SCALE = 365.0 / AUDIT_DAYS

print(f"Loaded {len(df):,} records | "
      f"${TOTAL_SPEND:,.2f} total spend over {AUDIT_DAYS} days\n")


# =============================================================================
# REMEDIATION 1: ZOMBIE CLUSTER
# =============================================================================

class ZombieRemediation:
    """
    FINDING:    ws-zombie-008 runs PREMIUM_ALL_PURPOSE_COMPUTE 24/7
                with autotermination_minutes=0 and no job_id. CV=2.0%.
    ROOT CAUSE: Legacy interactive cluster, auto-termination disabled,
                migration project ended but cluster was never cleaned up.
    FIXES:      1A Compute Policy (preventive) + 1B Nightly Sweep (reactive)
    """

    # ── 1A: Compute Policy — v2025+ syntax ───────────────────────────────────
    # Apply via: Databricks UI → Compute → Policies → Create Policy
    # API:       POST /api/2.0/policies/clusters/create

    COMPUTE_POLICY = {
        "name": "ghost-burn-anti-zombie-policy",
        "description": (
            "Enforces auto-termination on all interactive clusters. "
            "Prevents zombie clusters via mandatory idle timeout. "
            "Requires Unity Catalog USER_ISOLATION access mode."
        ),
        "definition": {
            "autotermination_minutes": {
                "type": "range",
                "minValue": 15,
                "maxValue": 120,
                "defaultValue": 60,
            },
            "spark_version": {
                "type": "allowlist",
                "values": ["auto:latest-lts", "auto:latest-lts-ml"],
                "defaultValue": "auto:latest-lts",
            },
            "num_workers": {
                "type": "range",
                "minValue": 0,
                "maxValue": 8,
                "defaultValue": 2,
            },
            "node_type_id": {
                "type": "allowlist",
                "values": [
                    "m5.2xlarge", "m5.4xlarge",
                    "r5.xlarge",  "r5.2xlarge",
                    "c5.4xlarge",
                ],
                "defaultValue": "m5.2xlarge",
            },
            "data_security_mode": {
                "type": "fixed",
                "value": "USER_ISOLATION",
            },
            # v2025+ CORRECT syntax: custom_tags as block, not dot-notation keys
            "custom_tags": {
                "team": {
                    "type":  "fixed",
                    "value": "REQUIRED",
                },
                "cost_center": {
                    "type":  "fixed",
                    "value": "REQUIRED",
                },
                "environment": {
                    "type": "allowlist",
                    "values": ["production", "development", "sandbox", "staging"],
                    "defaultValue": "development",
                },
            },
        },
    }

    # ── 1B: Nightly Zombie Sweep SQL (Databricks SQL syntax) ─────────────────
    ZOMBIE_SWEEP_SQL = """
    -- Nightly Zombie Sweep | Schedule: Daily 23:00 UTC
    -- Databricks Workflows → SQL Task → Alert on TERMINATE_IMMEDIATELY
    -- NOTE: Uses system.billing.usage (Databricks production schema)
    --       See DATABRICKS_MIGRATION.md for BigQuery equivalents

    WITH recent_activity AS (
        SELECT
            get_json_object(usage_metadata, '$.cluster_id')    AS cluster_id,
            get_json_object(usage_metadata, '$.cluster_name')  AS cluster_name,
            workspace_id,
            COUNT(*)                                           AS hours_active,
            COUNT(CASE WHEN get_json_object(usage_metadata, '$.job_id')
                       IS NOT NULL THEN 1 END)                 AS productive_hours,
            SUM(estimated_cost_usd)                            AS cost_today,
            ROUND(STDDEV(usage_quantity) /
                  AVG(usage_quantity) * 100, 2)                AS cv_pct,
            MIN(CAST(get_json_object(usage_metadata,
                '$.autotermination_minutes') AS INT))          AS min_autoterm
        FROM system.billing.usage
        WHERE
            DATE(usage_start_time) = CURRENT_DATE()
            AND sku_name IN (
                'STANDARD_ALL_PURPOSE_COMPUTE',
                'PREMIUM_ALL_PURPOSE_COMPUTE'
            )
        GROUP BY cluster_id, cluster_name, workspace_id
    )
    SELECT
        cluster_id, cluster_name, workspace_id,
        hours_active, productive_hours,
        ROUND(cost_today, 2)    AS cost_today_usd,
        cv_pct, min_autoterm,
        CASE
            WHEN productive_hours = 0 AND hours_active >= 12
                 AND min_autoterm = 0
                THEN 'CRITICAL — Terminate + Apply Policy'
            WHEN productive_hours = 0 AND hours_active >= 12
                THEN 'TERMINATE_IMMEDIATELY'
            WHEN productive_hours = 0 AND hours_active >= 6
                THEN 'WARN_AND_MONITOR'
            WHEN min_autoterm = 0
                THEN 'GOVERNANCE — Policy Required'
            ELSE 'HEALTHY'
        END                     AS action_required,
        ROUND(cost_today / NULLIF(hours_active, 0) * 24, 2)
                                AS projected_24h_cost_usd
    FROM recent_activity
    WHERE productive_hours = 0 OR min_autoterm = 0
    ORDER BY cost_today DESC;
    """

    @staticmethod
    def calculate_savings(df: pd.DataFrame) -> dict:
        mask = (
            (df["workspace_id"] == "ws-zombie-008") &
            (df["sku_name"] == "PREMIUM_ALL_PURPOSE_COMPUTE")
        )
        cost_before = df[mask]["estimated_cost_usd"].sum()
        cost_after  = cost_before * 0.05  # policy eliminates ~95% idle cost
        return {
            "finding":             "Zombie Cluster — 24/7 Idle Compute",
            "workspace":           "ws-zombie-008",
            "cost_before_90d":     round(cost_before, 2),
            "cost_after_90d":      round(cost_after, 2),
            "savings_90d":         round(cost_before - cost_after, 2),
            "savings_annual":      round((cost_before - cost_after) * ANNUAL_SCALE, 2),
            "fix_method":          "Compute Policy (auto-terminate 60 min) + Nightly sweep",
            "implementation_time": "30 minutes",
            "risk":                "Low",
        }


# =============================================================================
# REMEDIATION 2: SERVERLESS SQL SPIKE
# =============================================================================

class ServerlessSpikeRemediation:
    """
    FINDING:    ws-prod-bi-004: 513% spend spike (Z=25.72), Feb 10-13.
                Tableau extract refreshing every 5 min on serverless SQL.
    ROOT CAUSE: No query frequency guardrail on the SQL warehouse.
    FIXES:      2A SQL Warehouse Budget Policy (NEW v2) + 2B refresh fix
    """

    # ── 2A: SQL Warehouse Budget Policy ──────────────────────────────────────
    # This is the SERVERLESS governance pattern — separate from cluster policies.
    # Apply via: Databricks UI → SQL Warehouses → Edit
    # API:       PATCH /api/2.1/sql/warehouses/{warehouse_id}

    SQL_WAREHOUSE_POLICY = {
        "warehouse_name": "bi_reporting_serverless",
        "warehouse_id":   "sql-wh-bi-auto-004",
        "budget_policy": {
            "daily_dbu_limit":     300,           # ~$105/day at serverless rate
            "alert_threshold_pct": 80,            # alert at 80% of cap
            "alert_action":        "NOTIFY",
            "hard_stop_action":    "STOP_WAREHOUSE",
        },
        "auto_stop_mins":        10,
        "spot_instance_policy":  "COST_OPTIMIZED",
        "warehouse_type":        "PRO",
        "cluster_size":          "Medium",
        "tags": {
            "team":        "bi_reporting",
            "cost_center": "analytics",
            "alert_owner": "data-platform-oncall",
        },
    }

    QUERY_MONITOR_SQL = """
    -- Serverless Spike Monitor | Schedule: Every 1 hour
    -- Alert: Trigger if hourly cost exceeds 2x 24h baseline

    WITH warehouse_hourly AS (
        SELECT
            DATE_TRUNC('HOUR', usage_start_time)       AS hour_bucket,
            workspace_id,
            get_json_object(usage_metadata, '$.warehouse_id')
                                                       AS warehouse_id,
            get_json_object(usage_metadata, '$.warehouse_name')
                                                       AS warehouse_name,
            SUM(usage_quantity)                        AS hourly_dbus,
            SUM(estimated_cost_usd)                    AS hourly_cost,
            SUM(CAST(get_json_object(usage_metadata,
                '$.query_count_hourly') AS INT))        AS total_queries
        FROM system.billing.usage
        WHERE
            sku_name IN ('SERVERLESS_SQL_COMPUTE', 'SQL_PRO_COMPUTE')
            AND usage_start_time >= NOW() - INTERVAL 24 HOURS
        GROUP BY hour_bucket, workspace_id, warehouse_id, warehouse_name
    ),
    with_baseline AS (
        SELECT *,
            AVG(hourly_cost) OVER (
                PARTITION BY warehouse_id
                ORDER BY hour_bucket
                ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
            ) AS avg_24h_cost
        FROM warehouse_hourly
    )
    SELECT
        hour_bucket, workspace_id, warehouse_name,
        hourly_cost, avg_24h_cost, total_queries,
        ROUND((hourly_cost - avg_24h_cost)
              / NULLIF(avg_24h_cost, 0) * 100, 1)      AS pct_above_baseline,
        CASE
            WHEN hourly_cost > avg_24h_cost * 5
                THEN 'CRITICAL — >5x baseline'
            WHEN hourly_cost > avg_24h_cost * 2
                THEN 'WARNING — >2x baseline'
            ELSE 'NORMAL'
        END                                            AS alert_level
    FROM with_baseline
    WHERE hourly_cost > avg_24h_cost * 2
    ORDER BY pct_above_baseline DESC;
    """

    @staticmethod
    def calculate_savings(df: pd.DataFrame) -> dict:
        mask = (
            (df["workspace_id"] == "ws-prod-bi-004") &
            (df["usage_start_time"] >= "2026-02-10") &
            (df["usage_start_time"] <  "2026-02-13") &
            (df["sku_name"] == "SERVERLESS_SQL_COMPUTE")
        )
        cost_before = df[mask]["estimated_cost_usd"].sum()
        cost_after  = cost_before * 0.15  # budget cap + 30-min refresh = ~85% reduction
        return {
            "finding":             "Serverless SQL Spike — Runaway Dashboard Refresh",
            "workspace":           "ws-prod-bi-004",
            "cost_before_90d":     round(cost_before, 2),
            "cost_after_90d":      round(cost_after, 2),
            "savings_90d":         round(cost_before - cost_after, 2),
            "savings_annual":      round((cost_before - cost_after) * ANNUAL_SCALE, 2),
            "fix_method":          "SQL Warehouse budget policy ($105/day cap) + Tableau 30-min refresh",
            "implementation_time": "1 hour",
            "risk":                "Low — alerts before hard stop triggers",
        }


# =============================================================================
# REMEDIATION 3: GPU OVERPROVISIONING
# =============================================================================

class GPURightsizingRemediation:
    """
    FINDING:    ws-retail-forecast-002: nightly fine-tuning on p4d.24xlarge
                (8x A100, 320GB) at only 25% GPU utilization.
    ROOT CAUSE: Instance selected during experimentation, never reviewed
                after model stabilized into production.
    RIGHT-SIZING NOTE:
        Recommendation assumes workload fits g5.12xlarge (4x A10G, 96GB).
        VALIDATE WITH A TEST RUN before production deployment.
        If OOM errors occur: g5.48xlarge (8x A10G, 192GB) is the
        intermediate step before returning to p4d.24xlarge.
    FIXES:      3A GPU Compute Policy + 3B GPU utilization monitoring SQL
    """

    GPU_INSTANCE_REFERENCE = {
        "p4d.24xlarge": {"gpu": "A100 x8", "memory_gb": 320, "relative_cost": 3.2, "status": "OVERPROVISIONED"},
        "g5.12xlarge":  {"gpu": "A10G x4", "memory_gb": 96,  "relative_cost": 1.0, "status": "RECOMMENDED"},
        "g5.48xlarge":  {"gpu": "A10G x8", "memory_gb": 192, "relative_cost": 1.8, "status": "INTERMEDIATE"},
        "p3.2xlarge":   {"gpu": "V100 x1", "memory_gb": 16,  "relative_cost": 0.6, "status": "EXPERIMENTATION"},
        "g5.xlarge":    {"gpu": "A10G x1", "memory_gb": 24,  "relative_cost": 0.4, "status": "INFERENCE"},
        "g4dn.xlarge":  {"gpu": "T4 x1",   "memory_gb": 16,  "relative_cost": 0.3, "status": "LIGHTWEIGHT"},
    }

    # ── 3A: GPU Compute Policy ────────────────────────────────────────────────
    GPU_COMPUTE_POLICY = {
        "name": "ghost-burn-gpu-rightsizing-policy",
        "description": (
            "Enforces appropriate GPU instance sizing for ML workloads. "
            "Excludes A100-class instances by default. "
            "Requires gpu_workload_type tag for cost attribution."
        ),
        "definition": {
            "node_type_id": {
                "type": "allowlist",
                "values": [
                    "g4dn.xlarge",
                    "g5.xlarge",
                    "g5.12xlarge",   # default — fine-tuning sweet spot
                    "g5.48xlarge",
                    "p3.2xlarge",
                ],
                "defaultValue": "g5.12xlarge",
            },
            "autotermination_minutes": {
                "type": "range",
                "minValue": 15,
                "maxValue": 60,
                "defaultValue": 30,
            },
            "spark_version": {
                "type": "allowlist",
                "values": ["auto:latest-lts-ml"],
                "defaultValue": "auto:latest-lts-ml",
            },
            "num_workers": {
                "type": "range",
                "minValue": 0,
                "maxValue": 4,
                "defaultValue": 0,  # single-node default for GPU jobs
            },
            # v2025+ custom_tags block syntax
            "custom_tags": {
                "team": {
                    "type":  "fixed",
                    "value": "REQUIRED",
                },
                "cost_center": {
                    "type":  "fixed",
                    "value": "REQUIRED",
                },
                "gpu_workload_type": {
                    "type": "allowlist",
                    "values": ["inference", "fine_tuning", "training", "experimentation"],
                    "defaultValue": "fine_tuning",
                },
            },
        },
    }

    GPU_MONITOR_SQL = """
    -- GPU Utilization Monitor | Schedule: Daily 06:00 UTC
    -- Alert: Trigger if avg_gpu_util < 40% on any GPU job (7-day window)

    SELECT
        workspace_id,
        get_json_object(usage_metadata, '$.job_name')         AS job_name,
        get_json_object(usage_metadata, '$.instance_type')    AS instance_type,
        get_json_object(usage_metadata, '$.gpu_type')         AS gpu_type,
        ROUND(AVG(CAST(get_json_object(usage_metadata,
            '$.gpu_utilization_pct') AS FLOAT)), 1)           AS avg_gpu_util_pct,
        ROUND(SUM(estimated_cost_usd), 2)                      AS total_cost_7d,
        COUNT(*)                                               AS job_hours,
        CASE
            WHEN get_json_object(usage_metadata, '$.instance_type')
                 IN ('p4d.24xlarge', 'p3dn.24xlarge')
                 AND AVG(CAST(get_json_object(usage_metadata,
                     '$.gpu_utilization_pct') AS FLOAT)) < 50
                THEN 'Right-size to g5.12xlarge — saves ~50%'
            WHEN AVG(CAST(get_json_object(usage_metadata,
                '$.gpu_utilization_pct') AS FLOAT)) < 35
                THEN 'CRITICAL — Severely underutilized'
            WHEN AVG(CAST(get_json_object(usage_metadata,
                '$.gpu_utilization_pct') AS FLOAT)) < 60
                THEN 'WARNING — Below optimal threshold'
            ELSE 'HEALTHY'
        END                                                    AS recommendation
    FROM system.billing.usage
    WHERE
        sku_name IN ('GPU_SERVERLESS_COMPUTE', 'FOUNDATION_MODEL_TRAINING')
        AND usage_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY workspace_id, job_name, instance_type, gpu_type
    ORDER BY avg_gpu_util_pct ASC;
    """

    @staticmethod
    def calculate_savings(df: pd.DataFrame) -> dict:
        mask = (
            (df["sku_name"] == "GPU_SERVERLESS_COMPUTE") &
            (df["workspace_id"] == "ws-retail-forecast-002")
        )
        cost_before = df[mask]["estimated_cost_usd"].sum()
        cost_after  = cost_before * 0.50  # g5.12xlarge ~50% of p4d.24xlarge cost
        return {
            "finding":             "GPU Overprovisioning — p4d.24xlarge at 25% utilization",
            "workspace":           "ws-retail-forecast-002",
            "cost_before_90d":     round(cost_before, 2),
            "cost_after_90d":      round(cost_after, 2),
            "savings_90d":         round(cost_before - cost_after, 2),
            "savings_annual":      round((cost_before - cost_after) * ANNUAL_SCALE, 2),
            "fix_method":          "Right-size to g5.12xlarge + GPU compute policy",
            "implementation_time": "2 hours (includes test run validation)",
            "risk":                "Medium — validate workload fits g5.12xlarge memory first",
        }


# =============================================================================
# REMEDIATION 4: WEEKEND DEV WASTE
# =============================================================================

class WeekendWasteRemediation:
    """
    FINDING:    ws-sandbox-ds-005 + ws-health-analytics-003: interactive
                clusters run Fri evening through Mon morning with no policy.
    ROOT CAUSE: No workspace-level weekend lifecycle policy.
    FIX:        Databricks SDK notebook — terminate Fri 6 PM, log Mon 8 AM.
    """

    WEEKEND_LIFECYCLE_NOTEBOOK = '''
# Weekend Lifecycle Manager — Databricks SDK Notebook
# Workflows schedule:
#   Terminate: Cron 0 18 * * 5  (Friday 6 PM UTC)
#   Log check: Cron 0 8  * * 1  (Monday 8 AM UTC)

from databricks.sdk import WorkspaceClient
import datetime

w = WorkspaceClient()

NON_PROD_TAGS = ["sandbox", "development", "staging"]

def terminate_weekend_clusters():
    terminated = []
    for cluster in w.clusters.list():
        tags = cluster.custom_tags or {}
        env  = tags.get("environment", "").lower()
        if (
            cluster.state.value in ["RUNNING", "RESIZING"]
            and env in NON_PROD_TAGS
        ):
            w.clusters.terminate(cluster_id=cluster.cluster_id)
            terminated.append(cluster.cluster_name)
            print(f"Terminated: {cluster.cluster_name}")
    print(f"Weekend shutdown: {len(terminated)} clusters terminated.")

def log_monday_status():
    print(f"Monday check — {datetime.datetime.utcnow().isoformat()}")
    for cluster in w.clusters.list():
        print(f"  {cluster.cluster_name}: {cluster.state.value}")

day = datetime.datetime.utcnow().weekday()
if day == 4:    # Friday
    terminate_weekend_clusters()
elif day == 0:  # Monday
    log_monday_status()
    '''

    WEEKEND_MONITOR_SQL = """
    -- Weekend Waste Monitor | Schedule: Monday 09:00 UTC
    -- Chargeback data for off-hours spend per workspace

    SELECT
        workspace_id,
        get_json_object(usage_metadata, '$.cluster_name')        AS cluster_name,
        COUNTIF(DAYOFWEEK(DATE(usage_start_time)) IN (1, 7))      AS weekend_hours,
        ROUND(SUM(CASE
            WHEN DAYOFWEEK(DATE(usage_start_time)) IN (1, 7)
            THEN estimated_cost_usd ELSE 0 END), 2)               AS weekend_cost_usd,
        ROUND(SUM(estimated_cost_usd), 2)                          AS total_cost_usd,
        ROUND(SUM(CASE
            WHEN DAYOFWEEK(DATE(usage_start_time)) IN (1, 7)
            THEN estimated_cost_usd ELSE 0 END)
            / NULLIF(SUM(estimated_cost_usd), 0) * 100, 1)        AS weekend_pct,
        CASE
            WHEN SUM(CASE
                WHEN DAYOFWEEK(DATE(usage_start_time)) IN (1, 7)
                THEN estimated_cost_usd ELSE 0 END) > 500
                THEN 'HIGH — Weekend spend >$500'
            WHEN SUM(CASE
                WHEN DAYOFWEEK(DATE(usage_start_time)) IN (1, 7)
                THEN estimated_cost_usd ELSE 0 END) > 100
                THEN 'MODERATE — Review policy'
            ELSE 'NORMAL'
        END                                                       AS flag
    FROM system.billing.usage
    WHERE
        sku_name IN (
            'STANDARD_ALL_PURPOSE_COMPUTE',
            'PREMIUM_ALL_PURPOSE_COMPUTE'
        )
        AND usage_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY workspace_id, cluster_name
    HAVING weekend_hours > 0
    ORDER BY weekend_cost_usd DESC;
    """

    @staticmethod
    def calculate_savings(df: pd.DataFrame) -> dict:
        mask = (
            df["workspace_id"].isin(
                ["ws-sandbox-ds-005", "ws-health-analytics-003"]
            ) &
            (df["usage_start_time"].dt.weekday >= 5) &
            (df["sku_name"] == "STANDARD_ALL_PURPOSE_COMPUTE")
        )
        cost_before = df[mask]["estimated_cost_usd"].sum()
        cost_after  = cost_before * 0.05  # lifecycle policy eliminates ~95%
        return {
            "finding":             "Weekend Dev Waste — No Off-Hours Lifecycle Policy",
            "workspace":           "ws-sandbox-ds-005 + ws-health-analytics-003",
            "cost_before_90d":     round(cost_before, 2),
            "cost_after_90d":      round(cost_after, 2),
            "savings_90d":         round(cost_before - cost_after, 2),
            "savings_annual":      round((cost_before - cost_after) * ANNUAL_SCALE, 2),
            "fix_method":          "SDK lifecycle notebook (Fri terminate → Mon log)",
            "implementation_time": "45 minutes",
            "risk":                "Low — targets non-production workspaces only",
        }


# =============================================================================
# REMEDIATION 5: RUNAWAY DLT PIPELINE
# =============================================================================

class DLTPipelineRemediation:
    """
    FINDING:    ws-fsi-trading-001: pipe-risk-factor-etl in CONTINUOUS mode,
                67.4% idle rate (data only arrives business hours).
    ROOT CAUSE: CONTINUOUS mode chosen during setup for perceived low-latency
                benefit. Upstream data source updates hourly — not real-time.
    FIX:        Switch to TRIGGERED mode (60-min cron). ~60% savings.
    """

    # ── 5A: DLT Pipeline Config — CONTINUOUS → TRIGGERED ─────────────────────
    # Apply via: Databricks UI → Delta Live Tables → Edit Pipeline
    # API:       PUT /api/2.0/pipelines/{pipeline_id}

    DLT_PIPELINE_CONFIG = {
        "pipeline_id":   "pipe-risk-factor-etl",
        "pipeline_name": "risk_factor_bronze_to_silver",
        "target":        "silver.risk_factors_hourly",
        # KEY CHANGE: continuous: False (was True)
        # TRIGGERED: cluster starts on schedule, processes all available data, terminates
        # CONTINUOUS: cluster stays alive permanently — expensive for batch sources
        "continuous":    False,
        "trigger": {
            "cron": {
                "quartz_cron_expression": "0 0 * * * ?",  # every 60 minutes
                "timezone_id":            "UTC",
            }
        },
        "clusters": [
            {
                "label":       "default",
                "num_workers": 2,
                "node_type_id": "m5.2xlarge",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 4,
                    "mode":        "LEGACY",
                },
                "custom_tags": {
                    "team":          "data_engineering",
                    "cost_center":   "fsi_trading",
                    "pipeline_type": "triggered_batch",
                },
            }
        ],
        "budget_policy": {
            "daily_dbu_limit":     150,    # ~$30/day at jobs serverless rate
            "alert_threshold_pct": 80,
            "alert_action":        "NOTIFY",
        },
    }

    DLT_MONITOR_SQL = """
    -- DLT Pipeline Monitor | Schedule: Daily 07:00 UTC
    -- Alert: CONTINUOUS mode with idle_rate > 25%

    WITH pipeline_daily AS (
        SELECT
            DATE(usage_start_time)                               AS usage_date,
            workspace_id,
            get_json_object(usage_metadata, '$.pipeline_id')     AS pipeline_id,
            get_json_object(usage_metadata, '$.pipeline_name')   AS pipeline_name,
            get_json_object(usage_metadata, '$.pipeline_mode')   AS pipeline_mode,
            COUNT(*)                                             AS active_hours,
            ROUND(SUM(estimated_cost_usd), 2)                    AS daily_cost,
            SUM(CASE WHEN CAST(get_json_object(usage_metadata,
                '$.data_processed_gb') AS FLOAT) = 0
                THEN 1 ELSE 0 END)                               AS idle_hours,
            ROUND(SUM(CAST(get_json_object(usage_metadata,
                '$.data_processed_gb') AS FLOAT)), 2)            AS data_processed_gb
        FROM system.billing.usage
        WHERE
            sku_name = 'JOBS_SERVERLESS_COMPUTE'
            AND get_json_object(usage_metadata, '$.pipeline_id') IS NOT NULL
            AND usage_start_time >= CURRENT_DATE() - INTERVAL 7 DAYS
        GROUP BY usage_date, workspace_id, pipeline_id,
                 pipeline_name, pipeline_mode
    )
    SELECT *,
        ROUND(idle_hours / NULLIF(active_hours, 0) * 100, 1)    AS idle_rate_pct,
        ROUND(daily_cost * 365, 2)                               AS projected_annual_cost,
        ROUND(daily_cost * 0.60, 2)                              AS recoverable_daily,
        CASE
            WHEN pipeline_mode = 'CONTINUOUS'
                 AND idle_hours / NULLIF(active_hours, 0) > 0.50
                THEN 'CRITICAL — Switch to TRIGGERED immediately'
            WHEN pipeline_mode = 'CONTINUOUS'
                 AND idle_hours / NULLIF(active_hours, 0) > 0.25
                THEN 'HIGH — Significant idle in CONTINUOUS mode'
            WHEN pipeline_mode = 'CONTINUOUS'
                THEN 'REVIEW — Validate real-time requirement'
            ELSE 'NORMAL'
        END                                                      AS status
    FROM pipeline_daily
    ORDER BY daily_cost DESC;
    """

    @staticmethod
    def calculate_savings(df: pd.DataFrame) -> dict:
        mask = (
            (df["workspace_id"] == "ws-fsi-trading-001") &
            (df["sku_name"] == "JOBS_SERVERLESS_COMPUTE") &
            df["parsed_meta"].apply(lambda x: bool(x.get("pipeline_id")))
        )
        cost_before = df[mask]["estimated_cost_usd"].sum()
        cost_after  = cost_before * 0.40  # TRIGGERED mode: ~60% savings
        return {
            "finding":             "Runaway DLT Pipeline — CONTINUOUS mode on batch workload",
            "workspace":           "ws-fsi-trading-001",
            "cost_before_90d":     round(cost_before, 2),
            "cost_after_90d":      round(cost_after, 2),
            "savings_90d":         round(cost_before - cost_after, 2),
            "savings_annual":      round((cost_before - cost_after) * ANNUAL_SCALE, 2),
            "fix_method":          "Switch pipeline to TRIGGERED mode (60-min cron schedule)",
            "implementation_time": "1 hour",
            "risk":                "Low — TRIGGERED is standard pattern for hourly batch ingestion",
        }


# =============================================================================
# FINOPS ALERT ENGINE — PERSISTENT MONITORING
# =============================================================================

class FinOpsAlertEngine:
    """
    Persistent monitoring layer that runs after the initial 8-hour
    remediation sprint. Keeps the environment clean on an ongoing basis.

    Schedule:
        Daily   08:00 UTC  — Anomaly detection (yesterday's spend)
        Daily   23:00 UTC  — Zombie sweep (today's idle clusters)
        Monday  09:00 UTC  — Weekly cost attribution report
    """

    DAILY_ANOMALY_SQL = """
    -- Daily FinOps Anomaly Alert | Schedule: 08:00 UTC daily
    -- Dual signal: percentage deviation (CFO) + Z-score (engineering)

    WITH daily_spend AS (
        SELECT
            DATE(usage_start_time)              AS usage_date,
            workspace_id,
            ROUND(SUM(estimated_cost_usd), 2)   AS daily_cost,
            COUNT(DISTINCT sku_name)             AS sku_count,
            SUM(usage_quantity)                  AS daily_dbus
        FROM system.billing.usage
        GROUP BY usage_date, workspace_id
    ),
    with_baseline AS (
        SELECT *,
            ROUND(AVG(daily_cost) OVER (
                PARTITION BY workspace_id
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 2) AS baseline_7d_avg,
            ROUND(STDDEV(daily_cost) OVER (
                PARTITION BY workspace_id
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ), 2) AS baseline_7d_stddev
        FROM daily_spend
    )
    SELECT
        usage_date, workspace_id, daily_cost, baseline_7d_avg,
        ROUND(daily_cost - baseline_7d_avg, 2)          AS delta_usd,
        ROUND((daily_cost - baseline_7d_avg)
              / NULLIF(baseline_7d_avg, 0) * 100, 1)    AS delta_pct,
        ROUND((daily_cost - baseline_7d_avg)
              / NULLIF(baseline_7d_stddev, 0), 2)        AS z_score,
        CASE
            WHEN (daily_cost - baseline_7d_avg)
                 / NULLIF(baseline_7d_avg, 0) > 1.50
                THEN 'EMERGENCY — Auto-pause recommended'
            WHEN (daily_cost - baseline_7d_avg)
                 / NULLIF(baseline_7d_avg, 0) > 0.75
                THEN 'CRITICAL — Immediate review required'
            WHEN (daily_cost - baseline_7d_avg)
                 / NULLIF(baseline_7d_avg, 0) > 0.30
                THEN 'WARNING — Investigate within 24h'
            ELSE 'NORMAL'
        END                                              AS alert_level,
        daily_dbus, sku_count
    FROM with_baseline
    WHERE
        baseline_7d_avg IS NOT NULL
        AND usage_date = CURRENT_DATE() - INTERVAL 1 DAY
    ORDER BY delta_pct DESC;
    """

    WEEKLY_REPORT_SQL = """
    -- Weekly Cost Attribution | Schedule: Monday 09:00 UTC
    -- Finance chargeback with week-over-week delta per workspace

    SELECT
        workspace_id,
        CASE
            WHEN sku_name IN ('JOBS_COMPUTE','JOBS_SERVERLESS_COMPUTE',
                              'SQL_PRO_COMPUTE','SERVERLESS_SQL_COMPUTE')
                THEN 'Production'
            WHEN sku_name IN ('GPU_SERVERLESS_COMPUTE',
                              'FOUNDATION_MODEL_TRAINING',
                              'SERVERLESS_REAL_TIME_INFERENCE')
                THEN 'AI/ML'
            WHEN sku_name IN ('STANDARD_ALL_PURPOSE_COMPUTE',
                              'PREMIUM_ALL_PURPOSE_COMPUTE')
                THEN 'Interactive'
            ELSE 'Platform'
        END                                       AS spend_category,
        COUNT(*)                                   AS usage_events,
        ROUND(SUM(usage_quantity), 2)               AS total_dbus,
        ROUND(SUM(estimated_cost_usd), 2)           AS total_cost,
        ROUND(SUM(CASE
            WHEN DATE(usage_start_time) >= CURRENT_DATE() - INTERVAL 7 DAYS
            THEN estimated_cost_usd ELSE 0 END), 2) AS cost_this_week,
        ROUND(SUM(CASE
            WHEN DATE(usage_start_time) >= CURRENT_DATE() - INTERVAL 14 DAYS
             AND DATE(usage_start_time)  < CURRENT_DATE() - INTERVAL 7 DAYS
            THEN estimated_cost_usd ELSE 0 END), 2) AS cost_last_week,
        ROUND(
            SUM(CASE
                WHEN DATE(usage_start_time) >= CURRENT_DATE() - INTERVAL 7 DAYS
                THEN estimated_cost_usd ELSE 0 END)
            - SUM(CASE
                WHEN DATE(usage_start_time) >= CURRENT_DATE() - INTERVAL 14 DAYS
                 AND DATE(usage_start_time)  < CURRENT_DATE() - INTERVAL 7 DAYS
                THEN estimated_cost_usd ELSE 0 END),
            2
        )                                          AS wow_delta_usd
    FROM system.billing.usage
    WHERE DATE(usage_start_time) >= CURRENT_DATE() - INTERVAL 14 DAYS
    GROUP BY workspace_id, spend_category
    ORDER BY total_cost DESC;
    """


# =============================================================================
# EXECUTIVE SUMMARY
# =============================================================================

def generate_remediation_report(df: pd.DataFrame):
    """Consolidated impact report with before/after and implementation roadmap."""
    print("=" * 72)
    print("  PROJECT GHOST BURN v2 — REMEDIATION IMPACT ANALYSIS")
    print("  Converting Audit Findings into Implementable Annual Savings")
    print("=" * 72)

    remediations = [
        ZombieRemediation.calculate_savings(df),
        ServerlessSpikeRemediation.calculate_savings(df),
        GPURightsizingRemediation.calculate_savings(df),
        WeekendWasteRemediation.calculate_savings(df),
        DLTPipelineRemediation.calculate_savings(df),
    ]

    total_before = sum(r["cost_before_90d"]  for r in remediations)
    total_after  = sum(r["cost_after_90d"]   for r in remediations)
    total_90d    = sum(r["savings_90d"]       for r in remediations)
    total_annual = sum(r["savings_annual"]    for r in remediations)

    for i, r in enumerate(remediations, 1):
        print(f"\n{'─' * 72}")
        print(f"  REMEDIATION {i}: {r['finding']}")
        print(f"{'─' * 72}")
        print(f"  Workspace:             {r['workspace']}")
        print(f"  Cost Before (90d):     ${r['cost_before_90d']:>10,.2f}")
        print(f"  Cost After  (90d):     ${r['cost_after_90d']:>10,.2f}")
        print(f"  Savings (90d):         ${r['savings_90d']:>10,.2f}")
        print(f"  Savings (Annual):      ${r['savings_annual']:>10,.2f}")
        print(f"  Fix Method:            {r['fix_method']}")
        print(f"  Time to Implement:     {r['implementation_time']}")
        print(f"  Risk Level:            {r['risk']}")

    print(f"\n{'═' * 72}")
    print(f"  TOTAL REMEDIATION IMPACT")
    print(f"{'═' * 72}")
    print(f"  Total Waste (90d):       ${total_before:>10,.2f}")
    print(f"  Post-Fix Cost (90d):     ${total_after:>10,.2f}")
    print(f"  Net Savings (90d):       ${total_90d:>10,.2f}")
    print(f"  Net Savings (Annual):    ${total_annual:>10,.2f}")
    print(f"  Savings Rate:            {total_90d / total_before * 100:.1f}%")
    print(f"  % of Total Platform:     {total_90d / TOTAL_SPEND * 100:.1f}%")

    print(f"\n{'─' * 72}")
    print(f"  4-WEEK IMPLEMENTATION ROADMAP")
    print(f"{'─' * 72}")
    roadmap = [
        ("Week 1", "Deploy anti-zombie compute policy (all workspaces)",   "30 min",  remediations[0]["savings_annual"]),
        ("Week 1", "Configure SQL Warehouse budget policy + auto-stop",     "1 hr",    remediations[1]["savings_annual"]),
        ("Week 2", "Deploy weekend lifecycle manager notebook",             "45 min",  remediations[3]["savings_annual"]),
        ("Week 2", "Schedule FinOps monitoring queries (daily + weekly)",   "1 hr",    0),
        ("Week 3", "Right-size GPU: test run on g5.12xlarge",              "2 hrs",   remediations[2]["savings_annual"]),
        ("Week 3", "Migrate DLT pipeline to TRIGGERED mode (60-min cron)", "1 hr",    remediations[4]["savings_annual"]),
        ("Week 4", "Configure alert routing to Slack/email",               "30 min",  0),
        ("Week 4", "Review first weekly cost attribution report",           "1 hr",    0),
    ]

    for week, action, effort, savings in roadmap:
        note = f"→ ${savings:,.0f}/yr unlocked" if savings > 0 else "→ Monitoring active"
        print(f"  {week}  [{effort:>6}]  {action}")
        print(f"                     {note}")

    print(f"\n{'─' * 72}")
    print(f"  TOTAL EFFORT:    ~8 hours over 4 weeks")
    print(f"  TOTAL SAVINGS:   ${total_annual:,.0f} per year")
    print(f"  ROI:             1 business day of work → ${total_annual:,.0f}/yr")

    # Enterprise scale extrapolation
    per_ws = total_annual / 8
    print(f"\n{'═' * 72}")
    print(f"  WHAT THIS LOOKS LIKE AT SCALE")
    print(f"{'─' * 72}")
    print(f"  8 workspaces audited    → ${total_annual:>10,.0f} annual waste identified")
    print(f"  50 workspaces (est.)    → ${per_ws * 50:>10,.0f} preventable annual spend")
    print(f"  100 workspaces (est.)   → ${per_ws * 100:>10,.0f} preventable annual spend")
    print(f"\n  The SQL queries and compute policies scale linearly.")
    print(f"  The implementation effort does not.")
    print(f"{'═' * 72}")

    return remediations


# ── RUN ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    results = generate_remediation_report(df)
