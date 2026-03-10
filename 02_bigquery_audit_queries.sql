-- =============================================================================
-- PROJECT GHOST BURN v2 — BigQuery SQL Audit Queries
-- =============================================================================
-- Author:   Wyatt Barandino — Data Analyst
-- Version:  2.0.0 — March 2026
-- License:  MIT
--
-- Target:   Google BigQuery Sandbox (free tier)
-- Dataset:  ghost_burn.billing_usage
-- Schema:   Mirrors Databricks system.billing.usage (2026 spec)
--
-- PORTABILITY NOTE:
--   These queries are written for BigQuery syntax. They are designed against
--   the Databricks system.billing.usage schema specification. To run against
--   a real Databricks environment, see DATABRICKS_MIGRATION.md for the full
--   function mapping and the system.billing.list_prices join pattern required
--   to derive cost figures.
--
-- DISCLAIMER:
--   All data queried here is 100% synthetic. No real Databricks billing data,
--   customer information, or proprietary schemas were used. Schema modeled
--   from publicly available Databricks documentation only. This project is
--   not affiliated with or endorsed by Databricks, Inc.
--
-- QUERY INDEX:
--   Q1 — Top 5 Cost Centers by Workspace Spend
--   Q2 — SKU Breakdown: Production vs. AI/Innovation vs. Interactive
--   Q3 — Zombie Cluster Detection (CV-based + governance violation branch)
--   Q4 — Compute Efficiency Ratio (Cost-per-Job / Cost-per-Query)
--   Q5 — Anomaly Detection: 7-Day Moving Average Spike Alerts
--   Q6 — DLT Pipeline Audit: Continuous Mode Waste Detection (NEW v2)
-- =============================================================================


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q1 — TOP 5 COST CENTERS BY WORKSPACE SPEND                            │
-- │                                                                         │
-- │ Purpose: Identify which workspaces (business units) are driving the     │
-- │          most DBU spend over the audit window. This is the first         │
-- │          question any FinOps team asks — where is the money going?      │
-- │                                                                         │
-- │ Business value: Enables chargeback allocation, budget accountability,   │
-- │                 and prioritization of remediation effort.               │
-- └─────────────────────────────────────────────────────────────────────────┘

SELECT
    workspace_id,

    -- Volume metrics
    COUNT(usage_record_id)                                AS total_records,
    ROUND(SUM(usage_quantity), 2)                          AS total_dbus,
    ROUND(SUM(estimated_cost_usd), 2)                      AS total_cost_usd,

    -- Efficiency metrics
    ROUND(AVG(usage_quantity), 4)                           AS avg_dbu_per_record,
    ROUND(
        SUM(estimated_cost_usd) /
        NULLIF(
            DATE_DIFF(
                MAX(DATE(usage_start_time)),
                MIN(DATE(usage_start_time)),
                DAY
            ), 0
        ),
        2
    )                                                      AS avg_daily_cost_usd,

    -- Share of total platform spend
    ROUND(
        SUM(estimated_cost_usd) * 100.0 /
        SUM(SUM(estimated_cost_usd)) OVER (),
        1
    )                                                      AS pct_of_total_spend,

    -- Time bounds
    MIN(usage_start_time)                                  AS first_usage,
    MAX(usage_start_time)                                  AS last_usage

FROM
    `ghost_burn.billing_usage`

GROUP BY
    workspace_id

ORDER BY
    total_cost_usd DESC

LIMIT 5;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q2 — SKU BREAKDOWN: PRODUCTION vs. AI/INNOVATION vs. INTERACTIVE       │
-- │                                                                         │
-- │ Purpose: Distinguish value-generating compute (Jobs, SQL) from          │
-- │          exploratory compute (Interactive) and AI workloads (GPU).      │
-- │          Helps CFOs understand whether cloud spend aligns with          │
-- │          business-critical outcomes.                                    │
-- │                                                                         │
-- │ Business value: Surfaces the ratio of productive vs. exploratory spend. │
-- │                 A healthy Lakehouse environment skews toward            │
-- │                 Production + AI, not Interactive.                       │
-- └─────────────────────────────────────────────────────────────────────────┘

SELECT
    sku_name,

    -- Spend category classification
    CASE
        WHEN sku_name IN (
            'JOBS_COMPUTE',
            'JOBS_SERVERLESS_COMPUTE',
            'SQL_PRO_COMPUTE',
            'SERVERLESS_SQL_COMPUTE',
            'DELTA_STORAGE'
        ) THEN 'Production / Value'

        WHEN sku_name IN (
            'GPU_SERVERLESS_COMPUTE',
            'FOUNDATION_MODEL_TRAINING',
            'SERVERLESS_REAL_TIME_INFERENCE',
            'VECTOR_SEARCH_COMPUTE'
        ) THEN 'AI / Innovation'

        WHEN sku_name IN (
            'STANDARD_ALL_PURPOSE_COMPUTE',
            'PREMIUM_ALL_PURPOSE_COMPUTE'
        ) THEN 'Interactive / Exploration'

        ELSE 'Platform / Governance'
    END                                                    AS spend_category,

    -- Volume and cost
    COUNT(*)                                               AS usage_events,
    ROUND(SUM(usage_quantity), 2)                           AS total_dbus,
    ROUND(SUM(estimated_cost_usd), 2)                       AS total_cost_usd,

    -- Share of total spend
    ROUND(
        SUM(estimated_cost_usd) * 100.0 /
        SUM(SUM(estimated_cost_usd)) OVER (),
        1
    )                                                      AS pct_of_total_spend,

    -- Average cost per event
    ROUND(
        SAFE_DIVIDE(SUM(estimated_cost_usd), COUNT(*)),
        4
    )                                                      AS avg_cost_per_event

FROM
    `ghost_burn.billing_usage`

GROUP BY
    sku_name

ORDER BY
    total_cost_usd DESC

LIMIT 12;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q3 — ZOMBIE CLUSTER DETECTION                                          │
-- │                                                                         │
-- │ Purpose: Surface clusters with sustained uptime but zero productive     │
-- │          activity. Two detection branches are used:                     │
-- │                                                                         │
-- │   Branch A — Behavioral signal:                                         │
-- │     job_id IS NULL with high hourly consistency (low CV). A running     │
-- │     cluster with no workload variation is consuming DBUs without        │
-- │     delivering value.                                                   │
-- │                                                                         │
-- │   Branch B — Governance violation signal (NEW v2):                      │
-- │     autotermination_minutes = 0, regardless of job_id status.           │
-- │     Disabled auto-termination is itself a policy violation — a cluster  │
-- │     that cannot self-terminate is a future zombie by design.            │
-- │                                                                         │
-- │ CV threshold rationale:                                                 │
-- │   CV (coefficient of variation) = STDDEV / AVG × 100.                  │
-- │   Healthy interactive clusters show CV > 20% (variable workloads).     │
-- │   Zombie clusters show CV < 10% (flat, idle consumption pattern).       │
-- │   Threshold sourced from observed Databricks billing community          │
-- │   patterns and FinOps anomaly detection best practices.                │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH cluster_activity AS (
    SELECT
        workspace_id,
        JSON_VALUE(usage_metadata, '$.cluster_id')              AS cluster_id,
        JSON_VALUE(usage_metadata, '$.cluster_name')            AS cluster_name,
        JSON_VALUE(usage_metadata, '$.job_id')                  AS job_id,
        CAST(
            JSON_VALUE(usage_metadata, '$.autotermination_minutes')
            AS INT64
        )                                                        AS autotermination_min,
        JSON_VALUE(usage_metadata, '$.data_security_mode')      AS data_security_mode,

        -- Activity metrics
        COUNT(*)                                                 AS total_hours_active,
        ROUND(SUM(usage_quantity), 2)                             AS total_dbus,
        ROUND(SUM(estimated_cost_usd), 2)                         AS total_cost_usd,
        ROUND(AVG(usage_quantity), 4)                              AS avg_hourly_dbu,
        ROUND(STDDEV(usage_quantity), 4)                           AS stddev_hourly_dbu,

        -- Coefficient of variation: low CV = suspiciously flat usage = zombie signal
        ROUND(
            SAFE_DIVIDE(STDDEV(usage_quantity), AVG(usage_quantity)) * 100,
            2
        )                                                        AS usage_cv_pct,

        -- Projected annual cost if cluster continues unchanged
        ROUND(SUM(estimated_cost_usd) * (365.0 / 90), 2)         AS projected_annual_cost

    FROM
        `ghost_burn.billing_usage`

    WHERE
        sku_name IN (
            'STANDARD_ALL_PURPOSE_COMPUTE',
            'PREMIUM_ALL_PURPOSE_COMPUTE'
        )

    GROUP BY
        workspace_id,
        cluster_id,
        cluster_name,
        job_id,
        autotermination_min,
        data_security_mode
),

-- ── BRANCH A: Behavioral zombie detection (null job_id + flat usage) ─────
behavioral_zombies AS (
    SELECT
        *,
        'BEHAVIORAL' AS detection_branch
    FROM cluster_activity
    WHERE
        job_id IS NULL
        AND total_hours_active > 200
        AND usage_cv_pct < 10
),

-- ── BRANCH B: Governance violation detection (auto-termination disabled) ──
-- Catches clusters that WILL become zombies even if not yet idle.
-- autotermination_minutes = 0 means the cluster has no self-termination
-- capability — a policy violation independent of current activity level.
governance_violations AS (
    SELECT
        *,
        'GOVERNANCE_VIOLATION' AS detection_branch
    FROM cluster_activity
    WHERE
        autotermination_min = 0
        AND job_id IS NULL  -- restrict to non-job clusters for relevance
),

-- ── UNION both branches, deduplicate by cluster_id ────────────────────────
combined AS (
    SELECT * FROM behavioral_zombies
    UNION ALL
    SELECT * FROM governance_violations
)

SELECT DISTINCT
    workspace_id,
    cluster_id,
    cluster_name,
    total_hours_active,
    total_dbus,
    total_cost_usd,
    avg_hourly_dbu,
    usage_cv_pct,
    autotermination_min,
    data_security_mode,
    projected_annual_cost,
    detection_branch,

    -- Severity classification
    CASE
        WHEN job_id IS NULL
             AND total_hours_active > 400
             AND usage_cv_pct < 10
             AND autotermination_min = 0
            THEN '🔴 CRITICAL — Terminate + Policy Required'

        WHEN job_id IS NULL
             AND total_hours_active > 200
             AND usage_cv_pct < 10
            THEN '🟠 HIGH — Terminate Immediately'

        WHEN autotermination_min = 0
            THEN '🟡 GOVERNANCE — Auto-Termination Disabled'

        WHEN job_id IS NULL
             AND total_hours_active > 100
            THEN '🟡 SUSPECT — Review Required'

        ELSE '🟢 Monitor'
    END                                                          AS zombie_status,

    -- Recommended action
    CASE
        WHEN autotermination_min = 0
            THEN 'Apply ghost-burn-anti-zombie-policy immediately'
        WHEN job_id IS NULL AND total_hours_active > 200
            THEN 'Terminate cluster + apply compute policy'
        ELSE 'Add to monitoring queue'
    END                                                          AS recommended_action

FROM combined

ORDER BY
    total_cost_usd DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q4 — COMPUTE EFFICIENCY RATIO                                          │
-- │                                                                         │
-- │ Purpose: Measure cost-per-unit-of-work across workspaces and SKUs.     │
-- │          A high cost per productive unit signals optimization           │
-- │          opportunities — the cluster is spending more than it delivers. │
-- │                                                                         │
-- │ Efficiency grade thresholds:                                            │
-- │   These thresholds are derived from Databricks-published benchmark      │
-- │   ranges for interactive and job cluster cost efficiency, combined      │
-- │   with FinOps community standards for Lakehouse cost governance.        │
-- │   Adjust per-environment as baselines vary by workload type.           │
-- │                                                                         │
-- │   > $5.00 / productive unit → Low Efficiency                           │
-- │     Cost significantly exceeds productive output. Likely idle compute, │
-- │     oversized instance, or missing job orchestration.                   │
-- │                                                                         │
-- │   $1.00–$5.00 / productive unit → Moderate                             │
-- │     Borderline performance. Review scheduling and instance sizing.      │
-- │                                                                         │
-- │   < $1.00 / productive unit → Efficient                                │
-- │     Cost aligned with productive workload volume.                       │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH workspace_efficiency AS (
    SELECT
        workspace_id,
        sku_name,

        ROUND(SUM(estimated_cost_usd), 2)                       AS total_cost,
        ROUND(SUM(usage_quantity), 2)                             AS total_dbus,

        -- Productive work units: records with an associated job_id
        COUNTIF(
            JSON_VALUE(usage_metadata, '$.job_id') IS NOT NULL
        )                                                        AS records_with_jobs,

        -- Query volume for SQL/BI workloads
        SUM(
            SAFE_CAST(
                JSON_VALUE(usage_metadata, '$.query_count_hourly')
                AS INT64
            )
        )                                                        AS total_queries,

        -- GPU utilization average (AI/ML workloads)
        ROUND(AVG(
            SAFE_CAST(
                JSON_VALUE(usage_metadata, '$.gpu_utilization_pct')
                AS FLOAT64
            )
        ), 1)                                                    AS avg_gpu_util_pct,

        -- DLT pipeline data processed (governance signal for pipeline waste)
        ROUND(AVG(
            SAFE_CAST(
                JSON_VALUE(usage_metadata, '$.data_processed_gb')
                AS FLOAT64
            )
        ), 2)                                                    AS avg_data_processed_gb

    FROM
        `ghost_burn.billing_usage`

    GROUP BY
        workspace_id,
        sku_name
),

scored AS (
    SELECT
        *,
        -- Cost per productive unit (job event or query, whichever applies)
        ROUND(
            SAFE_DIVIDE(total_cost, NULLIF(records_with_jobs, 0)),
            4
        )                                                        AS cost_per_job_event,

        ROUND(
            SAFE_DIVIDE(total_cost, NULLIF(total_queries, 0)),
            6
        )                                                        AS cost_per_query,

        -- Efficiency denominator: use best available productivity signal
        GREATEST(
            COALESCE(records_with_jobs, 0),
            COALESCE(total_queries, 0),
            1
        )                                                        AS productive_units

    FROM workspace_efficiency
)

SELECT
    workspace_id,
    sku_name,
    total_cost,
    total_dbus,
    records_with_jobs,
    total_queries,
    avg_gpu_util_pct,
    avg_data_processed_gb,
    cost_per_job_event,
    cost_per_query,

    -- Efficiency grade (see threshold rationale in header comment)
    CASE
        WHEN SAFE_DIVIDE(total_cost, productive_units) > 5.0
            THEN '🔴 Low Efficiency'
        WHEN SAFE_DIVIDE(total_cost, productive_units) > 1.0
            THEN '🟡 Moderate'
        ELSE '🟢 Efficient'
    END                                                          AS efficiency_grade,

    -- GPU-specific flag (AI/ML workloads only)
    CASE
        WHEN avg_gpu_util_pct IS NOT NULL AND avg_gpu_util_pct < 35.0
            THEN '⚠️  GPU Underutilized — Review Instance Sizing'
        WHEN avg_gpu_util_pct IS NOT NULL AND avg_gpu_util_pct < 60.0
            THEN '🟡 GPU Moderate Utilization'
        WHEN avg_gpu_util_pct IS NOT NULL
            THEN '🟢 GPU Healthy'
        ELSE NULL
    END                                                          AS gpu_efficiency_flag

FROM scored

WHERE
    total_cost > 10

ORDER BY
    cost_per_job_event DESC NULLS LAST

LIMIT 25;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q5 — ANOMALY DETECTION: 7-DAY MOVING AVERAGE SPIKE ALERTS             │
-- │                                                                         │
-- │ Purpose: Surface spending anomalies that trigger FinOps alerts.         │
-- │          This is the early warning system — catching spikes before      │
-- │          they become line items on the monthly cloud bill.              │
-- │                                                                         │
-- │ Method: Compares each day's spend against the 7-day rolling average     │
-- │         for that workspace. Two signals are used:                       │
-- │           1. Percentage deviation (fast, CFO-friendly)                 │
-- │           2. Z-score (statistical rigor for engineering review)         │
-- │                                                                         │
-- │ Alert thresholds:                                                       │
-- │   EMERGENCY  > 150% above average  — Auto-pause recommended            │
-- │   CRITICAL   >  75% above average  — Immediate review required         │
-- │   WARNING    >  30% above average  — Investigate within 24h            │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH daily_spend AS (
    SELECT
        DATE(usage_start_time)                                   AS usage_date,
        workspace_id,
        ROUND(SUM(estimated_cost_usd), 2)                        AS daily_cost,
        ROUND(SUM(usage_quantity), 2)                             AS daily_dbus,
        COUNT(DISTINCT sku_name)                                  AS sku_count,
        COUNT(*)                                                  AS record_count
    FROM
        `ghost_burn.billing_usage`
    GROUP BY
        usage_date,
        workspace_id
),

with_baseline AS (
    SELECT
        *,
        -- 7-day rolling average (excludes current day)
        ROUND(
            AVG(daily_cost) OVER (
                PARTITION BY workspace_id
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ),
            2
        )                                                        AS moving_avg_7d,

        -- 7-day rolling standard deviation (for Z-score)
        ROUND(
            STDDEV(daily_cost) OVER (
                PARTITION BY workspace_id
                ORDER BY usage_date
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ),
            2
        )                                                        AS stddev_7d

    FROM daily_spend
)

SELECT
    usage_date,
    workspace_id,
    daily_cost,
    moving_avg_7d,
    stddev_7d,

    -- Absolute and percentage deviation
    ROUND(daily_cost - moving_avg_7d, 2)                         AS cost_delta_usd,
    ROUND(
        SAFE_DIVIDE(daily_cost - moving_avg_7d, moving_avg_7d) * 100,
        1
    )                                                            AS pct_above_avg,

    -- Z-score: number of standard deviations above the rolling baseline
    -- Values > 2.0 are statistically significant anomalies
    ROUND(
        SAFE_DIVIDE(
            daily_cost - moving_avg_7d,
            NULLIF(stddev_7d, 0)
        ),
        2
    )                                                            AS z_score,

    daily_dbus,
    sku_count,

    -- Alert classification (percentage-based — CFO-friendly)
    CASE
        WHEN SAFE_DIVIDE(daily_cost - moving_avg_7d, moving_avg_7d) > 1.50
            THEN '🔴 EMERGENCY — >150% above average, auto-pause recommended'
        WHEN SAFE_DIVIDE(daily_cost - moving_avg_7d, moving_avg_7d) > 0.75
            THEN '🟠 CRITICAL — >75% above average, immediate review required'
        WHEN SAFE_DIVIDE(daily_cost - moving_avg_7d, moving_avg_7d) > 0.30
            THEN '🟡 WARNING — >30% above average, investigate within 24h'
        ELSE '🟢 Normal'
    END                                                          AS alert_level,

    -- Statistical classification (Z-score — engineering-grade signal)
    CASE
        WHEN SAFE_DIVIDE(
                daily_cost - moving_avg_7d,
                NULLIF(stddev_7d, 0)
             ) > 3.0
            THEN '📊 Extreme outlier (Z > 3.0)'
        WHEN SAFE_DIVIDE(
                daily_cost - moving_avg_7d,
                NULLIF(stddev_7d, 0)
             ) > 2.0
            THEN '📊 Statistical anomaly (Z > 2.0)'
        WHEN SAFE_DIVIDE(
                daily_cost - moving_avg_7d,
                NULLIF(stddev_7d, 0)
             ) > 1.5
            THEN '📊 Elevated (Z > 1.5)'
        ELSE '📊 Within normal range'
    END                                                          AS statistical_classification

FROM
    with_baseline

WHERE
    moving_avg_7d IS NOT NULL
    AND SAFE_DIVIDE(daily_cost - moving_avg_7d, moving_avg_7d) > 0.30

ORDER BY
    pct_above_avg DESC;


-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │ Q6 — DLT PIPELINE AUDIT: CONTINUOUS MODE WASTE DETECTION (NEW v2)     │
-- │                                                                         │
-- │ Purpose: Detect Delta Live Tables pipelines configured in CONTINUOUS    │
-- │          mode on workloads that only need hourly batch triggers.        │
-- │          Continuous mode keeps the pipeline cluster alive permanently,  │
-- │          consuming DBUs even when no new data arrives.                  │
-- │                                                                         │
-- │ Why this matters in 2026:                                               │
-- │   DLT is Databricks' flagship orchestration product. As adoption grows, │
-- │   CONTINUOUS mode misconfiguration has emerged as a top-5 source of    │
-- │   unexpected DBU consumption in enterprise Lakehouse environments.      │
-- │   This query surfaces the pattern before it appears on the monthly      │
-- │   cloud bill.                                                           │
-- │                                                                         │
-- │ Detection signals:                                                      │
-- │   - pipeline_mode = 'CONTINUOUS' (should be TRIGGERED for batch data)  │
-- │   - data_processed_gb = 0 for significant % of active hours            │
-- │     (pipeline running but no data flowing = pure waste)                │
-- │   - trigger_interval IS NULL (continuous has no trigger — by design)   │
-- │                                                                         │
-- │ Fix: Switch pipeline to TRIGGERED mode with appropriate schedule.      │
-- │      Estimated savings: 55–65% reduction in pipeline compute hours.    │
-- └─────────────────────────────────────────────────────────────────────────┘

WITH pipeline_activity AS (
    SELECT
        workspace_id,
        JSON_VALUE(usage_metadata, '$.pipeline_id')              AS pipeline_id,
        JSON_VALUE(usage_metadata, '$.pipeline_name')            AS pipeline_name,
        JSON_VALUE(usage_metadata, '$.pipeline_mode')            AS pipeline_mode,
        JSON_VALUE(usage_metadata, '$.trigger_interval')         AS trigger_interval,
        JSON_VALUE(usage_metadata, '$.target_table')             AS target_table,

        -- Activity metrics
        COUNT(*)                                                  AS total_hours_active,
        ROUND(SUM(usage_quantity), 2)                              AS total_dbus,
        ROUND(SUM(estimated_cost_usd), 2)                          AS total_cost_usd,
        ROUND(AVG(usage_quantity), 4)                               AS avg_hourly_dbu,

        -- Idle hours: pipeline running but processing zero data
        COUNTIF(
            SAFE_CAST(
                JSON_VALUE(usage_metadata, '$.data_processed_gb')
                AS FLOAT64
            ) = 0
        )                                                         AS idle_hours,

        -- Total data processed
        ROUND(SUM(
            SAFE_CAST(
                JSON_VALUE(usage_metadata, '$.data_processed_gb')
                AS FLOAT64
            )
        ), 2)                                                     AS total_data_processed_gb,

        -- Projected annual cost if unchanged
        ROUND(SUM(estimated_cost_usd) * (365.0 / 90), 2)          AS projected_annual_cost

    FROM
        `ghost_burn.billing_usage`

    WHERE
        -- DLT pipelines run on JOBS_SERVERLESS_COMPUTE
        sku_name = 'JOBS_SERVERLESS_COMPUTE'
        AND JSON_VALUE(usage_metadata, '$.pipeline_id') IS NOT NULL

    GROUP BY
        workspace_id,
        pipeline_id,
        pipeline_name,
        pipeline_mode,
        trigger_interval,
        target_table
),

scored_pipelines AS (
    SELECT
        *,

        -- Idle rate: what % of active hours had zero data flowing
        ROUND(
            SAFE_DIVIDE(idle_hours, total_hours_active) * 100,
            1
        )                                                         AS idle_rate_pct,

        -- Estimated recoverable waste (60% savings from TRIGGERED mode)
        ROUND(total_cost_usd * 0.60, 2)                            AS recoverable_waste_usd,
        ROUND(projected_annual_cost * 0.60, 2)                     AS annual_savings_if_fixed

    FROM pipeline_activity
)

SELECT
    workspace_id,
    pipeline_id,
    pipeline_name,
    pipeline_mode,
    trigger_interval,
    target_table,
    total_hours_active,
    idle_hours,
    idle_rate_pct,
    total_dbus,
    total_cost_usd,
    total_data_processed_gb,
    recoverable_waste_usd,
    projected_annual_cost,
    annual_savings_if_fixed,

    -- Waste classification
    CASE
        WHEN pipeline_mode = 'CONTINUOUS'
             AND idle_rate_pct > 50
            THEN '🔴 HIGH WASTE — CONTINUOUS mode on primarily idle pipeline'
        WHEN pipeline_mode = 'CONTINUOUS'
             AND idle_rate_pct > 25
            THEN '🟠 MODERATE WASTE — CONTINUOUS mode, significant idle periods'
        WHEN pipeline_mode = 'CONTINUOUS'
            THEN '🟡 REVIEW — CONTINUOUS mode, validate if streaming required'
        ELSE '🟢 Appropriate Mode'
    END                                                           AS waste_classification,

    -- Recommended fix
    CASE
        WHEN pipeline_mode = 'CONTINUOUS' AND idle_rate_pct > 25
            THEN 'Switch to TRIGGERED mode — recommended interval: 60 min'
        WHEN pipeline_mode = 'CONTINUOUS'
            THEN 'Validate if real-time streaming is required; consider TRIGGERED'
        ELSE 'No action required'
    END                                                           AS recommended_fix

FROM scored_pipelines

ORDER BY
    recoverable_waste_usd DESC;
