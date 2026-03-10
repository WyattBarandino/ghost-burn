# DATABRICKS_MIGRATION.md

# Ghost Burn v2 — BigQuery → Databricks SQL Migration Guide

> **Purpose:** This document is a production portability reference. Every SQL
> query in `02_bigquery_audit_queries.sql` was written against BigQuery syntax
> for free-tier accessibility. This guide provides the exact Databricks SQL
> equivalents — function by function, pattern by pattern — so any query can be
> lifted and run against a real `system.billing.usage` environment with
> minimal modification.

---

## Contents

1. [Schema Differences](#1-schema-differences)
2. [Function Mapping Reference](#2-function-mapping-reference)
3. [Cost Derivation Pattern](#3-cost-derivation-pattern)
4. [Query-by-Query Migration Notes](#4-query-by-query-migration-notes)
5. [System Table Access Requirements](#5-system-table-access-requirements)
6. [Production-Ready Query Template](#6-production-ready-query-template)

---

## 1. Schema Differences

The Ghost Burn simulation mirrors the `system.billing.usage` schema as
closely as possible. Two differences exist by design:

| Field | Ghost Burn (BigQuery) | Real `system.billing.usage` | Notes |
|---|---|---|---|
| `usage_record_id` | UUID4 string | UUID string | Format matches — no change needed |
| `workspace_id` | String | String | Matches |
| `sku_name` | String | String | Matches |
| `usage_start_time` | TIMESTAMP | TIMESTAMP | Matches |
| `usage_end_time` | TIMESTAMP | TIMESTAMP | Matches |
| `usage_quantity` | FLOAT64 | DOUBLE | Functionally identical |
| `usage_metadata` | STRING (JSON) | STRING (JSON) | Matches — same JSON structure |
| `estimated_cost_usd` | **Simulation only** | **Does not exist** | See Section 3 |
| `record_id` | Not used (v1 only) | Not applicable | Removed in v2 |

### The `estimated_cost_usd` Column

This is the most important schema difference. In the Ghost Burn simulation,
`estimated_cost_usd` is pre-computed as `usage_quantity × SKU_rate` and
stored as a column for query convenience.

**In the real `system.billing.usage` schema, this column does not exist.**

Cost must be derived by joining `system.billing.usage` with
`system.billing.list_prices`. See [Section 3](#3-cost-derivation-pattern)
for the exact join pattern.

---

## 2. Function Mapping Reference

Every BigQuery-specific function used in the audit queries has a direct
Databricks SQL equivalent. Replace these before running against
`system.billing.usage`.

### JSON Extraction

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `JSON_VALUE(col, '$.field')` | `get_json_object(col, '$.field')` | Direct replacement — same JSONPath syntax |
| `JSON_QUERY(col, '$.nested')` | `get_json_object(col, '$.nested')` | Same replacement |

**Example:**
```sql
-- BigQuery
JSON_VALUE(usage_metadata, '$.cluster_id')

-- Databricks SQL
get_json_object(usage_metadata, '$.cluster_id')
```

---

### Date and Time Functions

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `DATE_DIFF(date1, date2, DAY)` | `datediff(date1, date2)` | Databricks returns days by default |
| `DATE(timestamp)` | `DATE(timestamp)` | Identical |
| `DATE_TRUNC('HOUR', ts)` | `DATE_TRUNC('HOUR', ts)` | Identical |
| `CURRENT_DATE()` | `CURRENT_DATE()` | Identical |
| `NOW()` | `NOW()` or `CURRENT_TIMESTAMP()` | Identical |
| `date - INTERVAL 7 DAY` | `date - INTERVAL 7 DAYS` | Minor syntax variation |
| `DATE_SUB(date, INTERVAL 7 DAY)` | `date - INTERVAL 7 DAYS` | Databricks uses operator form |

**Example:**
```sql
-- BigQuery
DATE_DIFF(MAX(DATE(usage_start_time)), MIN(DATE(usage_start_time)), DAY)

-- Databricks SQL
datediff(MAX(DATE(usage_start_time)), MIN(DATE(usage_start_time)))
```

---

### Conditional Aggregation

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `COUNTIF(condition)` | `COUNT(CASE WHEN condition THEN 1 END)` | Standard CASE-WHEN pattern |
| `COUNTIF(x IS NOT NULL)` | `COUNT(x)` | Simpler form when testing for non-null |

**Example:**
```sql
-- BigQuery
COUNTIF(JSON_VALUE(usage_metadata, '$.job_id') IS NOT NULL)

-- Databricks SQL
COUNT(CASE WHEN get_json_object(usage_metadata, '$.job_id') IS NOT NULL THEN 1 END)
-- or more concisely:
COUNT(get_json_object(usage_metadata, '$.job_id'))
```

---

### Safe Division

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `SAFE_DIVIDE(numerator, denominator)` | `numerator / NULLIF(denominator, 0)` | NULLIF prevents divide-by-zero |
| `SAFE_CAST(value AS INT64)` | `CAST(value AS INT)` | Databricks does not have SAFE_CAST — wrap in TRY() if needed |

**Example:**
```sql
-- BigQuery
SAFE_DIVIDE(SUM(estimated_cost_usd), COUNT(*))

-- Databricks SQL
SUM(estimated_cost_usd) / NULLIF(COUNT(*), 0)
```

---

### Type Casting

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `CAST(x AS INT64)` | `CAST(x AS INT)` | INT64 → INT |
| `CAST(x AS FLOAT64)` | `CAST(x AS DOUBLE)` | FLOAT64 → DOUBLE |
| `SAFE_CAST(x AS INT64)` | `TRY(CAST(x AS INT))` | Wrap with TRY() for fault tolerance |

**Example:**
```sql
-- BigQuery
SAFE_CAST(JSON_VALUE(usage_metadata, '$.autotermination_minutes') AS INT64)

-- Databricks SQL
TRY(CAST(get_json_object(usage_metadata, '$.autotermination_minutes') AS INT))
```

---

### Window Functions

Window function syntax is identical between BigQuery and Databricks SQL.
No changes required for `OVER (PARTITION BY ... ORDER BY ... ROWS BETWEEN ...)`.

```sql
-- This syntax works unchanged in both BigQuery and Databricks SQL
AVG(daily_cost) OVER (
    PARTITION BY workspace_id
    ORDER BY usage_date
    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
)
```

---

### Table References

| BigQuery | Databricks SQL | Notes |
|---|---|---|
| `` `ghost_burn.billing_usage` `` | `system.billing.usage` | Replace dataset.table with catalog.schema.table |
| Backtick quoting | Backtick optional in Databricks | Use backticks only if names contain special characters |

**Example:**
```sql
-- BigQuery
FROM `ghost_burn.billing_usage`

-- Databricks SQL
FROM system.billing.usage
```

---

## 3. Cost Derivation Pattern

This is the most critical migration step. Every query in Ghost Burn that
references `estimated_cost_usd` must be rewritten to join
`system.billing.list_prices`.

### The Production Cost Join

```sql
-- ============================================================
-- PRODUCTION COST DERIVATION PATTERN
-- Use this CTE at the top of any query that needs cost figures
-- ============================================================

WITH billing_with_cost AS (
    SELECT
        b.usage_record_id,
        b.workspace_id,
        b.sku_name,
        b.usage_start_time,
        b.usage_end_time,
        b.usage_quantity,
        b.usage_metadata,

        -- Derive cost: usage_quantity × list price for matching SKU
        -- list_prices uses pricing.default for standard (non-contract) rates
        ROUND(
            b.usage_quantity * p.pricing.default,
            4
        )                                           AS estimated_cost_usd

    FROM system.billing.usage b

    -- Join on SKU name and ensure price record was active during usage period
    LEFT JOIN system.billing.list_prices p
        ON  b.sku_name          = p.sku_name
        AND b.usage_start_time >= p.price_start_time
        AND (
            p.price_end_time IS NULL
            OR b.usage_start_time < p.price_end_time
        )

    -- Optional: filter to relevant date range for performance
    WHERE b.usage_start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
)

-- All subsequent CTEs and SELECT statements reference billing_with_cost
-- instead of system.billing.usage directly, and use estimated_cost_usd
-- exactly as written in the Ghost Burn BigQuery queries.

SELECT
    workspace_id,
    ROUND(SUM(estimated_cost_usd), 2) AS total_cost_usd
FROM billing_with_cost
GROUP BY workspace_id
ORDER BY total_cost_usd DESC;
```

### Notes on List Prices

- `system.billing.list_prices` contains Databricks public list prices.
  If your organization has negotiated contract pricing, actual charges
  may differ. Use this join for directional cost analysis and anomaly
  detection — not for exact invoice reconciliation.

- The `pricing` column is a struct. Access the default rate via
  `p.pricing.default`. For contract-specific rates, consult your
  Databricks account team about the `system.billing.usage_cost` view
  (available on some contract types).

- Price records are time-bounded. The join condition on
  `price_start_time` and `price_end_time` ensures the correct rate
  is applied even if pricing changed during the audit window.

---

## 4. Query-by-Query Migration Notes

### Q1 — Top 5 Cost Centers

**Changes required:**
- Replace `` `ghost_burn.billing_usage` `` with the `billing_with_cost` CTE
- Replace `COUNT(usage_record_id)` with `COUNT(b.usage_record_id)`
- Replace `DATE_DIFF(..., DAY)` with `datediff(...)`

**Effort:** 5 minutes

---

### Q2 — SKU Breakdown

**Changes required:**
- Replace table reference with `billing_with_cost` CTE
- No function changes needed — this query uses only standard aggregations

**Effort:** 2 minutes

---

### Q3 — Zombie Cluster Detection

**Changes required:**
- Replace table reference with `billing_with_cost` CTE
- Replace all `JSON_VALUE(...)` with `get_json_object(...)`
- Replace `SAFE_DIVIDE(...)` with `... / NULLIF(..., 0)`
- Replace `SAFE_CAST(... AS INT64)` with `TRY(CAST(... AS INT))`

**Effort:** 10 minutes

**Note:** The zombie detection logic — coefficient of variation threshold,
`autotermination_minutes = 0` governance branch, and severity classification
— requires zero changes. The detection logic is schema-agnostic.

---

### Q4 — Compute Efficiency Ratio

**Changes required:**
- Replace table reference with `billing_with_cost` CTE
- Replace `COUNTIF(...)` with `COUNT(CASE WHEN ... THEN 1 END)`
- Replace `JSON_VALUE(...)` with `get_json_object(...)`
- Replace `SAFE_DIVIDE(...)` with `... / NULLIF(..., 0)`
- Replace `SAFE_CAST(...)` with `TRY(CAST(...))`

**Effort:** 10 minutes

---

### Q5 — Anomaly Detection (7-Day Moving Average)

**Changes required:**
- Replace table reference with `billing_with_cost` CTE
- Replace `DATE(usage_start_time)` grouping — identical in Databricks
- Window function syntax is unchanged

**Effort:** 5 minutes

**Note:** The Z-score calculation and alert threshold logic require zero
changes. Window function syntax is fully portable.

---

### Q6 — DLT Pipeline Audit

**Changes required:**
- Replace table reference with `billing_with_cost` CTE
- Replace `JSON_VALUE(...)` with `get_json_object(...)`
- Replace `SAFE_CAST(...)` with `TRY(CAST(...))`
- Replace `COUNTIF(...)` with `COUNT(CASE WHEN ... THEN 1 END)`

**Effort:** 10 minutes

**Note:** The `pipeline_mode = 'CONTINUOUS'` detection logic and idle rate
calculation are fully portable — these reference metadata fields that exist
in real DLT billing records.

---

## 5. System Table Access Requirements

Running these queries against a live Databricks environment requires the
following permissions. This information is provided for reference — access
configuration should be handled by your Databricks Account Admin.

| Table | Required Permission | Scope |
|---|---|---|
| `system.billing.usage` | `SELECT` on `system.billing` schema | Account-level (all workspaces) |
| `system.billing.list_prices` | `SELECT` on `system.billing` schema | Account-level |
| `system.compute.clusters` | `SELECT` on `system.compute` schema | Account-level (for cluster metadata) |

### Enabling System Tables

System tables must be enabled by an Account Admin before they are queryable:

```
Databricks Account Console
→ Settings
→ Feature enablement
→ System tables
→ Enable
```

Once enabled, system tables are available in the `system` catalog in any
Unity Catalog-enabled workspace. The billing tables typically have a
24-hour data lag — yesterday's usage is queryable today.

### Minimum Workspace Requirements

- Unity Catalog must be enabled on the workspace
- Databricks Runtime 11.3 LTS or higher
- SQL warehouse or cluster with access to the `system` catalog

---

## 6. Production-Ready Query Template

The following is a complete, production-ready version of Q3 (Zombie Cluster
Detection) migrated to Databricks SQL. Use this as a reference pattern for
migrating the remaining queries.

```sql
-- ============================================================
-- Q3 — ZOMBIE CLUSTER DETECTION (Databricks SQL)
-- Production-ready version of Ghost Burn audit query 3
-- Target: system.billing.usage + system.billing.list_prices
-- Minimum runtime: Databricks 11.3 LTS, Unity Catalog enabled
-- ============================================================

WITH billing_with_cost AS (
    SELECT
        b.usage_record_id,
        b.workspace_id,
        b.sku_name,
        b.usage_start_time,
        b.usage_end_time,
        b.usage_quantity,
        b.usage_metadata,
        ROUND(b.usage_quantity * p.pricing.default, 4) AS estimated_cost_usd
    FROM system.billing.usage b
    LEFT JOIN system.billing.list_prices p
        ON  b.sku_name          = p.sku_name
        AND b.usage_start_time >= p.price_start_time
        AND (p.price_end_time IS NULL
             OR b.usage_start_time < p.price_end_time)
    WHERE b.usage_start_time >= CURRENT_DATE() - INTERVAL 90 DAYS
),

cluster_activity AS (
    SELECT
        workspace_id,
        get_json_object(usage_metadata, '$.cluster_id')         AS cluster_id,
        get_json_object(usage_metadata, '$.cluster_name')        AS cluster_name,
        get_json_object(usage_metadata, '$.job_id')              AS job_id,
        TRY(CAST(
            get_json_object(usage_metadata, '$.autotermination_minutes')
            AS INT
        ))                                                       AS autotermination_min,
        get_json_object(usage_metadata, '$.data_security_mode')  AS data_security_mode,
        COUNT(*)                                                  AS total_hours_active,
        ROUND(SUM(usage_quantity), 2)                              AS total_dbus,
        ROUND(SUM(estimated_cost_usd), 2)                          AS total_cost_usd,
        ROUND(AVG(usage_quantity), 4)                               AS avg_hourly_dbu,
        ROUND(STDDEV(usage_quantity), 4)                            AS stddev_hourly_dbu,
        ROUND(
            STDDEV(usage_quantity) / NULLIF(AVG(usage_quantity), 0) * 100,
            2
        )                                                         AS usage_cv_pct,
        ROUND(SUM(estimated_cost_usd) * (365.0 / 90), 2)          AS projected_annual_cost
    FROM billing_with_cost
    WHERE sku_name IN (
        'STANDARD_ALL_PURPOSE_COMPUTE',
        'PREMIUM_ALL_PURPOSE_COMPUTE'
    )
    GROUP BY
        workspace_id, cluster_id, cluster_name,
        job_id, autotermination_min, data_security_mode
),

behavioral_zombies AS (
    SELECT *, 'BEHAVIORAL' AS detection_branch
    FROM cluster_activity
    WHERE job_id IS NULL
      AND total_hours_active > 200
      AND usage_cv_pct < 10
),

governance_violations AS (
    SELECT *, 'GOVERNANCE_VIOLATION' AS detection_branch
    FROM cluster_activity
    WHERE autotermination_min = 0
      AND job_id IS NULL
),

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
    CASE
        WHEN job_id IS NULL
             AND total_hours_active > 400
             AND usage_cv_pct < 10
             AND autotermination_min = 0
            THEN 'CRITICAL — Terminate + Policy Required'
        WHEN job_id IS NULL
             AND total_hours_active > 200
             AND usage_cv_pct < 10
            THEN 'HIGH — Terminate Immediately'
        WHEN autotermination_min = 0
            THEN 'GOVERNANCE — Auto-Termination Disabled'
        ELSE 'Monitor'
    END                                                           AS zombie_status,
    CASE
        WHEN autotermination_min = 0
            THEN 'Apply ghost-burn-anti-zombie-policy immediately'
        WHEN job_id IS NULL AND total_hours_active > 200
            THEN 'Terminate cluster + apply compute policy'
        ELSE 'Add to monitoring queue'
    END                                                           AS recommended_action
FROM combined
ORDER BY total_cost_usd DESC;
```

---

## Summary Checklist

Use this checklist when migrating any Ghost Burn query to production:

- [ ] Add the `billing_with_cost` CTE at the top of the query
- [ ] Replace all `` `ghost_burn.billing_usage` `` references with `billing_with_cost`
- [ ] Replace all `JSON_VALUE(col, '$.field')` with `get_json_object(col, '$.field')`
- [ ] Replace all `DATE_DIFF(d1, d2, DAY)` with `datediff(d1, d2)`
- [ ] Replace all `COUNTIF(condition)` with `COUNT(CASE WHEN condition THEN 1 END)`
- [ ] Replace all `SAFE_DIVIDE(a, b)` with `a / NULLIF(b, 0)`
- [ ] Replace all `SAFE_CAST(x AS INT64)` with `TRY(CAST(x AS INT))`
- [ ] Replace all `SAFE_CAST(x AS FLOAT64)` with `TRY(CAST(x AS DOUBLE))`
- [ ] Verify `system.billing` schema access with Account Admin
- [ ] Confirm Unity Catalog is enabled on the target workspace
- [ ] Test on a 7-day window before running the full 90-day audit

---

*Ghost Burn v2 — March 2026*
*Schema reference: Databricks system.billing.usage documentation (public)*
*Not affiliated with or endorsed by Databricks, Inc.*
