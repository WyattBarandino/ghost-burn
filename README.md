# Ghost Burn

**Uncovering Invisible DBU Waste in Databricks Lakehouse Environments**

A FinOps audit framework that recovers $54,635 in annualized Databricks spend across 8 simulated workspaces and ships every fix as a copy-paste ready policy config or SQL query.

[**→ View Dashboard**](https://wyattbarandino.github.io/ghost-burn/) · [**LinkedIn**](https://linkedin.com/in/wyattbarandino)

---

## What This Is

Ghost Burn is a reactive FinOps audit case study built around the `system.billing.usage` System Table schema. It simulates a realistic Q1 2026 billing environment, runs six SQL audit queries against it, identifies five anomaly patterns, and produces an implementable remediation playbook with:

- Databricks Compute Policy JSON (v2025+ syntax, copy-paste ready)
- SQL Warehouse Budget Policy for serverless governance
- Automated monitoring queries schedulable via Databricks Workflows
- A Databricks SDK lifecycle notebook for off-hours cluster management
- A 4-week phased implementation roadmap

---

## Audit Results

| Metric | Value |
|---|---|
| Audit window | 90 days (Jan 1 – Mar 31, 2026) |
| Total platform spend | $89,995 |
| Workspaces audited | 8 |
| Recoverable waste (90d) | $13,472 |
| Annualized waste | $56,580 |
| Net annual savings | $54,635 |
| Implementation effort | ~8 hours |

### Five Anomaly Findings

| # | Finding | Workspace | 90-Day Cost | Annual Savings | Effort | Risk |
|---|---|---|---|---|---|---|
| R1 | Zombie Cluster | ws-zombie-008 | $5,007 | $19,289 | 30 min | Low |
| R2 | Serverless SQL Spike | ws-prod-bi-004 | $1,214 | $4,183 | 1 hr | Low |
| R3 | GPU Overprovisioning | ws-retail-forecast-002 | $11,759 | $23,845 | 2 hrs | Medium |
| R4 | Weekend Dev Waste | ws-sandbox + ws-health | $946 | $3,645 | 45 min | Low |
| R5 | Runaway DLT Pipeline | ws-fsi-trading-001 | $1,510 | $3,673 | 1 hr | Low |
| | **Total** | **8 workspaces** | **$20,435** | **$54,635** | **~8 hrs** | |

### Scale Extrapolation

The audit covers 8 workspaces. The SQL queries and compute policies scale linearly — the implementation effort does not.

- 50 workspaces → **$341,470** preventable annual spend
- 100 workspaces → **$682,940** preventable annual spend

---

## Project Structure

```
ghost-burn/
├── index.html                        # Interactive dashboard (GitHub Pages)
├── README.md                         # This file
├── DATABRICKS_MIGRATION.md           # BigQuery → Databricks SQL reference
├── LICENSE                           # MIT
├── .gitignore
│
├── 01_data_simulation.py             # Synthetic billing data generator
├── 02_bigquery_audit_queries.sql     # Six audit queries (BigQuery syntax)
├── 03_visualizations.py              # Static Matplotlib charts for report
├── 04_remediation_playbook.py        # Policy configs + monitoring SQL
│
├── GHOST_BURN_EXECUTIVE_REPORT.pdf    # 9-page executive report with figures
│
├── fig1_burn_trend.png               # 90-Day Platform Burn Trend
├── fig2_sku_breakdown.png            # Spend by Compute Category
├── fig3_anomaly_matrix.png           # Anomaly Cost Matrix — 5 Findings
├── fig4_gpu_utilization.png          # GPU Utilization vs. 90-Day Cost
├── fig5_remediation_waterfall.png    # Annual Spend: Before vs. After
│
└── sample_billing_50rows.csv         # 50-row preview (no PII, synthetic only)
```

---

## File Guide

### `01_data_simulation.py`
Generates the 15,022-record synthetic billing dataset. Implements an `AnomalyEngine` class with five static methods — one per anomaly type. Uses Poisson arrival modeling for realistic temporal patterns. Outputs `ghost_burn_billing.csv`.

Key design decisions:
- `GPU_INSTANCE_CATALOG` enforces correct AWS GPU instance types (p4d, g5, g4dn, p3) — no storage or compute instances in GPU records
- `estimated_cost_usd` is a simulation convenience column (`usage_quantity × SKU_rate`). It does **not** exist in the real `system.billing.usage` schema — see `DATABRICKS_MIGRATION.md` for the production `list_prices` join pattern
- UUID4 `usage_record_id` matches the real schema field name

**Run in Google Colab:** outputs `ghost_burn_billing.csv` and `sample_billing_50rows.csv`

---

### `02_bigquery_audit_queries.sql`
Six SQL audit queries against the simulated dataset. Written in BigQuery syntax for free-tier accessibility. Each query targets a specific waste pattern:

| Query | Detects | Key Signal |
|---|---|---|
| Q1 | Top cost centers | Workspace spend concentration |
| Q2 | SKU breakdown | Category-level spend distribution |
| Q3 | Zombie clusters | CV < 10% + autotermination = 0 (two detection branches) |
| Q4 | Compute efficiency | Cost-per-job-hour by SKU |
| Q5 | Spend anomalies | 7-day moving avg + Z-score dual signal |
| Q6 | DLT pipeline waste | CONTINUOUS mode idle rate |

Q3 uses two independent detection branches: behavioral (coefficient of variation analysis) and governance violation (autotermination policy audit). Q5 uses both percentage deviation and Z-score so the output serves both finance (%) and engineering (Z) audiences.

**Databricks SQL equivalents:** see `DATABRICKS_MIGRATION.md`

---

### `03_visualizations.py`
Generates five publication-quality Matplotlib charts for the executive report. White background, 200 DPI, print-safe. Outputs to the repo root.

| Figure | Chart | Key Insight |
|---|---|---|
| Fig 1 | Burn Trend (bar + rolling avg) | Feb 10–13 spike at 513% above baseline |
| Fig 2 | SKU Breakdown (horizontal bar) | AI/ML = 53.1% of total spend |
| Fig 3 | Anomaly Cost Matrix (grouped bar) | GPU overprovisioning dominates at $23,845/yr |
| Fig 4 | GPU Utilization Scatter (bubble) | ws-retail-forecast-002 isolated at 27.9% util |
| Fig 5 | Remediation Waterfall | $56,580 → $1,945 net after five fixes |

**Run in Google Colab:** requires `ghost_burn_billing.csv` in the working directory

---

### `04_remediation_playbook.py`
The operational core of the project. Five remediation classes — one per finding — each containing:

- The root cause analysis
- A copy-paste ready Databricks config (Compute Policy JSON or SQL Warehouse Policy)
- A monitoring SQL query schedulable via Databricks Workflows
- A `calculate_savings()` method with before/after cost projection

Plus a `FinOpsAlertEngine` class with three persistent monitoring queries (daily anomaly detection, nightly zombie sweep, weekly cost attribution).

Executing `generate_remediation_report(df)` prints the full impact analysis and 4-week roadmap.

**Compute policy syntax note:** All policy JSON uses the v2025+ Databricks Clusters API specification. Custom tags use block-level syntax under a `"custom_tags"` key — not dot-notation keys. Verified against the current Databricks documentation.

---

### `DATABRICKS_MIGRATION.md`
Production portability guide. Every BigQuery-specific function used across all six audit queries is mapped to its Databricks SQL equivalent, with before/after examples.

Covers: `JSON_VALUE` → `get_json_object`, `DATE_DIFF` → `datediff`, `COUNTIF` → `COUNT(CASE WHEN)`, `SAFE_DIVIDE` → `NULLIF`, `SAFE_CAST` → `TRY(CAST)`.

Most importantly: the complete `billing_with_cost` CTE with the `system.billing.list_prices` join pattern — the critical step that replaces the simulation's `estimated_cost_usd` convenience column in production.

Closes with a 10-item migration checklist and a fully migrated production-ready version of Q3 (Zombie Cluster Detection) as a reference pattern.

---

### The Dual-State Remediation Engine
The dashboard utilizes a global toggle to transition between two logical states, visualizing the direct impact of FinOps remediation:

* **Burn State (Baseline Environment)**
    * **KPI Suite:** 4 dynamic cards tracking waste, annual projections, waste rate, and ROI.
    * **Infrastructure Audit:** 7 integrated Chart.js visualizations and a comprehensive remediation table identifying "ghost" instances.
* **Extinguished State (Remediated Environment)**
    * **Reactive KPI Animation:** Values animate in real-time to show waste counting down to residual levels; ROI panels flip to reveal net savings.
    * **Comparative Benchmarking:** "Ghost" comparison lines appear under each KPI (e.g., *"was: $X lost"*) for historical context.
    * **Anomaly Suppression:** The burn trend re-renders with a flattened anomaly window and a **15% lower baseline**.
    * **Compute Efficiency:** The GPU scatter plot re-renders to visualize the optimization of `ws-retail-forecast-002` from **27.9% → 71.5% utilization**.
    * **Enterprise Scaling:** Dynamic ROI panels appear, projecting savings across 8, 50, and 100 workspace deployments.

**Tech Stack**
* **Visualizations:** Chart.js 4.4.1 (via cdnjs)
* **Typography:** DM Sans & DM Mono (via Google Fonts)

---

## Setup

### Run in Google Colab (recommended)

```python
# Step 1: Clone the repo
!git clone https://github.com/wyattbarandino/ghost-burn.git
%cd ghost-burn

# Step 2: Install dependencies
!pip install pandas numpy matplotlib

# Step 3: Generate the dataset
!python 01_data_simulation.py

# Step 4: Run the audit queries (requires BigQuery connection)
# Or load the CSV directly for local exploration

# Step 5: Generate visualizations
!python 03_visualizations.py

# Step 6: Run remediation analysis
!python 04_remediation_playbook.py
```

### View the dashboard locally

```bash
# No build step needed — open directly
open index.html
```

Or deploy to GitHub Pages by enabling Pages in repo settings (source: main branch, root).

---

## Schema Reference

The simulation mirrors the `system.billing.usage` System Table schema. Two intentional differences:

1. **`estimated_cost_usd`** — pre-computed simulation convenience column. Does not exist in production. Use the `list_prices` join pattern in `DATABRICKS_MIGRATION.md`.
2. **`usage_metadata`** fields — some metadata fields (e.g., `gpu_utilization_pct`, `pipeline_mode`) are simulation extensions. Production metadata fields vary by SKU and are documented in the Databricks System Tables reference.

Real schema access requires Unity Catalog + `SELECT` on `system.billing`. See `DATABRICKS_MIGRATION.md` §5 for enablement steps.

---

## Technical Notes

**GPU instance types:** The simulation uses only valid AWS GPU instance types (`p4d.24xlarge`, `g5.12xlarge`, `g5.xlarge`, `g4dn.xlarge`, `p3.2xlarge`). Storage-optimized (`i3`, `i4i`) and general-purpose (`m5`, `r5`) instances are used for non-GPU workloads only. This is correct — Databricks GPU SKUs map to GPU-class EC2 instances.

**GPU right-sizing caveat:** R3 recommends migrating from `p4d.24xlarge` to `g5.12xlarge`. This assumes the fine-tuning workload fits within `g5.12xlarge` memory (4× A10G, 96 GB). Validate with a test run before production deployment. If OOM errors occur, `g5.48xlarge` (8× A10G, 192 GB) is the intermediate step.

**Zombie detection branches:** Q3 uses two independent detection branches rather than a single compound condition. Behavioral detection (CV < 10%, no job association) catches clusters that are technically auto-terminatable but idle in practice. Governance detection (autotermination = 0) catches policy violations regardless of utilization pattern. A cluster can trigger either or both.

**Annualization:** Savings projections use a 365/90 annualization factor (4.056×). This is applied consistently across all five remediations.

---

## AI Transparency

Dashboard UI and code implementation were developed with AI assistance. All analytical design, schema architecture, anomaly detection logic, and remediation recommendations are my own work, validated against Databricks documentation.

---

## Synthetic Data Disclaimer

All data used in this project is 100% synthetically generated. No real Databricks billing data, customer information, workspace configurations, or proprietary schemas were used at any point. The `system.billing.usage` schema is modeled exclusively from publicly available Databricks documentation. This project is entirely independent and is not affiliated with, endorsed by, sponsored by, or produced in partnership with Databricks, Inc. No confidential or proprietary information of Databricks or any of its customers was accessed, used, or referenced in the creation of this project.

---

## License

MIT — see `LICENSE`

---

*Ghost Burn v2 · Wyatt Barandino · March 2026 · [linkedin.com/in/wyattbarandino](https://linkedin.com/in/wyattbarandino)*
