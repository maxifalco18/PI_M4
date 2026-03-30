================================================================================
DEMO GUIDE — PROJECT PRESENTATION (PIM4)
================================================================================

## 1. KEY FILES TO SHOW

### 1.1 Orchestration
- **File path**: [`orchestration/dags/master_pipeline_dag.py`](file:///c:/Users/CTI23994/Downloads/PIM4/orchestration/dags/master_pipeline_dag.py)
- **Purpose**: Centralized coordination of the entire Medallion lifecycle.
- **Demo Value**: Shows zero-hardcoding (parameterization), failure alerting (Slack), and the standalone DQ audit task.

### 1.2 The Modular Heart
- **File path**: [`src/lib/environment.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/lib/environment.py)
- **Purpose**: System initialization for AWS Glue workers.
- **Demo Value**: Proves seniority by moving from "scripting" to "software engineering" (modular imports and standardized logging).

### 1.3 Transformation Logic
- **File path**: [`src/transformations/gold.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/transformations/gold.py)
- **Purpose**: Business Layer creation (OBT) and Lambda Unification.
- **Demo Value**: Highlights **SCD Type 2** for history and `unify_lambda_orders` for real-time + batch merging.

### 1.4 Quality Shield
- **File path**: [`src/components/dq/validator.py`](file:///c:/Users/CTI23994/Downloads/PIM4/src/components/dq/validator.py)
- **Purpose**: Generalized Data Quality Framework.
- **Demo Value**: Reusable logic for uniqueness, null-checks, and global referential integrity.

---

## 2. DEMO FLOW (STEP-BY-STEP)

### Step 1 — Show input (raw data)
- **Path**: `s3://pi-m4-datalake-maxi/raw/batch/`
- **Command to inspect**: `aws s3 ls s3://pi-m4-datalake-maxi/raw/batch/olist_orders/`
- **Explain**: Data is stored in immutable Parquet format. This is our "Single Version of Fact."

### Step 2 — Show orchestration (Airflow)
- **URL**: `http://<EC2_PUBLIC_IP>:8080`
- **What DAG to run**: `pi_m4_master_datalake_pipeline`
- **Highlight**: The Graph View showing the sequential flow: `Ingesta -> Silver -> DQ Audit -> Gold`.

### Step 3 — Show processing logic
- **Files**: `src/transformations/silver.py`
- **Transformations**: Explain **Deduplication**, **Schema Enforcement**, and **Date Normalization**.

### Step 4 — Show final data (curated)
- **Path**: `gold.sales_obt` (Table in Athena)
- **Command**: `SELECT * FROM gold.sales_obt LIMIT 10;`
- **Expected Result**: Enriched data ready for BI, including Category names and unified Real-time events.

---

## 3. COMMANDS TO EXECUTE (LIVE)

### Local / Setup
- **Packaging Logic**:
```powershell
.\infrastructure\package_glue_lib.ps1
```

### EC2 / Airflow
- **Check Docker Health**:
```bash
docker ps
```
- **Force DAG Refresh**:
```bash
airflow dags reserialize
```

---

## 4. PIPELINE VALIDATION (CRITICAL)

### Count Consistency Check
```sql
-- Check if Gold volume matches Source volume (deduplicated)
SELECT 
    (SELECT COUNT(*) FROM gold.sales_obt) as cnt_gold,
    (SELECT COUNT(DISTINCT order_id) FROM silver.fact_orders) as cnt_silver;
```

---

## 5. AIRFLOW VERIFICATION
- **Task Status**: Show that `dq_audit_silver_glue` is green.
- **Logs**: Open the logs for `spark_raw_to_silver_glue` to show the **Environment Loader** initializing.

---

## 6. DATA QUALITY CHECKS

### Uniqueness Check
```sql
SELECT order_id, COUNT(*)
FROM silver.fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1; -- Expected: ZERO rows
```

---

## 7. RISKS DURING DEMO
- **Kafka Connectivity**: If the producer fails, events won't show in Gold.
- **Fix**: Run `python spark/kafka_producer_sim.py` manually to restart the stream.

---

## 8. BACKUP PLAN
- If Airflow is unresponsive: Show the **GitHub Actions** logs to prove the deploy worked.
- If Athena is slow: Show the files directly in S3 as Parquet to prove they exist.

================================================================================
**FINAL TIP**: Emphasize that the platform is **IDEMPOTENT**. Rerunning the demo won't break the data.
================================================================================
