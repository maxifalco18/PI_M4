# PIM4: Technical Specifications & Data Engineering Handbook

This document provides a low-level technical deep-dive into the components, data structures, and logic of the PIM4 Data Lake.

---

## 1. Components Detail

### 1.1 Airbyte Cloud (Batch Ingestor)
- **Responsibility**: Managed extraction of relational data (PostgreSQL) and semi-structured data (Public API).
- **Inputs**: Source credentials, replication frequency, and target S3 bucket location.
- **Outputs**: Raw JSON/Parquet files in S3 under `s3://{BUCKET}/raw/batch/`.
- **Dependencies**: Connectivity to the source database and Airbyte Cloud API.

### 1.2 AWS Glue - `raw_to_silver.py`
- **Responsibility**: Schema refinement, basic cleaning, and Dimension evolution (SCD2).
- **Inputs**: Data from `s3://{BUCKET}/raw/batch/`.
- **Outputs**: Enforced schemas in `s3://{BUCKET}/processed/batch/`.
- **Dependencies**: `dq_framework.py` for quality assertions; PySpark 3.x.

### 1.3 AWS Glue - `silver_to_gold.py`
- **Responsibility**: Analytical transformation, Lambda unification, and Delta Lake merges.
- **Inputs**: Cleaned data from Silver + Processed events from Speed Layer.
- **Outputs**: Curated "Gold" tables in `s3://{BUCKET}/gold/`.
- **Dependencies**: Delta Lake libraries, Broadcast join optimization, critical DQ gates.

### 1.4 Apache Airflow (Orchestrator)
- **Responsibility**: Coordinating the end-to-end execution, dependency management, and alerting.
- **Inputs**: S3 triggers, time schedules.
- **Outputs**: Command execution (Airbyte syncs, Glue jobs), Slack notifications.

---

## 2. Data Structures

The system follows a strict Medallion schema evolution.

### 2.1 Silver Layer (Processed)
| Table | Key Fields | Partitioning | Logic |
|---|---|---|---|
| `dim_customers` | `customer_id` | `customer_state` | SCD Type 2 (effective_date, end_date, is_current). |
| `fact_orders` | `order_id` | `year`, `month` | Temporal partitioning based on purchase timestamp. |
| `fact_payments` | `order_id` | `payment_type` | Categorical partitioning for query efficiency. |
| `dim_geolocation` | `zip_code_prefix` | `geolocation_state` | Aggressive deduplication. |

### 2.2 Gold Layer (Analytical - OBT)
- **`gold_sales_by_category_time`**:
    - **Keys**: `product_category_name`, `year`, `month`.
    - **Metrics**: `total_revenue`, `total_items_sold`.
- **`gold_sales_by_region`**:
    - **Keys**: `customer_state`, `year`, `month`.
    - **Metrics**: `total_orders`.
- **`gold_sales_by_payment`**:
    - **Keys**: `payment_type`, `year`, `month`.
    - **Metrics**: `total_revenue`, `total_transactions`.

---

## 3. Processing Logic

### 3.1 Silver Logic Flow
1. **Extraction**: Read immutable Parquet files from Raw.
2. **DQ Check (Soft Gate)**: Run `DataQualityValidator` assertions (e.g., `< 1% NULLs`). Results are logged but fail-safe.
3. **Deduplication**: Apply `dropDuplicates` based on business primary keys (e.g., `product_id`).
4. **SCD Implementation**: For `dim_customers`, timestamps are added to track the current version of the record.
5. **Persistence**: Write to S3 using `Dynamic Partition Overwrite` to ensure idempotency.

### 3.2 Gold Logic Flow
1. **Lambda Unification**: Batch data from Silver is `unionByName` with the processed Speed Layer events.
2. **Deduplication**: A final deduplication on `order_id` ensures no overlaps between layers.
3. **DQ Gate (Hard Gate)**: Critical validation on unique keys. If uniqueness fails, the entire job throws an exception and halts.
4. **Master Joins**: Big fact tables are joined with small dimension tables using **Broadcast Joins** to minimize network shuffles.
5. **Aggregation**: `groupBy` operations generate the OBTs.
6. **Delta Merge**: Instead of overwriting, a `MERGE` statement is used to synchronize the analytical view with late-arriving data.

---

## 4. Integrations

### 4.1 Airbyte Cloud API
- **Endpoint**: `https://api.airbyte.com/v1/jobs`
- **Method**: POST
- **Function**: Transitions from Airflow to Airbyte to trigger the sync process and polls the `status` until completion (`succeeded`/`failed`).

### 4.2 Slack Webhook
- **Functionality**: `on_failure_callback` in Airflow Dags.
- **Payload**: JSON containing the `task_id`, `dag_id`, and a critical alert message.

---

## 5. Error Handling & Reliability

### 5.1 Data Quality Tiers
- **Tier 1 (Silver)**: Monitoring only. Failures are recorded in logs for audit but do not stop data flow.
- **Tier 2 (Gold)**: Enforced integrity. Any violation of Primary Key constraints halts the pipeline to prevent reporting errors.

### 5.2 Idempotency Strategy
- **Spark Writers**: Use `mode("overwrite")` with `.option("partitionOverwriteMode", "dynamic")`.
- **Delta Lake**: Use `MERGE INTO` logic for the Gold layer.
- **Airflow**: `retries: 2` with a `retry_delay` of 5 minutes to handle transient cloud connectivity issues.

### 5.3 Small Files Mitigation
- Spark configuration `spark.sql.shuffle.partitions` is set to `8` (optimized for Glue small-scale instances) and `snappy` compression is enforced to optimize Athena reading costs.

---
*Technical Documentation - PIM4 Project*
