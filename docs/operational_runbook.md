# PIM4: Operational Runbook & Incident Response

This document provides actionable procedures for maintaining, monitoring, and troubleshooting the PIM4 Data Lake.

---

## 1. System Operation

### 1.1 Orchestration
The system is orchestrated by **Apache Airflow** running on an AWS EC2 instance.
- **Control Plane**: `http://{EC2_IP}:8080`
- **Main DAG**: `pi_m4_master_datalake_pipeline` (Daily schedule)
- **Maintenance DAG**: `delta_maintenance_job` (Weekly schedule)

### 1.2 Execution Schedule
1. **00:00 UTC**: Airbyte starts Postgres and API syncs.
2. **~00:30 UTC**: Glue `raw_to_silver` job triggers upon ingestion completion.
3. **~01:00 UTC**: Glue `silver_to_gold` job processes analytical views.
4. **~01:30 UTC**: Speed layer reconciliation and Delta maintenance.

---

## 2. Monitoring & Alerting

### 2.1 Health Monitoring
- **Airflow Dashboard**: Visual check for "Failed" (Red) or "Upstream Failed" (Orange) tasks.
- **Task Duration**: Monitor for "Long-running" tasks that might indicate Cloud connectivity issues or massive source data spikes.

### 2.2 Logging
- **Application Logs**: Available in the Airflow UI for each task instance.
- **Processing Logs**: AWS CloudWatch Logs.
    - Group: `/aws-glue/jobs/output`
    - Search Query: `"DQ REPORT"` to find quality validation results.
- **Airbyte Logs**: Internal to the Airbyte Cloud dashboard or visible in Airflow `trigger_airbyte` logs if the API returns an error.

### 2.3 Alerting
Critical failures trigger a **Slack Webhook** notification. Check the `#data-alerts` channel (or configured channel) for:
- Task ID
- DAG ID
- Execution Timestamp

---

## 3. Common Issues & Symptoms

| Symptom | Probable Cause | Impact |
|---|---|---|
| **Airflow UI "Internal Server Error"** | EC2 Memory Exhaustion (Low Disk/RAM) | Orchestration halts. |
| **"Critical DQ Failure" in logs** | Uniqueness violation in Gold layer mappings. | Gold tables are not updated. |
| **Airbyte Job Status "failed"** | Source database credentials expired or API rate limits. | Raw data is missing for the current day. |
| **Glue Job "OOM" (Out of Memory)** | Skewed data in Silver joins. | Transformation fails mid-way. |

---

## 4. Troubleshooting Guide

### 4.1 Step 1: Identify the Failure
- Locate the red task in the Airflow Graph View.
- Read the **Log** tab in Airflow to see if it was an Orchestration error (API call) or a Processing error (Glue/Spark).

### 4.2 Step 2: Investigate Spark/Glue Errors
- If the error is in a Glue Job, open the **AWS Glue Console**.
- Find the specific run and click **View CloudWatch Logs**.
- Search for `ERROR` or `Exception`. Common Spark errors include `AnalysisException` (schema mismatch) or `java.lang.OutOfMemoryError`.

### 4.3 Step 3: Inspect Data Quality Failures
- If the job failed with "Critical DQ Failure", search the logs for `❌ be_unique on 'order_id'`.
- This indicates that the upstream source or the unification logic duplicated records.

---

## 5. Recovery Procedures

### 5.1 Standard Task Restart
1. Go to Airflow UI.
2. Select the failed Task.
3. Click **Clear** -> Check **Downstream** and **Recursive**.
4. Click **OK!**. The pipeline will resume from the failure point, leveraging idempotency.

### 5.2 Manual Backfill
If data is missing for a specific date (e.g., 2026-03-24):
1. In the Airflow UI, find the `pi_m4_master_datalake_pipeline`.
2. Use the **Trigger DAG w/ config** or manually set the `execution_date` to the missing date.
3. Ensure the `raw` data exists in S3 for that date; if not, re-run the Airbyte sync first.

### 5.3 Data Rollback (Delta Lake)
If a Gold table was corrupted by bad logic:
- Execute a Spark command: `RESTORE TABLE business_gold.gold_sales_by_category_time TO VERSION AS OF {N}`.
- Alternatively, re-run the Silver-to-Gold job with `overwrite` mode for the affected period.

### 5.4 EC2 Reboot
If Airflow is unresponsive:
1. SSH into the EC2 instance.
2. Run `docker-compose down && docker-compose up -d`.
3. Check memory availability with `free -m`.

---
*Operational Runbook - PIM4 Project*
