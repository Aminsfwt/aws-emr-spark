# 🚕 NYC Taxi Analytics — AWS EMR Spark Pipeline

A cloud-native data engineering pipeline built with **Apache Spark on AWS EMR**, processing NYC Green and Yellow taxi trip data and generating a unified service report stored in **Amazon S3**.

---

## 📐 Architecture Overview

```
Amazon S3 (Input)
        │
        ├── processed/green/*/*   → Green Taxi Parquet files
        └── processed/yellow/*/*  → Yellow Taxi Parquet files
                │
                ▼
┌──────────────────────────────────────┐
│         AWS EMR Cluster              │
│                                      │
│  Master:  1x m5.xlarge              │
│  Core:    2x m5.xlarge              │
│                                      │
│  spark-submit emr_deployment.py      │
│    1. Load green + yellow data       │
│    2. Normalize column names         │
│    3. Add service_type label         │
│    4. Union both datasets            │
│    5. SQL query → trips in 2020      │
│    6. Write report to S3             │
└──────────────────────────────────────┘
                │
                ▼
Amazon S3 (Output)
└── report/nyc_taxi_service_report/    → Final Parquet Report
```

---

## 🗂️ Project Structure

```
project/
├── create_aws_emr.sh       # AWS CLI command to create EMR cluster + submit job
└── emr_deployment.py       # PySpark job — transform and report logic
```

---

## ☁️ AWS Infrastructure

### EMR Cluster — `create_aws_emr.sh`

| Property | Value |
|---|---|
| Cluster Name | `nyc-taxi-spark-cluster` |
| EMR Release | `emr-7.12.0` |
| Application | Spark |
| Region | `us-east-1` |
| Auto-terminate | Yes (shuts down after job completes) |

### Instance Groups

| Role | Type | Count | Purpose |
|---|---|---|---|
| Master | `m5.xlarge` | 1 | Cluster manager + Driver |
| Core | `m5.xlarge` | 2 | Executors + HDFS storage |

**Total capacity:** 3 nodes, each with 4 vCPUs and 16GB RAM

### S3 Bucket Layout

```
s3://nyc-taxi-pyspark-de-bootcamp/
├── logs/                              # EMR cluster logs
├── processed/
│   ├── green/*/*                      # Green taxi input Parquet files
│   └── yellow/*/*                     # Yellow taxi input Parquet files
├── emr_deployment.py                  # PySpark script (uploaded before running)
└── report/
    └── nyc_taxi_service_report/       # Output report Parquet files
```

---

## ⚙️ Prerequisites

- AWS CLI installed and configured (`aws configure`)
- IAM permissions for EMR, S3, and EC2
- Default EMR roles created (`EMR_DefaultRole`, `EMR_EC2_DefaultRole`)
- Input Parquet files already uploaded to S3 under `processed/green/` and `processed/yellow/`
- PySpark script uploaded to S3

---

## 🚀 How to Run

### Step 1 — Upload the PySpark Script to S3

```bash
aws s3 cp emr_deployment.py s3://nyc-taxi-pyspark-de-bootcamp/emr_deployment.py
```

### Step 2 — Create EMR Cluster and Submit Job

```bash
bash create_aws_emr.sh
```

This single command:
- Creates the EMR cluster
- Installs Spark
- Submits `emr_deployment.py` as a Spark step
- Auto-terminates the cluster when the job completes
- Streams logs to `s3://nyc-taxi-pyspark-de-bootcamp/logs/`

### Step 3 — Monitor the Job

**Via AWS Console:**
```
AWS Console → EMR → Clusters → nyc-taxi-spark-cluster → Steps
```

**Via AWS CLI:**
```bash
# List clusters to get cluster ID
aws emr list-clusters --active --region us-east-1

# Check step status
aws emr describe-step \
    --cluster-id <your-cluster-id> \
    --step-id <your-step-id> \
    --region us-east-1
```

### Step 4 — Verify Output

```bash
aws s3 ls s3://nyc-taxi-pyspark-de-bootcamp/report/nyc_taxi_service_report/
```

---

## 🔄 Job Logic — `emr_deployment.py`

### Input

| Argument | Description | Example |
|---|---|---|
| `--input_green` | S3 path to green taxi Parquet | `s3://bucket/processed/green/*/*` |
| `--input_yellow` | S3 path to yellow taxi Parquet | `s3://bucket/processed/yellow/*/*` |
| `--output` | S3 path for output report | `s3://bucket/report/nyc_taxi_service_report` |

---

### Processing Steps

**1. Load Green Taxi Data**
```python
green_df = spark.read.parquet(input_green)

# Normalize column names
green_df = green_df.select(
    col('lpep_pickup_datetime').alias('pickup_datetime'),
    col('lpep_dropoff_datetime').alias('dropoff_datetime')
)
green_df = green_df.withColumn('service_type', lit('green'))
```

**2. Load Yellow Taxi Data**
```python
yellow_df = spark.read.parquet(input_yellow)

# Normalize column names
yellow_df = yellow_df.select(
    col('tpep_pickup_datetime').alias('pickup_datetime'),
    col('tpep_dropoff_datetime').alias('dropoff_datetime')
)
yellow_df = yellow_df.withColumn('service_type', lit('yellow'))
```

**3. Schema-Safe Union**
```python
# Find common columns between both DataFrames
common_columns = [col for col in green_df.columns if col in set(yellow_df.columns)]

# Union only on shared columns to avoid schema mismatch errors
trip_df = green_df.select(common_columns).union(yellow_df.select(common_columns))
```

**4. SQL Report — Trips per Service Type in 2020**
```sql
SELECT
    service_type,
    COUNT(*) AS total_trips
FROM trips
WHERE pickup_datetime >= '2020-01-01'
GROUP BY service_type
```

**5. Write Output**
```python
report_df.write.parquet(output, mode="overwrite")
```

---

### Output Schema

| Column | Type | Description |
|---|---|---|
| `service_type` | String | `green` or `yellow` |
| `total_trips` | Long | Total trip count for 2020 |

**Example output:**
```
+------------+-----------+
|service_type|total_trips|
+------------+-----------+
|       green|    1234567|
|      yellow|    9876543|
+------------+-----------+
```

---

## 💡 Key Design Decisions

| Decision | Reason |
|---|---|
| `--deploy-mode cluster` | Driver runs on EMR master node, not local machine — safer for production |
| `--auto-terminate` | Cluster shuts down after job — avoids paying for idle EC2 instances |
| Schema-safe union | Green and Yellow taxis have different column names — renaming before union prevents errors |
| `SparkSession` as context manager | Ensures session closes cleanly even on failure |
| Wildcard S3 paths `/*/*` | Reads all partitioned subdirectories recursively in one call |
| `mode="overwrite"` | Allows re-running the job without manual cleanup of previous output |

---

## 🛑 Common Errors & Fixes

| Error | Cause | Fix |
|---|---|---|
| `An error occurred (ValidationException)` | Default EMR roles missing | Run `aws emr create-default-roles` |
| `NoSuchKey` on S3 path | Script or data not uploaded | Re-run `aws s3 cp` commands |
| `AnalysisException: column not found` | Schema mismatch on union | Common columns logic handles this — check column names in both datasets |
| Step status `FAILED` | Job error | Check logs at `s3://nyc-taxi-pyspark-de-bootcamp/logs/` |
| `AccessDenied` on S3 | IAM permissions | Ensure EMR EC2 role has `s3:GetObject` and `s3:PutObject` on the bucket |

---

## 💰 Cost Estimate

| Resource | Type | Count | Approx Cost |
|---|---|---|---|
| Master node | m5.xlarge | 1 | ~$0.19/hr |
| Core nodes | m5.xlarge | 2 | ~$0.38/hr |
| EMR surcharge | — | — | ~$0.17/hr |
| **Total** | | | **~$0.74/hr** |

> Since `--auto-terminate` is enabled, the cluster only runs for the duration of the job. A typical job on this dataset takes 5–15 minutes, costing less than **$0.20 per run**.

---

## 🖥️ Monitoring & Logs

| Location | What You Find |
|---|---|
| AWS Console → EMR → Steps | Step status: Pending / Running / Completed / Failed |
| `s3://nyc-taxi-pyspark-de-bootcamp/logs/` | Full cluster and step logs |
| `s3://.../logs/<cluster-id>/steps/<step-id>/stdout` | `print()` output from your PySpark script |
| `s3://.../logs/<cluster-id>/steps/<step-id>/stderr` | Error traces and Spark logs |

---

## 🔮 Next Steps

- **Add Apache Airflow** to trigger `create_aws_emr.sh` on a schedule using `EmrCreateJobFlowOperator`
- **Extend the report** — add revenue, distance, and tip metrics to the SQL query
- **Partition the output** by `service_type` for faster downstream queries
- **Add data quality checks** before writing using Great Expectations
- **Connect to AWS Athena** to query the output Parquet report with SQL directly from S3
- **Visualize in Power BI** by connecting to Athena or loading the output Parquet files
