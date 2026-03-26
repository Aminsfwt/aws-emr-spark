# 🚕 NYC Taxi Analytics — AWS EMR PySpark Pipeline

A production-grade data engineering pipeline that processes **NYC Green and Yellow taxi trip data ** using **Apache Spark on AWS EMR**, generating **10 analytical reports** written as Parquet to **Amazon S3**.

---

## 📐 Architecture Overview

```
Amazon S3 (Input)
        │
        ├── processed/green/*/*      → Green Taxi Parquet files
        └── processed/yellow/*/*     → Yellow Taxi Parquet files
                │
                ▼
┌──────────────────────────────────────────┐
│            AWS EMR Cluster               │
│                                          │
│  Master : 1x m5.xlarge  (Driver)        │
│  Core   : 2x m5.xlarge  (Executors)     │
│                                          │
│  emr_deployment.py                       │
│  ├── Load & normalize green + yellow     │
│  ├── Schema-safe union → trips_df        │
│  ├── Data quality filters                │
│  └── Generate 10 analytical reports     │
└──────────────────────────────────────────┘
                │
                ▼
Amazon S3 (Output)
└── report/nyc_taxi/
    ├── trips_per_service_type_report/
    ├── fare_analysis_report/
    ├── yoy_trip_volume_report/
    ├── speed_by_zone_report/
    ├── top_pickup_zones_report/
    ├── top_dropoff_zones_report/
    ├── payment_behavior_report/
    ├── hourly_patterns_report/
    ├── ratecode_analysis_report/
    └── congestion_impact_report/
```

---

## 🗂️ Project Structure

```
project/
├── create_aws_emr.sh       # AWS CLI command — creates cluster and submits job
├── emr_deployment.py       # PySpark ETL job — all transformation and report logic
└── README.md
```

---

## ☁️ AWS Infrastructure

### EMR Cluster (`create_aws_emr.sh`)

| Property | Value |
|---|---|
| Cluster Name | `nyc-taxi-spark-cluster` |
| EMR Release | `emr-7.12.0` |
| Application | Spark |
| Region | `us-east-1` |
| Auto-terminate | ✅ Yes — shuts down after job completes |
| Deploy Mode | `cluster` — driver runs on master node |

### Instance Groups

| Role | Type | Count | vCPU | RAM |
|---|---|---|---|---|
| Master | `m5.xlarge` | 1 | 4 | 16GB |
| Core | `m5.xlarge` | 2 | 4 | 16GB |

### S3 Bucket Layout

```
s3://nyc-taxi-pyspark-de-bootcamp/
├── logs/                              # EMR cluster and step logs
├── emr_deployment.py                  # PySpark script (upload before running)
├── raw/
    └── taxi_zones/*                   # taxi zones name parquet
├── processed/
│   ├── green/*/*                      # Green taxi Parquet — partitioned by year/month
│   └── yellow/*/*                     # Yellow taxi Parquet — partitioned by year/month
└── report/
    └── nyc_taxi/                      # All 10 output reports written here
```

---

## ⚙️ Prerequisites

- AWS CLI installed and configured (`aws configure`)
- IAM permissions: `AmazonEMRFullAccess`, `AmazonS3FullAccess`, `AmazonEC2FullAccess`
- Default EMR roles exist (run `aws emr create-default-roles` if not)
- Input Parquet files uploaded to S3
- Python 3.8+ (for local testing only)
- PySpark 3.5+ (matches EMR 7.x)

---

## 🚀 How to Run

### Step 1 — Upload the PySpark Script to S3

```bash
aws s3 cp emr_deployment.py s3://nyc-taxi-pyspark-de-bootcamp/emr_deployment.py
```

### Step 2 — Create the Cluster and Submit the Job

```bash
bash create_aws_emr.sh
```

This single command creates the EMR cluster, installs Spark, submits the job as a Spark step, and auto-terminates the cluster when done.

### Step 3 — Monitor the Job

**AWS Console:**
```
AWS Console → EMR → Clusters → nyc-taxi-spark-cluster → Steps tab
```

**AWS CLI:**
```bash
# Get the cluster ID
aws emr list-clusters --active --region us-east-1

# Watch step status
aws emr describe-step \
    --cluster-id <cluster-id> \
    --step-id <step-id> \
    --region us-east-1
```

### Step 4 — Verify Outputs

```bash
aws s3 ls s3://nyc-taxi-pyspark-de-bootcamp/report/nyc_taxi/ --recursive
```

---

## 🔄 Job Logic — `emr_deployment.py`

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--input_green` | ✅ | S3 path to green taxi Parquet files |
| `--input_yellow` | ✅ | S3 path to yellow taxi Parquet files |
| `--input_zones` | ✅ | S3 path to taxi zones lookup Parquet files |
| `--output` | ✅ | S3 base path for all output reports |

---

### Processing Pipeline

```
Step 1  Load green Parquet    → rename lpep_ columns → add service_type='green'
Step 2  Load yellow Parquet   → rename tpep_ columns → add service_type='yellow'
Step 3  Find common columns   → schema-safe union → trips_df
Step 4  Data quality filters  → null payment_type → 0, remove RatecodeID=99
Step 5  Generate 10 reports   → write each as Parquet to S3
```

---

## 📊 Reports Generated

### Report 1 — Trips per Service Type (2020+)
**Answers:** How many trips did green vs yellow taxis make from 2020 onwards?

| Column | Description |
|---|---|
| `service_type` | `green` or `yellow` |
| `total_trips` | Total trip count from 2020-01-01 |

---

### Report 2 — Revenue & Fare Analysis
**Answers:** Where does the money come from across service types and payment methods?

| Column | Description |
|---|---|
| `service_type` | green / yellow |
| `payment_type` | Payment method code |
| `total_revenue_K` | Total revenue in thousands |
| `avg_fare` | Average base fare |
| `avg_tip` | Average tip amount |
| `avg_tolls` | Average tolls |
| `avg_congestion` | Average congestion surcharge |
| `total_trips` | Trip count |

---

### Report 3 — Year-over-Year Trip Volume (2009–2026)
**Answers:** How did taxi demand change over 17 years? (COVID crash visible in 2020)

| Column | Description |
|---|---|
| `year` | Trip year |
| `service_type` | green / yellow |
| `total_trips` | Annual trip count |
| `total_revenue` | Annual revenue |
| `avg_distance` | Average trip distance |

---

### Report 4 — Trip Duration & Speed by Zone
**Answers:** Which pickup zones are most congested?

| Column | Description |
|---|---|
| `PULocationID` | Pickup zone ID |
| `avg_speed` | Average speed in mph |
| `avg_duration` | Average trip duration in minutes |
| `trip_count` | Number of trips from this zone |

**Data quality filters applied:**
- `trip_duration_min` between 0 and 300 minutes
- `avg_speed_mph` under 100 mph

---

### Report 5 — Top Pickup Zones
**Answers:** Which zones generate the most pickups?

| Column | Description |
|---|---|
| `PULocationID` | Pickup zone ID |
| `pickup_count` | Total pickups from this zone |
| `avg_revenue_per_trip` | Average fare per trip |

---

### Report 6 — Top Dropoff Zones
**Answers:** Which dropoff zones generate the most revenue?

| Column | Description |
|---|---|
| `DOLocationID` | Dropoff zone ID |
| `dropoff_count` | Total dropoffs to this zone |
| `total_revenue` | Total revenue from dropoffs here |

> 💡 Join Reports 5 & 6 with the [NYC Taxi Zone Lookup CSV](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to get human-readable neighborhood names.

---

### Report 7 — Payment Type Behavior
**Answers:** How do cash vs card riders behave differently?

| Column | Description |
|---|---|
| `payment_type` | 0=Voided, 1=Card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown |
| `service_type` | green / yellow |
| `trip_count` | Number of trips |
| `avg_tip` | Average tip (card trips tip more) |
| `avg_total` | Average total fare |
| `avg_distance` | Average trip distance |

---

### Report 8 — Peak Hours & Day-of-Week Patterns
**Answers:** When are taxis busiest and most profitable?

| Column | Description |
|---|---|
| `hour` | Hour of day (0–23) |
| `day_of_week` | Full day name (Monday–Sunday) |
| `trip_count` | Number of trips in this slot |
| `avg_revenue` | Average revenue per trip |
| `avg_tip` | Average tip per trip |

---

### Report 9 — Rate Code Analysis
**Answers:** How do airport trips compare to standard trips?

| Column | Description |
|---|---|
| `RatecodeID` | 1=Standard, 2=JFK, 3=Newark, 4=Nassau, 5=Negotiated, 6=Group |
| `trip_count` | Number of trips |
| `avg_fare` | Average base fare |
| `avg_distance` | Average distance |
| `avg_tip` | Average tip |
| `total_revenue_K` | Total revenue in thousands |

---

### Report 10 — Congestion Surcharge Impact
**Answers:** How did NYC's 2019 congestion pricing law affect riders and trip volumes?

| Column | Description |
|---|---|
| `era` | Pre-Congestion / Transition / Post-Congestion |
| `service_type` | green / yellow |
| `trip_count` | Number of trips in that era |
| `avg_total` | Average total fare |
| `avg_fare` | Average base fare |
| `avg_congestion_charge` | Average congestion surcharge added |

---



---

## 💰 Cost Estimate

| Resource | Type | Count | Rate |
|---|---|---|---|
| Master node | m5.xlarge | 1 | ~$0.192/hr |
| Core nodes | m5.xlarge | 2 | ~$0.384/hr |
| EMR surcharge | — | — | ~$0.17/hr |
| **Total** | | | **~$0.75/hr** |

With `--auto-terminate`, a typical job run costs under **$0.20**.

---

## 🖥️ Logs & Monitoring

| Location | Contents |
|---|---|
| `s3://.../logs/<cluster-id>/steps/<step-id>/stdout` | All `print()` output from the job |
| `s3://.../logs/<cluster-id>/steps/<step-id>/stderr` | Spark logs and Python tracebacks |
| AWS Console → EMR → Steps | Visual status for each step |

---

## 🔮 Next Steps

- **Airflow orchestration** — trigger `create_aws_emr.sh` on a schedule using `EmrCreateJobFlowOperator`
- **AWS Athena** — register output Parquet as Athena tables for ad-hoc SQL queries
- **Power BI / Tableau** — connect directly to S3 Parquet via Athena for live dashboards
- **Great Expectations** — add data quality checks before writing reports
