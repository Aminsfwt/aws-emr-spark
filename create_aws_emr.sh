#!/usr/bin/env bash

echo "Starting to Launch an EMR cluster......."

# Launch an EMR cluster, submit the Spark job as a step, and terminate the
# cluster automatically after the step completes.
aws emr create-cluster \
  --name "nyc-taxi-spark-cluster" \
  --release-label emr-7.12.0 \
  --applications Name=Spark \
  --instance-groups '[
    {"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","InstanceCount":1},
    {"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","InstanceCount":2}
  ]' \
  --use-default-roles \
  --log-uri s3://nyc-taxi-pyspark-de-bootcamp/logs/ \
  --auto-terminate \
  --region us-east-1 \
  --steps '[
    {
      "Type": "Spark",
      "Name": "nyc_taxi_job",
      "ActionOnFailure": "CONTINUE",
      "Args": [
        "--deploy-mode", "cluster",
        "s3://nyc-taxi-pyspark-de-bootcamp/emr_deployment.py",
        "--input_green",  "s3://nyc-taxi-pyspark-de-bootcamp/processed/green/*/*",
        "--input_yellow", "s3://nyc-taxi-pyspark-de-bootcamp/processed/yellow/*/*",
        "--input_zones",  "s3://nyc-taxi-pyspark-de-bootcamp/raw/zones/*",
        "--output",       "s3://nyc-taxi-pyspark-de-bootcamp/report/nyc_taxi_service_report"
      ]
    }
  ]'

echo "The Launch Successfully Done"