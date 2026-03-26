"""EMR Spark job for generating taxi analytics reports.

This script reads green and yellow taxi parquet datasets, normalizes the
schemas, combines both services into one trip dataset, and writes a group of
analytics reports back to parquet output directories.
"""

import argparse

from pyspark.sql import SparkSession, functions as f
from pyspark.sql.functions import (
    avg,
    col,
    coalesce,
    count,
    date_format,
    hour,
    lit,
    round,
    sum,
    unix_timestamp,
    when,
    year,
)


def report(input_green: str, input_yellow: str, input_zones: str ,output: str) -> None:
    """Build service-level and operational reports from NYC taxi trip data.

    Args:
        input_green: Path to the green taxi parquet dataset.
        input_yellow: Path to the yellow taxi parquet dataset.
        output: Base path where parquet reports will be written.
    """
    try:
        with SparkSession.builder.appName("spark_cluster").getOrCreate() as spark:
            spark.sparkContext.setLogLevel("INFO")
            output_base = output.rstrip("/")

            # Select the shared business columns and rename service-specific
            # timestamps to a common schema so both datasets can be unioned.
            selected_columns = [
                "pickup_datetime",
                "dropoff_datetime",
                "payment_type",
                "RatecodeID",
                "total_amount",
                "fare_amount",
                "tip_amount",
                "tolls_amount",
                "congestion_surcharge",
                "trip_distance",
                "PULocationID",
                "DOLocationID",
                "service_type",
            ]

            print(f"Loading green taxi data from: {input_green}")
            green_df = spark.read.parquet(input_green)
            print(f"Green data loaded: {green_df.count()} rows")

            green_df = green_df.select(
                col("lpep_pickup_datetime").alias("pickup_datetime"),
                col("lpep_dropoff_datetime").alias("dropoff_datetime"),
                col("payment_type"),
                col("RatecodeID"),
                col("total_amount"),
                col("fare_amount"),
                col("tip_amount"),
                col("tolls_amount"),
                col("congestion_surcharge"),
                col("trip_distance"),
                col("PULocationID"),
                col("DOLocationID"),
                lit("green").alias("service_type"),
            )

            print(f"Loading yellow taxi data from: {input_yellow}")
            yellow_df = spark.read.parquet(input_yellow)
            print(f"Yellow data loaded: {yellow_df.count()} rows")

            yellow_df = yellow_df.select(
                col("tpep_pickup_datetime").alias("pickup_datetime"),
                col("tpep_dropoff_datetime").alias("dropoff_datetime"),
                col("payment_type"),
                col("RatecodeID"),
                col("total_amount"),
                col("fare_amount"),
                col("tip_amount"),
                col("tolls_amount"),
                col("congestion_surcharge"),
                col("trip_distance"),
                col("PULocationID"),
                col("DOLocationID"),
                lit("yellow").alias("service_type"),
            )

            print("Merging normalized datasets...")
            trip_df = green_df.select(selected_columns).unionByName(
                yellow_df.select(selected_columns)
            )
            print(f"Merged dataframe rows: {trip_df.count()}")

            # Fill missing categorical values before building downstream
            # reports, and remove invalid rate codes used as placeholders.
            trip_df = trip_df.withColumn(
                "payment_type", coalesce(col("payment_type"), lit(0))
            )
            trip_df = trip_df.filter(col("RatecodeID") != 99)
            
            # read zones lookup
            zones_df = spark.read.csv(input_zones, header=True)

            trip_df.createOrReplaceTempView("trips")

            # Count total trips per service type beginning in 2020.
            print("Running SQL query to count total trips per service type in 2020...")
            trips_per_service_type_report = spark.sql(
                """
                SELECT
                    service_type,
                    COUNT(*) AS total_trips
                FROM trips
                WHERE pickup_datetime >= '2020-01-01'
                GROUP BY service_type
                """
            )
            print(f"Writing output to: {output_base}")
            trips_per_service_type_report.write.parquet(
                f"{output_base}/trips_per_service_type_report", mode="overwrite"
            )

            # Revenue and fare metrics show where each service earns money and
            # how rider payment behavior changes trip economics.
            fare_analysis = (
                trip_df.groupBy("service_type", "payment_type")
                .agg(
                    round(sum("total_amount") / 1000, 2).alias("total_revenue_k"),
                    round(avg("fare_amount"), 2).alias("avg_fare"),
                    round(avg("tip_amount"), 2).alias("avg_tip"),
                    round(avg("tolls_amount"), 2).alias("avg_tolls"),
                    round(avg("congestion_surcharge"), 2).alias("avg_congestion"),
                    count("*").alias("total_trips"),
                )
                .orderBy("service_type", "payment_type")
            )
            fare_analysis.write.parquet(
                f"{output_base}/fare_analysis_report", mode="overwrite"
            )

            # Year-over-year metrics give a simple trend view for trip volume,
            # revenue, and trip distance across services.
            yoy_df = (
                trip_df.groupBy(year("pickup_datetime").alias("year"), "service_type")
                .agg(
                    count("*").alias("total_trips"),
                    round(sum("total_amount"), 2).alias("total_revenue"),
                    round(avg("trip_distance"), 2).alias("avg_distance"),
                )
                .orderBy("year")
            )
            yoy_df.write.parquet(
                f"{output_base}/yoy_trip_volume_report", mode="overwrite"
            )

            # Derive trip duration and speed so we can compare efficiency by
            # pickup zone after filtering out clearly bad records.
            trip_df = trip_df.withColumn(
                "trip_duration_min",
                round(
                    (
                        unix_timestamp("dropoff_datetime")
                        - unix_timestamp("pickup_datetime")
                    )
                    / 60,
                    2,
                ),
            ).withColumn(
                "avg_speed_mph",
                round(col("trip_distance") / (col("trip_duration_min") / 60), 2),
            )

            trip_df = trip_df.filter(
                (col("trip_duration_min") > 0)
                & (col("trip_duration_min") < 300)
                & (col("avg_speed_mph") < 100)
            )

            speed_by_zone = (
                trip_df.groupBy("PULocationID")
                .agg(
                    round(avg("avg_speed_mph"), 2).alias("avg_speed"),
                    round(avg("trip_duration_min"), 2).alias("avg_duration"),
                    count("*").alias("trip_count"),
                )
                .orderBy("avg_speed")
            )
            speed_by_zone.write.parquet(
                f"{output_base}/speed_by_zone_report", mode="overwrite"
            )

            # Top pickup and dropoff zones reveal where taxi activity and
            # revenue are concentrated in the network.
            top_pickup = (
                trip_df.groupBy("PULocationID")
                .agg(
                    count("*").alias("pickup_count"),
                    round(avg("total_amount"), 2).alias("avg_revenue_per_trip"),
                )
                .orderBy("pickup_count", ascending=False)
            )
            # join with zones table to get location names
            top_pickup = top_pickup.join(zones_df, top_pickup.PULocationID == zones_df.LocationID, "left")
            top_pickup.write.parquet(
                f"{output_base}/top_pickup_zones_report", mode="overwrite"
            )

            top_dropoff = (
                trip_df.groupBy("DOLocationID")
                .agg(
                    count("*").alias("dropoff_count"),
                    round(sum("total_amount"), 2).alias("total_revenue"),
                )
                .orderBy("total_revenue", ascending=False)
            )
            # join with zones table to get location names
            top_dropoff = top_dropoff.join(zones_df, top_dropoff.DOLocationID == zones_df.LocationID, "left")
            top_dropoff.write.parquet(
                f"{output_base}/top_dropoff_zones_report", mode="overwrite"
            )

            # Compare customer behavior across payment methods.
            payment_df = (
                trip_df.groupBy("payment_type", "service_type")
                .agg(
                    count("*").alias("trip_count"),
                    round(avg("tip_amount"), 2).alias("avg_tip"),
                    round(avg("total_amount"), 2).alias("avg_total"),
                    round(avg("trip_distance"), 2).alias("avg_distance"),
                )
                .orderBy("payment_type")
            )
            payment_df.write.parquet(
                f"{output_base}/payment_behavior_report", mode="overwrite"
            )

            # Hour-of-day and weekday summaries help identify busy, profitable
            # operating windows for each service.
            time_df = (
                trip_df.withColumn("hour", hour("pickup_datetime"))
                .withColumn("day_of_week", date_format("pickup_datetime", "EEEE"))
                .withColumn("year", year("pickup_datetime"))
            )

            hourly_patterns = (
                time_df.groupBy("hour", "day_of_week")
                .agg(
                    count("*").alias("trip_count"),
                    round(avg("total_amount"), 2).alias("avg_revenue"),
                    round(avg("tip_amount"), 2).alias("avg_tip"),
                )
                .orderBy("day_of_week", "hour")
            )
            hourly_patterns.write.parquet(
                f"{output_base}/hourly_patterns_report", mode="overwrite"
            )

            # Rate codes capture special trip types such as airport rides and
            # make it easy to compare them with the standard fare population.
            ratecode_df = (
                trip_df.groupBy("RatecodeID")
                .agg(
                    count("*").alias("trip_count"),
                    round(avg("fare_amount"), 2).alias("avg_fare"),
                    round(avg("trip_distance"), 2).alias("avg_distance"),
                    round(avg("tip_amount"), 2).alias("avg_tip"),
                    round(sum("total_amount") / 1000, 2).alias("total_revenue_k"),
                )
                .orderBy("RatecodeID")
            )
            ratecode_df.write.parquet(
                f"{output_base}/ratecode_analysis_report", mode="overwrite"
            )

            # Split the data into broad policy eras to estimate the impact of
            # congestion surcharges on total rider cost.
            congestion_df = (
                trip_df.withColumn(
                    "era",
                    when(year("pickup_datetime") < 2019, "Pre-Congestion")
                    .when(year("pickup_datetime") == 2019, "Transition")
                    .otherwise("Post-Congestion"),
                )
                .groupBy("era", "service_type")
                .agg(
                    count("*").alias("trip_count"),
                    round(avg("total_amount"), 2).alias("avg_total"),
                    round(avg("fare_amount"), 2).alias("avg_fare"),
                    round(avg("congestion_surcharge"), 2).alias(
                        "avg_congestion_charge"
                    ),
                )
                .orderBy("era")
            )
            congestion_df.write.parquet(
                f"{output_base}/congestion_impact_report", mode="overwrite"
            )

            print("Job completed successfully!")

    except Exception as e:
        print(f"ERROR in report function: {str(e)}")
        import traceback

        traceback.print_exc()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform taxi data with Spark")
    parser.add_argument(
        "--input_green",
        type=str,
        required=True,
        help="Path to the input green taxi parquet directory",
    )
    parser.add_argument(
        "--input_yellow",
        type=str,
        required=True,
        help="Path to the input yellow taxi parquet directory",
    )
    parser.add_argument(
        "--input_zones",
        type=str,
        required=True,
        help="Path to the input zones taxi csv directory",
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Base path for the output parquet reports",
    )
    args = parser.parse_args()

    report(args.input_green, args.input_yellow, args.input_zones, args.output)
