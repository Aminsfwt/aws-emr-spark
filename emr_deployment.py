import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import col

def report(input_green: str, input_yellow: str, output: str) -> None:
    try:
        with SparkSession.builder.appName("spark_cluster").getOrCreate() as spark:
            spark.sparkContext.setLogLevel("INFO")
            
            # Load green taxi data
            print(f"Loading green taxi data from: {input_green}")
            green_df = spark.read.parquet(input_green)
            print(f"Green data loaded: {green_df.count()} rows")
            print(f"Green columns: {green_df.columns}")
            
            green_df = green_df.select(
                    col('lpep_pickup_datetime').alias('pickup_datetime'), 
                    col('lpep_dropoff_datetime').alias('dropoff_datetime')) 
            green_df = green_df.withColumn('service_type', f.lit('green'))
            print(f"Green columns after transform: {green_df.columns}")
            
            # Load yellow taxi data
            print(f"Loading yellow taxi data from: {input_yellow}")
            yellow_df = spark.read.parquet(input_yellow)
            print(f"Yellow data loaded: {yellow_df.count()} rows")
            print(f"Yellow columns: {yellow_df.columns}")
            
            yellow_df = yellow_df.select(
                    col('tpep_pickup_datetime').alias('pickup_datetime'), 
                    col('tpep_dropoff_datetime').alias('dropoff_datetime')) 
            yellow_df = yellow_df.withColumn('service_type', f.lit('yellow'))
            print(f"Yellow columns after transform: {yellow_df.columns}")

            # Find common columns for a schema-safe union
            common_columns = []
            yellow_columns = set(yellow_df.columns)
            for column in green_df.columns:
                if column in yellow_columns:
                    common_columns.append(column)
            print(f"Common columns: {common_columns}")

            # Union the dataframes
            print("Merging dataframes...")
            trip_df = green_df.select(common_columns) \
                      .union(yellow_df.select(common_columns))
            print(f"Merged dataframe rows: {trip_df.count()}")

            # create a temporary view for SQL queries
            trip_df.createOrReplaceTempView("trips")

            # run a SQL query to count total trips per service type in 2020
            print("Running SQL query...")
            report_df = spark.sql(""" 
                    SELECT 
                        service_type,       
                        COUNT(*) AS total_trips 
                    FROM trips
                    WHERE pickup_datetime >= '2020-01-01'    
                    GROUP BY service_type             
                    """)
            
            result_count = report_df.count()
            print(f"Query result rows: {result_count}")
            report_df.show()
            
            # write output
            print(f"Writing output to: {output}")
            report_df.write.parquet(output, mode="overwrite")  
            print("Job completed successfully!")
            
    except Exception as e:
        print(f"ERROR in report function: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform data")
    parser.add_argument("--input_green", type=str, required=True, help="Path to the input green taxi Parquet directory")
    parser.add_argument("--input_yellow", type=str, required=True, help="Path to the input yellow taxi Parquet directory")
    parser.add_argument("--output", type=str, required=True, help="Path to the output Parquet directory")
    args = parser.parse_args()

    report(args.input_green, args.input_yellow, args.output)



    
