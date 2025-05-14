import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, substring, concat_ws, to_timestamp,
    dayofweek, lpad, to_date, split
)

# Configurable values from env or external file
JARS_PATH = os.getenv("SPARK_JARS", "/path/to/spark-bigquery.jar,/path/to/gcs-connector.jar")
KEYFILE = os.getenv("GCP_KEYFILE", "/path/to/service_account.json")
GCS_BUCKET = os.getenv("GCS_BUCKET", "your_temp_gcs_bucket")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_DATASET = os.getenv("BQ_DATASET", "flight_delay_analytics")
CSV_FLIGHT = os.getenv("CSV_FLIGHT", "/path/to/Combined_Flights_2022.csv")
CSV_CAUSE = os.getenv("CSV_CAUSE", "/path/to/merged_flights_2022.csv")


# Create Spark session
def create_spark_session(app_name="Flight Delay Loader"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", JARS_PATH) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE) \
        .config("temporaryGcsBucket", GCS_BUCKET) \
        .getOrCreate()

def load_csv(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def preprocess_main_flight_data(df):
    df = df.select(
        'FlightDate', 'Airline', 'Origin', 'Dest', 'CRSDepTime',
        'DepDelayMinutes', 'ArrDelayMinutes', 'Cancelled', 'Diverted', 'OriginState',
        'OriginStateName', 'Distance', 'DepDel15', 'DepTimeBlk', 'ArrTimeBlk', 'OriginCityName'
    ).dropna(subset=['FlightDate', 'Airline', 'Origin', 'CRSDepTime', 'DepDelayMinutes'])

    df = df.withColumn("scheduled_hour", substring("CRSDepTime", 1, 2).cast("int"))
    df = df.withColumn("is_peak_hour", when((col("scheduled_hour").between(7, 10)) | (col("scheduled_hour").between(17, 20)), 1).otherwise(0))
    df = df.withColumn("on_time_flag", when(col("DepDelayMinutes") < 15, 1).otherwise(0))
    df = df.withColumn("delay_bucket", when(col("DepDelayMinutes") < 15, "on_time")
                       .when((col("DepDelayMinutes") < 30), "15-30 min")
                       .when((col("DepDelayMinutes") < 60), "30-60 min")
                       .otherwise(">60 min"))
    df = df.withColumn("cancel_flag", col("Cancelled").cast("int"))
    df = df.withColumn("diverted_flag", col("Diverted").cast("int"))
    df = df.withColumn("CRSDepTime", lpad(col("CRSDepTime").cast("string"), 4, "0"))
    df = df.withColumn("ScheduledDepartureTime", to_timestamp(concat_ws(" ", col("FlightDate"), col("CRSDepTime")), "yyyy-MM-dd HHmm"))
    df = df.withColumn("day_of_week", dayofweek("FlightDate"))

    return df

def preprocess_cause_delay_data(df):
    df = df.withColumnRenamed("FL_DATE", "FlightDate")
    df = df.withColumn("FlightDate", split(col("FlightDate"), " ").getItem(0))
    df = df.withColumn("FlightDate", to_date(col("FlightDate"), "M/d/yyyy"))
    return df

def save_to_bigquery(df, table_name):
    print(f"💾 Saving to BigQuery table: {table_name}")
    df.write.format("bigquery") \
        .option("parentProject", PROJECT_ID) \
        .option("table", f"{PROJECT_ID}.{BQ_DATASET}.{table_name}") \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("overwrite") \
        .save()

def run_static_loader():
    spark = create_spark_session()
    flight_df = load_csv(spark, CSV_FLIGHT)
    flight_df = preprocess_main_flight_data(flight_df)
    save_to_bigquery(flight_df, "raw_flight_delays")

    cause_df = load_csv(spark, CSV_CAUSE)
    cause_df = preprocess_cause_delay_data(cause_df)
    save_to_bigquery(cause_df, "raw_cause_delays")

    print("✅ Both datasets loaded to BigQuery.")
    spark.stop()

if __name__ == "__main__":
    run_static_loader()
