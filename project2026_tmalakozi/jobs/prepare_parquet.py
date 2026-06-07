import argparse
import time
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import (
    col, to_timestamp, to_date, dayofmonth, hour, year, unix_timestamp, min, max
)

def main():
    # Λήψη ορισμάτων γραμμής εντολών όπως ζητάει η Ενότητα 10
    parser = argparse.ArgumentParser(description="Prepare Taxi Data to Parquet")
    parser.add_argument("--student-id", type=int, required=True, help="Αριθμός Μητρώου")
    parser.add_argument("--input-base", type=str, required=True, help="Βασική διαδρομή εισόδου στο HDFS (π.χ. hdfs://.../data)")
    parser.add_argument("--output-base", type=str, required=True, help="Βασική διαδρομή εξόδου στο HDFS")
    args = parser.parse_args()

    # Δημιουργία SparkSession
    spark = SparkSession.builder \
        .appName(f"Project2026_PrepareParquet_{args.student_id}") \
        .getOrCreate()

    start_time = time.time()
    metrics = {}

    # --- 1. ΟΡΙΣΜΟΣ ΡΗΤΩΝ ΣΧΗΜΑΤΩΝ (Ενότητα 1.1 & 1.2) ---
    
    schema_2015 = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True), # String αρχικά για να το κάνουμε parse σωστά
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

    schema_2024 = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", StringType(), True),
        StructField("tpep_dropoff_datetime", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])

    schema_lookup = StructType([
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    # --- 2. ΕΠΕΞΕΡΓΑΣΙΑ 2015 ---
    print("Επεξεργασία 2015...")
    df_2015_raw = spark.read.csv(f"{args.input_base}/yellow_tripdata_2015.csv", header=True, schema=schema_2015)
    raw_count_2015 = df_2015_raw.count()

    # Μετατροπή Timestamp (yyyy-MM-dd HH:mm:ss) και μετονομασία
    df_2015 = df_2015_raw \
        .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumnRenamed("VendorID", "vendor_id") \
        .withColumnRenamed("RateCodeID", "rate_code_id")

    # Καταγραφή γραμμών με null timestamps
    null_ts_2015 = df_2015.filter(col("pickup_ts").isNull()).count()
    df_2015 = df_2015.filter(col("pickup_ts").isNotNull())

    # Φιλτράρισμα λάθος ετών
    bad_year_2015 = df_2015.filter(year(col("pickup_ts")) != 2015).count()
    df_2015 = df_2015.filter(year(col("pickup_ts")) == 2015)

    # Βοηθητικές στήλες
    df_2015 = df_2015 \
        .withColumn("pickup_date", to_date(col("pickup_ts"))) \
        .withColumn("pickup_day", dayofmonth(col("pickup_ts"))) \
        .withColumn("pickup_hour", hour(col("pickup_ts"))) \
        .withColumn("duration_minutes", (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0) \
        .withColumn("trip_distance_km", col("trip_distance") * 1.60934)

    # Min/Max ts
    ts_stats_2015 = df_2015.select(min("pickup_ts").alias("min_ts"), max("pickup_ts").alias("max_ts")).first()
    
    # Εγγραφή στο HDFS (Partitioned by pickup_hour)
    out_2015 = f"{args.output_base}/data/parquet/yellow_tripdata_2015"
    df_2015.write.mode("overwrite").partitionBy("pickup_hour").parquet(out_2015)
    prep_count_2015 = df_2015.count()

    # --- 3. ΕΠΕΞΕΡΓΑΣΙΑ 2024 ---
    print("Επεξεργασία 2024...")
    df_2024_raw = spark.read.csv(f"{args.input_base}/yellow_tripdata_2024.csv", header=True, schema=schema_2024)
    raw_count_2024 = df_2024_raw.count()

    # Μετατροπή Timestamp (yyyy-MM-dd'T'HH:mm:ss.SSS)
    df_2024 = df_2024_raw \
        .withColumn("pickup_ts", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .withColumn("dropoff_ts", to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
        .withColumnRenamed("VendorID", "vendor_id") \
        .withColumnRenamed("RatecodeID", "rate_code_id") \
        .withColumnRenamed("PULocationID", "pu_location_id") \
        .withColumnRenamed("DOLocationID", "do_location_id") \
        .withColumnRenamed("Airport_fee", "airport_fee")

    # Καταγραφή γραμμών με null timestamps
    null_ts_2024 = df_2024.filter(col("pickup_ts").isNull()).count()
    df_2024 = df_2024.filter(col("pickup_ts").isNotNull())

    # Φιλτράρισμα λάθος ετών
    bad_year_2024 = df_2024.filter(year(col("pickup_ts")) != 2024).count()
    df_2024 = df_2024.filter(year(col("pickup_ts")) == 2024)

    # Βοηθητικές στήλες
    df_2024 = df_2024 \
        .withColumn("pickup_date", to_date(col("pickup_ts"))) \
        .withColumn("pickup_day", dayofmonth(col("pickup_ts"))) \
        .withColumn("pickup_hour", hour(col("pickup_ts"))) \
        .withColumn("duration_minutes", (unix_timestamp(col("dropoff_ts")) - unix_timestamp(col("pickup_ts"))) / 60.0) \
        .withColumn("trip_distance_km", col("trip_distance") * 1.60934)

    ts_stats_2024 = df_2024.select(min("pickup_ts").alias("min_ts"), max("pickup_ts").alias("max_ts")).first()

    # Εγγραφή στο HDFS (Partitioned by pickup_day)
    out_2024 = f"{args.output_base}/data/parquet/yellow_tripdata_2024"
    df_2024.write.mode("overwrite").partitionBy("pickup_day").parquet(out_2024)
    prep_count_2024 = df_2024.count()

    # --- 4. ΕΠΕΞΕΡΓΑΣΙΑ TAXI ZONE LOOKUP ---
    print("Επεξεργασία Zone Lookup...")
    df_lookup = spark.read.csv(f"{args.input_base}/taxi_zone_lookup.csv", header=True, schema=schema_lookup)
    raw_count_lookup = df_lookup.count()
    out_lookup = f"{args.output_base}/data/parquet/taxi_zone_lookup"
    # Χωρίς διαμερισμό σύμφωνα με την εκφώνηση
    df_lookup.write.mode("overwrite").parquet(out_lookup)

    total_time = time.time() - start_time

    # --- 5. ΚΑΤΑΓΡΑΦΗ ΜΕΤΡΙΚΩΝ (Ενότητα 4) ---
    metrics = {
        "execution_time_seconds": round(total_time, 2),
        "yellow_tripdata_2015": {
            "raw_rows": raw_count_2015,
            "prepared_rows": prep_count_2015,
            "null_pickup_ts_rejected": null_ts_2015,
            "bad_year_rejected": bad_year_2015,
            "min_pickup_ts": str(ts_stats_2015["min_ts"]),
            "max_pickup_ts": str(ts_stats_2015["max_ts"]),
            "partition_columns": ["pickup_hour"],
            "output_path": out_2015
        },
        "yellow_tripdata_2024": {
            "raw_rows": raw_count_2024,
            "prepared_rows": prep_count_2024,
            "null_pickup_ts_rejected": null_ts_2024,
            "bad_year_rejected": bad_year_2024,
            "min_pickup_ts": str(ts_stats_2024["min_ts"]),
            "max_pickup_ts": str(ts_stats_2024["max_ts"]),
            "partition_columns": ["pickup_day"],
            "output_path": out_2024
        },
        "taxi_zone_lookup": {
            "raw_rows": raw_count_lookup,
            "prepared_rows": raw_count_lookup, # Δεν φιλτράραμε κάτι εδώ
            "partition_columns": [],
            "output_path": out_lookup
        }
    }

    # Αποθήκευση των μετρικών τοπικά για να τις ανεβάσεις εύκολα ή να τις διαβάσεις
    # Στο HDFS η εντολή hdfs dfs -du -s -h θα σου δώσει το συνολικό μέγεθος.
    os.makedirs("results/metrics", exist_ok=True)
    with open("results/metrics/prepare_parquet_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, ensure_ascii=False)
        
    print("Η προετοιμασία ολοκληρώθηκε επιτυχώς! Τα metrics αποθηκεύτηκαν στο results/metrics/prepare_parquet_metrics.json")
    spark.stop()

if __name__ == "__main__":
    main()
