import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, sum

def main():
    parser = argparse.ArgumentParser(description="Q3: Top Routes using Joins")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q3_2121247").getOrCreate()
    start_time = time.time()

    # 1. Φόρτωση δεδομένων
    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    # 2. Φιλτράρισμα βάσει ΑΜ (Διατηρούμε το χρονικό παράθυρο του Q1 για συνέπεια)
    # Ημέρες: 12, 13, 14 | Ώρες: 7, 8, 9, 10
    filtered_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0)
    )

    # 3. Προετοιμασία των DataFrames για το Join
    # Φτιάχνουμε δύο "εκδοχές" του zones_df, μία για την Επιβίβαση και μία για την Αποβίβαση
    pu_zones = zones_df.select(
        col("LocationID").alias("pu_location_id"),
        col("Zone").alias("Pickup_Zone")
    )
    
    do_zones = zones_df.select(
        col("LocationID").alias("do_location_id"),
        col("Zone").alias("Dropoff_Zone")
    )

    # 4. Εκτέλεση των Joins
    # Συνδέουμε πρώτα το Pickup και μετά το Dropoff
    joined_df = filtered_trips.join(pu_zones, "pu_location_id", "inner") \
                              .join(do_zones, "do_location_id", "inner")

    # Αποκλείουμε διαδρομές που η περιοχή είναι "Unknown" (Άγνωστη)
    joined_df = joined_df.filter(
        (col("Pickup_Zone") != "Unknown") & 
        (col("Dropoff_Zone") != "Unknown")
    )

    # 5. Ομαδοποίηση για να βρούμε τα πιο συχνά δρομολόγια (Top 5 Routes)
    top_routes_df = joined_df.groupBy("Pickup_Zone", "Dropoff_Zone").agg(
        count("*").alias("total_trips"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance_miles"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        spark_round(sum("total_amount"), 2).alias("total_revenue")
    ).orderBy(col("total_trips").desc()).limit(5)

    # Φυσικό Σχέδιο Εκτέλεσης (Για να δούμε το Broadcast Join)
    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (CATALYST EXPLAIN FORMATTED)")
    print("="*50)
    top_routes_df.explain("formatted")

    # --- ΑΠΟΘΗΚΕΥΣΗ ΣΤΟ HDFS ---
    results_list = [row.asDict() for row in top_routes_df.collect()]
    execution_time = time.time() - start_time

    metrics = {
        "execution_time_seconds": round(execution_time, 2),
        "top_5_routes": results_list
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q3_metrics"
    
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    print(f"\nΤο Q3 ολοκληρώθηκε σε {round(execution_time, 2)} δευτερόλεπτα!")
    print("=== METRICS JSON ===")
    print(json_string)

    spark.stop()

if __name__ == "__main__":
    main()
