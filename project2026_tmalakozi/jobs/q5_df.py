import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, avg, round as spark_round, lower, coalesce, lit

def main():
    parser = argparse.ArgumentParser(description="Q5: Airports and Borough Flows (DataFrame API)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q5_DF_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    # 1. Βασικό Φιλτράρισμα (Ημέρες 12,13,14 | Ώρες 7,8,9,10)
    filtered_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("total_amount") > 0)
    )

    # Καθαρισμός Airport_fee (αν είναι null το κάνουμε 0)
    if "Airport_fee" in filtered_trips.columns:
        filtered_trips = filtered_trips.withColumn("Airport_fee", coalesce(col("Airport_fee"), lit(0.0)))
    else:
        filtered_trips = filtered_trips.withColumn("Airport_fee", lit(0.0))

    # 2. Προετοιμασία Πινάκων Περιοχών για τη Διπλή Συνένωση
    pu_zones = zones_df.select(
        col("LocationID").alias("pu_id"),
        col("Borough").alias("pu_borough"),
        col("Zone").alias("pu_zone")
    )
    do_zones = zones_df.select(
        col("LocationID").alias("do_id"),
        col("Borough").alias("do_borough"),
        col("Zone").alias("do_zone")
    )

    # 3. Διπλή Συνένωση (Double Join)
    joined_df = filtered_trips.join(pu_zones, filtered_trips.pu_location_id == pu_zones.pu_id, "inner") \
                              .join(do_zones, filtered_trips.do_location_id == do_zones.do_id, "inner")

    # 4. Λογική Αναγνώρισης Αεροδρομίου (Ονομασία, Borough EWR ή Χρέωση)
    def is_airport_logic(borough_col, zone_col):
        return (lower(col(zone_col)).rlike("jfk|laguardia|newark|airport")) | (col(borough_col) == "EWR")

    processed_df = joined_df.withColumn(
        "pu_is_airport", when(is_airport_logic("pu_borough", "pu_zone"), 1).otherwise(0)
    ).withColumn(
        "do_is_airport", when(is_airport_logic("do_borough", "do_zone"), 1).otherwise(0)
    ).withColumn(
        "is_airport_trip", 
        when((col("pu_is_airport") == 1) | (col("do_is_airport") == 1) | (col("Airport_fee") > 0), 1).otherwise(0)
    )

    total_personal_trips = processed_df.count()

    # 5. Υπολογισμός Ροών (Borough to Borough)
    flows_df = processed_df.groupBy("pu_borough", "do_borough").agg(
        count("*").alias("trips"),
        spark_round((count("*") / total_personal_trips) * 100, 2).alias("trip_share_percent"),
        spark_round(sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        spark_round((sum("is_airport_trip") / count("*")) * 100, 2).alias("airport_trip_share_percent")
    ).orderBy(col("trips").desc()).limit(17) # Κρατάμε τα top-17 (ΑΜ)

    # 6. Μετρικές μόνο για Διαδρομές Αεροδρομίου (Top-K Routes pu_zone -> do_zone)
    airport_trips_df = processed_df.filter(col("is_airport_trip") == 1)
    
    top_airport_routes = airport_trips_df.groupBy("pu_zone", "do_zone").agg(
        count("*").alias("trips"),
        spark_round(avg("Airport_fee"), 2).alias("avg_airport_fee"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance")
    ).orderBy(col("trips").desc()).limit(17)

    # 7. Γενική Σύγκριση (Airport vs Non-Airport) για την αναφορά
    comparison_df = processed_df.groupBy("is_airport_trip").agg(
        count("*").alias("trips"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount")
    )

    # Φυσικό Σχέδιο Εκτέλεσης
    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (ΚΥΡΙΑ ΥΛΟΠΟΙΗΣΗ DATAFRAME API)")
    print("="*50)
    flows_df.explain("formatted")

    # --- ΑΠΟΘΗΚΕΥΣΗ ---
    metrics = {
        "total_trips_in_window": total_personal_trips,
        "airport_vs_non_airport_comparison": [row.asDict() for row in comparison_df.collect()],
        "top_17_borough_flows": [row.asDict() for row in flows_df.collect()],
        "top_17_airport_routes": [row.asDict() for row in top_airport_routes.collect()],
        "execution_time_seconds": round(time.time() - start_time, 2)
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q5_df_metrics"
    
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    print("=== METRICS JSON ===")
    print(json_string)

    spark.stop()

if __name__ == "__main__":
    main()
