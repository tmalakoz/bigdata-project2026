import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, when, round as spark_round, coalesce, lit
)

def main():
    parser = argparse.ArgumentParser(description="Q3: Revenue vs Efficiency (DataFrame API)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q3_DF_2121247").getOrCreate()
    start_time = time.time()

    # 1. Φόρτωση δεδομένων
    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    # 2. Φιλτράρισμα βάσει ΑΜ και κανόνων
    # Ημέρες: 12, 13, 14 | Ώρες: 7, 8, 9, 10
    filtered_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )

    # 3. Δημιουργία Distance Buckets
    bucketed_df = filtered_trips.withColumn("distance_bucket",
        when(col("trip_distance") < 1, "very_short")
        .when((col("trip_distance") >= 1) & (col("trip_distance") < 3), "short")
        .when((col("trip_distance") >= 3) & (col("trip_distance") < 10), "medium")
        .otherwise("long")
    )

    # Αντικατάσταση τυχόν null surcharges με 0
    surcharge_cols = ["extra", "mta_tax", "tolls_amount", "improvement_surcharge", "congestion_surcharge", "Airport_fee"]
    for c in surcharge_cols:
        if c in bucketed_df.columns:
            bucketed_df = bucketed_df.withColumn(c, coalesce(col(c), lit(0.0)))
        else:
            bucketed_df = bucketed_df.withColumn(c, lit(0.0))

    bucketed_df = bucketed_df.withColumn(
        "total_surcharges", 
        col("extra") + col("mta_tax") + col("tolls_amount") + col("improvement_surcharge") + col("congestion_surcharge") + col("Airport_fee")
    )

    # 4. Συνένωση (Join) με τις περιοχές Επιβίβασης
    joined_df = bucketed_df.join(zones_df, bucketed_df.pu_location_id == zones_df.LocationID, "inner") \
                           .withColumnRenamed("Borough", "pickup_borough") \
                           .withColumnRenamed("Zone", "pickup_zone")

    # 5. Υπολογισμός Metrics ανά (Borough, Zone, Distance_Bucket)
    agg_df = joined_df.groupBy("pickup_borough", "pickup_zone", "distance_bucket").agg(
        count("*").alias("trips"),
        spark_round(sum("total_amount"), 2).alias("total_revenue"),
        spark_round(avg("total_amount"), 2).alias("avg_revenue_per_trip"),
        spark_round(sum("total_amount") / sum("trip_distance"), 2).alias("revenue_per_mile"),
        spark_round(sum("total_amount") / sum("duration_minutes"), 2).alias("revenue_per_minute"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
        spark_round(avg("tip_amount"), 2).alias("avg_tip_amount"),
        spark_round(sum("total_surcharges") / sum("total_amount"), 4).alias("surcharge_share")
    )

    # 6. Υπολογισμός Ορίου Υποστήριξης (Support Threshold)
    total_personal_trips = filtered_trips.count()
    one_percent = int(0.01 * total_personal_trips)
    support_threshold = max(10, min(50, one_percent))

    print(f"\n--- ΥΠΟΛΟΓΙΣΜΟΣ ΟΡΙΟΥ ΥΠΟΣΤΗΡΙΞΗΣ ---")
    print(f"Συνολικά Ταξίδια: {total_personal_trips}")
    print(f"1% των Ταξιδιών: {one_percent}")
    print(f"Τελικό Όριο (Threshold): {support_threshold} trips\n")

    # Για το ερώτημα (β): Ακραίο παράδειγμα αποδοτικότητας χωρίς φίλτρο
    crazy_efficient_no_filter = agg_df.orderBy(col("revenue_per_mile").desc()).limit(1)

    # 7. Εφαρμογή Ορίου
    valid_agg_df = agg_df.filter(col("trips") >= support_threshold)

    # 8. Εξαγωγή των Top 17
    K = 17
    top_revenue = valid_agg_df.orderBy(col("total_revenue").desc()).limit(K)
    top_rpmile = valid_agg_df.orderBy(col("revenue_per_mile").desc()).limit(K)
    top_rpmin = valid_agg_df.orderBy(col("revenue_per_minute").desc()).limit(K)

    # --- ΑΠΟΘΗΚΕΥΣΗ JSON ---
    metrics = {
        "support_threshold_used": support_threshold,
        "crazy_efficient_example_no_threshold": [row.asDict() for row in crazy_efficient_no_filter.collect()],
        "top_17_by_total_revenue": [row.asDict() for row in top_revenue.collect()],
        "top_17_by_revenue_per_mile": [row.asDict() for row in top_rpmile.collect()],
        "top_17_by_revenue_per_minute": [row.asDict() for row in top_rpmin.collect()]
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q3_df_metrics"
    
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
