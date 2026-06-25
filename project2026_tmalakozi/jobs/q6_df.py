import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, when, abs, coalesce, lit, round as spark_round

def main():
    parser = argparse.ArgumentParser(description="Q6: Pickups vs Dropoffs Imbalance (DataFrame API)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q6_DF_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    # 1. Βασικό Φιλτράρισμα (Ημέρες 12,13,14 | Ώρες 7,8,9,10)
    valid_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("total_amount") > 0)
    )

    # 2. Σύνολο Pickups
    pickups_df = valid_trips.groupBy(
        col("pickup_hour").alias("pu_hour"), 
        col("pu_location_id").alias("location_id")
    ).agg(count("*").alias("pickups"))

    # 3. Σύνολο Dropoffs (Χρησιμοποιώντας pickup_hour ως κοινή χρονική βάση)
    dropoffs_df = valid_trips.groupBy(
        col("pickup_hour").alias("do_hour"), 
        col("do_location_id").alias("location_id")
    ).agg(count("*").alias("dropoffs"))

    # 4. Full Outer Join
    # Επειδή κάνουμε outer join, πρέπει να προσέξουμε τα null keys
    join_cond = (pickups_df.pu_hour == dropoffs_df.do_hour) & (pickups_df.location_id == dropoffs_df.location_id)
    outer_joined = pickups_df.join(dropoffs_df, join_cond, "full_outer")

    # Ενοποίηση των στηλών κλειδιών και αντικατάσταση κενών με 0
    unified_df = outer_joined.select(
        coalesce(col("pu_hour"), col("do_hour")).alias("pickup_hour"),
        coalesce(pickups_df.location_id, dropoffs_df.location_id).alias("location_id"),
        coalesce(col("pickups"), lit(0)).alias("pickups"),
        coalesce(col("dropoffs"), lit(0)).alias("dropoffs")
    )

    # 5. Υπολογισμός Μετρικών
    metrics_df = unified_df.withColumn("activity", col("pickups") + col("dropoffs")) \
                           .withColumn("net_pickups", col("pickups") - col("dropoffs"))

    metrics_df = metrics_df.withColumn(
        "imbalance_ratio",
        when(col("activity") > 0, col("net_pickups") / col("activity")).otherwise(0.0)
    ).withColumn("abs_imbalance_ratio", abs(col("imbalance_ratio")))

    # 6. Συνένωση με το taxi_zone_lookup
    enriched_df = metrics_df.join(zones_df, metrics_df.location_id == zones_df.LocationID, "left")

    # 7. Εφαρμογή Ορίου Δραστηριότητας
    filtered_df = enriched_df.filter(col("activity") >= 30)

    # 8. Εξαγωγή των Top-17
    top_positive_net = filtered_df.orderBy(col("net_pickups").desc()).limit(17)
    top_negative_net = filtered_df.orderBy(col("net_pickups").asc()).limit(17)
    top_abs_imbalance = filtered_df.orderBy(col("abs_imbalance_ratio").desc()).limit(17)

    # 9. Σύνοψη ανά Ώρα
    hourly_summary = enriched_df.groupBy("pickup_hour").agg(
        sum(abs(col("net_pickups"))).alias("total_abs_net_pickups"),
        spark_round(avg("abs_imbalance_ratio"), 4).alias("avg_abs_imbalance_ratio")
    ).orderBy("pickup_hour")

    # --- ΑΠΟΘΗΚΕΥΣΗ ΣΤΟ HDFS ---
    metrics = {
        "top_17_positive_net_pickups": [row.asDict() for row in top_positive_net.collect()],
        "top_17_negative_net_pickups": [row.asDict() for row in top_negative_net.collect()],
        "top_17_abs_imbalance_ratio": [row.asDict() for row in top_abs_imbalance.collect()],
        "hourly_summary": [row.asDict() for row in hourly_summary.collect()],
        "execution_time_seconds": round(time.time() - start_time, 2)
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q6_df_metrics"
    
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    print(f"\nΤο Q6 ολοκληρώθηκε σε {metrics['execution_time_seconds']} δευτερόλεπτα!")
    print("=== METRICS JSON ===")
    print(json_string)

    spark.stop()

if __name__ == "__main__":
    main()
