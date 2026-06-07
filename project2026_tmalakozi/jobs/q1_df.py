import argparse
import time
import json
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum, avg, countDistinct, when, dayofweek, round as spark_round
)

def main():
    parser = argparse.ArgumentParser(description="Q1: Temporal Demand Profile (DataFrame API)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Project2026_Q1_DF_2121247") \
        .getOrCreate()

    start_time = time.time()

    # 1. Φόρτωση δεδομένων Parquet
    df = spark.read.parquet(args.input_parquet)

    # 2. ΕΞΑΤΟΜΙΚΕΥΣΗ (Φίλτρα για ΑΜ: 2121247)
    # Ημέρες: 12, 13, 14 | Ώρες: 7, 8, 9, 10
    personal_df = df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("total_amount") > 0)
    )

    # 3. Δημιουργία στηλών weekday, is_weekend και time_band
    personal_df = personal_df.withColumn("weekday", dayofweek(col("pickup_date"))) \
        .withColumn("is_weekend", when(col("weekday").isin(1, 7), True).otherwise(False)) \
        .withColumn("time_band", 
            when((col("pickup_hour") >= 0) & (col("pickup_hour") < 6), "Night")
            .when((col("pickup_hour") >= 6) & (col("pickup_hour") < 12), "Morning")
            .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 17), "Afternoon")
            .when((col("pickup_hour") >= 17) & (col("pickup_hour") < 22), "Evening")
            .otherwise("Late")
        )

    # Υπολογισμός συνολικού αριθμού trips στο προσωπικό παράθυρο
    total_personal_trips = personal_df.count()

    # 4. Ομαδοποίηση ανά (pickup_date, pickup_hour)
    grouped_df = personal_df.groupBy("pickup_date", "pickup_hour", "time_band").agg(
        count("*").alias("trips"),
        countDistinct("pu_location_id").alias("unique_pickup_zones"),
        spark_round(avg("passenger_count"), 2).alias("avg_passenger_count"),
        spark_round(avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        spark_round(sum("total_amount"), 2).alias("total_revenue")
    )

    grouped_df = grouped_df.withColumn(
        "trip_share_in_personal_window", 
        spark_round((col("trips") / total_personal_trips) * 100, 2)
    )

    # 5. Ταξινόμηση και Top-K (K = 17 για το ΑΜ σου)
    top_17_df = grouped_df.orderBy(
        col("trips").desc(),
        col("total_revenue").desc(),
        col("pickup_date").asc(),
        col("pickup_hour").asc()
    ).limit(17)

    # 6. Δεύτερος μικρός πίνακας: Σύνοψη ανά time_band
    time_band_summary = personal_df.groupBy("time_band").agg(
        count("*").alias("total_trips"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount")
    ).orderBy(col("total_trips").desc())

    # --- ΑΠΟΘΗΚΕΥΣΗ & ΕΞΑΓΩΓΗ ---
    top_17_results = [row.asDict() for row in top_17_df.collect()]
    time_band_results = [row.asDict() for row in time_band_summary.collect()]
    
    for row in top_17_results:
        row["pickup_date"] = str(row["pickup_date"])

    execution_time = time.time() - start_time

    # Αποθήκευση αποτελεσμάτων (Metrics Dictionary)
    metrics = {
        "execution_time_seconds": round(execution_time, 2),
        "total_personal_trips": total_personal_trips,
        "top_17_demand_hours": top_17_results,
        "time_band_summary": time_band_results
    }

    # 1. Μετατρέπουμε τα metrics σε JSON string
    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    
    # 2. Το σώζουμε στο HDFS ως αρχείο κειμένου
    metrics_hdfs_path = f"{args.output_base}/metrics/q1_df_metrics"
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    # 3. Γράψιμο και των Parquet αποτελεσμάτων
    output_path = f"{args.output_base}/q1_df_top17"
    top_17_df.write.mode("overwrite").parquet(output_path)

    print(f"Το Q1 ολοκληρώθηκε! Τα metrics σώθηκαν στο HDFS: {metrics_hdfs_path}")
    
    print("=== METRICS JSON ===")
    print(json_string)
    
    spark.stop()

if __name__ == "__main__":
    main()
