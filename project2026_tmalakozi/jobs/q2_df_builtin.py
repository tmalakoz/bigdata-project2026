import argparse
import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sin, cos, atan2, sqrt, radians, pow as spark_pow, 
    when, avg, count, sum, round as spark_round, unix_timestamp
)

def main():
    parser = argparse.ArgumentParser(description="Q2: Haversine & Congestion (Built-in Functions)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2015 Parquet data")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    # Ορίζουμε το SparkSession
    spark = SparkSession.builder.appName("Project2026_Q2_Builtin_2121247").getOrCreate()
    start_time = time.time()

    # 1. Φόρτωση δεδομένων (2015)
    df = spark.read.parquet(args.input_parquet)

    # 2. Φιλτράρισμα βάσει ΑΜ (Ημέρες 12, 13, 14 | Ώρες 7, 8, 9, 10)
    # Αποκλείουμε και τα "χαλασμένα" GPS που δίνουν συντεταγμένες 0.0 (στον ωκεανό)
    filtered_df = df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("pickup_longitude") != 0) & 
        (col("pickup_latitude") != 0) &
        (col("dropoff_longitude") != 0) & 
        (col("dropoff_latitude") != 0)
    )

    # 3. Υπολογισμός Απόστασης Haversine (Με ενσωματωμένες συναρτήσεις Catalyst)
    R = 6371.0 # Ακτίνα της Γης σε χιλιόμετρα
    
    lat1 = radians(col("pickup_latitude"))
    lon1 = radians(col("pickup_longitude"))
    lat2 = radians(col("dropoff_latitude"))
    lon2 = radians(col("dropoff_longitude"))

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Ο Τύπος: a = sin²(dlat/2) + cos(lat1)*cos(lat2)*sin²(dlon/2)
    a = spark_pow(sin(dlat / 2), 2) + cos(lat1) * cos(lat2) * spark_pow(sin(dlon / 2), 2)
    # c = 2 * atan2(√a, √(1-a))
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance_km = R * c

    # 4. Υπολογισμός Διάρκειας και Ταχύτητας
    # Υπολογίζουμε τη διάρκεια σε ώρες για να βρούμε χιλιόμετρα ανά ώρα (km/h)
    duration_seconds = unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
    duration_hours = duration_seconds / 3600.0

    processed_df = filtered_df.withColumn("haversine_dist_km", distance_km) \
                              .withColumn("duration_hours", duration_hours)

    # Κρατάμε μόνο έγκυρες διαδρομές
    processed_df = processed_df.filter((col("duration_hours") > 0) & (col("haversine_dist_km") > 0))
    processed_df = processed_df.withColumn("speed_kmh", col("haversine_dist_km") / col("duration_hours"))

    # 5. Ενδείξεις Συμφόρησης (ορίζουμε αυθαίρετα ότι ταχύτητα < 15 km/h σημαίνει κίνηση)
    processed_df = processed_df.withColumn("is_congested", when(col("speed_kmh") < 15, 1).otherwise(0))

    # 6. Ομαδοποίηση ανά ώρα για να δούμε τα στατιστικά
    results_df = processed_df.groupBy("pickup_hour").agg(
        count("*").alias("total_trips"),
        spark_round(avg("haversine_dist_km"), 2).alias("avg_distance_km"),
        spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
        spark_round((sum("is_congested") / count("*")) * 100, 2).alias("congestion_percentage")
    ).orderBy("pickup_hour")

    # --- ΑΠΟΘΗΚΕΥΣΗ ΣΤΟ HDFS ΚΑΙ ΕΚΤΥΠΩΣΗ ---
    results_list = [row.asDict() for row in results_df.collect()]
    execution_time = time.time() - start_time

    metrics = {
        "implementation": "Built-in Catalyst Functions",
        "execution_time_seconds": round(execution_time, 2),
        "hourly_metrics": results_list
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q2_builtin_metrics"
    
    # Διαγραφή παλιού φακέλου αν υπάρχει
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    print(f"\nΤο Q2 (Built-in) ολοκληρώθηκε σε {round(execution_time, 2)} δευτερόλεπτα!")
    print("=== METRICS JSON ===")
    print(json_string)

    spark.stop()

if __name__ == "__main__":
    main()
