import argparse
import time
import json
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, count, sum, round as spark_round, unix_timestamp, udf
from pyspark.sql.types import FloatType

# 1. Ορισμός της Python συνάρτησης για τη Haversine
def calculate_haversine(lat1, lon1, lat2, lon2):
    R = 6371.0 # Ακτίνα Γης σε km
    
    # Μετατροπή σε ακτίνια
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return float(R * c)

def main():
    parser = argparse.ArgumentParser(description="Q2: Haversine & Congestion (Python UDF)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2015 Parquet data")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q2_UDF_2121247").getOrCreate()
    start_time = time.time()

    # Καταχώρηση της UDF στη Spark
    haversine_udf = udf(calculate_haversine, FloatType())

    df = spark.read.parquet(args.input_parquet)

    # Φιλτράρισμα βάσει ΑΜ και έγκυρων συντεταγμένων
    filtered_df = df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("pickup_longitude") != 0) & 
        (col("pickup_latitude") != 0) &
        (col("dropoff_longitude") != 0) & 
        (col("dropoff_latitude") != 0)
    )

    # 2. Εφαρμογή της UDF για τον υπολογισμό απόστασης
    processed_df = filtered_df.withColumn(
        "haversine_dist_km", 
        haversine_udf(col("pickup_latitude"), col("pickup_longitude"), col("dropoff_latitude"), col("dropoff_longitude"))
    )

    # Υπολογισμός Διάρκειας και Ταχύτητας
    duration_seconds = unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
    processed_df = processed_df.withColumn("duration_hours", duration_seconds / 3600.0)

    # Φίλτρο για αποφυγή διαίρεσης με το μηδέν
    processed_df = processed_df.filter((col("duration_hours") > 0) & (col("haversine_dist_km") > 0))
    processed_df = processed_df.withColumn("speed_kmh", col("haversine_dist_km") / col("duration_hours"))

    # Ενδείξεις Συμφόρησης (< 15 km/h)
    processed_df = processed_df.withColumn("is_congested", when(col("speed_kmh") < 15, 1).otherwise(0))

    # Ομαδοποίηση
    results_df = processed_df.groupBy("pickup_hour").agg(
        count("*").alias("total_trips"),
        spark_round(avg("haversine_dist_km"), 2).alias("avg_distance_km"),
        spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
        spark_round((sum("is_congested") / count("*")) * 100, 2).alias("congestion_percentage")
    ).orderBy("pickup_hour")

    # --- ΑΠΟΘΗΚΕΥΣΗ ---
    results_list = [row.asDict() for row in results_df.collect()]
    execution_time = time.time() - start_time

    metrics = {
        "implementation": "Python UDF",
        "execution_time_seconds": round(execution_time, 2),
        "hourly_metrics": results_list
    }

    json_string = json.dumps(metrics, indent=4, ensure_ascii=False)
    metrics_hdfs_path = f"{args.output_base}/metrics/q2_udf_metrics"
    
    try:
        spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
            .delete(spark._jvm.org.apache.hadoop.fs.Path(metrics_hdfs_path), True)
    except:
        pass
        
    spark.sparkContext.parallelize([json_string]).coalesce(1).saveAsTextFile(metrics_hdfs_path)

    print(f"\nΤο Q2 (UDF) ολοκληρώθηκε σε {round(execution_time, 2)} δευτερόλεπτα!")
    print("=== METRICS JSON ===")
    print(json_string)

    spark.stop()

if __name__ == "__main__":
    main()
