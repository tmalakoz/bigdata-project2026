import argparse
import time
import json
from decimal import Decimal
from pyspark.sql import SparkSession

# Μετατροπέας για τα Spark Decimals ώστε να γίνονται JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def main():
    parser = argparse.ArgumentParser(description="Q4: Payment & Tip Behavior (Spark SQL)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--output-base", type=str, required=True, help="Base path for results")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q4_SQL_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    trips_df.createOrReplaceTempView("trips")

    base_query = """
    CREATE OR REPLACE TEMP VIEW valid_trips AS
    SELECT *,
           (tip_amount / fare_amount) AS current_tip_rate,
           CASE WHEN tip_amount = 0 THEN 1.0 ELSE 0.0 END AS is_zero_tip
    FROM trips
    WHERE pickup_day IN (12, 13, 14)
      AND pickup_hour IN (7, 8, 9, 10)
      AND duration_minutes > 0
      AND trip_distance > 0
      AND fare_amount > 0
      AND total_amount > 0
      AND payment_type IN (1, 2)
    """
    spark.sql(base_query)

    hourly_payment_query = """
    WITH hourly_stats AS (
        SELECT pickup_hour,
               payment_type,
               COUNT(*) AS trips,
               AVG(fare_amount) AS avg_fare_amount,
               AVG(total_amount) AS avg_total_amount,
               AVG(tip_amount) AS avg_tip_amount,
               AVG(current_tip_rate) AS tip_rate,
               AVG(is_zero_tip) AS zero_tip_share,
               AVG(trip_distance) AS avg_trip_distance,
               AVG(duration_minutes) AS avg_duration_minutes
        FROM valid_trips
        GROUP BY pickup_hour, payment_type
    )
    SELECT pickup_hour,
           payment_type,
           trips,
           ROUND(trips / SUM(trips) OVER (PARTITION BY pickup_hour), 4) AS payment_share_in_hour,
           ROUND(avg_fare_amount, 2) AS avg_fare_amount,
           ROUND(avg_total_amount, 2) AS avg_total_amount,
           ROUND(avg_tip_amount, 2) AS avg_tip_amount,
           ROUND(tip_rate, 4) AS tip_rate,
           ROUND(zero_tip_share, 4) AS zero_tip_share,
           ROUND(avg_trip_distance, 2) AS avg_trip_distance,
           ROUND(avg_duration_minutes, 2) AS avg_duration_minutes
    FROM hourly_stats
    ORDER BY pickup_hour, payment_type
    """
    hourly_payment_df = spark.sql(hourly_payment_query)

    vendor_query = """
    WITH v_stats AS (
        SELECT vendor_id,
               payment_type,
               COUNT(*) AS trips
        FROM valid_trips
        GROUP BY vendor_id, payment_type
    )
    SELECT vendor_id,
           payment_type,
           trips,
           ROUND(trips / SUM(trips) OVER (PARTITION BY vendor_id), 4) AS payment_share
    FROM v_stats
    ORDER BY vendor_id, payment_type
    """
    vendor_df = spark.sql(vendor_query)

    card_vs_cash_query = """
    SELECT pickup_hour,
           SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS card_trips,
           SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
           ROUND(SUM(CASE WHEN payment_type = 1 THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS card_share,
           ROUND(AVG(CASE WHEN payment_type = 1 THEN total_amount ELSE NULL END), 2) AS avg_total_card,
           ROUND(AVG(CASE WHEN payment_type = 2 THEN total_amount ELSE NULL END), 2) AS avg_total_cash,
           ROUND(AVG(CASE WHEN payment_type = 1 THEN fare_amount ELSE NULL END), 2) AS avg_fare_card,
           ROUND(AVG(CASE WHEN payment_type = 2 THEN fare_amount ELSE NULL END), 2) AS avg_fare_cash,
           ROUND(AVG(CASE WHEN payment_type = 1 THEN current_tip_rate ELSE NULL END), 4) AS avg_tip_rate_card
    FROM valid_trips
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """
    card_vs_cash_df = spark.sql(card_vs_cash_query)

    metrics = {
        "hourly_payment_stats": [row.asDict() for row in hourly_payment_df.collect()],
        "vendor_summary": [row.asDict() for row in vendor_df.collect()],
        "card_vs_cash_comparison": [row.asDict() for row in card_vs_cash_df.collect()]
    }

    execution_time = time.time() - start_time
    metrics["execution_time_seconds"] = round(execution_time, 2)

    # Προσθήκη του cls=DecimalEncoder
    json_string = json.dumps(metrics, indent=4, ensure_ascii=False, cls=DecimalEncoder)
    metrics_hdfs_path = f"{args.output_base}/metrics/q4_sql_metrics"
    
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