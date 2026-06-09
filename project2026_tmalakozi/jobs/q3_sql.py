import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Q3: Revenue vs Efficiency (Spark SQL)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q3_SQL_2121247").getOrCreate()
    start_time = time.time()

    # 1. Φόρτωση δεδομένων και δημιουργία Temp Views
    trips_df = spark.read.parquet(args.input_parquet)
    trips_df.createOrReplaceTempView("trips")

    zones_df = spark.read.option("header", "true").csv(args.input_csv)
    zones_df.createOrReplaceTempView("zones")

    # 2. Το συνολικό SQL Query
    query = """
    WITH filtered_trips AS (
        SELECT 
            t.*,
            CASE 
                WHEN trip_distance < 1 THEN 'very_short'
                WHEN trip_distance >= 1 AND trip_distance < 3 THEN 'short'
                WHEN trip_distance >= 3 AND trip_distance < 10 THEN 'medium'
                ELSE 'long'
            END AS distance_bucket,
            COALESCE(extra, 0.0) + COALESCE(mta_tax, 0.0) + COALESCE(tolls_amount, 0.0) + 
            COALESCE(improvement_surcharge, 0.0) + COALESCE(congestion_surcharge, 0.0) + COALESCE(Airport_fee, 0.0) AS total_surcharges
        FROM trips t
        WHERE pickup_day IN (12, 13, 14)
          AND pickup_hour IN (7, 8, 9, 10)
          AND duration_minutes > 0
          AND trip_distance > 0
          AND fare_amount > 0
          AND total_amount > 0
    ),
    joined_data AS (
        SELECT 
            z.Borough AS pickup_borough,
            z.Zone AS pickup_zone,
            f.distance_bucket,
            f.trip_distance,
            f.duration_minutes,
            f.fare_amount,
            f.tip_amount,
            f.total_amount,
            f.total_surcharges
        FROM filtered_trips f
        JOIN zones z ON f.pu_location_id = z.LocationID
    ),
    aggregated_data AS (
        SELECT 
            pickup_borough,
            pickup_zone,
            distance_bucket,
            COUNT(*) AS trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip,
            ROUND(SUM(total_amount) / SUM(trip_distance), 2) AS revenue_per_mile,
            ROUND(SUM(total_amount) / SUM(duration_minutes), 2) AS revenue_per_minute,
            ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
            ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
            ROUND(SUM(total_surcharges) / SUM(total_amount), 4) AS surcharge_share
        FROM joined_data
        GROUP BY pickup_borough, pickup_zone, distance_bucket
    )
    SELECT * FROM aggregated_data
    WHERE trips >= 50
    ORDER BY total_revenue DESC
    LIMIT 17
    """

    results_df = spark.sql(query)

    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (CATALYST EXPLAIN FORMATTED)")
    print("="*50)
    results_df.explain("formatted")

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ SPARK SQL: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
