import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Q1: Temporal Demand Profile (Spark SQL)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Project2026_Q1_SQL_2121247") \
        .getOrCreate()

    start_time = time.time()

    # 1. Φόρτωση δεδομένων και δημιουργία Temporary View για να γράψουμε SQL
    df = spark.read.parquet(args.input_parquet)
    df.createOrReplaceTempView("trips")

    # 2. Το τεράστιο SQL Query που κάνει ακριβώς ό,τι έκανε και το DataFrame
    query = """
    WITH filtered_trips AS (
        SELECT *,
               CASE
                   WHEN pickup_hour >= 0 AND pickup_hour < 6 THEN 'Night'
                   WHEN pickup_hour >= 6 AND pickup_hour < 12 THEN 'Morning'
                   WHEN pickup_hour >= 12 AND pickup_hour < 17 THEN 'Afternoon'
                   WHEN pickup_hour >= 17 AND pickup_hour < 22 THEN 'Evening'
                   ELSE 'Late'
               END AS time_band
        FROM trips
        WHERE pickup_day IN (12, 13, 14)
          AND pickup_hour IN (7, 8, 9, 10)
          AND duration_minutes > 0
          AND trip_distance > 0
          AND total_amount > 0
    ),
    total_personal AS (
        -- Υπολογίζουμε το συνολικό πλήθος για να βρούμε το ποσοστό (share)
        SELECT COUNT(*) AS total_trips FROM filtered_trips
    )
    SELECT
        f.pickup_date,
        f.pickup_hour,
        f.time_band,
        COUNT(*) AS trips,
        COUNT(DISTINCT f.pu_location_id) AS unique_pickup_zones,
        ROUND(AVG(f.passenger_count), 2) AS avg_passenger_count,
        ROUND(AVG(f.duration_minutes), 2) AS avg_duration_minutes,
        ROUND(AVG(f.trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(f.total_amount), 2) AS avg_total_amount,
        ROUND(SUM(f.total_amount), 2) AS total_revenue,
        ROUND((COUNT(*) / MAX(t.total_trips)) * 100, 2) AS trip_share_in_personal_window
    FROM filtered_trips f 
    CROSS JOIN total_personal t
    GROUP BY f.pickup_date, f.pickup_hour, f.time_band
    ORDER BY trips DESC, total_revenue DESC, f.pickup_date ASC, f.pickup_hour ASC
    LIMIT 17
    """

    # Εκτέλεση του Query
    result_df = spark.sql(query)

    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (CATALYST EXPLAIN FORMATTED)")
    print("="*50)
    # Εδώ τυπώνουμε το σχέδιο για να το κάνεις copy-paste!
    result_df.explain("formatted")

    print("\n" + "="*50)
    print("ΑΠΟΤΕΛΕΣΜΑΤΑ TOP-17")
    print("="*50)
    result_df.show(truncate=False)

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ SPARK SQL: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
