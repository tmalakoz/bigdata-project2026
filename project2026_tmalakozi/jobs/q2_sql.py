#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession

def get_personal_hours(student_id):
    A = int(student_id)
    h = A % 24
    L = 4
    hours = [(h + i) % 24 for i in range(L)]
    return hours

def main():
    parser = argparse.ArgumentParser(description="Q2 Spark SQL Implementation")
    parser.add_argument("--student-id", required=True, help="Student AM")
    parser.add_argument("--input-parquet", required=True, help="Path to 2015 parquet data")
    parser.add_argument("--output-base", required=False, help="Output directory")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q2_SQL").getOrCreate()

    # 1. Ανάγνωση δεδομένων και δημιουργία Temp View
    df = spark.read.parquet(args.input_parquet)
    df.createOrReplaceTempView("trips_2015")

    # 2. Υπολογισμός προσωπικού παραθύρου
    hours = get_personal_hours(args.student_id)
    hours_str = ", ".join(map(str, hours))

    # 3. Το βασικό SQL Ερώτημα
    sql_query = f"""
    WITH filtered AS (
        SELECT 
            pickup_hour,
            duration_minutes,
            trip_distance,
            (trip_distance * 1.60934) AS distance_km,
            (duration_minutes / 60.0) AS duration_hours,
            -- Haversine formula
            (6371 * 2 * ASIN(SQRT(
                POWER(SIN(RADIANS(dropoff_latitude - pickup_latitude) / 2), 2) +
                COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude)) *
                POWER(SIN(RADIANS(dropoff_longitude - pickup_longitude) / 2), 2)
            ))) AS haversine_km
        FROM trips_2015
        WHERE year(pickup_ts) = 2015
          AND pickup_hour IN ({hours_str})
          AND pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL
          AND duration_minutes > 0 AND trip_distance > 0
          -- Ρεαλιστικά όρια Νέας Υόρκης
          AND pickup_latitude BETWEEN 40.0 AND 41.5
          AND pickup_longitude BETWEEN -74.5 AND -73.0
          AND dropoff_latitude BETWEEN 40.0 AND 41.5
          AND dropoff_longitude BETWEEN -74.5 AND -73.0
    ),
    metrics AS (
        SELECT 
            pickup_hour,
            duration_minutes,
            distance_km,
            duration_hours,
            haversine_km,
            (distance_km / duration_hours) AS speed_kmh,
            (distance_km - haversine_km) AS distance_gap_km,
            CASE WHEN haversine_km > 0.2 THEN distance_km / haversine_km ELSE NULL END AS detour_ratio,
            CASE WHEN (distance_km / duration_hours) < 10 AND distance_km >= 1 THEN 1 ELSE 0 END AS is_congested
        FROM filtered
    )
    SELECT 
        pickup_hour,
        COUNT(*) AS trips,
        AVG(duration_minutes) AS avg_duration_minutes,
        PERCENTILE_APPROX(duration_minutes, 0.5) AS median_duration_minutes,
        PERCENTILE_APPROX(duration_minutes, 0.9) AS p90_duration_minutes,
        AVG(speed_kmh) AS avg_speed_kmh,
        SUM(distance_km) / SUM(duration_hours) AS agg_speed_kmh,
        AVG(haversine_km) AS avg_haversine_km,
        AVG(distance_gap_km) AS avg_distance_gap_km,
        AVG(detour_ratio) AS avg_detour_ratio,
        SUM(is_congested) / COUNT(*) AS congestion_candidate_share
    FROM metrics
    GROUP BY pickup_hour
    ORDER BY pickup_hour
    """

    print(f"Executing Q2 SQL for hours: {hours_str}")
    result_df = spark.sql(sql_query)
    
    # Αποθήκευση ή εμφάνιση
    if args.output_base:
        out_path = f"{args.output_base}/summary"
        result_df.write.mode("overwrite").parquet(out_path)
        print(f"Results saved to {out_path}")
        
        # Αποθήκευση φυσικού σχεδίου (Physical Plan) για την αναφορά
        plan_path = f"{args.output_base}/plan.txt"
        explain_str = result_df._jdf.queryExecution().simpleString()
        spark.sparkContext.parallelize([explain_str]).coalesce(1).saveAsTextFile(plan_path)
    else:
        result_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
