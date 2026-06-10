import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Q5: Borough Flows (Spark SQL)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q5_SQL_2121247").getOrCreate()
    start_time = time.time()

    # Φόρτωση και Temp Views
    trips_df = spark.read.parquet(args.input_parquet)
    trips_df.createOrReplaceTempView("trips")

    zones_df = spark.read.option("header", "true").csv(args.input_csv)
    zones_df.createOrReplaceTempView("zones")

    # Το τεράστιο Query που κάνει τα πάντα: Διπλό Join, Φίλτρα, και Υπολογισμό Ποσοστών
    query = """
    WITH valid_trips AS (
        SELECT * FROM trips
        WHERE pickup_day IN (12, 13, 14)
          AND pickup_hour IN (7, 8, 9, 10)
          AND duration_minutes > 0
          AND trip_distance > 0
          AND total_amount > 0
    ),
    joined_trips AS (
        SELECT 
            t.*,
            COALESCE(t.Airport_fee, 0.0) as clean_airport_fee,
            p.Borough AS pu_borough,
            p.Zone AS pu_zone,
            d.Borough AS do_borough,
            d.Zone AS do_zone,
            CASE WHEN LOWER(p.Zone) RLIKE 'jfk|laguardia|newark|airport' OR p.Borough = 'EWR' THEN 1 ELSE 0 END AS pu_is_airport,
            CASE WHEN LOWER(d.Zone) RLIKE 'jfk|laguardia|newark|airport' OR d.Borough = 'EWR' THEN 1 ELSE 0 END AS do_is_airport
        FROM valid_trips t
        JOIN zones p ON t.pu_location_id = p.LocationID
        JOIN zones d ON t.do_location_id = d.LocationID
    ),
    airport_flagged AS (
        SELECT 
            *,
            CASE WHEN pu_is_airport = 1 OR do_is_airport = 1 OR clean_airport_fee > 0 THEN 1 ELSE 0 END AS is_airport_trip
        FROM joined_trips
    ),
    totals AS (
        SELECT COUNT(*) as total_trips FROM airport_flagged
    )
    SELECT 
        pu_borough, 
        do_borough,
        COUNT(*) AS trips,
        ROUND((COUNT(*) / (SELECT total_trips FROM totals)) * 100, 2) AS trip_share_percent,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_total_amount,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(duration_minutes), 2) AS avg_duration_minutes,
        ROUND((SUM(is_airport_trip) / COUNT(*)) * 100, 2) AS airport_trip_share_percent
    FROM airport_flagged
    GROUP BY pu_borough, do_borough
    ORDER BY trips DESC
    LIMIT 17
    """

    flows_df = spark.sql(query)
    
    print("\n" + "="*50)
    print("TOP 17 ΡΟΕΣ (SPARK SQL)")
    print("="*50)
    flows_df.show()

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ SPARK SQL: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
