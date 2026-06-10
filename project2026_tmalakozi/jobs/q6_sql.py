import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Q6: Imbalance with Spark SQL")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q6_SQL_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    trips_df.createOrReplaceTempView("trips")

    zones_df = spark.read.option("header", "true").csv(args.input_csv)
    zones_df.createOrReplaceTempView("zones")

    query = """
    WITH valid_trips AS (
        SELECT * FROM trips
        WHERE pickup_day IN (12, 13, 14)
          AND pickup_hour IN (7, 8, 9, 10)
          AND duration_minutes > 0
          AND total_amount > 0
    ),
    pickups AS (
        SELECT pickup_hour, pu_location_id AS location_id, COUNT(*) AS pickups
        FROM valid_trips
        GROUP BY pickup_hour, pu_location_id
    ),
    dropoffs AS (
        SELECT pickup_hour, do_location_id AS location_id, COUNT(*) AS dropoffs
        FROM valid_trips
        GROUP BY pickup_hour, do_location_id
    ),
    outer_join AS (
        SELECT 
            COALESCE(p.pickup_hour, d.pickup_hour) AS pickup_hour,
            COALESCE(p.location_id, d.location_id) AS location_id,
            COALESCE(p.pickups, 0) AS pickups,
            COALESCE(d.dropoffs, 0) AS dropoffs
        FROM pickups p
        FULL OUTER JOIN dropoffs d 
          ON p.pickup_hour = d.pickup_hour AND p.location_id = d.location_id
    ),
    metrics AS (
        SELECT 
            o.*,
            (pickups + dropoffs) AS activity,
            (pickups - dropoffs) AS net_pickups,
            CASE WHEN (pickups + dropoffs) > 0 THEN (pickups - dropoffs) / (pickups + dropoffs) ELSE 0.0 END AS imbalance_ratio
        FROM outer_join o
    ),
    enriched AS (
        SELECT 
            m.*,
            ABS(m.imbalance_ratio) AS abs_imbalance_ratio,
            z.Borough,
            z.Zone,
            z.service_zone
        FROM metrics m
        LEFT JOIN zones z ON m.location_id = z.LocationID
    )
    SELECT * FROM enriched
    WHERE activity >= 30
    ORDER BY net_pickups DESC
    LIMIT 17
    """

    results_df = spark.sql(query)
    
    print("\n" + "="*50)
    print("TOP 17 POSITIVE NET PICKUPS (SPARK SQL)")
    print("="*50)
    results_df.show()

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ SPARK SQL: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
