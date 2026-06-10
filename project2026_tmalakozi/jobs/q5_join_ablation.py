import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    parser = argparse.ArgumentParser(description="Q5: Join Strategy Ablation Study")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    args = parser.parse_args()

    # Δημιουργία Session
    spark = SparkSession.builder.appName("Project2026_Q5_JoinAblation_2121247").getOrCreate()

    # 🚨 ΤΟ ΜΥΣΤΙΚΟ: Απενεργοποιούμε το αυτόματο Broadcast θέτοντας το όριο στο -1
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    filtered_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("total_amount") > 0)
    )

    # Εκτέλεση Join (λόγω της ρύθμισης, θα γίνει SortMergeJoin αντί για BroadcastHashJoin)
    joined_df = filtered_trips.join(zones_df, filtered_trips.pu_location_id == zones_df.LocationID, "inner")

    # Action για να εκτελεστεί το πλάνο
    joined_df.limit(5).collect()

    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (ΧΩΡΙΣ BROADCAST)")
    print("="*50)
    joined_df.explain("formatted")

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ ΜΕ SORT MERGE JOIN: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
