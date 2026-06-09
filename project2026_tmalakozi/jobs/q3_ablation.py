import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, dayofmonth

def main():
    parser = argparse.ArgumentParser(description="Q3: Ablation Study (No Partition Pruning)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to taxi_zone_lookup.csv")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q3_Ablation_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)
    zones_df = spark.read.option("header", "true").csv(args.input_csv)

    # ΠΕΙΡΑΜΑ ABLATION: 
    # Αντί να χρησιμοποιήσουμε τη βελτιστοποιημένη στήλη 'pickup_day', 
    # χρησιμοποιούμε τη συνάρτηση dayofmonth πάνω στο timestamp. 
    # Αυτό εμποδίζει το Partition/Predicate Pruning και αναγκάζει Full Scan.
    bad_filter_df = trips_df.filter(
        (dayofmonth(col("tpep_pickup_datetime")).isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0)
    )

    # Κάνουμε ένα απλό Join και Aggregation (δεν χρειαζόμαστε όλη τη λογική, 
    # απλά θέλουμε να δούμε τον χρόνο και το Plan)
    joined_df = bad_filter_df.join(zones_df, bad_filter_df.pu_location_id == zones_df.LocationID, "inner")

    results_df = joined_df.groupBy("Zone").agg(
        count("*").alias("trips"),
        sum("total_amount").alias("revenue")
    )

    # Μαζεύουμε τα αποτελέσματα (action) για να εκτελεστεί το δέντρο
    results_df.limit(5).collect()

    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (ΧΩΡΙΣ PRUNING)")
    print("="*50)
    results_df.explain("formatted")

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ ABLATION STUDY: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
