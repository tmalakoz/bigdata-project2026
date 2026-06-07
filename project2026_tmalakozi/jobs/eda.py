import argparse
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, round as spark_round, desc, floor

def main():
    parser = argparse.ArgumentParser(description="Exploratory Data Analysis (EDA)")
    parser.add_argument("--input-base", type=str, required=True, help="HDFS base path for Parquet files")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("Project2026_EDA") \
        .getOrCreate()

    print("Φόρτωση δεδομένων Parquet...")
    df_2015 = spark.read.parquet(f"{args.input_base}/yellow_tripdata_2015")
    df_2024 = spark.read.parquet(f"{args.input_base}/yellow_tripdata_2024")
    df_lookup = spark.read.parquet(f"{args.input_base}/taxi_zone_lookup")

    eda_results = {}

    # --- 1. Πίνακας Nulls (Ποσοστό null ανά στήλη) ---
    print("Υπολογισμός Nulls...")
    def get_null_percentages(df):
        total_rows = df.count()
        if total_rows == 0: return {}
        null_exprs = [spark_round((count(when(col(c).isNull(), c)) / total_rows) * 100, 4).alias(c) for c in df.columns]
        nulls_row = df.select(*null_exprs).first().asDict()
        # Ταξινόμηση φθίνουσα
        return dict(sorted(nulls_row.items(), key=lambda item: item[1], reverse=True))

    eda_results["null_percentages_2015"] = get_null_percentages(df_2015)
    eda_results["null_percentages_2024"] = get_null_percentages(df_2024)

    # --- 2. Κατανομή Ωρών (2015 & 2024) ---
    print("Υπολογισμός κατανομής ωρών...")
    hours_2015 = df_2015.groupBy("pickup_hour").count().orderBy("pickup_hour").collect()
    hours_2024 = df_2024.groupBy("pickup_hour").count().orderBy("pickup_hour").collect()
    eda_results["hourly_distribution_2015"] = {row['pickup_hour']: row['count'] for row in hours_2015}
    eda_results["hourly_distribution_2024"] = {row['pickup_hour']: row['count'] for row in hours_2024}

    # --- 3. Κατανομή Ημερών (μόνο 2024) ---
    print("Υπολογισμός κατανομής ημερών 2024...")
    days_2024 = df_2024.groupBy("pickup_day").count().orderBy("pickup_day").collect()
    eda_results["daily_distribution_2024"] = {row['pickup_day']: row['count'] for row in days_2024}

    # --- 4. Joinability Check & Top 10 Zones (2024) ---
    print("Έλεγχος Joinability και Top 10 Zones...")
    total_2024 = df_2024.count()
    joined_df = df_2024.join(df_lookup, df_2024.pu_location_id == df_lookup.LocationID, "left")
    
    unmatched_count = joined_df.filter(col("LocationID").isNull()).count()
    joinability_pct = ((total_2024 - unmatched_count) / total_2024) * 100
    eda_results["joinability_2024"] = round(joinability_pct, 4)

    top_10_zones = joined_df.groupBy("Zone").count().orderBy(desc("count")).limit(10).collect()
    eda_results["top_10_pickup_zones_2024"] = {row['Zone']: row['count'] for row in top_10_zones}

    # --- 5. Κατανομές για Ιστογράμματα (2024) ---
    # Ομαδοποιούμε ανά ακέραιο (floor) για να είναι εύκολη η σχεδίαση του ιστογράμματος τοπικά
    print("Εξαγωγή δεδομένων ιστογραμμάτων...")
    dist_hist = df_2024.withColumn("dist_bucket", floor(col("trip_distance"))) \
        .groupBy("dist_bucket").count().orderBy("dist_bucket").limit(100).collect()
    eda_results["trip_distance_histogram_2024"] = {row['dist_bucket']: row['count'] for row in dist_hist if row['dist_bucket'] is not None}

    amt_hist = df_2024.withColumn("amt_bucket", floor(col("total_amount"))) \
        .groupBy("amt_bucket").count().orderBy("amt_bucket").limit(200).collect()
    eda_results["total_amount_histogram_2024"] = {row['amt_bucket']: row['count'] for row in amt_hist if row['amt_bucket'] is not None}

    # --- 6. Δείγμα Ανωμαλιών (2024) ---
    print("Εύρεση ανωμαλιών...")
    eda_results["anomalies_2024"] = {
        "largest_distance": [row.asDict() for row in df_2024.select("vendor_id", "pickup_ts", "trip_distance", "total_amount").orderBy(desc("trip_distance")).limit(5).collect()],
        "largest_duration_minutes": [row.asDict() for row in df_2024.select("vendor_id", "pickup_ts", "duration_minutes", "trip_distance").orderBy(desc("duration_minutes")).limit(5).collect()],
        "negative_fare_amount": [row.asDict() for row in df_2024.select("vendor_id", "pickup_ts", "fare_amount", "total_amount").filter(col("fare_amount") < 0).limit(5).collect()],
        "zero_duration": [row.asDict() for row in df_2024.select("vendor_id", "pickup_ts", "duration_minutes", "total_amount").filter(col("duration_minutes") == 0).limit(5).collect()]
    }

    # Μετατροπή των datetimes σε strings για να γίνει σωστά serialize σε JSON
    for key in eda_results["anomalies_2024"]:
        for item in eda_results["anomalies_2024"][key]:
            if "pickup_ts" in item and item["pickup_ts"]:
                item["pickup_ts"] = str(item["pickup_ts"])

    # Αποθήκευση αποτελεσμάτων
    os.makedirs("results/metrics", exist_ok=True)
    with open("results/metrics/eda_metrics.json", "w", encoding="utf-8") as f:
        json.dump(eda_results, f, indent=4, ensure_ascii=False)

    print("Το EDA ολοκληρώθηκε! Τα metrics αποθηκεύτηκαν στο results/metrics/eda_metrics.json")
    spark.stop()

if __name__ == "__main__":
    main()
