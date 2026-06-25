import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round as spark_round, when

def main():
    parser = argparse.ArgumentParser(description="Q6: OD-Halves Scaling Experiment")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to FULL 2024 Parquet data")
    args = parser.parse_args()

    # Το όνομα της εφαρμογής θα το περνάμε δυναμικά από την εντολή spark-submit
    spark = SparkSession.builder.getOrCreate()
    app_name = spark.conf.get("spark.app.name")
    print(f"\n{'='*50}\nΕΝΑΡΞΗ ΠΕΙΡΑΜΑΤΟΣ: {app_name}\n{'='*50}\n")
    
    start_time = time.time()

    # 1. Διάβασμα ΟΛΟΚΛΗΡΟΥ του 2024 χωρίς εξατομίκευση & Φιλτράρισμα
    trips_df = spark.read.parquet(args.input_parquet)
    valid_trips = trips_df.filter(
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0)
    )

    # 2. Διαχωρισμός σε Ημίχρονα (H1 και H2)
    h1_df = valid_trips.filter(col("pickup_day") <= 15)
    h2_df = valid_trips.filter(col("pickup_day") > 15)

    # 3. Ομαδοποίηση για το H1
    h1_agg = h1_df.groupBy("pu_location_id", "do_location_id", "pickup_hour").agg(
        count("*").alias("trips_h1"),
        avg("fare_amount").alias("avg_fare_h1"),
        avg("total_amount").alias("avg_total_h1")
    )

    # 4. Ομαδοποίηση για το H2
    h2_agg = h2_df.groupBy("pu_location_id", "do_location_id", "pickup_hour").agg(
        count("*").alias("trips_h2"),
        avg("fare_amount").alias("avg_fare_h2"),
        avg("total_amount").alias("avg_total_h2")
    )

    # 5. Inner Join των δύο ημιχρόνων
    joined_df = h1_agg.join(
        h2_agg, 
        ["pu_location_id", "do_location_id", "pickup_hour"], 
        "inner"
    )

    # 6. Υπολογισμός Ποσοστιαίων Αλλαγών
    calc_df = joined_df.withColumn(
        "trips_change_pct", spark_round((col("trips_h2") - col("trips_h1")) * 100 / col("trips_h1"), 2)
    ).withColumn(
        "fare_change_pct", spark_round((col("avg_fare_h2") - col("avg_fare_h1")) * 100 / col("avg_fare_h1"), 2)
    )

    # Φιλτράρισμα στατιστικά ασήμαντων ζευγών (π.χ. ζεύγη με λιγότερα από 50 ταξίδια και στα 2 ημίχρονα)
    calc_df = calc_df.filter((col("trips_h1") >= 50) & (col("trips_h2") >= 50))

    # 7. Εξαγωγή Top-K (Βάζουμε K=5 για να τρέξει πιο γρήγορα το collect)
    top_increase = calc_df.orderBy(col("trips_change_pct").desc()).limit(5)
    top_decrease = calc_df.orderBy(col("trips_change_pct").asc()).limit(5)

    # Εκτέλεση και εμφάνιση (για να δημιουργηθούν τα stages στο History Server)
    print("\n--- TOP 5 ΑΥΞΗΣΕΙΣ ---")
    top_increase.show()
    print("\n--- TOP 5 ΜΕΙΩΣΕΙΣ ---")
    top_decrease.show()

    # 8. Εκτύπωση Φυσικού Σχεδίου (για την απαίτηση δ)
    print("\n" + "="*50)
    print("ΦΥΣΙΚΟ ΣΧΕΔΙΟ ΕΚΤΕΛΕΣΗΣ (CATALYST EXPLAIN)")
    print("="*50)
    calc_df.explain("formatted")

    execution_time = time.time() - start_time
    print(f"\n=== ΣΥΝΟΛΙΚΟΣ ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ: {round(execution_time, 2)} δευτερόλεπτα ===")

    # Κοιμίζει το script για 2 λεπτά ώστε να προλάβεις να βγάλεις screenshots!
    print("\n[PAUSE] Το Spark UI (localhost:4040) θα μείνει ανοιχτό για 2 λεπτά. Βγάλε τα screenshots σου τώρα...")
    time.sleep(120)
    
    spark.stop()

if __name__ == "__main__":
    main()
