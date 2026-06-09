import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, count, avg, round as spark_round

def main():
    parser = argparse.ArgumentParser(description="Q4: Payment Behavior (DataFrame API)")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q4_DF_2121247").getOrCreate()
    start_time = time.time()

    trips_df = spark.read.parquet(args.input_parquet)

    # 1. Βασικό Φιλτράρισμα
    valid_trips = trips_df.filter(
        (col("pickup_day").isin(12, 13, 14)) &
        (col("pickup_hour").isin(7, 8, 9, 10)) &
        (col("duration_minutes") > 0) &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0) &
        (col("payment_type").isin(1, 2))
    )

    # Προσθήκη στήλης tip_rate (ΜΟΝΟ για κάρτα, για τα μετρητά βάζουμε null)
    processed_df = valid_trips.withColumn(
        "current_tip_rate", 
        when(col("payment_type") == 1, col("tip_amount") / col("fare_amount")).otherwise(None)
    )

    # 2. Υπολογισμός Συγκριτικού Πίνακα
    card_vs_cash_df = processed_df.groupBy("pickup_hour").agg(
        sum(when(col("payment_type") == 1, 1).otherwise(0)).alias("card_trips"),
        sum(when(col("payment_type") == 2, 1).otherwise(0)).alias("cash_trips"),
        spark_round(sum(when(col("payment_type") == 1, 1.0).otherwise(0.0)) / count("*"), 4).alias("card_share"),
        spark_round(avg(when(col("payment_type") == 1, col("total_amount"))), 2).alias("avg_total_card"),
        spark_round(avg(when(col("payment_type") == 2, col("total_amount"))), 2).alias("avg_total_cash"),
        spark_round(avg(when(col("payment_type") == 1, col("fare_amount"))), 2).alias("avg_fare_card"),
        spark_round(avg(when(col("payment_type") == 2, col("fare_amount"))), 2).alias("avg_fare_cash"),
        spark_round(avg(col("current_tip_rate")), 4).alias("avg_tip_rate_card")
    ).orderBy("pickup_hour")

    # Εμφάνιση Αποτελεσμάτων στο τερματικό
    print("\n" + "="*50)
    print("ΣΥΓΚΡΙΤΙΚΟΣ ΠΙΝΑΚΑΣ CARD VS CASH (DATAFRAME API)")
    print("="*50)
    card_vs_cash_df.show()

    execution_time = time.time() - start_time
    print(f"\n=== ΧΡΟΝΟΣ ΕΚΤΕΛΕΣΗΣ DATAFRAME API: {round(execution_time, 2)} δευτερόλεπτα ===")

    spark.stop()

if __name__ == "__main__":
    main()
