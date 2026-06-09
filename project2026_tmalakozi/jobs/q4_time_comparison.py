import argparse
import time
from pyspark.sql import SparkSession

def main():
    parser = argparse.ArgumentParser(description="Q4: CSV vs Parquet Time Comparison")
    parser.add_argument("--input-parquet", type=str, required=True, help="Path to 2024 Parquet data")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to 2024 CSV data")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q4_Compare_2121247").getOrCreate()

    # Απλοποιημένο query: Μετράμε απλά τα ταξίδια ανά payment_type
    # Αυτό αρκεί για να αναγκάσει το CSV σε Full Scan και το Parquet σε Column Scan.
    query = """
    SELECT payment_type, COUNT(*) as trips
    FROM trips_data
    WHERE payment_type IN ('1', '2')
    GROUP BY payment_type
    """

    # --- 1. ΕΚΤΕΛΕΣΗ ΜΕ PARQUET ---
    print("\n" + "="*50)
    print("ΞΕΚΙΝΑΕΙ Η ΕΚΤΕΛΕΣΗ ΜΕ PARQUET")
    print("="*50)
    start_parquet = time.time()
    
    df_parquet = spark.read.parquet(args.input_parquet)
    df_parquet.createOrReplaceTempView("trips_data")
    spark.sql(query).show()
    
    time_parquet = time.time() - start_parquet
    print(f">>> ΧΡΟΝΟΣ PARQUET: {round(time_parquet, 2)} δευτερόλεπτα <<<\n")


    # --- 2. ΕΚΤΕΛΕΣΗ ΜΕ CSV ---
    print("\n" + "="*50)
    print("ΞΕΚΙΝΑΕΙ Η ΕΚΤΕΛΕΣΗ ΜΕ CSV (ΑΝΑΜΕΝΟΜΕΝΟΣ ΧΡΟΝΟΣ ~15-20 ΛΕΠΤΑ...)")
    print("="*50)
    start_csv = time.time()
    
    df_csv = spark.read.option("header", "true").csv(args.input_csv)
    df_csv.createOrReplaceTempView("trips_data")
    spark.sql(query).show()
    
    time_csv = time.time() - start_csv
    print(f">>> ΧΡΟΝΟΣ CSV: {round(time_csv, 2)} δευτερόλεπτα <<<\n")

    spark.stop()

if __name__ == "__main__":
    main()