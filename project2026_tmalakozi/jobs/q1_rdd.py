import argparse
import time
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession

def parse_row(line):
    # Παράδειγμα 2024: VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge,Airport_fee
    parts = line.split(',')
    if len(parts) < 17: return None
    try:
        # pickup_ts: parts[1] (2024-09-01T00:05:51.000)
        ts_str = parts[1].replace('T', ' ')
        dt = datetime.strptime(ts_str[:19], '%Y-%m-%d %H:%M:%S')
        
        # Φιλτράρισμα εξατομίκευσης (ΑΜ: 2121247)
        # Ώρες: 7, 8, 9, 10 | Ημέρες: 12, 13, 14 | Έτος: 2024
        if dt.year != 2024 or dt.day not in [12, 13, 14] or dt.hour not in [7, 8, 9, 10]:
            return None
            
        # Φιλτράρισμα εγκυρότητας (duration > 0, dist > 0, total > 0)
        dist = float(parts[4])
        total = float(parts[16])
        if dist <= 0 or total <= 0:
            return None
            
        return ((dt.date(), dt.hour), (1, total))
    except:
        return None

def main():
    parser = argparse.ArgumentParser(description="Q1: Temporal Demand Profile (RDD API)")
    parser.add_argument("--input-csv", type=str, required=True, help="Path to 2024 CSV")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Project2026_Q1_RDD_2121247").getOrCreate()
    sc = spark.sparkContext
    start_time = time.time()

    # 1. Φόρτωση CSV
    raw_rdd = sc.textFile(args.input_csv)
    
    # Αφαίρεση header
    header = raw_rdd.first()
    rdd = raw_rdd.filter(lambda line: line != header)

    # 2. Map, Filter, Reduce (Manual logic)
    # Αποτέλεσμα: ((date, hour), (trips, revenue))
    processed_rdd = rdd.map(parse_row).filter(lambda x: x is not None)
    
    reduced_rdd = processed_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # 3. Ταξινόμηση (Top 17)
    # (key, (trips, revenue)) -> key: (date, hour)
    # sort by: trips DESC, revenue DESC, date ASC, hour ASC
    top_17 = reduced_rdd.map(lambda x: (x[0][0], x[0][1], x[1][0], x[1][1])) \
                        .takeOrdered(17, key=lambda x: (-x[2], -x[3], x[0], x[1]))

    # 4. Μορφοποίηση για JSON
    results = []
    for row in top_17:
        results.append({
            "pickup_date": str(row[0]),
            "pickup_hour": row[1],
            "trips": row[2],
            "total_revenue": round(float(row[3]), 2)
        })

    execution_time = time.time() - start_time
    
    metrics = {
        "execution_time_seconds": round(execution_time, 2),
        "top_17": results
    }

    print("=== METRICS RDD ===")
    print(json.dumps(metrics, indent=4))

    spark.stop()

if __name__ == "__main__":
    main()
