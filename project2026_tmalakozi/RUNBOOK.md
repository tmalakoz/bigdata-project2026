# Οδηγός Αναπαραγωγής Αποτελεσμάτων (Runbook) — Big Data Project 2026

Ο παρών οδηγός περιέχει αναλυτικά τις εντολές για την αναπαραγωγή των αποτελεσμάτων στα ερωτήματα Q1 έως Q6. Η εκτέλεση βασίζεται σε Spark σε περιβάλλον Kubernetes/HDFS.

Πριν ξεκινήσετε, ορίστε τη μεταβλητή χρήστη:
export VDCLOUD_USER="tmalakoz"

---

## Q1: Φόρτωση & Προετοιμασία Δεδομένων (CSV to Parquet)
hdfs dfs -mkdir -p /user/$VDCLOUD_USER/project2026/data/parquet/

spark-submit jobs/q1_data_ingestion.py \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv \
  --output-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024

---

## Q2: Ανάλυση με Χρήση UDF
spark-submit jobs/q2_udf_analysis.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/results

---

## Q3: Μελέτη Αφαίρεσης (Native Spark APIs)
spark-submit jobs/q3_ablation.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/results

---

## Q4: Σύγκριση Χρόνου Parquet vs CSV
spark-submit jobs/q4_time_comparison.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv

---

## Q5: Αεροδρόμια και Ροές (DataFrame & SQL)
# DataFrame API
spark-submit jobs/q5_df.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/results

# Spark SQL
spark-submit jobs/q5_sql.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv

# Πείραμα Join (Χωρίς Broadcast)
spark-submit jobs/q5_join_ablation.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv

---

## Q6: Ανισορροπία Ζωνών & Πείραμα Κλιμάκωσης

# Ανισορροπία DataFrame API
spark-submit jobs/q6_df.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv \
  --output-base hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/results

# Ανισορροπία SQL API
spark-submit jobs/q6_sql.py \
  --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024 \
  --input-csv hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv

# Πείραμα Κλιμάκωσης (Παράδειγμα για Διάταξη C - 8 Executors)
spark-submit --name "Experiment_C_Cold" --conf spark.executor.instances=8 --conf spark.executor.cores=2 --conf spark.executor.memory=2g jobs/q6_scaling.py --input-parquet hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$VDCLOUD_USER/project2026/data/parquet/yellow_tripdata_2024

---
## Συλλογή Αποτελεσμάτων
Τα τελικά μετρικά εξάγονται τοπικά μέσω HDFS, π.χ.:
hdfs dfs -cat /user/$VDCLOUD_USER/project2026/results/metrics/q6_df_metrics/part-* > results/metrics/q6_df_metrics.json
