import argparse
from pyspark.sql import SparkSession

def parse_common_args():
    parser = argparse.ArgumentParser(description="Big Data 2026 Project")
    parser.add_argument("--input-base", type=str, help="Base path for input data (HDFS)")
    parser.add_argument("--output-base", type=str, help="Base path for output results (HDFS)")
    parser.add_argument("--input-parquet", type=str, help="Direct path to Parquet input (used in Q2, Q3, Q5, Q6)")
    parser.add_argument("--input-csv", type=str, help="Direct path to CSV input (used in Q4, Q5, Q6)")
    parser.add_argument("--student-id", type=str, required=True, help="Student Registration Number (AM)")
    args, _ = parser.parse_known_args()
    return args

def get_personal_params(am_str):
    if len(am_str) != 7 or not am_str.isdigit():
        raise ValueError("Το ΑΜ πρέπει να είναι 7ψήφιος αριθμός.")
    
    digits = [int(d) for d in am_str]
    sum_first_5 = sum(digits[:5])
    h = sum_first_5 % 20
    last_digit = digits[-1]
    L = 3 + (last_digit % 4)
    second_to_last = digits[-2]
    d = 10 + (second_to_last % 10)
    sum_all = sum(digits)
    K = 3 + (sum_all % 4)
    
    return {"h": h, "L": L, "d": d, "K": K}

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

import time
class Timer:
    def __init__(self):
        self.start_time = None
    def start(self):
        self.start_time = time.time()
    def stop(self):
        if self.start_time is None:
            return 0
        end_time = time.time()
        elapsed = end_time - self.start_time
        self.start_time = None
        return round(elapsed, 2)
