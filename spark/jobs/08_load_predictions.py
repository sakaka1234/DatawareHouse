"""08 - Load a predictions CSV produced by the ML toolbox into a Hive table.

Usage: spark-submit 08_load_predictions.py --csv /opt/agri/artifacts/ml/yield_predictions.csv --table ml_yield_predictions
"""

import argparse
import sys

sys.path.insert(0, "/opt/agri/spark/jobs")
from common import hdfs_path, spark_session, table_count, write_table  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    spark = spark_session("agri_08_load_" + args.table)
    csv_path = args.csv
    if "://" not in csv_path:
        csv_path = "file://" + csv_path
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )
    write_table(spark, df, args.table, hdfs_path(args.table))
    print("rows loaded:", table_count(spark, "agri_dwh." + args.table))


if __name__ == "__main__":
    main()
