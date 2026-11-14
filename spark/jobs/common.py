"""Shared Spark helpers for the agri-dwh batch jobs (Python 3.7 compatible)."""

import os

from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:9000"
WAREHOUSE = HDFS + "/user/hive/warehouse"
RAW = HDFS + "/data/agri/raw"
PROCESSED = HDFS + "/data/agri/processed"
METASTORE = "thrift://hive-metastore:9083"


def spark_session(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", METASTORE)
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.hadoop.hive.metastore.warehouse.dir", WAREHOUSE)
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE DATABASE IF NOT EXISTS agri_dwh")
    return spark


def hdfs_path(*parts):
    return "/".join([PROCESSED.rstrip("/")] + [str(p).strip("/") for p in parts])


def write_table(spark, df, name, path, partition_by=None):
    """Overwrite a Parquet dataset and re-register it as a Hive external table."""
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)

    spark.sql("DROP TABLE IF EXISTS agri_dwh." + name)
    columns = ", ".join(
        "%s %s" % (field.name, _hive_type(field.dataType))
        for field in df.schema.fields
        if not partition_by or field.name not in partition_by
    )
    partition_clause = ""
    if partition_by:
        partition_clause = " PARTITIONED BY (%s)" % ", ".join(
            "%s INT" % column for column in partition_by
        )
    spark.sql(
        "CREATE EXTERNAL TABLE agri_dwh.%s (%s)%s "
        "STORED AS PARQUET LOCATION '%s'" % (name, columns, partition_clause, path)
    )
    if partition_by:
        spark.sql("MSCK REPAIR TABLE agri_dwh." + name)


def _hive_type(data_type):
    name = data_type.simpleString()
    return {
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "string": "STRING",
        "double": "DOUBLE",
        "float": "FLOAT",
        "int": "INT",
        "bigint": "BIGINT",
        "boolean": "BOOLEAN",
    }.get(name, name.upper())


def table_count(spark, table):
    return spark.sql("SELECT COUNT(*) AS c FROM " + table).collect()[0]["c"]


def write_metrics(path, payload):
    """Write a small JSON file for notebooks and the Prometheus exporter."""
    import json

    directory = os.path.dirname(path)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
