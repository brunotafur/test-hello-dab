# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    TimestampType,
    IntegerType,
)
import dlt

# COMMAND ----------

source_bucket = "s3://bronze-316962180131"
# Define the schema for the JSON data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("homeDivision", BooleanType(), True),
    StructField("selfUri", StringType(), True)
])

# COMMAND ----------

@dlt.table(name="bronze_divisions", comment="Raw data from conversation job")
def bronze_divisions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(source_bucket + "/DIVISION/*/*/*/*")
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
        )
    )
dlt.create_streaming_table("stg_silver_divisions")

dlt.apply_changes(
  target = "stg_silver_divisions",
  source = "bronze_divisions",
  keys = ["id"],
  sequence_by = F.col("file_modification_time"),
  stored_as_scd_type = "2"
)
