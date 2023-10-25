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

# Define the schema for the JSON structure
schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("dateModified", TimestampType(), True),
    StructField("state", StringType(), True),
    StructField("version", StringType(), True),
    StructField("selfUri", StringType(), True)
])

# COMMAND ----------

@dlt.table(name="bronze_routing_skills", comment="Raw data from conversation job")
def bronze_routing_skills():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(source_bucket + "/ROUTING_SKILLS/*/*/*/")
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
        )
        .dropDuplicates(["id"])
    )
