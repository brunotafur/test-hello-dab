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
    StructField("division", StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("selfUri", StringType(), True)
    ]), True),
    StructField("chat", StructType([
        StructField("jabberId", StringType(), True)
    ]), True),
    StructField("department", StringType(), True),
    StructField("email", StringType(), True),
    StructField("primaryContactInfo", ArrayType(StructType([
        StructField("address", StringType(), True),
        StructField("mediaType", StringType(), True),
        StructField("type", StringType(), True)
    ]), True), True),
    StructField("addresses", ArrayType(StringType(), True), True),
    StructField("state", StringType(), True),
    StructField("username", StringType(), True),
    StructField("version", StringType(), True),
    StructField("locations", ArrayType(StringType(), True), True),
    StructField("acdAutoAnswer", BooleanType(), True),
    StructField("selfUri", StringType(), True)
])

# COMMAND ----------

@dlt.table(name="bronze_user", comment="Raw data from conversation job")
def bronze_user():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine","true")
        .schema(schema)
        .load(source_bucket + "/USER/*/*/*/")
        .select(
            "*",
            "_metadata.*",
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
        )
    )
