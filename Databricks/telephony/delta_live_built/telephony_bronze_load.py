# Databricks notebook source
import dlt
from pyspark.sql.types import *
import pyspark.sql.functions as F

# source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 1,Conversation Job - Bronze table load
@dlt.table(
    name = "bronze_conversation_job",
    comment = "Raw data from conversation job"
)
def ingest_conversion_jobs():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/CONVERSATION_JOB/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(
    name = "bronze_division",
    comment = "Raw data from division"
)
def bronze_division():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/DIVISION/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(
    name = "bronze_participant_attributes",
    comment = "Raw data from participant attributes"
)
def bronze_participant_attributes():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/PARTICIPANT_ATTRIBUTES/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(
    name = "bronze_routing_queues",
    comment = "Raw data from routing queues"
)
def bronze_routing_queues():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/ROUTING_QUEUES/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(
    name = "bronze_routing_skills",
    comment = "Raw data from routing skills"
)
def bronze_routing_queues():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/ROUTING_SKILLS/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table(
    name = "bronze_user",
    comment = "Raw data from user"
)
def bronze_routing_queues():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("s3://bronze-316962180131/USER/*/*/*/*")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )
